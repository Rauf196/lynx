use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use lynx_protocol::{Message, Response, encode_frame, try_extract_response};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

/// convert String errors to anyhow
fn str_err<T>(r: Result<T, String>) -> Result<T> {
    r.map_err(|s| anyhow!(s))
}

#[derive(Parser, Debug, Clone)]
#[command(name = "lynx-load")]
#[command(about = "Load tester for Lynx chat server")]
struct Args {
    /// server address
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    /// server port
    #[arg(short, long, default_value_t = 6006)]
    port: u16,

    /// number of clients to spawn
    #[arg(short, long, default_value_t = 100)]
    clients: u32,

    /// number of rooms to distribute clients across
    #[arg(short, long, default_value_t = 10)]
    rooms: u32,

    /// messages per client
    #[arg(short, long, default_value_t = 10)]
    messages: u32,

    /// percentage of room messages (0-100)
    #[arg(long, default_value_t = 70)]
    room_msg_pct: u8,

    /// percentage of direct messages (0-100)
    #[arg(long, default_value_t = 15)]
    dm_pct: u8,

    /// percentage of list users requests (0-100)
    #[arg(long, default_value_t = 10)]
    list_pct: u8,

    /// percentage of room joins (0-100)
    #[arg(long, default_value_t = 5)]
    join_pct: u8,

    /// batch size for connection staggering
    #[arg(long, default_value_t = 50)]
    batch_size: u32,

    /// delay between batches in ms
    #[arg(long, default_value_t = 10)]
    batch_delay_ms: u64,

    /// login timeout in seconds
    #[arg(long, default_value_t = 60)]
    login_timeout_secs: u64,

    /// message send interval in ms
    #[arg(long, default_value_t = 100)]
    send_interval_ms: u64,

    /// duration to run in seconds (0 = until all messages sent)
    #[arg(short, long, default_value_t = 0)]
    duration_secs: u64,
}

struct Stats {
    connected: AtomicU64,
    connect_failed: AtomicU64,
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    errors: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            connected: AtomicU64::new(0),
            connect_failed: AtomicU64::new(0),
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }
}

#[derive(Clone, Copy)]
enum MessageType {
    RoomMessage,
    DirectMessage,
    ListUsers,
    JoinRoom,
}

struct Client {
    id: u32,
    username: String,
    room: String,
    args: Args,
    stats: Arc<Stats>,
    rng: SmallRng,
}

impl Client {
    fn new(id: u32, session_id: &str, args: Args, stats: Arc<Stats>) -> Self {
        let username = format!("user_{}_{}", session_id, id);
        let room_num = id % args.rooms;
        let room = format!("room_{}", room_num);
        let rng = SmallRng::seed_from_u64(id as u64);

        Self {
            id,
            username,
            room,
            args,
            stats,
            rng,
        }
    }

    fn pick_message_type(&mut self) -> MessageType {
        let roll: u8 = self.rng.gen_range(0..100);

        let room_end = self.args.room_msg_pct;
        let dm_end = room_end + self.args.dm_pct;
        let list_end = dm_end + self.args.list_pct;

        if roll < room_end {
            MessageType::RoomMessage
        } else if roll < dm_end {
            MessageType::DirectMessage
        } else if roll < list_end {
            MessageType::ListUsers
        } else {
            MessageType::JoinRoom
        }
    }

    fn generate_message(&mut self, all_users: &[String]) -> Message {
        match self.pick_message_type() {
            MessageType::RoomMessage => Message::SendRoomMessage {
                text: format!("msg_{}", self.rng.r#gen::<u32>()),
            },
            MessageType::DirectMessage => {
                // pick random user (could be self, that's fine for load testing)
                let to = if all_users.is_empty() {
                    self.username.clone()
                } else {
                    let idx = self.rng.gen_range(0..all_users.len());
                    all_users[idx].clone()
                };
                Message::SendPrivateMessage {
                    to,
                    text: format!("dm_{}", self.rng.r#gen::<u32>()),
                }
            }
            MessageType::ListUsers => Message::ListUsers,
            MessageType::JoinRoom => {
                let new_room = format!("room_{}", self.rng.gen_range(0..self.args.rooms));
                self.room = new_room.clone();
                Message::JoinRoom {
                    room_name: new_room,
                }
            }
        }
    }

    async fn run(mut self) -> Result<()> {
        let addr = format!("{}:{}", self.args.host, self.args.port);

        // connect with timeout
        let stream = match timeout(
            Duration::from_secs(self.args.login_timeout_secs),
            TcpStream::connect(&addr),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                self.stats.connect_failed.fetch_add(1, Ordering::Relaxed);
                return Err(e).context("connect failed");
            }
            Err(_) => {
                self.stats.connect_failed.fetch_add(1, Ordering::Relaxed);
                return Err(anyhow::anyhow!("connect timeout"));
            }
        };

        let (mut read_half, mut write_half) = stream.into_split();

        // send connect message
        let connect_msg = Message::Connect {
            username: self.username.clone(),
        };
        let frame = str_err(encode_frame(&connect_msg))?;
        write_half.write_all(&frame).await?;

        // wait for login response with timeout
        let mut accumulator = Vec::with_capacity(4096);
        let mut buf = [0u8; 4096];

        let login_result = timeout(Duration::from_secs(self.args.login_timeout_secs), async {
            loop {
                let n = read_half.read(&mut buf).await?;
                if n == 0 {
                    return Err(anyhow::anyhow!("connection closed during login"));
                }
                accumulator.extend_from_slice(&buf[..n]);

                if let Some((resp, consumed)) = str_err(try_extract_response(&accumulator))? {
                    accumulator.drain(..consumed);
                    return Ok(resp);
                }
            }
        })
        .await;

        match login_result {
            Ok(Ok(Response::Success { .. })) => {
                self.stats.connected.fetch_add(1, Ordering::Relaxed);
                debug!(username = %self.username, "connected");
            }
            Ok(Ok(Response::Error { message })) => {
                self.stats.connect_failed.fetch_add(1, Ordering::Relaxed);
                return Err(anyhow::anyhow!("login rejected: {}", message));
            }
            Ok(Ok(_)) => {
                self.stats.connect_failed.fetch_add(1, Ordering::Relaxed);
                return Err(anyhow::anyhow!("unexpected response during login"));
            }
            Ok(Err(e)) => {
                self.stats.connect_failed.fetch_add(1, Ordering::Relaxed);
                return Err(e);
            }
            Err(_) => {
                self.stats.connect_failed.fetch_add(1, Ordering::Relaxed);
                return Err(anyhow::anyhow!("login timeout"));
            }
        }

        // join initial room
        let join_msg = Message::JoinRoom {
            room_name: self.room.clone(),
        };
        let frame = str_err(encode_frame(&join_msg))?;
        write_half.write_all(&frame).await?;

        // spawn reader task
        let stats_clone = self.stats.clone();
        let reader_task = tokio::spawn(async move {
            loop {
                let n = match read_half.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };
                accumulator.extend_from_slice(&buf[..n]);

                // drain all complete responses
                loop {
                    match try_extract_response(&accumulator) {
                        Ok(Some((_, consumed))) => {
                            stats_clone
                                .messages_received
                                .fetch_add(1, Ordering::Relaxed);
                            accumulator.drain(..consumed);
                        }
                        Ok(None) => break,
                        Err(_) => {
                            stats_clone.errors.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            }
        });

        // generate fake user list for DM targets
        let fake_users: Vec<String> = (0..100).map(|i| format!("user_{}_{}", "fake", i)).collect();

        // send messages
        let send_interval = Duration::from_millis(self.args.send_interval_ms);
        let start = Instant::now();

        for msg_num in 0..self.args.messages {
            // check duration limit
            if self.args.duration_secs > 0
                && start.elapsed() > Duration::from_secs(self.args.duration_secs)
            {
                break;
            }

            let msg = self.generate_message(&fake_users);
            let frame = str_err(encode_frame(&msg))?;

            if let Err(e) = write_half.write_all(&frame).await {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                warn!(client_id = self.id, error = %e, msg = msg_num, "send failed");
                break;
            }

            self.stats.messages_sent.fetch_add(1, Ordering::Relaxed);

            // throttle sends
            tokio::time::sleep(send_interval).await;
        }

        // send disconnect
        let disconnect_msg = Message::Disconnect;
        let frame = str_err(encode_frame(&disconnect_msg))?;
        let _ = write_half.write_all(&frame).await;

        // allow reader to finish
        reader_task.abort();

        debug!(username = %self.username, "disconnected");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("lynx_load=info".parse().unwrap()),
        )
        .init();

    let args = Args::parse();

    // validate percentages
    let total_pct = args.room_msg_pct + args.dm_pct + args.list_pct + args.join_pct;
    if total_pct != 100 {
        warn!(
            total = total_pct,
            "traffic percentages don't sum to 100, will use proportional distribution"
        );
    }

    info!(
        clients = args.clients,
        rooms = args.rooms,
        messages = args.messages,
        "starting load test"
    );

    // generate session ID for multi-machine testing
    let session_id: String = {
        let mut rng = SmallRng::from_entropy();
        (0..6)
            .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
            .collect()
    };

    info!(session_id = %session_id, "session started");

    let stats = Arc::new(Stats::new());
    let mut handles = Vec::with_capacity(args.clients as usize);

    let start_time = Instant::now();

    // spawn clients in batches with staggering
    for batch_start in (0..args.clients).step_by(args.batch_size as usize) {
        let batch_end = (batch_start + args.batch_size).min(args.clients);

        for client_id in batch_start..batch_end {
            let client = Client::new(client_id, &session_id, args.clone(), stats.clone());
            let handle = tokio::spawn(async move {
                if let Err(e) = client.run().await {
                    debug!(client_id, error = %e, "client error");
                }
            });
            handles.push(handle);
        }

        // stagger between batches
        if batch_end < args.clients {
            tokio::time::sleep(Duration::from_millis(args.batch_delay_ms)).await;
        }

        let connected = stats.connected.load(Ordering::Relaxed);
        let failed = stats.connect_failed.load(Ordering::Relaxed);
        info!(spawned = batch_end, connected, failed, "batch complete");
    }

    // wait for all clients
    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start_time.elapsed();

    // print final stats
    let connected = stats.connected.load(Ordering::Relaxed);
    let connect_failed = stats.connect_failed.load(Ordering::Relaxed);
    let messages_sent = stats.messages_sent.load(Ordering::Relaxed);
    let messages_received = stats.messages_received.load(Ordering::Relaxed);
    let errors = stats.errors.load(Ordering::Relaxed);

    println!("\n=== Load Test Results ===");
    println!("Duration:          {:.2}s", elapsed.as_secs_f64());
    println!("Clients spawned:   {}", args.clients);
    println!("Connected:         {}", connected);
    println!("Connect failed:    {}", connect_failed);
    println!("Messages sent:     {}", messages_sent);
    println!("Messages received: {}", messages_received);
    println!("Errors:            {}", errors);
    println!(
        "Throughput:        {:.0} msg/s",
        messages_sent as f64 / elapsed.as_secs_f64()
    );

    if connect_failed > 0 {
        error!(failed = connect_failed, "some connections failed");
    }

    Ok(())
}
