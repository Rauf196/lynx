use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, broadcast};
use tokio::task::JoinSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use dashmap::DashMap;
use lynx_protocol::{Message, Response, decode_frame, encode_response};
use lynx_server::Config;
use anyhow::Result;
use tracing::{info, warn, error, debug, instrument};
use tracing_subscriber::EnvFilter;
use metrics::{counter, gauge, histogram};
use std::time::Instant;

type ClientSender = mpsc::Sender<Response>;

struct ClientInfo {
    sender: ClientSender,
    room: String,
}

type Clients = Arc<DashMap<String, ClientInfo>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // load .env file if it exists (ignore errors)
    let _ = dotenvy::dotenv();

    // load configuration (defaults -> config.toml -> env vars)
    let config = Config::load()?;

    // initialize tracing subscriber
    // RUST_LOG env var takes priority, otherwise use config.loglevel
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(&config.loglevel))
        )
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    info!(
        host = %config.host,
        port = %config.port,
        maxconnections = %config.maxconnections,
        loglevel = %config.loglevel,
        "configuration loaded"
    );

    // Initialize metrics server
    lynx_server::metrics::init(&config.metrics_address())?;
    info!(
        metrics_address = %config.metrics_address(),
        "metrics server started"
    );

    let address = config.address();

    let listener = match TcpListener::bind(&address).await {
        Ok(l) => {
            info!(address = %address, "server started");
            l
        }
        Err(e) => {
            error!(address = %address, error = %e, "failed to bind to address");
            return Err(e.into());
        }
    };

    let clients: Clients = Arc::new(DashMap::new());

    // atomic counter for accurate active connection tracking
    let active_connections = Arc::new(AtomicUsize::new(0));

    // track spawned client tasks so we can wait for them on shutdown
    let mut client_tasks: JoinSet<()> = JoinSet::new();

    // broadcast channel to notify all clients of shutdown
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // create shutdown signal and pin it for reuse across loop iterations
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    // accept loop
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (socket, addr) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!(error = %e, "failed to accept connection");
                        continue;
                    }
                };

                // Track connection metrics
                counter!("lynx_connections_total").increment(1);
                gauge!("lynx_connections_active").increment(1.0);
                active_connections.fetch_add(1, Ordering::Relaxed);

                info!(client_addr = %addr, "new connection");

                let clients = clients.clone();
                let shutdown_rx = shutdown_tx.subscribe();
                let active_connections = active_connections.clone();

                client_tasks.spawn(async move {
                    if let Err(e) = handle_client(socket, addr, clients, shutdown_rx).await {
                        error!(client_addr = %addr, error = %e, "client handler failed");
                    }
                    // Decrement active connections (handles all exit paths)
                    gauge!("lynx_connections_active").decrement(1.0);
                    active_connections.fetch_sub(1, Ordering::Relaxed);
                });
            }
            _ = &mut shutdown => {
                info!("shutdown signal received");
                break;
            }
        }
    }

    // drop sender to notify all clients that shutdown is happening
    drop(shutdown_tx);

    // wait for all client tasks to finish
    let remaining = active_connections.load(Ordering::Relaxed);
    if remaining > 0 {
        info!(active_clients = remaining, "waiting for clients to disconnect");
    }
    while client_tasks.join_next().await.is_some() {}

    info!("server shutdown complete");
    Ok(())
}

#[instrument(skip(socket, clients, shutdown_rx), fields(client_addr = %addr))]
async fn handle_client(
    socket: TcpStream,
    addr: SocketAddr,
    clients: Clients,
    mut shutdown_rx: broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    debug!("handling client connection");

    let mut buffer = vec![0u8; 4096];

    let (tx, mut rx) = mpsc::channel::<Response>(100); // 100 = buffer size, rx will be used for write task

    let (mut read_half, mut write_half) = socket.into_split();

    // spawn write task
    let write_task = tokio::spawn(async move {
        while let Some(response) = rx.recv().await {
            let frame = encode_response(&response).map_err(|e| anyhow::anyhow!(e))?;
            write_half.write_all(&frame).await?;
        }

        Ok::<(), anyhow::Error>(())
    });

    // spawn read task
    let read_task = tokio::spawn(async move {

        let mut current_username: Option<String> = None;

        loop {
        // race: read from socket OR receive shutdown signal
        let num_bytes = tokio::select! {
            result = read_half.read(&mut buffer) => {
                match result {
                    Ok(0) => {
                        // client disconnected normally (TCP EOF)
                        counter!("lynx_messages_processed_total", "message_type" => "disconnect").increment(1);
                        if let Some(ref username) = current_username {
                            clients.remove(username);
                            info!(username = %username, "client disconnected");
                        } else {
                            info!("client disconnected (unregistered)");
                        }
                        break;
                    }
                    Ok(n) => n,
                    Err(e) => return Err(anyhow::anyhow!(e)),
                }
            }
            _ = shutdown_rx.recv() => {
                // server is shutting down
                counter!("lynx_messages_processed_total", "message_type" => "disconnect").increment(1);
                if let Some(ref username) = current_username {
                    clients.remove(username);
                    info!(username = %username, "client disconnected (server shutdown)");
                } else {
                    info!("client disconnected (server shutdown, unregistered)");
                }
                break;
            }
        };

        let message = match decode_frame(&buffer[0..num_bytes]) {
            Ok(msg) => msg,
            Err(e) => {
                counter!("lynx_errors_total", "error_type" => "decode_error").increment(1);
                return Err(anyhow::anyhow!(e));
            }
        };
        debug!(message = ?message, "received message");

        // Start timing message processing
        let processing_start = Instant::now();

        match message {
            Message::Connect { ref username } => {
                if clients.contains_key(username) {
                    warn!(username = %username, "username already taken");
                    counter!("lynx_errors_total", "error_type" => "username_taken").increment(1);
                    let response = Response::Error {
                        message: "username already taken".to_string()
                    };
                    tx.send(response).await?;
                } else {
                    clients.insert(username.clone(), ClientInfo {
                        sender: tx.clone(),
                        room: "general".to_string(),
                    });
                    current_username = Some(username.clone());
                    info!(username = %username, room = "general", "user registered");

                    let response = Response::Success {
                        message: format!("welcome, {}!", username)
                    };
                    tx.send(response).await?;
                }
            }

            Message::SendRoomMessage { ref text } => {
                if let Some(ref sender_username) = current_username {
                    // get sender's room
                    let sender_room = clients.get(sender_username)
                        .map(|entry| entry.room.clone())
                        .unwrap_or_else(|| "general".to_string());

                    // go through all the clients' senders for {sender_room}
                    for entry in clients.iter() {
                        if entry.room == sender_room {
                            let msg = Response::IncomingMessage {
                                from: sender_username.clone(),
                                text: text.clone(),
                                room: Some(sender_room.clone()),
                            };
                            let _ = entry.sender.send(msg).await;
                        }
                    }
                } else {
                    send_not_registered_error(&tx).await?;
                }
            }

            Message::ListUsers => {
                if current_username.is_some() {
                    let users: Vec<String> = clients.iter()
                        .map(|entry| entry.key().clone())
                        .collect();
                    let response = Response::UserList { users };
                    tx.send(response).await?;
                } else {
                    send_not_registered_error(&tx).await?;
                }
            }

            Message::SendPrivateMessage { ref to, ref text } => {
                if let Some(ref sender_username) = current_username {
                    if let Some(recipient) = clients.get(to) {
                        let client_tx = recipient.value();

                        let msg = Response::IncomingMessage {
                            from: sender_username.clone(),
                            text: text.clone(),
                            room: None,
                        };

                        let _ = client_tx.sender.send(msg).await;
                    } else {
                        // recipient not registered - send error
                        counter!("lynx_errors_total", "error_type" => "recipient_not_found").increment(1);
                        let response = Response::Error {
                            message: "recipient with that username could not be found".to_string()
                        };
                        tx.send(response).await?;
                    }
                } else {
                    send_not_registered_error(&tx).await?;
                }
            }

            Message::JoinRoom { ref room_name } => {
                if let Some(ref username) = current_username {
                    if let Some(mut entry) = clients.get_mut(username) {
                        entry.room = room_name.clone();
                    }

                    let response = Response::Success {
                        message: format!("joined room : {}", room_name)
                    };
                    tx.send(response).await?;
                } else {
                    send_not_registered_error(&tx).await?;
                }
            }

            _ => {
                // for now acknowledge other messages
                let response = Response::Success {
                    message: "message received".to_string()
                };
                tx.send(response).await?;
            }
        }

        // Track message metrics
        let message_type = match &message {
            Message::Connect { .. } => "connect",
            Message::SendRoomMessage { .. } => "send_room_message",
            Message::SendPrivateMessage { .. } => "send_private_message",
            Message::JoinRoom { .. } => "join_room",
            Message::ListUsers => "list_users",
            Message::Disconnect => "disconnect",
        };
        counter!("lynx_messages_processed_total", "message_type" => message_type).increment(1);

        // Record processing duration
        histogram!(
            "lynx_message_processing_duration_seconds",
            "message_type" => message_type
        ).record(processing_start.elapsed().as_secs_f64());
    }
        Ok::<(), anyhow::Error>(())
    });

    // wait for both tasks
    let (read_result, write_result) = tokio::try_join!(read_task, write_task)?;
    read_result?;
    write_result?;

    Ok(())
}

// helper function to send "not registered" error
async fn send_not_registered_error(tx: &ClientSender) -> Result<(), mpsc::error::SendError<Response>> {
    counter!("lynx_errors_total", "error_type" => "not_registered").increment(1);
    tx.send(Response::Error {
        message: "you must connect with a username first".to_string(),
    }).await
}

// helper function to wait for shutdown signal (Ctrl+C)
async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install ctrl+c handler");
}
