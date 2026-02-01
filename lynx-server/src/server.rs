//! Core server implementation.

use crate::Config;
use crate::rate_limiter::TokenBucket;
use anyhow::Result;
use dashmap::DashMap;
use lynx_protocol::{Message, Response, encode_response, try_extract_frame};
use metrics::{counter, gauge, histogram};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tracing::{debug, error, info, instrument, warn};

type ClientSender = mpsc::Sender<Response>;

/// per-client state stored in the registry.
struct ClientInfo {
    sender: ClientSender,
    room: String,
    dropped_count: AtomicUsize,
    rate_limiter: TokenBucket,
}

type Clients = Arc<DashMap<String, ClientInfo>>;

/// Handle for controlling a running server.
///
/// Returned by [`Server::bind`] alongside the server instance.
/// Use this to get the bound address or trigger shutdown.
pub struct ServerHandle {
    /// The socket address the server is listening on.
    ///
    /// Useful when binding to port 0 (ephemeral port) to discover
    /// the actual assigned port.
    pub local_addr: SocketAddr,
    shutdown_tx: broadcast::Sender<()>,
}

impl ServerHandle {
    /// Triggers graceful shutdown of the server.
    ///
    /// The server will:
    /// 1. Stop accepting new connections
    /// 2. Notify all connected clients
    /// 3. Wait for client tasks to complete
    /// 4. Return from [`Server::run`]
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// The Lynx chat server.
///
/// Handles TCP connections, message routing, and client lifecycle.
///
/// # Example
///
/// ```no_run
/// use lynx_server::{Config, Server};
///
/// # async fn example() -> std::io::Result<()> {
/// let (server, handle) = Server::bind("127.0.0.1:6006", Config::default()).await?;
///
/// // run until shutdown
/// tokio::spawn(async move {
///     server.run().await.unwrap();
/// });
///
/// // later: trigger shutdown
/// handle.shutdown();
/// # Ok(())
/// # }
/// ```
pub struct Server {
    listener: TcpListener,
    clients: Clients,
    shutdown_tx: broadcast::Sender<()>,
    active_connections: Arc<AtomicUsize>,
    config: Arc<Config>,
}

impl Server {
    /// Returns a shared reference to the active connection counter.
    ///
    /// Used by health check endpoints to report server capacity.
    pub fn active_connections(&self) -> Arc<AtomicUsize> {
        self.active_connections.clone()
    }

    /// Returns the maximum allowed connections from config.
    pub fn max_connections(&self) -> usize {
        self.config.maxconnections
    }

    /// Binds the server to a TCP address.
    ///
    /// Use port 0 to let the OS assign an ephemeral port (useful for testing).
    /// The actual bound address is available via `ServerHandle::local_addr`.
    ///
    /// # Errors
    ///
    /// Returns an error if the address cannot be bound (e.g., port in use).
    pub async fn bind(addr: &str, config: Config) -> io::Result<(Self, ServerHandle)> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;

        let (shutdown_tx, _) = broadcast::channel::<()>(1);

        let server = Self {
            listener,
            clients: Arc::new(DashMap::new()),
            shutdown_tx: shutdown_tx.clone(),
            active_connections: Arc::new(AtomicUsize::new(0)),
            config: Arc::new(config),
        };

        let handle = ServerHandle {
            local_addr,
            shutdown_tx,
        };

        Ok((server, handle))
    }

    /// Runs the server until shutdown is triggered.
    ///
    /// This method consumes the server and blocks until:
    /// 1. [`ServerHandle::shutdown`] is called, or
    /// 2. An unrecoverable error occurs
    ///
    /// During graceful shutdown, the server waits for all client
    /// tasks to complete before returning.
    pub async fn run(self) -> Result<()> {
        let mut client_tasks: JoinSet<()> = JoinSet::new();

        info!(address = %self.listener.local_addr()?, "server accepting connections");

        // subscribe once for the accept loop shutdown check
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                result = self.listener.accept() => {
                    match result {
                        Ok((socket, addr)) => {
                            // check connection limit before accepting
                            let current = self.active_connections.load(Ordering::Relaxed);
                            if current >= self.config.maxconnections {
                                counter!("lynx_connections_rejected_total").increment(1);
                                warn!(
                                    client_addr = %addr,
                                    current = current,
                                    max = self.config.maxconnections,
                                    "connection rejected: server at capacity"
                                );
                                tokio::spawn(async move {
                                    let _ = reject_connection(socket).await;
                                });
                                continue;
                            }

                            counter!("lynx_connections_total").increment(1);
                            gauge!("lynx_connections_active").increment(1.0);
                            self.active_connections.fetch_add(1, Ordering::Relaxed);

                            info!(client_addr = %addr, "new connection");

                            let clients = self.clients.clone();
                            let shutdown_tx = self.shutdown_tx.clone();
                            let active_connections = self.active_connections.clone();
                            let slow_client_threshold = self.config.slow_client_threshold;
                            let rate_limit_per_second = self.config.rate_limit_per_second;
                            let rate_limit_burst = self.config.rate_limit_burst;

                            client_tasks.spawn(async move {
                                if let Err(e) = handle_client(
                                    socket,
                                    addr,
                                    clients,
                                    shutdown_tx,
                                    slow_client_threshold,
                                    rate_limit_per_second,
                                    rate_limit_burst,
                                ).await {
                                    error!(client_addr = %addr, error = %e, "client handler error");
                                }
                                gauge!("lynx_connections_active").decrement(1.0);
                                active_connections.fetch_sub(1, Ordering::Relaxed);
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "accept failed");
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("shutdown signal received");
                    break;
                }
            }
        }

        // wait for all client tasks to complete
        let remaining = self.active_connections.load(Ordering::Relaxed);
        if remaining > 0 {
            info!(
                active_clients = remaining,
                "waiting for clients to disconnect"
            );
        }
        while client_tasks.join_next().await.is_some() {}

        info!("server shutdown complete");
        Ok(())
    }
}

#[instrument(skip(socket, clients, shutdown_tx, slow_client_threshold, rate_limit_per_second, rate_limit_burst), fields(client_addr = %addr))]
async fn handle_client(
    socket: TcpStream,
    addr: SocketAddr,
    clients: Clients,
    shutdown_tx: broadcast::Sender<()>,
    slow_client_threshold: usize,
    rate_limit_per_second: f64,
    rate_limit_burst: usize,
) -> Result<()> {
    debug!("handling client connection");

    let (tx, mut rx) = mpsc::channel::<Response>(100);
    let (mut read_half, mut write_half) = socket.into_split();

    // separate receivers for read and write tasks
    let mut write_shutdown_rx = shutdown_tx.subscribe();
    let mut read_shutdown_rx = shutdown_tx.subscribe();

    let write_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                response = rx.recv() => {
                    match response {
                        Some(resp) => {
                            let frame = encode_response(&resp).map_err(|e| anyhow::anyhow!(e))?;
                            write_half.write_all(&frame).await?;
                        }
                        None => break,
                    }
                }
                _ = write_shutdown_rx.recv() => {
                    break;
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    let read_task = tokio::spawn(async move {
        let mut current_username: Option<String> = None;
        let mut buffer = vec![0u8; 4096];
        let mut accumulator: Vec<u8> = Vec::with_capacity(8192);
        let mut task_error: Option<anyhow::Error> = None;

        'main: loop {
            // process complete messages in accumulator
            loop {
                match try_extract_frame(&accumulator) {
                    Ok(Some((message, consumed))) => {
                        if let Err(e) = process_message(
                            &message,
                            &mut current_username,
                            &clients,
                            &tx,
                            slow_client_threshold,
                            rate_limit_per_second,
                            rate_limit_burst,
                        )
                        .await
                        {
                            task_error = Some(e);
                            break 'main;
                        }
                        accumulator.drain(..consumed);
                    }
                    Ok(None) => break,
                    Err(e) => {
                        counter!("lynx_errors_total", "error_type" => "decode_error").increment(1);
                        task_error = Some(anyhow::anyhow!(e));
                        break 'main;
                    }
                }
            }

            // read from socket or shutdown
            let num_bytes = tokio::select! {
                read_result = read_half.read(&mut buffer) => {
                    match read_result {
                        Ok(0) => {
                            counter!("lynx_messages_processed_total", "message_type" => "disconnect").increment(1);
                            break;
                        }
                        Ok(n) => n,
                        Err(e) => {
                            task_error = Some(anyhow::anyhow!(e));
                            break 'main;
                        }
                    }
                }
                _ = read_shutdown_rx.recv() => {
                    counter!("lynx_messages_processed_total", "message_type" => "disconnect").increment(1);
                    break;
                }
            };

            accumulator.extend_from_slice(&buffer[..num_bytes]);
        }

        // cleanup - always runs regardless of how loop exited
        if let Some(username) = current_username {
            clients.remove(&username);
            if task_error.is_some() {
                warn!(username = %username, "client removed (error)");
            } else {
                info!(username = %username, "client disconnected");
            }
        }

        match task_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    });

    let (read_result, write_result) = tokio::try_join!(read_task, write_task)?;
    read_result?;
    write_result?;

    Ok(())
}

async fn process_message(
    message: &Message,
    current_username: &mut Option<String>,
    clients: &Clients,
    tx: &ClientSender,
    slow_client_threshold: usize,
    rate_limit_per_second: f64,
    rate_limit_burst: usize,
) -> Result<()> {
    let processing_start = Instant::now();

    match message {
        Message::Connect { username } => {
            if current_username.is_some() {
                warn!(username = %username, "already authenticated, rejecting connect");
                counter!("lynx_errors_total", "error_type" => "already_authenticated").increment(1);
                tx.send(Response::Error {
                    message: "already connected with a username".to_string(),
                })
                .await?;
            } else if clients.contains_key(username) {
                warn!(username = %username, "username taken");
                counter!("lynx_errors_total", "error_type" => "username_taken").increment(1);
                tx.send(Response::Error {
                    message: "username already taken".to_string(),
                })
                .await?;
            } else {
                clients.insert(
                    username.clone(),
                    ClientInfo {
                        sender: tx.clone(),
                        room: "general".to_string(),
                        dropped_count: AtomicUsize::new(0),
                        rate_limiter: TokenBucket::new(rate_limit_per_second, rate_limit_burst),
                    },
                );
                *current_username = Some(username.clone());
                info!(username = %username, room = "general", "user registered");
                tx.send(Response::Success {
                    message: format!("welcome, {}!", username),
                })
                .await?;
            }
        }

        Message::SendRoomMessage { text } => {
            if let Some(sender_username) = current_username {
                // check rate limit
                if let Some(info) = clients.get(sender_username)
                    && !info.rate_limiter.try_acquire()
                {
                    counter!("lynx_rate_limited_total").increment(1);
                    tx.send(Response::Error {
                        message: "rate limited, slow down".to_string(),
                    })
                    .await?;
                    return Ok(());
                }

                let sender_room = clients
                    .get(sender_username)
                    .map(|e| e.room.clone())
                    .unwrap_or_else(|| "general".to_string());

                let mut slow_clients: Vec<String> = Vec::new();
                for entry in clients.iter() {
                    if entry.room == sender_room {
                        let msg = Response::IncomingMessage {
                            from: sender_username.clone(),
                            text: text.clone(),
                            room: Some(sender_room.clone()),
                        };
                        // try_send to avoid deadlock on slow consumers
                        if entry.sender.try_send(msg).is_err() {
                            counter!("lynx_messages_dropped_total").increment(1);
                            let count = entry.dropped_count.fetch_add(1, Ordering::Relaxed) + 1;
                            if count >= slow_client_threshold {
                                slow_clients.push(entry.key().clone());
                            }
                        }
                    }
                }
                // disconnect slow clients after iteration completes
                for username in slow_clients {
                    clients.remove(&username);
                    counter!("lynx_clients_slow_disconnected_total").increment(1);
                    warn!(username = %username, "disconnecting slow client");
                }
            } else {
                send_not_registered_error(tx).await?;
            }
        }

        Message::ListUsers => {
            if current_username.is_some() {
                let users: Vec<String> = clients.iter().map(|e| e.key().clone()).collect();
                tx.send(Response::UserList { users }).await?;
            } else {
                send_not_registered_error(tx).await?;
            }
        }

        Message::SendPrivateMessage { to, text } => {
            if let Some(sender_username) = current_username {
                // check rate limit
                if let Some(info) = clients.get(sender_username)
                    && !info.rate_limiter.try_acquire()
                {
                    counter!("lynx_rate_limited_total").increment(1);
                    tx.send(Response::Error {
                        message: "rate limited, slow down".to_string(),
                    })
                    .await?;
                    return Ok(());
                }

                if let Some(recipient) = clients.get(to) {
                    let msg = Response::IncomingMessage {
                        from: sender_username.clone(),
                        text: text.clone(),
                        room: None,
                    };
                    // try_send to avoid deadlock on slow consumers
                    if recipient.sender.try_send(msg).is_err() {
                        counter!("lynx_messages_dropped_total").increment(1);
                        let count = recipient.dropped_count.fetch_add(1, Ordering::Relaxed) + 1;
                        if count >= slow_client_threshold {
                            let recipient_name = to.clone();
                            drop(recipient); // release the DashMap guard before remove
                            clients.remove(&recipient_name);
                            counter!("lynx_clients_slow_disconnected_total").increment(1);
                            warn!(username = %recipient_name, "disconnecting slow client");
                        }
                    }
                } else {
                    counter!("lynx_errors_total", "error_type" => "recipient_not_found")
                        .increment(1);
                    tx.send(Response::Error {
                        message: "recipient not found".to_string(),
                    })
                    .await?;
                }
            } else {
                send_not_registered_error(tx).await?;
            }
        }

        Message::JoinRoom { room_name } => {
            if let Some(username) = current_username {
                if let Some(mut entry) = clients.get_mut(username) {
                    entry.room = room_name.clone();
                }
                info!(username = %username, room = %room_name, "user joined room");
                tx.send(Response::Success {
                    message: format!("joined room: {}", room_name),
                })
                .await?;
            } else {
                send_not_registered_error(tx).await?;
            }
        }

        Message::Disconnect => {
            if let Some(username) = current_username.take() {
                clients.remove(&username);
                info!(username = %username, "client disconnected");
            }
            // ignore send error - client may already be gone
            let _ = tx
                .send(Response::Success {
                    message: "goodbye".to_string(),
                })
                .await;
        }
    }

    let message_type = match message {
        Message::Connect { .. } => "connect",
        Message::SendRoomMessage { .. } => "send_room_message",
        Message::SendPrivateMessage { .. } => "send_private_message",
        Message::JoinRoom { .. } => "join_room",
        Message::ListUsers => "list_users",
        Message::Disconnect => "disconnect",
    };
    counter!("lynx_messages_processed_total", "message_type" => message_type).increment(1);
    histogram!(
        "lynx_message_processing_duration_seconds",
        "message_type" => message_type
    )
    .record(processing_start.elapsed().as_secs_f64());

    Ok(())
}

async fn send_not_registered_error(
    tx: &ClientSender,
) -> Result<(), mpsc::error::SendError<Response>> {
    counter!("lynx_errors_total", "error_type" => "not_registered").increment(1);
    tx.send(Response::Error {
        message: "you must connect with a username first".to_string(),
    })
    .await
}

/// send error response and close connection when server is at capacity
async fn reject_connection(mut socket: TcpStream) -> Result<()> {
    let response = Response::Error {
        message: "server at capacity, try again later".to_string(),
    };
    let frame = encode_response(&response).map_err(|e| anyhow::anyhow!(e))?;
    socket.write_all(&frame).await?;
    socket.shutdown().await?;
    Ok(())
}
