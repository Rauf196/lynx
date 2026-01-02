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

struct ClientInfo {
    sender: ClientSender,
    room: String,
}

type Clients = Arc<DashMap<String, ClientInfo>>;

pub struct ServerHandle {
    pub local_addr: SocketAddr,
    shutdown_tx: broadcast::Sender<()>,
}

impl ServerHandle {
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

pub struct Server {
    listener: TcpListener,
    clients: Clients,
    shutdown_tx: broadcast::Sender<()>,
    active_connections: Arc<AtomicUsize>,
}

impl Server {
    /// bind to address. use port 0 for ephemeral port.
    pub async fn bind(addr: &str) -> io::Result<(Self, ServerHandle)> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;

        let (shutdown_tx, _) = broadcast::channel::<()>(1);

        let server = Self {
            listener,
            clients: Arc::new(DashMap::new()),
            shutdown_tx: shutdown_tx.clone(),
            active_connections: Arc::new(AtomicUsize::new(0)),
        };

        let handle = ServerHandle {
            local_addr,
            shutdown_tx,
        };

        Ok((server, handle))
    }

    /// run until shutdown is triggered via handle.shutdown()
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
                            counter!("lynx_connections_total").increment(1);
                            gauge!("lynx_connections_active").increment(1.0);
                            self.active_connections.fetch_add(1, Ordering::Relaxed);

                            info!(client_addr = %addr, "new connection");

                            let clients = self.clients.clone();
                            let shutdown_tx = self.shutdown_tx.clone();
                            let active_connections = self.active_connections.clone();

                            client_tasks.spawn(async move {
                                if let Err(e) = handle_client(socket, addr, clients, shutdown_tx).await {
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

#[instrument(skip(socket, clients, shutdown_tx), fields(client_addr = %addr))]
async fn handle_client(
    socket: TcpStream,
    addr: SocketAddr,
    clients: Clients,
    shutdown_tx: broadcast::Sender<()>,
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
                        if let Err(e) = process_message(&message, &mut current_username, &clients, &tx).await {
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
                let sender_room = clients
                    .get(sender_username)
                    .map(|e| e.room.clone())
                    .unwrap_or_else(|| "general".to_string());

                for entry in clients.iter() {
                    if entry.room == sender_room {
                        let msg = Response::IncomingMessage {
                            from: sender_username.clone(),
                            text: text.clone(),
                            room: Some(sender_room.clone()),
                        };
                        // try_send to avoid deadlock on slow consumers
                        let _ = entry.sender.try_send(msg);
                    }
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
                if let Some(recipient) = clients.get(to) {
                    let msg = Response::IncomingMessage {
                        from: sender_username.clone(),
                        text: text.clone(),
                        room: None,
                    };
                    // try_send to avoid deadlock on slow consumers
                    let _ = recipient.sender.try_send(msg);
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
            let _ = tx.send(Response::Success {
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
