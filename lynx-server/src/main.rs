use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use std::net::SocketAddr;
use std::sync::Arc;
use dashmap::DashMap;
use lynx_protocol::{Message, Response, decode_frame, encode_response};
use lynx_server::server_addr;
use anyhow::Result;
use tracing::{info, warn, error, debug, instrument};
use tracing_subscriber::EnvFilter;

type ClientSender = mpsc::Sender<Response>;

struct ClientInfo {
    sender: ClientSender,
    room: String,
}

type Clients = Arc<DashMap<String, ClientInfo>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber
    // RUST_LOG env var controls log level (e.g., RUST_LOG=debug, RUST_LOG=lynx_server=debug)
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
        )
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    let address = server_addr();

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

    // accept loop
    loop {
        let (socket, addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                error!(error = %e, "failed to accept connection");
                continue;
            }
        };
        info!(client_addr = %addr, "new connection");

        let clients = clients.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, addr, clients).await {
                error!(client_addr = %addr, error = %e, "client handler failed");
            }
        });
    }
}

#[instrument(skip(socket, clients), fields(client_addr = %addr))]
async fn handle_client(socket: TcpStream, addr: SocketAddr, clients: Clients) -> Result<(), Box<dyn std::error::Error>> {
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
        // read message
        let num_bytes = read_half.read(&mut buffer).await?;

        if num_bytes == 0 {
            if let Some(username) = current_username {
                clients.remove(&username);
                info!(username = %username, "client disconnected");
            } else {
                info!("client disconnected (unregistered)");
            }
            break;
        }

        let message = decode_frame(&buffer[0..num_bytes]).map_err(|e| anyhow::anyhow!(e))?;
        debug!(message = ?message, "received message");

        match message {
            Message::Connect { username } => {
                if clients.contains_key(&username) {
                    warn!(username = %username, "username already taken");
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

            Message::SendRoomMessage { text } => {
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

            Message::SendPrivateMessage { to, text } => {
                if let Some(ref sender_username) = current_username {
                    if let Some(recipient) = clients.get(&to) {
                        let client_tx = recipient.value();

                        let msg = Response::IncomingMessage {
                            from: sender_username.clone(),
                            text: text.clone(),
                            room: None,
                        };

                        let _ = client_tx.sender.send(msg).await;
                    } else {
                        // recipient not registered - send error
                        let response = Response::Error {
                            message: "recipient with that username could not be found".to_string()
                        };
                        tx.send(response).await?;
                    }
                } else {
                    send_not_registered_error(&tx).await?;
                }
            }

            Message::JoinRoom { room_name } => {
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
    tx.send(Response::Error {
        message: "you must connect with a username first".to_string(),
    }).await
}
