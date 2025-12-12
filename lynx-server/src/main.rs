use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use std::net::SocketAddr;
use std::sync::Arc;
use dashmap::DashMap;
use lynx_protocol::{Message, Response, decode_frame, encode_response};
use lynx_server::server_addr;
use anyhow::Result;

type ClientSender = mpsc::Sender<Response>;
type Clients = Arc<DashMap<String, ClientSender>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let address = server_addr();

    let listener = TcpListener::bind(&address).await?;
    println!("Server listening on {}", address);

    let clients: Clients = Arc::new(DashMap::new());

    // accept loop
    loop {
        // socket is TcpStream , addr is their IP address
        let (socket, addr) = listener.accept().await?;
        println!("New connection from {}", addr);

        let clients = clients.clone(); // Arc, not the dashmap

        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, addr, clients).await {
                eprintln!{"Error handling client {}: {}", addr, e};
            }
        });
    }
}

async fn handle_client(socket: TcpStream, addr: SocketAddr, clients: Clients) -> Result<(), Box<dyn std::error::Error>> {
    println!("Handling client: {}", addr);

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
                println!("client {} (user: {}) disconnected", addr, username);
            } else {
                println!("client {} disconnected (not registered)", addr);
            }
            break;
        }

        let message = decode_frame(&buffer[0..num_bytes]).map_err(|e| anyhow::anyhow!(e))?;
        println!("Message from {} - {:?}", addr, message);

        match message {
            Message::Connect { username } => {
                // check if username is taken
                if clients.contains_key(&username) {
                    println!("user {} already exists", username);
                    let response = Response::Error {
                        message: "username already taken".to_string()
                    };
                    tx.send(response).await?;
                } else {
                    // register client
                    clients.insert(username.clone(), tx.clone());
                    current_username = Some(username.clone());
                    println!("user {} registered", username);

                    let response = Response::Success {
                        message: format!("welcome, {}!", username)
                    };
                    tx.send(response).await?;
                }
            }

            Message::SendRoomMessage { text } => {
                if let Some(ref sender_username) = current_username {
                    // go through all the clients' senders
                    for entry in clients.iter() {
                        let client_tx = entry.value();

                        // send to each client
                        let msg = Response::IncomingMessage {
                            from: sender_username.clone(),
                            text: text.clone(),
                            room: Some("default".to_string()),
                        };

                        let _ = client_tx.send(msg).await;
                    }
                } else {
                    // user not registered - send error
                    let response = Response::Error {
                        message: "you must connect with a username first".to_string()
                    };
                    tx.send(response).await?;
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
                    // user not registered - send error
                    let response = Response::Error {
                        message: "you must connect with a username first".to_string()
                    };
                    tx.send(response).await?;
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
    tokio::try_join!(read_task, write_task)?;

    Ok(())
}
