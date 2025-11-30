use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use std::net::SocketAddr;
use std::sync::Arc;
use dashmap::DashMap;
use lynx_protocol::{Message, Response, decode_frame, encode_response};
use lynx_server::server_addr;

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

async fn handle_client(mut socket: TcpStream, addr: SocketAddr, clients: Clients) -> Result<(), Box<dyn std::error::Error>> {
    println!("Handling client: {}", addr);

    let mut buffer = vec![0u8; 4096];

    let (tx, _rx) = mpsc::channel::<Response>(100); // 100 = buffer size, rx will be used for write task

    loop {
        // read message
        let num_bytes = socket.read(&mut buffer).await?;

        if num_bytes == 0 {
            println!("Client {} disconnected", addr);
            break;
        }

        let message = decode_frame(&buffer[0..num_bytes])?;
        println!("Message from {} - {:?}", addr, message);

        match message {
            Message::Connect { username } => {
                // check if username is taken
                if clients.contains_key(&username) {
                    let response = Response::Error {
                        message: "username already taken".to_string()
                    };
                    let frame = encode_response(&response)?;
                    socket.write_all(&frame).await?;
                } else {
                    // register client
                    clients.insert(username.clone(), tx.clone());
                    println!("user {} registered", username);

                    let response = Response::Success {
                        message: format!("welcome, {}!", username)
                    };
                    let frame = encode_response(&response)?;
                    socket.write_all(&frame).await?;
                }
            }

            _ => {
                // for now acknowledge other messages
                let response = Response::Success {
                    message: "message received".to_string()
                };
                let frame = encode_response(&response)?;
                socket.write_all(&frame).await?;
            }
        }
    }

    Ok(())
}
