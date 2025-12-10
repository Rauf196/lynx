use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, AsyncBufReadExt};
use lynx_protocol::{Message, Response, encode_frame, decode_response};
use lynx_server::server_addr;
use std::io::{self, Write};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ask for username
    print!("Enter username: ");
    io::stdout().flush()?;

    let mut username = String::new();
    io::stdin().read_line(&mut username)?;
    let username = username.trim().to_string();

    // connect to server
    let mut socket = TcpStream::connect(server_addr()).await?;
    println!("Connected to server");

    // send Connect message
    let msg = Message::Connect { username: username.clone() };
    let frame = encode_frame(&msg)?;
    socket.write_all(&frame).await?;

    // read the welcome response
    let mut buffer = vec![0u8; 4096];
    let n = socket.read(&mut buffer).await?;
    let response = decode_response(&buffer[..n])?;

    // check if connection was successful
    match response {
        Response::Success { message } => {
            println!("{}", message);
        }
        Response::Error { message } => {
            eprintln!("Connection failed: {}", message);
            return Ok(());
        }
        _ => {
            eprintln!("Unexpected response: {:?}", response);
            return Ok(());
        }
    }

    // split socket for concurrent reading/writing
    let (mut read_half, mut write_half) = socket.into_split();

    // spawn task to read from server
    let read_task = tokio::spawn(async move {
        let mut buffer = vec![0u8; 4096];
        loop {
            match read_half.read(&mut buffer).await {
                Ok(0) => {
                    println!("\nServer disconnected");
                    break;
                }
                Ok(n) => {
                    if let Ok(response) = decode_response(&buffer[..n]) {
                        match response {
                            Response::IncomingMessage { from, text, room } => {
                                if let Some(room) = room {
                                    println!("[{}] {}: {}", room, from, text);
                                } else {
                                    println!("[DM] {}: {}", from, text);
                                }
                            }
                            _ => println!("{:?}", response),
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Read error: {}", e);
                    break;
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    // spawn task to read from stdin and send to server
    let write_task = tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);
        let mut line = String::new();

        println!("\nYou can now send messages. Type and press Enter:");

        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break, // EOF
                Ok(_) => {
                    let text = line.trim().to_string();
                    if text.is_empty() {
                        continue;
                    }

                    // Send as room message
                    let msg = Message::SendRoomMessage { text };
                    if let Ok(frame) = encode_frame(&msg) {
                        if write_half.write_all(&frame).await.is_err() {
                            break;
                        }
                    }
                }
                Err(_) => break,
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    // wait for either task to finish
    tokio::select! {
        _ = read_task => {},
        _ = write_task => {},
    }

    println!("Disconnected");
    Ok(())
}
