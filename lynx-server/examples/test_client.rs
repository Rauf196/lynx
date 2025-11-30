use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use lynx_protocol::{Message, Response, encode_frame, decode_response};

use lynx_server::server_addr;
use std::io::{self, Write};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Ask for username
    print!("Enter username: ");
    io::stdout().flush()?;  // Make sure prompt shows before input

    let mut username = String::new();
    io::stdin().read_line(&mut username)?;
    let username = username.trim().to_string();  // Remove newline

    // connect to server
    let mut socket = TcpStream::connect(server_addr()).await?;
    println!("Connected to server");

    // send a Connect Message
    let msg = Message::Connect { username };
    let frame = encode_frame(&msg)?;
    socket.write_all(&frame).await?;
    println!("Sent: {:?}", msg);

    // read response
    let mut buffer = vec![0u8; 4096];
    let n = socket.read(&mut buffer).await?;
    let response = decode_response(&buffer[..n])?;
    println!("Received: {:?}", response);

    Ok(())
}
