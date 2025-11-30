use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use lynx_protocol::{encode_frame, decode_response};

use lynx_server::server_addr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // connect to server
    let mut socket = TcpStream::connect(server_addr()).await?;
    println!("Connected to server");

    // send a Connect Message
    let msg = lynx_protocol::Message::Connect {
        username: "Rauf".to_string(),
    };
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
