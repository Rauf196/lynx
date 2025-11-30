use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::net::SocketAddr;

use lynx_protocol::{decode_frame, encode_response};

use lynx_server::server_addr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let address = server_addr();

    let listener = TcpListener::bind(&address).await?;
    println!("Server listening on {}", address);

    // accept loop
    loop {
        // socket is TcpStream , addr is their IP address
        let (socket, addr) = listener.accept().await?;
        println!("New connection from {}", addr);

        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, addr).await {
                eprintln!{"Error handling client {}: {}", addr, e};
            }
        });
    }
}

async fn handle_client(mut socket: TcpStream, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    println!("Handling client: {}", addr);

    let mut buffer = vec![0u8; 4096];

    loop {
        // read message
        let num_bytes = socket.read(&mut buffer).await?;

        if num_bytes == 0 {
            println!("Client {} disconnected", addr);
            break;
        }

        let message = decode_frame(&buffer[0..num_bytes])?;
        println!("Message from {} - {:?}", addr, message);

        // write response
        let response = lynx_protocol::Response::Success {
            message: "Welcome!".to_string()
        };
        let frame = encode_response(&response)?;

        socket.write_all(&frame).await?;
    }

    Ok(())
}
