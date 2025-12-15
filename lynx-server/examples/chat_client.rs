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
    println!("[*] connected to server");

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
            println!("[*] {}", message);
        }
        Response::Error { message } => {
            eprintln!("[!] connection failed: {}", message);
            return Ok(());
        }
        _ => {
            eprintln!("[!] unexpected response: {:?}", response);
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
                    println!("[*] server disconnected");
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
                            },

                            Response::UserList { users } => {
                                println!("[*] online users: {}", users.join(", "));
                            },

                            Response::Error { message } => {
                                eprintln!("[!] {}", message);
                            }

                            Response::Success { message } => {
                                println!("[*] {}", message);
                            }

                            _ => println!("{:?}", response),
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[!] read error: {}", e);
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

        println!("[*] ready to chat (type /help for commands)");

        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break, // EOF
                Ok(_) => {
                    let text = line.trim().to_string();
                    if text.is_empty() {
                        continue;
                    }

                    // parse input: command or regular message
                    let msg = if text.starts_with("/") {
                        // split into command and rest
                        let parts: Vec<&str> = text.splitn(2, ' ').collect();
                        let command = parts[0];
                        let args = parts.get(1).copied(); // Option<&str>

                        match command {
                            "/quit" | "/q" => break,

                            "/users" => Message::ListUsers,

                            "/join" => {
                                if let Some(room_name) = args {
                                    Message::JoinRoom { room_name: room_name.to_string() }
                                } else {
                                    println!("usage: /join <room>");
                                    continue;
                                }
                            }

                            "/msg" => {
                                if let Some(rest) = args {
                                    let msg_parts: Vec<&str> = rest.splitn(2, ' ').collect();
                                    if msg_parts.len() < 2 {
                                        println!("usage: /msg <name> <message>");
                                        continue;
                                    }
                                    Message::SendPrivateMessage {
                                        to: msg_parts[0].to_string(),
                                        text: msg_parts[1].to_string(),
                                    }
                                } else {
                                    println!("usage: /msg <name> <message>");
                                    continue;
                                }
                            }

                            "/help" | "/h" => {
                                println!("Commands:");
                                println!("  /help, /h           - show this help");
                                println!("  /users              - list online users");
                                println!("  /join <room>        - join a room");
                                println!("  /msg <user> <text>  - send private message");
                                println!("  /quit, /q           - disconnect");
                                println!();
                                println!("To send a message to the room, just type and press enter.");
                                continue;
                            }

                            _ => {
                                eprintln!("[!] unknown command: {}", command);
                                continue;
                            }
                        }
                    } else {
                        Message::SendRoomMessage { text }
                    };

                    // encode and send
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

    println!("[*] disconnected");
    Ok(())
}
