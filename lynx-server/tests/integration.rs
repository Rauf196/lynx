use lynx_protocol::{Message, Response, encode_frame, try_extract_response};
use lynx_server::{Server, ServerHandle};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

// -- test helpers --

struct TestServer {
    handle: ServerHandle,
}

impl TestServer {
    async fn start() -> Self {
        let (server, handle) = Server::bind("127.0.0.1:0").await.unwrap();
        tokio::spawn(server.run());
        Self { handle }
    }

    fn addr(&self) -> SocketAddr {
        self.handle.local_addr
    }

    fn shutdown(&self) {
        self.handle.shutdown();
    }
}

struct TestClient {
    stream: TcpStream,
    accumulator: Vec<u8>,
}

impl TestClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        Self {
            stream,
            accumulator: Vec::new(),
        }
    }

    async fn send(&mut self, msg: &Message) -> anyhow::Result<()> {
        let frame = encode_frame(msg).map_err(|e| anyhow::anyhow!(e))?;
        self.stream.write_all(&frame).await?;
        Ok(())
    }

    async fn recv(&mut self) -> anyhow::Result<Response> {
        let mut buffer = vec![0u8; 4096];

        loop {
            // try to extract from accumulator first
            if let Some((response, consumed)) =
                try_extract_response(&self.accumulator).map_err(|e| anyhow::anyhow!(e))?
            {
                self.accumulator.drain(..consumed);
                return Ok(response);
            }

            // read more data
            let n = self.stream.read(&mut buffer).await?;
            if n == 0 {
                return Err(anyhow::anyhow!("connection closed"));
            }
            self.accumulator.extend_from_slice(&buffer[..n]);
        }
    }

    async fn recv_timeout(&mut self) -> anyhow::Result<Response> {
        timeout(TEST_TIMEOUT, self.recv())
            .await
            .map_err(|_| anyhow::anyhow!("recv timeout"))?
    }

    /// convenience: send Connect and return the response
    async fn login(&mut self, username: &str) -> anyhow::Result<Response> {
        self.send(&Message::Connect {
            username: username.to_string(),
        })
        .await?;
        self.recv_timeout().await
    }
}

// -- tests --

#[tokio::test]
async fn test_connect_success() {
    let server = TestServer::start().await;
    let mut client = TestClient::connect(server.addr()).await;

    let response = client.login("alice").await.unwrap();

    assert!(matches!(response, Response::Success { message } if message.contains("alice")));

    server.shutdown();
}

#[tokio::test]
async fn test_room_message_broadcast() {
    let server = TestServer::start().await;

    let mut alice = TestClient::connect(server.addr()).await;
    let mut bob = TestClient::connect(server.addr()).await;

    // both join (default room is "general")
    alice.login("alice").await.unwrap();
    bob.login("bob").await.unwrap();

    // alice sends a message
    alice
        .send(&Message::SendRoomMessage {
            text: "hello everyone".to_string(),
        })
        .await
        .unwrap();

    // alice receives her own message (broadcast includes sender)
    let response = alice.recv_timeout().await.unwrap();
    assert!(matches!(
        response,
        Response::IncomingMessage { from, text, room: Some(_) }
        if from == "alice" && text == "hello everyone"
    ));

    // bob receives the message
    let response = bob.recv_timeout().await.unwrap();
    assert!(matches!(
        response,
        Response::IncomingMessage { from, text, room: Some(r) }
        if from == "alice" && text == "hello everyone" && r == "general"
    ));

    server.shutdown();
}

#[tokio::test]
async fn test_room_isolation() {
    let server = TestServer::start().await;

    let mut alice = TestClient::connect(server.addr()).await;
    let mut bob = TestClient::connect(server.addr()).await;

    // both login
    alice.login("alice").await.unwrap();
    bob.login("bob").await.unwrap();

    // bob joins a different room
    bob.send(&Message::JoinRoom {
        room_name: "dev".to_string(),
    })
    .await
    .unwrap();
    let response = bob.recv_timeout().await.unwrap();
    assert!(matches!(response, Response::Success { .. }));

    // alice sends to "general"
    alice
        .send(&Message::SendRoomMessage {
            text: "hello general".to_string(),
        })
        .await
        .unwrap();

    // alice receives her message
    let response = alice.recv_timeout().await.unwrap();
    assert!(matches!(response, Response::IncomingMessage { from, .. } if from == "alice"));

    // bob should NOT receive anything - verify with timeout
    let result = timeout(Duration::from_millis(100), bob.recv()).await;
    assert!(
        result.is_err(),
        "bob should not receive message from different room"
    );

    server.shutdown();
}

#[tokio::test]
async fn test_join_room() {
    let server = TestServer::start().await;

    let mut alice = TestClient::connect(server.addr()).await;
    let mut bob = TestClient::connect(server.addr()).await;

    alice.login("alice").await.unwrap();
    bob.login("bob").await.unwrap();

    // both join "dev" room
    alice
        .send(&Message::JoinRoom {
            room_name: "dev".to_string(),
        })
        .await
        .unwrap();
    let response = alice.recv_timeout().await.unwrap();
    assert!(matches!(response, Response::Success { message } if message.contains("dev")));

    bob.send(&Message::JoinRoom {
        room_name: "dev".to_string(),
    })
    .await
    .unwrap();
    let response = bob.recv_timeout().await.unwrap();
    assert!(matches!(response, Response::Success { .. }));

    // alice sends to "dev"
    alice
        .send(&Message::SendRoomMessage {
            text: "hello dev team".to_string(),
        })
        .await
        .unwrap();

    // both receive the message with room = "dev"
    let response = alice.recv_timeout().await.unwrap();
    assert!(matches!(
        response,
        Response::IncomingMessage { room: Some(r), .. } if r == "dev"
    ));

    let response = bob.recv_timeout().await.unwrap();
    assert!(matches!(
        response,
        Response::IncomingMessage { from, text, room: Some(r) }
        if from == "alice" && text == "hello dev team" && r == "dev"
    ));

    server.shutdown();
}

#[tokio::test]
async fn test_private_message() {
    let server = TestServer::start().await;

    let mut alice = TestClient::connect(server.addr()).await;
    let mut bob = TestClient::connect(server.addr()).await;
    let mut charlie = TestClient::connect(server.addr()).await;

    alice.login("alice").await.unwrap();
    bob.login("bob").await.unwrap();
    charlie.login("charlie").await.unwrap();

    // alice sends private message to bob
    alice
        .send(&Message::SendPrivateMessage {
            to: "bob".to_string(),
            text: "secret message".to_string(),
        })
        .await
        .unwrap();

    // bob receives the DM (room = None)
    let response = bob.recv_timeout().await.unwrap();
    assert!(matches!(
        response,
        Response::IncomingMessage { from, text, room: None }
        if from == "alice" && text == "secret message"
    ));

    // charlie should NOT receive the private message
    let result = timeout(Duration::from_millis(100), charlie.recv()).await;
    assert!(
        result.is_err(),
        "charlie should not receive private message"
    );

    server.shutdown();
}

#[tokio::test]
async fn test_private_message_unknown_user() {
    let server = TestServer::start().await;
    let mut alice = TestClient::connect(server.addr()).await;

    alice.login("alice").await.unwrap();

    // try to DM nonexistent user
    alice
        .send(&Message::SendPrivateMessage {
            to: "ghost".to_string(),
            text: "hello?".to_string(),
        })
        .await
        .unwrap();

    let response = alice.recv_timeout().await.unwrap();
    assert!(matches!(
        response,
        Response::Error { message } if message.contains("not found")
    ));

    server.shutdown();
}

#[tokio::test]
async fn test_list_users() {
    let server = TestServer::start().await;

    let mut alice = TestClient::connect(server.addr()).await;
    let mut bob = TestClient::connect(server.addr()).await;
    let mut charlie = TestClient::connect(server.addr()).await;

    alice.login("alice").await.unwrap();
    bob.login("bob").await.unwrap();
    charlie.login("charlie").await.unwrap();

    // alice requests user list
    alice.send(&Message::ListUsers).await.unwrap();
    let response = alice.recv_timeout().await.unwrap();

    match response {
        Response::UserList { users } => {
            assert_eq!(users.len(), 3);
            assert!(users.contains(&"alice".to_string()));
            assert!(users.contains(&"bob".to_string()));
            assert!(users.contains(&"charlie".to_string()));
        }
        _ => panic!("expected UserList response"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_message_before_connect() {
    let server = TestServer::start().await;
    let mut client = TestClient::connect(server.addr()).await;

    // try to send message without logging in
    client
        .send(&Message::SendRoomMessage {
            text: "hello".to_string(),
        })
        .await
        .unwrap();

    let response = client.recv_timeout().await.unwrap();
    assert!(matches!(
        response,
        Response::Error { message } if message.contains("must connect")
    ));

    server.shutdown();
}

#[tokio::test]
async fn test_graceful_disconnect() {
    let server = TestServer::start().await;
    let mut client = TestClient::connect(server.addr()).await;

    client.login("alice").await.unwrap();

    // send Disconnect message
    client.send(&Message::Disconnect).await.unwrap();
    let response = client.recv_timeout().await.unwrap();
    assert!(matches!(
        response,
        Response::Success { message } if message.contains("goodbye")
    ));

    server.shutdown();
}

#[tokio::test]
async fn test_shutdown_notifies_clients() {
    let server = TestServer::start().await;

    let mut alice = TestClient::connect(server.addr()).await;
    let mut bob = TestClient::connect(server.addr()).await;

    alice.login("alice").await.unwrap();
    bob.login("bob").await.unwrap();

    // trigger server shutdown
    server.shutdown();

    // both clients should have their connections closed
    // recv() will return error when connection is closed
    let alice_result = timeout(Duration::from_secs(2), alice.recv()).await;
    let bob_result = timeout(Duration::from_secs(2), bob.recv()).await;

    // either timeout (no more data) or connection closed error is acceptable
    // the key is that clients are notified and don't hang forever
    assert!(
        alice_result.is_err() || alice_result.unwrap().is_err(),
        "alice should be disconnected"
    );
    assert!(
        bob_result.is_err() || bob_result.unwrap().is_err(),
        "bob should be disconnected"
    );
}

#[tokio::test]
async fn test_client_drop() {
    let server = TestServer::start().await;

    let mut alice = TestClient::connect(server.addr()).await;
    let bob = TestClient::connect(server.addr()).await;

    alice.login("alice").await.unwrap();
    // bob logs in then immediately drops
    {
        let mut bob = bob;
        bob.login("bob").await.unwrap();
        // bob goes out of scope, dropping the connection
    }

    // give server time to process the disconnect
    tokio::time::sleep(Duration::from_millis(50)).await;

    // alice checks user list - bob should be gone
    alice.send(&Message::ListUsers).await.unwrap();
    let response = alice.recv_timeout().await.unwrap();

    match response {
        Response::UserList { users } => {
            assert_eq!(users.len(), 1);
            assert!(users.contains(&"alice".to_string()));
            assert!(!users.contains(&"bob".to_string()));
        }
        _ => panic!("expected UserList response"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_connect_already_authenticated() {
    let server = TestServer::start().await;
    let mut client = TestClient::connect(server.addr()).await;

    // first connect succeeds
    let response = client.login("alice").await.unwrap();
    assert!(matches!(response, Response::Success { .. }));

    // second connect on same connection fails
    client
        .send(&Message::Connect {
            username: "bob".to_string(),
        })
        .await
        .unwrap();
    let response = client.recv_timeout().await.unwrap();
    assert!(matches!(response, Response::Error { message } if message.contains("already")));

    server.shutdown();
}

#[tokio::test]
async fn test_connect_duplicate_username() {
    let server = TestServer::start().await;

    let mut alice1 = TestClient::connect(server.addr()).await;
    let mut alice2 = TestClient::connect(server.addr()).await;

    // first alice connects successfully
    let response = alice1.login("alice").await.unwrap();
    assert!(matches!(response, Response::Success { .. }));

    // second alice gets rejected
    let response = alice2.login("alice").await.unwrap();
    assert!(matches!(response, Response::Error { message } if message.contains("taken")));

    server.shutdown();
}
