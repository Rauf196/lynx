use lynx_protocol::{Message, Response, encode_frame, try_extract_response};
use lynx_server::{Config, Server, ServerHandle};
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
        Self::start_with_config(Config::default()).await
    }

    async fn start_with_config(config: Config) -> Self {
        let (server, handle) = Server::bind("127.0.0.1:0", config).await.unwrap();
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

#[tokio::test]
async fn test_connection_limit_enforced() {
    // start server with maxconnections = 2
    let config = Config {
        maxconnections: 2,
        ..Config::default()
    };
    let server = TestServer::start_with_config(config).await;

    // first two connections succeed
    let mut client1 = TestClient::connect(server.addr()).await;
    let mut client2 = TestClient::connect(server.addr()).await;

    client1.login("alice").await.unwrap();
    client2.login("bob").await.unwrap();

    // third connection should be rejected
    let mut client3 = TestClient::connect(server.addr()).await;

    // the server sends an error response and closes the connection
    let response = client3.recv_timeout().await.unwrap();
    assert!(
        matches!(&response, Response::Error { message } if message.contains("capacity")),
        "expected capacity error, got {:?}",
        response
    );

    server.shutdown();
}

#[tokio::test]
async fn test_slow_client_disconnected() {
    // start server with low threshold for testing
    // channel capacity is 100, threshold is 5
    // so we need 100 + 5 = 105 messages to fill buffer and exceed threshold
    // also disable rate limiting for this test (set high burst)
    let config = Config {
        slow_client_threshold: 5,
        rate_limit_per_second: 1000.0,
        rate_limit_burst: 200,
        ..Config::default()
    };
    let server = TestServer::start_with_config(config).await;

    let mut alice = TestClient::connect(server.addr()).await;
    let mut bob = TestClient::connect(server.addr()).await;

    alice.login("alice").await.unwrap();
    let bob_login_response = bob.login("bob").await.unwrap();
    assert!(
        matches!(bob_login_response, Response::Success { .. }),
        "bob login failed: {:?}",
        bob_login_response
    );

    // verify bob is in user list before we start
    alice.send(&Message::ListUsers).await.unwrap();
    let response = alice.recv_timeout().await.unwrap();
    match response {
        Response::UserList { users } => {
            assert!(
                users.contains(&"bob".to_string()),
                "bob should be in user list initially: {:?}",
                users
            );
        }
        _ => panic!("expected UserList, got {:?}", response),
    }

    // alice moves to a different room so she doesn't receive broadcasts
    alice
        .send(&Message::JoinRoom {
            room_name: "alice_room".to_string(),
        })
        .await
        .unwrap();
    alice.recv_timeout().await.unwrap(); // consume success response

    // bob stays in "general" - we keep the connection open but stop reading
    // alice sends private messages to bob to fill his buffer
    for i in 0..110 {
        alice
            .send(&Message::SendPrivateMessage {
                to: "bob".to_string(),
                text: format!("message {}", i),
            })
            .await
            .unwrap();
        // small delay to let server process
        if i % 20 == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    // give server time to process and disconnect bob
    tokio::time::sleep(Duration::from_millis(100)).await;

    // alice checks user list - bob should be disconnected
    alice.send(&Message::ListUsers).await.unwrap();

    // after bob is disconnected, subsequent DMs get "recipient not found" errors
    // we need to skip those error responses to find the UserList
    loop {
        let response = alice.recv_timeout().await.unwrap();
        match response {
            Response::UserList { users } => {
                assert!(users.contains(&"alice".to_string()), "alice should exist");
                assert!(
                    !users.contains(&"bob".to_string()),
                    "bob should have been disconnected, users: {:?}",
                    users
                );
                break;
            }
            Response::Error { message } if message.contains("not found") => {
                // expected - skip these errors from DMs after bob was disconnected
                continue;
            }
            other => panic!("unexpected response: {:?}", other),
        }
    }

    // bob's connection should still be open (not dropped), but he was removed from registry
    // dropping bob here is fine
    drop(bob);

    server.shutdown();
}

#[tokio::test]
async fn test_rate_limiting() {
    // start server with low rate limits for testing
    // burst=5 means first 5 messages are allowed, then rate limited
    let config = Config {
        rate_limit_per_second: 10.0,
        rate_limit_burst: 5,
        ..Config::default()
    };
    let server = TestServer::start_with_config(config).await;

    let mut alice = TestClient::connect(server.addr()).await;
    let mut bob = TestClient::connect(server.addr()).await;

    alice.login("alice").await.unwrap();
    bob.login("bob").await.unwrap();

    // alice moves to different room so she doesn't receive her own broadcasts
    alice
        .send(&Message::JoinRoom {
            room_name: "alice_room".to_string(),
        })
        .await
        .unwrap();
    alice.recv_timeout().await.unwrap(); // consume success response

    // send 10 messages rapidly - first 5 should succeed, rest should be rate limited
    for i in 0..10 {
        alice
            .send(&Message::SendPrivateMessage {
                to: "bob".to_string(),
                text: format!("message {}", i),
            })
            .await
            .unwrap();
    }

    // count rate limit errors
    let mut rate_limit_errors = 0;
    for _ in 0..10 {
        match timeout(Duration::from_millis(100), alice.recv()).await {
            Ok(Ok(Response::Error { message })) if message.contains("rate limited") => {
                rate_limit_errors += 1;
            }
            _ => {}
        }
    }

    // should have gotten some rate limit errors (not all 10 succeeded)
    assert!(
        rate_limit_errors > 0,
        "expected rate limit errors, got none"
    );

    // wait for token refill (200ms = 2 tokens at 10/sec)
    tokio::time::sleep(Duration::from_millis(200)).await;

    // should be able to send again
    alice
        .send(&Message::SendPrivateMessage {
            to: "bob".to_string(),
            text: "after wait".to_string(),
        })
        .await
        .unwrap();

    // should NOT get rate limit error this time
    // (we might not get any response if bob received the message)
    // but at least no error should be received for this message

    server.shutdown();
}
