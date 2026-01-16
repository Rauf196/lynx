use criterion::{Criterion, black_box, criterion_group, criterion_main};
use lynx_protocol::{
    Message, Response,
    encode_frame, encode_response,
    decode_frame, decode_response,
    try_extract_frame, try_extract_response,
};

// test fixtures
const USERNAME: &str = "benchmark1";
const ROOM_NAME: &str = "general-chat-1";
const CHAT_TEXT: &str = "The quick brown fox jumps over the lazy dog nearby.";

fn bench_encode_messages(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode/message");

    // Connect - small payload
    let msg_connect = Message::Connect {
        username: USERNAME.to_string(),
    };
    group.bench_function("connect", |b| {
        b.iter(|| encode_frame(black_box(&msg_connect)))
    });

    // SendRoomMessage - medium payload (most common)
    let msg_room = Message::SendRoomMessage {
        text: CHAT_TEXT.to_string(),
    };
    group.bench_function("send_room", |b| {
        b.iter(|| encode_frame(black_box(&msg_room)))
    });

    // JoinRoom - small payload
    let msg_join = Message::JoinRoom {
        room_name: ROOM_NAME.to_string(),
    };
    group.bench_function("join_room", |b| {
        b.iter(|| encode_frame(black_box(&msg_join)))
    });

    // SendPrivateMessage - medium payload
    let msg_private = Message::SendPrivateMessage {
        to: USERNAME.to_string(),
        text: CHAT_TEXT.to_string(),
    };
    group.bench_function("send_private", |b| {
        b.iter(|| encode_frame(black_box(&msg_private)))
    });

    // ListUsers - minimal payload
    let msg_list = Message::ListUsers;
    group.bench_function("list_users", |b| {
        b.iter(|| encode_frame(black_box(&msg_list)))
    });

    // Disconnect - minimal payload
    let msg_disconnect = Message::Disconnect;
    group.bench_function("disconnect", |b| {
        b.iter(|| encode_frame(black_box(&msg_disconnect)))
    });

    group.finish();
}

// baseline: raw bincode vs our length-prefixed framing
fn bench_encode_baseline(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode/baseline");

    let msg = Message::SendRoomMessage {
        text: CHAT_TEXT.to_string(),
    };

    // raw bincode (no length prefix)
    group.bench_function("bincode_only", |b| {
        b.iter(|| bincode::serialize(black_box(&msg)))
    });

    // our framing (bincode + length prefix)
    group.bench_function("with_framing", |b| {
        b.iter(|| encode_frame(black_box(&msg)))
    });

    group.finish();
}

fn bench_encode_responses(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode/response");

    // Success - small payload
    let resp_success = Response::Success {
        message: "welcome, benchmark1!".to_string(),
    };
    group.bench_function("success", |b| {
        b.iter(|| encode_response(black_box(&resp_success)))
    });

    // Error - small payload
    let resp_error = Response::Error {
        message: "username already taken".to_string(),
    };
    group.bench_function("error", |b| {
        b.iter(|| encode_response(black_box(&resp_error)))
    });

    // IncomingMessage - medium payload (most common server->client)
    let resp_incoming = Response::IncomingMessage {
        from: USERNAME.to_string(),
        text: CHAT_TEXT.to_string(),
        room: Some(ROOM_NAME.to_string()),
    };
    group.bench_function("incoming_message", |b| {
        b.iter(|| encode_response(black_box(&resp_incoming)))
    });

    // SystemNotification - small payload
    let resp_system = Response::SystemNotification {
        text: "Server is shutting down".to_string(),
    };
    group.bench_function("system_notification", |b| {
        b.iter(|| encode_response(black_box(&resp_system)))
    });

    group.finish();
}

// userlist scaling (separate group)
fn bench_encode_userlist_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode/response/user_list");

    for count in [10, 100, 1000] {
        let users: Vec<String> = (0..count)
            .map(|i| format!("user_{:04}", i))
            .collect();
        let resp = Response::UserList { users };

        group.bench_function(format!("{}_users", count), |b| {
            b.iter(|| encode_response(black_box(&resp)))
        });
    }

    group.finish();
}

fn bench_decode_messages(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode/message");

    // pre-encode all messages
    let frame_connect = encode_frame(&Message::Connect {
        username: USERNAME.to_string(),
    }).unwrap();

    let frame_room = encode_frame(&Message::SendRoomMessage {
        text: CHAT_TEXT.to_string(),
    }).unwrap();

    let frame_join = encode_frame(&Message::JoinRoom {
        room_name: ROOM_NAME.to_string(),
    }).unwrap();

    let frame_private = encode_frame(&Message::SendPrivateMessage {
        to: USERNAME.to_string(),
        text: CHAT_TEXT.to_string(),
    }).unwrap();

    let frame_list = encode_frame(&Message::ListUsers).unwrap();
    let frame_disconnect = encode_frame(&Message::Disconnect).unwrap();

    group.bench_function("connect", |b| {
        b.iter(|| decode_frame(black_box(&frame_connect)))
    });

    group.bench_function("send_room", |b| {
        b.iter(|| decode_frame(black_box(&frame_room)))
    });

    group.bench_function("join_room", |b| {
        b.iter(|| decode_frame(black_box(&frame_join)))
    });

    group.bench_function("send_private", |b| {
        b.iter(|| decode_frame(black_box(&frame_private)))
    });

    group.bench_function("list_users", |b| {
        b.iter(|| decode_frame(black_box(&frame_list)))
    });

    group.bench_function("disconnect", |b| {
        b.iter(|| decode_frame(black_box(&frame_disconnect)))
    });

    group.finish();
}

fn bench_decode_responses(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode/response");

    let frame_success = encode_response(&Response::Success {
        message: "welcome, benchmark1!".to_string(),
    }).unwrap();

    let frame_incoming = encode_response(&Response::IncomingMessage {
        from: USERNAME.to_string(),
        text: CHAT_TEXT.to_string(),
        room: Some(ROOM_NAME.to_string()),
    }).unwrap();

    // UserList at different sizes
    let users_100: Vec<String> = (0..100).map(|i| format!("user_{:04}", i)).collect();
    let frame_userlist = encode_response(&Response::UserList {
        users: users_100,
    }).unwrap();

    group.bench_function("success", |b| {
        b.iter(|| decode_response(black_box(&frame_success)))
    });

    group.bench_function("incoming_message", |b| {
        b.iter(|| decode_response(black_box(&frame_incoming)))
    });

    group.bench_function("user_list_100", |b| {
        b.iter(|| decode_response(black_box(&frame_userlist)))
    });

    group.finish();
}

// accumulator pattern - what the server actually uses
fn bench_try_extract(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode/try_extract");

    let frame_room = encode_frame(&Message::SendRoomMessage {
        text: CHAT_TEXT.to_string(),
    }).unwrap();

    let frame_incoming = encode_response(&Response::IncomingMessage {
        from: USERNAME.to_string(),
        text: CHAT_TEXT.to_string(),
        room: Some(ROOM_NAME.to_string()),
    }).unwrap();

    group.bench_function("message", |b| {
        b.iter(|| try_extract_frame(black_box(&frame_room)))
    });

    group.bench_function("response", |b| {
        b.iter(|| try_extract_response(black_box(&frame_incoming)))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_encode_messages,
    bench_encode_baseline,
    bench_encode_responses,
    bench_encode_userlist_scaling,
    bench_decode_messages,
    bench_decode_responses,
    bench_try_extract,
);
criterion_main!(benches);
