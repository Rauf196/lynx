use criterion::{Criterion, black_box, criterion_group, criterion_main};
use dashmap::DashMap;
use lynx_protocol::Response;
use std::sync::Arc;
use tokio::sync::mpsc;

// mirrors server's ClientInfo (kept private there)
struct ClientInfo {
    sender: mpsc::Sender<Response>,
    room: String,
}

type Clients = Arc<DashMap<String, ClientInfo>>;

// setup helper: create N clients in specified rooms
fn setup_clients(total: usize, room_distribution: &[(String, usize)]) -> (Clients, Vec<mpsc::Receiver<Response>>) {
    let clients: Clients = Arc::new(DashMap::new());
    let mut receivers = Vec::with_capacity(total);

    let mut idx = 0;
    for (room, count) in room_distribution {
        for _ in 0..*count {
            let (tx, rx) = mpsc::channel(100);
            let username = format!("user_{:05}", idx);
            clients.insert(username, ClientInfo {
                sender: tx,
                room: room.clone(),
            });
            receivers.push(rx);
            idx += 1;
        }
    }

    (clients, receivers)
}

// simulates server's broadcast loop
fn broadcast_to_room(clients: &Clients, sender_room: &str, sender_username: &str, text: &str) {
    for entry in clients.iter() {
        if entry.room == sender_room {
            let msg = Response::IncomingMessage {
                from: sender_username.to_string(),
                text: text.to_string(),
                room: Some(sender_room.to_string()),
            };
            let _ = entry.sender.try_send(msg);
        }
    }
}

fn bench_broadcast_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("broadcast/scaling");

    for count in [10, 100, 1000, 10000] {
        let (clients, _receivers) = setup_clients(count, &[("general".to_string(), count)]);

        group.bench_function(format!("{}_clients", count), |b| {
            b.iter(|| {
                broadcast_to_room(
                    black_box(&clients),
                    "general",
                    "user_00000",
                    "test broadcast message",
                )
            })
        });
    }

    group.finish();
}

fn bench_room_filtering(c: &mut Criterion) {
    let mut group = c.benchmark_group("broadcast/room_filter");
    let total = 1000;

    // 10% in target room (100 clients), 90% elsewhere
    let (clients_10pct, _) = setup_clients(total, &[
        ("target".to_string(), 100),
        ("other".to_string(), 900),
    ]);
    group.bench_function("10pct_match", |b| {
        b.iter(|| {
            broadcast_to_room(black_box(&clients_10pct), "target", "user_00000", "msg")
        })
    });

    // 50% in target room
    let (clients_50pct, _) = setup_clients(total, &[
        ("target".to_string(), 500),
        ("other".to_string(), 500),
    ]);
    group.bench_function("50pct_match", |b| {
        b.iter(|| {
            broadcast_to_room(black_box(&clients_50pct), "target", "user_00000", "msg")
        })
    });

    // 90% in target room
    let (clients_90pct, _) = setup_clients(total, &[
        ("target".to_string(), 900),
        ("other".to_string(), 100),
    ]);
    group.bench_function("90pct_match", |b| {
        b.iter(|| {
            broadcast_to_room(black_box(&clients_90pct), "target", "user_00000", "msg")
        })
    });

    group.finish();
}

criterion_group!(benches, bench_broadcast_scaling, bench_room_filtering);
criterion_main!(benches);
