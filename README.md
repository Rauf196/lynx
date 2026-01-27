<p align="center">
  <img src="docs/logo/lynx-logo-v2-nobg.png" alt="Lynx Logo" width="200">
</p>

<h1 align="center">Lynx</h1>

<p align="center">
  <strong>High-performance TCP chat server in Rust</strong>
</p>

<p align="center">
  <a href="#features">Features</a> •
  <a href="#performance">Performance</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#architecture">Architecture</a> •
  <a href="#configuration">Configuration</a>
</p>

---

## What is Lynx?

Lynx is an async TCP chat server built with Rust and Tokio, designed for high concurrency and low latency. It uses a custom binary protocol for efficient message framing and supports room-based messaging, private messages, and real-time user presence.

Built for learning production-grade async Rust patterns.

## Performance

Tested on a single machine (Linux, Intel):

| Clients | Connected | Throughput | Notes |
|---------|-----------|------------|-------|
| 10,000 | 10,000 | 33,700 msg/s | Clean run |
| 25,000 | 25,000 | 21,000 msg/s | Near port limit |
| 50,000 | 50,000 | 18,500 msg/s | Port recycling |
| 70,000 | 70,000 | 16,100 msg/s | Port recycling |

```bash
# Test command used (rooms = clients/100)
cargo run -p lynx-load --release -- -c <clients> -r <rooms> -m 50 --batch-size 50 --batch-delay-ms 50
```

**Server configuration affecting performance:**
- Per-client channel buffer: 100 messages (broadcasts use `try_send`, drops on full)
- Read buffer: 4KB per client
- Accumulator: 8KB initial capacity

**Profiling results:** Server is I/O bound (37% in kernel `sendto`), with no application-level bottlenecks. Tokio runtime overhead is ~1%. See [profiling details](docs/profiling/RESULTS.md).

## Benchmarks

Micro-benchmarks using Criterion:

| Operation | Time | Throughput |
|-----------|------|------------|
| Encode message | 50-177 ns | 5-20M/s |
| Decode message | 12-135 ns | 7-80M/s |
| Broadcast to 1K clients | 89 us | 11K/s |
| Broadcast to 10K clients | 1.1 ms | 900/s |

```bash
# Run benchmarks
cargo bench --workspace
```

See [docs/benchmark/BENCHMARKS.md](docs/benchmark/BENCHMARKS.md) for detailed results and analysis.

## Features

- **Async I/O** - Built on Tokio for efficient handling of thousands of concurrent connections
- **Binary Protocol** - Length-prefixed frames with bincode serialization
- **Room-based Chat** - Join rooms, broadcast messages to room members
- **Private Messaging** - Direct messages between users
- **Graceful Shutdown** - Clean disconnect notifications to all clients
- **Prometheus Metrics** - Active connections, message throughput, latency histograms
- **Layered Configuration** - Defaults → config.toml → environment variables
- **Connection Limits** - Configurable max connections with graceful rejection
- **Rate Limiting** - Per-user token bucket rate limiting to prevent spam
- **Backpressure Handling** - Automatic disconnection of slow clients
- **Health Endpoints** - `/health` and `/ready` endpoints for orchestration

## Quick Start

### Prerequisites

- Rust 1.75+ (2024 edition)
- Linux recommended for load testing

### Run the Server

```bash
# Clone and build
git clone https://github.com/Rauf196/lynx.git
cd lynx
cargo build --release

# Start server (default: 127.0.0.1:6006)
cargo run -p lynx-server --release

# In another terminal, run the example client
cargo run -p lynx-server --release --example chat_client
```

### Client Commands

```
/join <room>        Join a chat room
/msg <user> <text>  Send private message
/users              List online users
/quit               Disconnect
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        lynx-server                          │
│  ┌─────────────┐  ┌─────────────┐  ┌───────────────────────┐│
│  │ TcpListener │→ │handle_client│→ │ process_message       ││
│  │ (accept)    │  │ (per client)│  │ (Connect, Send, etc)  ││
│  └─────────────┘  └─────────────┘  └───────────────────────┘│
│                          ↓                                  │
│  ┌─────────────────────────────────────────────────────────┐│
│  │              DashMap<username, ClientInfo>              ││
│  │                (concurrent client registry)             ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘

┌─────────────────┐     ┌─────────────────┐
│  lynx-protocol  │     │    lynx-load    │
│  - Message enum │     │  - Load tester  │
│  - Response enum│     │  - Configurable │
│  - encode/decode│     │    traffic mix  │
└─────────────────┘     └─────────────────┘
```

**Per-client architecture:**
- Read task: decode frames, process messages
- Write task: encode responses, send to socket
- MPSC channel between them (backpressure via `try_send`)

## Protocol

Length-prefixed binary frames using bincode:

```
┌──────────┬─────────────────────┐
│ len (4B) │ payload (bincode)   │
└──────────┴─────────────────────┘
```

**Messages (client → server):**
- `Connect { username }` - Register with username
- `SendRoomMessage { text }` - Broadcast to current room
- `JoinRoom { room_name }` - Switch rooms
- `SendPrivateMessage { to, text }` - Direct message
- `ListUsers` - Get online users
- `Disconnect` - Clean disconnect

**Responses (server → client):**
- `Success { message }` - Operation succeeded
- `Error { message }` - Operation failed
- `IncomingMessage { from, text, room }` - Chat message
- `UserList { users }` - List of usernames
- `SystemNotification { text }` - Server announcements

## Configuration

Configuration is layered: defaults → `config.toml` → environment variables.

```toml
# config.toml
host = "127.0.0.1"
port = 6006
log_level = "info"
max_connections = 100000
metrics_host = "127.0.0.1"
metrics_port = 9090

# resource management
slow_client_threshold = 50      # dropped messages before disconnect
rate_limit_per_second = 10.0    # messages per second per user
rate_limit_burst = 20           # burst capacity
```

Environment variables (override config file):
```bash
LYNX_HOST=0.0.0.0
LYNX_PORT=6006
LYNX_LOGLEVEL=debug
LYNX_MAXCONNECTIONS=50000
LYNX_METRICSHOST=0.0.0.0
LYNX_METRICSPORT=9090

# resource management
LYNX_SLOW_CLIENT_THRESHOLD=50
LYNX_RATE_LIMIT_PER_SECOND=10.0
LYNX_RATE_LIMIT_BURST=20
```

## Metrics & Health

Prometheus metrics and health endpoints exposed at `http://localhost:9090`:

**Health Endpoints:**
| Endpoint | Description |
|----------|-------------|
| `GET /health` | Liveness check - always returns 200 OK |
| `GET /ready` | Readiness check - 200 if accepting, 503 if at capacity or shutting down |
| `GET /metrics` | Prometheus metrics |

**Metrics:**
| Metric | Type | Description |
|--------|------|-------------|
| `lynx_connections_active` | Gauge | Current active connections |
| `lynx_connections_total` | Counter | Total connections since start |
| `lynx_connections_rejected_total` | Counter | Connections rejected at capacity |
| `lynx_messages_processed_total` | Counter | Messages by type |
| `lynx_messages_dropped_total` | Counter | Messages dropped to slow clients |
| `lynx_message_processing_duration_seconds` | Histogram | Processing latency |
| `lynx_errors_total` | Counter | Errors by type |
| `lynx_clients_slow_disconnected_total` | Counter | Slow clients disconnected |
| `lynx_rate_limited_total` | Counter | Messages rejected by rate limiter |

## Load Testing

```bash
# Basic test: 100 clients, 10 rooms
cargo run -p lynx-load --release

# High load: 10K clients across 100 rooms
cargo run -p lynx-load --release -- -c 10000 -r 100 -m 10

# Stress test: 50K clients (requires OS tuning)
ulimit -n 65536  # Run in BOTH terminals
cargo run -p lynx-load --release -- -c 50000 -r 100 -m 100 \
  --batch-size 50 --batch-delay-ms 20
```

See [OS Tuning](#os-tuning) for high-load testing requirements.

## OS Tuning

For load tests above 1K clients:

```bash
# Increase file descriptor limit (run in BOTH server and client terminals)
ulimit -n 65536

# Check current limits
ulimit -n        # Soft limit
ulimit -Hn       # Hard limit

# Verify TCP settings
cat /proc/sys/net/core/somaxconn           # Should be 65535
cat /proc/sys/net/ipv4/ip_local_port_range # Ephemeral ports
```

## Project Structure

```
lynx/
├── lynx-server/          # Main server binary
│   ├── src/
│   │   ├── main.rs       # Entry point
│   │   ├── server.rs     # Connection handling
│   │   ├── config.rs     # Configuration
│   │   ├── metrics.rs    # Prometheus + health endpoints (axum)
│   │   └── rate_limiter.rs # Token bucket rate limiter
│   ├── tests/
│   │   └── integration.rs # 16 integration tests
│   ├── benches/
│   │   └── broadcast.rs  # Broadcast scaling benchmarks
│   └── examples/
│       └── chat_client.rs # Interactive CLI client
├── lynx-protocol/        # Shared protocol library
│   ├── src/lib.rs        # Message types, encoding
│   └── benches/
│       └── protocol.rs   # Encode/decode benchmarks
└── lynx-load/            # Load testing tool
    └── src/main.rs       # Configurable load generator
```

## Testing

```bash
# Run all tests (27 tests, ~80% coverage)
cargo test --workspace

# Run with output
cargo test --workspace -- --nocapture

# Run benchmarks
cargo bench --workspace

# Run specific benchmark group
cargo bench -p lynx-protocol -- "encode/"
cargo bench -p lynx-server -- "broadcast/"
```

## Roadmap

- [x] Phase 1: Async foundations, protocol design
- [x] Phase 2: Core server, room-based chat
- [x] Phase 3: Metrics, configuration, graceful shutdown
- [x] Phase 4: Integration tests, load tests, profiling, benchmarking
- [x] Phase 5: Resource management (connection limits, rate limiting, health endpoints)
- [ ] Phase 6: CI/CD, Docker, documentation polish

## License

MIT OR Apache-2.0
