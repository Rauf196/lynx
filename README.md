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

<p align="center">
  <a href="https://github.com/Rauf196/lynx/actions/workflows/ci.yml">
    <img src="https://github.com/Rauf196/lynx/actions/workflows/ci.yml/badge.svg" alt="CI">
  </a>
</p>

---

## What is Lynx?

Lynx is an async TCP chat server built with Rust and Tokio, designed for high concurrency and low latency. It uses a custom binary protocol for efficient message framing and supports room-based messaging, private messages, and real-time user presence.

Built for learning production-grade async Rust patterns.

## Performance

Server and load tester on same machine. Latency measured via Prometheus/Grafana (docker-compose).

### Laptop: ThinkPad X1 Carbon 6th Gen (i7-8550U, 16GB RAM)

| Ramp Speed | Spawn Rate | Spawned | Peak Concurrent | Throughput | p50 | p95 | p99 | Result |
|------------|------------|---------|-----------------|------------|-----|-----|-----|--------|
| Fast | 1000/sec | 5,000 | 5,000 | 6,915 msg/s | 166µs | 1.72ms | 9.70ms | Clean |
| Fast | 1000/sec | 10,000 | 10,000 | 10,204 msg/s | 169µs | 1.38ms | 6.82ms | Clean |
| Fast | 1000/sec | 15,000 | 14,000 | 11,794 msg/s | 188µs | 1.65ms | 100ms | Clean |
| Fast | 1000/sec | 19,000 | 18,000 | 16,588 msg/s | - | - | - | Clean |
| Fast | 1000/sec | 22,000 | 18,750 | - | - | - | - | Crashed |
| Medium | 250/sec | 25,000 | 22,500 | 10,628 msg/s | 330µs | 2.48ms | 100ms | Clean |
| Slow | 125/sec | 25,000 | 24,000 | - | 440µs | 2.94ms | 8.12ms | Clean |

**Limits:**

| Ramp Speed | Spawn Rate | Max Stable | Crash Point | Limiting Factor |
|------------|------------|------------|-------------|-----------------|
| Fast | 1000/sec | ~18k | ~19k | Connection burst pressure |
| Medium | 250/sec | ~23k | ~24k | Memory + burst |
| Slow | 125/sec | ~24k | ~25k | CPU saturation (99%) |

### PC: Windows WSL2 (Ryzen 5 7600X @ 5.3GHz, 24GB RAM to WSL)

**Native (no docker-compose, no latency data):**

| Ramp Speed | Spawn Rate | Spawned | Peak Concurrent | Throughput | Result |
|------------|------------|---------|-----------------|------------|--------|
| Slow | 125/sec | 35,000 | 28,000 | 14,595 msg/s | Clean (port-limited) |
| Slow | 125/sec | 45,000 | 42,500 | 10,830 msg/s | Clean (after expanding ports) |

The 28k → 42.5k jump came from expanding the ephemeral port range:
```bash
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"
```

**With docker-compose (Prometheus + Grafana monitoring):**

| Ramp Speed | Spawn Rate | Spawned | Peak Concurrent | Throughput | p50 | p95 | p99 | Result |
|------------|------------|---------|-----------------|------------|-----|-----|-----|--------|
| Slow | 125/sec | 45,000 | 41,000 | 10,505 msg/s | 410µs | 3.5ms | 100ms | Clean |

Latency at non-peak: p50=337µs, p95=2ms, p99=5.4ms. Docker adds ~3% overhead vs native.

### Key Insights

- **Connection burst rate matters:** Slower ramp allows 33% more connections and 12x better p99 latency (8ms vs 100ms)
- **Hardware scales linearly:** 42.5k on 24GB/6-core vs 24k on 16GB/4-core
- **Know your limits:** Port range (\~28k default), RAM (\~700KB per connection at peak), and CPU all cap concurrency
- **Memory behavior:** Server retains ~7GB after 42k test (buffers not returned to OS, not a leak)

### Network Test (Cross-Machine)

Load tester on Windows PC (32GB RAM) → Server on Linux laptop over 100Mbps ethernet:

| Concurrent | Throughput | p50 | p95 | p99 | Result |
|------------|------------|-----|-----|-----|--------|
| 12,467 | 6,962 msg/s | 340µs | 1.61ms | 4.25ms | Crashed |

**Finding:** 100Mbps network became the bottleneck, not the server. Same test locally reached 18k+ concurrent.

### Throughput (Total Connections Over Time)

Short-lived connections (1 sec lifetime) to test connection handling without accumulating state:

| Total Clients | Duration | Peak In-Flight | Result |
|---------------|----------|----------------|--------|
| 70,000 | 73s | ~1k | Clean |
| 90,000+ | 158s | ~20-30k | Port exhaustion |

### Memory Behavior

| Scenario | RAM Usage | Notes |
|----------|-----------|-------|
| 70k short-lived (1s) | ~70MB | Minimal growth |
| 13k long-lived (100s) | 3GB → 14GB | Accumulated message queues |

**Key insight:** Memory scales with `concurrent clients × message rate × client lifetime`, not just connection count. Each client has a channel buffer (100 messages), and broadcast messages multiply across recipients.

### Load Test Math

Understanding how to calculate max concurrent connections:

```
Spawn rate     = batch_size / batch_delay_ms
Client lifetime = messages × send_interval_ms
Max concurrent  = spawn_rate × lifetime
```

**Example (fast ramp):**
- `--batch-size 50 --batch-delay-ms 50` → 1000 clients/sec
- `-m 60 --send-interval-ms 500` → 30 second lifetime
- Max concurrent: 1000 × 30 = **30,000** (or total clients, whichever is lower)

**Example (slow ramp):**
- `--batch-size 25 --batch-delay-ms 200` → 125 clients/sec
- `-m 200 --send-interval-ms 1000` → 200 second lifetime
- Max concurrent: 125 × 200 = **25,000**

Slow ramp reduces connection burst pressure, allowing higher peak concurrent.

### Load Test Commands

```bash
# Fast ramp - find crash threshold
cargo run -p lynx-load --release -- -c 20000 -r 200 -m 60 \
  --batch-size 50 --batch-delay-ms 50 --send-interval-ms 500 --dm-pct 0

# Slow ramp - find stable maximum
cargo run -p lynx-load --release -- -c 30000 -r 300 -m 150 \
  --batch-size 50 --batch-delay-ms 200 --send-interval-ms 1000 --dm-pct 0

# Throughput test (short-lived connections)
cargo run -p lynx-load --release -- -c 70000 -r 700 -m 10 \
  --batch-size 50 --batch-delay-ms 50 --send-interval-ms 100 --dm-pct 0
```

### Why Not 100k Concurrent?

100k simultaneous connections on a single server isn't realistic:

1. **Port exhaustion:** ~28k ephemeral ports per machine for load tester
2. **RAM:** 100k connections × ~100KB each = 10GB minimum, plus message buffers
3. **Single runtime:** All connections share one Tokio async runtime
4. **Broadcast multiplication:** 1 message to 100-client room = 100 channel sends

Production systems like Discord handle scale through horizontal sharding, message queues (pub/sub), gateway proxies, and distributed architecture. This server demonstrates solid single-process async Rust performance - the foundation that would be replicated across a cluster.

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
- **Layered Configuration** - CLI args → env vars → config.toml → defaults
- **Connection Limits** - Configurable max connections with graceful rejection
- **Rate Limiting** - Per-user token bucket rate limiting to prevent spam
- **Backpressure Handling** - Automatic disconnection of slow clients
- **Health Endpoints** - `/health` and `/ready` endpoints for orchestration

## Quick Start

### Prerequisites

- Rust 1.85+ (2024 edition)
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

### Server CLI Options

```bash
lynx-server --help                     # Show all options
lynx-server --port 8080                # Override port
lynx-server --log-level debug          # Override log level
lynx-server --config /path/to/cfg.toml # Use custom config file
```

**Priority:** CLI args → env vars → config.toml → defaults

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

### Design Decisions

| Choice | Why | Trade-off |
|--------|-----|-----------|
| **Binary protocol (bincode)** | ~10x faster than JSON, smaller payloads | Not human-readable |
| **DashMap** | Lock-free reads, sharded writes | Slightly more memory than Mutex<HashMap> |
| **Task-per-connection** | Concurrent read/write, no head-of-line blocking | More tasks (but Tokio tasks are cheap) |
| **Bounded channels + try_send** | Prevents slow clients from blocking server | Drops messages to slow clients |
| **Token bucket rate limiting** | O(1) per request, allows bursts | Per-client memory overhead |

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
ulimit -n 100000

# Check current limits
ulimit -n        # Soft limit
ulimit -Hn       # Hard limit

# Verify TCP settings
cat /proc/sys/net/core/somaxconn           # Should be 65535
cat /proc/sys/net/ipv4/ip_local_port_range # Ephemeral ports (default ~28k)
```

For load tests above 28K clients (expand ephemeral port range):

```bash
# Expand port range from ~28k to ~64k ports
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"

# Verify
cat /proc/sys/net/ipv4/ip_local_port_range
```

### Monitoring During Tests

```bash
# Watch active connections in real-time
watch -n1 'curl -s localhost:9090/metrics | grep lynx_connections_active'

# Or with docker-compose, open Grafana at http://localhost:3000
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

## License

MIT
