# Lynx Benchmark Results

Criterion 0.5, Linux 6.x, AMD Ryzen / Intel Core (results are representative, not absolute).

## Protocol (lynx-protocol)

### Encoding

| Message Type | Time | Throughput |
|--------------|------|------------|
| `Connect` | 115 ns | 8.7M/s |
| `SendRoomMessage` | 156 ns | 6.4M/s |
| `JoinRoom` | 158 ns | 6.3M/s |
| `SendPrivateMessage` | 177 ns | 5.6M/s |
| `ListUsers` | 53 ns | 18.9M/s |
| `Disconnect` | 50 ns | 20M/s |

| Response Type | Time | Throughput |
|---------------|------|------------|
| `Success` | 175 ns | 5.7M/s |
| `Error` | 152 ns | 6.6M/s |
| `IncomingMessage` | 169 ns | 5.9M/s |
| `SystemNotification` | 151 ns | 6.6M/s |

### Decoding

| Type | Time |
|------|------|
| `Message::Connect` | 51 ns |
| `Message::SendRoomMessage` | 52 ns |
| `Message::SendPrivateMessage` | 85 ns |
| `Message::ListUsers` | 12 ns |
| `Response::Success` | 50 ns |
| `Response::IncomingMessage` | 135 ns |

Decoding is faster than encoding (no allocation, just parsing into stack memory).

### UserList Scaling

| Users | Encode | Decode |
|-------|--------|--------|
| 10 | 225 ns | ~200 ns |
| 100 | 906 ns | 8.0 µs |
| 1,000 | 6.6 µs | ~65 µs |

Linear scaling dominated by string allocation.

### Framing Overhead

| Operation | Time |
|-----------|------|
| Raw bincode | 30 ns |
| With length-prefix | 152 ns |
| **Overhead** | 122 ns (5x) |

Cause: `encode_frame` allocates twice (bincode returns `Vec`, then copied into framed `Vec`). Fix: pre-allocate and serialize directly into buffer.

## Server (lynx-server)

### Broadcast Scaling

All clients in same room, measuring DashMap iteration + `try_send`:

| Clients | Time | Per-Client |
|---------|------|------------|
| 10 | 2.4 µs | 240 ns |
| 100 | 9.5 µs | 95 ns |
| 1,000 | 89 µs | 89 ns |
| 10,000 | 1.1 ms | 110 ns |

Linear scaling. Per-client cost stabilizes at ~90-110 ns (amortized iteration overhead).

### Room Filtering

1,000 total clients, varying target room membership:

| Match % | Recipients | Time | Relative |
|---------|------------|------|----------|
| 10% | 100 | 31 µs | 35% |
| 50% | 500 | 58 µs | 65% |
| 90% | 900 | 81 µs | 91% |
| 100% | 1,000 | 89 µs | 100% |

DashMap iteration dominates. Filtering overhead is negligible; time reduction comes from fewer `try_send` calls.

## Throughput Summary

**Protocol layer:**
- Encoding: 5-20M msg/s (payload dependent)
- Decoding: 7-80M msg/s

**Server layer (single thread):**
- 100 clients: ~100K broadcasts/s
- 1K clients: ~11K broadcasts/s
- 10K clients: ~900 broadcasts/s

## Identified Bottlenecks

1. **Framing allocation** - 5x overhead vs raw bincode (fixable)
2. **Broadcast** - linear with client count (expected, unavoidable)
3. **DashMap iteration** - efficient, no contention issues observed

Profiling confirms the server is I/O bound (37% in `sendto`). These micro-benchmarks measure CPU overhead which is not the limiting factor at scale.

## Potential Optimizations

| Optimization | Expected Gain | Complexity |
|--------------|---------------|------------|
| Pre-allocate frame buffer | 3-4x encode speedup | Low |
| Pre-serialize broadcast message | Avoid N clones | Medium |
| Room membership index | O(room_size) vs O(total) | Medium |

Not implemented - current performance (70K clients, 16K msg/s) meets requirements.

## Running

```bash
cargo bench --workspace                          # all
cargo bench -p lynx-protocol -- "encode/"        # protocol encode
cargo bench -p lynx-server -- "broadcast/"       # server broadcast
```

HTML reports: `target/criterion/report/index.html`
