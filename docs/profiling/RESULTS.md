# Profiling Results

Date: 2026-01-02
Tool: cargo-flamegraph (perf-based)
Load: 500 clients, 10 rooms, 100 messages each

## Flamegraph Analysis

| Function | % Time | Meaning |
|----------|--------|---------|
| `__x64_sys_sendto` | 37.45% | TCP writes - main cost |
| `__x64_sys_futex` | ~2.5% | Lock/sync operations (DashMap, channels) |
| `__wake_up_*` | ~1.5% | Waking async tasks |
| `__x64_sys_recvfrom` | ~0.7% | TCP reads |
| `__x64_sys_epoll_wait` | ~0.8% | Async I/O polling |
| Tokio internals | ~1% | Runtime overhead |

## Key Findings

1. **Server is I/O bound** - 37% of time spent in kernel `sendto` syscall. This is expected and healthy for a network server.

2. **No application-level hotspots** - Rust code (message processing, serialization, broadcasting) is so fast it barely registers in the profile.

3. **Low lock contention** - ~2.5% in futex is healthy for a concurrent server using DashMap.

4. **Minimal runtime overhead** - Tokio async runtime adds ~1% overhead.

## Conclusion

The server is well-optimized. The profile shows a classic I/O-bound network service with no obvious optimization targets in user-space code.

Potential micro-optimizations (diminishing returns):
- Batching writes with `writev()` / `write_vectored()`
- Using `io_uring` instead of epoll (significant complexity)

## Profiling Setup

```bash
# Cargo.toml
[profile.release]
debug = true  # keep symbols for profiling

# Run with lower sampling frequency to avoid data overload
cargo flamegraph -p lynx-server -o flamegraph.svg -F 99

# Gentler load for profiled server
cargo run -p lynx-load --release -- \
  -c 500 -r 10 -m 100 \
  --batch-size 10 --batch-delay-ms 50
```

## Files

- `flamegraph.svg` - interactive flamegraph (open in browser)
