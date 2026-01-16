# Profiling Results

Date: 2025-01-02
Tool: cargo-flamegraph (perf-based)
Load: 500 clients, 10 rooms, 100 messages each

## Flamegraph Analysis

| Function | % Time | Notes |
|----------|--------|-------|
| `__x64_sys_sendto` | 37.45% | TCP writes (dominant cost) |
| `__x64_sys_futex` | ~2.5% | Lock/sync (DashMap, channels) |
| `__wake_up_*` | ~1.5% | Waking async tasks |
| `__x64_sys_epoll_wait` | ~0.8% | Async I/O polling |
| `__x64_sys_recvfrom` | ~0.7% | TCP reads |
| Tokio internals | ~1% | Runtime overhead |

## Findings

1. **I/O bound** - 37% in kernel `sendto`. Application code is not the bottleneck.

2. **No user-space hotspots** - Message processing, serialization, and broadcasting don't appear in the profile (sub-millisecond per operation).

3. **Low lock contention** - 2.5% futex is typical for DashMap-based concurrency.

4. **Minimal async overhead** - Tokio runtime contributes ~1%.

## Conclusion

Profile shows a typical I/O-bound network service. No actionable optimization targets in application code.

Possible micro-optimizations (low ROI):
- Batch writes via `writev()` / `write_vectored()`
- Replace epoll with `io_uring` (significant complexity)

## Profiling Setup

```bash
# Cargo.toml
[profile.release]
debug = true  # symbols for profiling

# Lower sampling frequency
cargo flamegraph -p lynx-server -o flamegraph.svg -F 99

# Moderate load for profiled server
cargo run -p lynx-load --release -- \
  -c 500 -r 10 -m 100 \
  --batch-size 10 --batch-delay-ms 50
```

## Files

- `flamegraph.svg` - interactive flamegraph (open in browser)
