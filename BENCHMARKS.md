# BENCHMARKS.md

Measured 2026-10-16 on an Apple M4 (10 cores), Redis 7.2.7 on the same
machine (loopback, no persistence), release builds. Numbers are what to
expect order-of-magnitude; rerun on your own hardware before relying on them.

## How to run

```sh
cargo bench -p plimsoll-core                                    # no I/O
PLIMSOLL_REDIS_URL=redis://127.0.0.1:6379/ cargo bench -p plimsoll-redis
PLIMSOLL_REDIS_URL=redis://127.0.0.1:6379/ cargo run --release -p plimsoll-redis --example load
```

The load example takes `CONCURRENCY`, `CONNECTIONS`, `SECONDS`, `KEYS`,
`LIMIT` and `WINDOW_SECS` as environment variables.

## In-process (`cargo bench -p plimsoll-core`)

| Operation | Time |
|---|---|
| `WindowLog::check`, allowed (any log size) | ~5 ns |
| `WindowLog::check`, denied, cost 1 (any log size) | ~5 ns |
| `WindowLog::check`, denied, cost = limit, 1 000 entries | ~380 ns |
| `WindowLog::check`, denied, cost = limit, 100 000 entries | ~34 µs |
| `LocalStore::check_sync`, one key | ~100 ns |
| `LocalStore::check_sync`, 10 000 keys round-robin | ~190 ns |
| `Limiter::check` on `LocalStore` | ~140 ns |
| `Decision::headers()` | ~200 ns |

A denial walks the log only until enough cost would expire, so cost-1
denials are constant time; a denial for a large cost is linear in the log.

## One Redis check (`cargo bench -p plimsoll-redis`)

Single connection, one request at a time, so this is round-trip latency.

| Check | limit 10 | limit 1 000 | limit 10 000 |
|---|---|---|---|
| allowed | ~105 µs | | |
| denied, cost 1 | ~120 µs | ~135 µs | ~125 µs |
| denied, cost = limit | ~130 µs | ~200 µs | ~1.3 ms |

**Fixed while benchmarking (2026-10-16):** the Lua script used to copy the
key's whole log (`LRANGE 0 -1` on both lists) on every denial. A cost-1
denial at limit 10 000 took **1.23 ms**, blocking Redis for every other
client, and denials are exactly what a limiter serves most under abuse. The
script now reads costs in doubling batches (64, 128, ...) and stops when
enough is freed: cost-1 denials are flat at ~120 µs (10x faster at limit
10 000) and a full walk dropped from 1.7 ms to 1.3 ms. Behaviour is
unchanged; the differential tests, including a new trace that crosses every
batch boundary up to 200 entries, still match `WindowLog` exactly.

## Under load (`--example load`, defaults)

64 tasks, 4 connections, 1 000 keys, limit 100 per 60 s, 10 s:

| Metric | Value |
|---|---|
| Throughput | ~53 000 checks/s |
| Admitted | exactly 100 000 (1 000 keys x 100), no over-admission |
| Latency p50 / p90 / p99 / p99.9 | 0.98 ms / 1.7 ms / 5.6 ms / 14 ms |
| Degraded | 0 |

Redis executes scripts one at a time, so throughput is bounded by one Redis
core; past that point more concurrency only adds queueing latency. Redis
Cluster (Phase 3) is the way to scale beyond it.

## Redis memory per `(rule, key)`

All three keys (`ts`, `cost`, `sum`), measured with `MEMORY USAGE`, window
full:

| Limit | Bytes | Per entry |
|---|---|---|
| 10 | ~410 | ~41 |
| 100 | ~1 500 | ~15 |
| 1 000 | ~12 700 | ~13 |
| 10 000 | ~128 000 | ~13 |

Rule of thumb: about **13 bytes per request in the window**, plus ~300 bytes
per active key. 1 million active users at a limit of 100 is roughly 1.5 GB.
That is the sliding-window-log trade-off (exact counts cost memory); keys
expire one window after their last request.
