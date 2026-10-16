# plimsoll

[![CI](https://github.com/krishkuchroo/plimsoll/actions/workflows/ci.yml/badge.svg)](https://github.com/krishkuchroo/plimsoll/actions/workflows/ci.yml)

> The Plimsoll line marks the safe load limit on a ship's hull. **plimsoll** marks it for your API.

**One rate limit, shared by every copy of your service.** plimsoll is a Rust library that keeps the count in Redis, so "100 requests per minute" means 100 in total, not 100 per server.

**Status:** pre-release. Works and is tested, but not on crates.io yet. See [PLAN.md](PLAN.md) for the roadmap.

## The problem it solves

If each server counts requests on its own, a client can spread requests across servers and get N times the limit. The fix is a shared counter, and a shared counter has to be updated atomically or two servers can both see "one slot left" and both say yes. plimsoll does every check-and-update in one Redis Lua script, so that race can't happen.

## Quick start

```rust
use plimsoll::{Limiter, Rule};
use plimsoll::redis::RedisStore;
use std::time::Duration;

// 1. Connect to Redis and build a limiter (once, at startup).
let store = RedisStore::connect("redis://127.0.0.1/").await?;
let limiter = Limiter::builder(store).instances(2).build();

// 2. Define a rule: 100 requests per 60 seconds.
let api = Rule::builder("api")
    .limit(100)
    .window(Duration::from_secs(60))
    .build()?;

// 3. Check each request. The key is whatever you limit by: user id, API key, IP.
let decision = limiter.check(&api, "user:123").await?;
if !decision.allowed {
    // Reply 429 Too Many Requests and add decision.headers()
}
```

That's the whole API: build rules once, call `check` per request, act on the `Decision`.

## What you get back

A `Decision` tells you:

| Field | Meaning |
|---|---|
| `allowed` | Let the request through or not |
| `remaining` | How much of the limit is left in the current window |
| `retry_after` | When denied: how long until a retry would succeed |
| `reset_after` | How long until the oldest request leaves the window |
| `degraded` | `true` if Redis was unreachable and a fallback answered |

`decision.headers()` gives you the standard HTTP headers to send back:

```text
HTTP/1.1 429 Too Many Requests
ratelimit-policy: "api";q=100;w=60
ratelimit: "api";r=0;t=17
retry-after: 17
cache-control: no-store
```

These follow [RFC 6585](https://www.rfc-editor.org/rfc/rfc6585#section-4) and the IETF `RateLimit` header draft. You write the response body.

## Features

- **Exact sliding window.** Counts requests over a rolling window, so there is no burst of 2× the limit at window edges like fixed windows allow.
- **Weighted requests.** `limiter.check_cost(&rule, key, 5)` makes an expensive call count as 5.
- **One clock.** Time comes from Redis, so servers with drifting clocks still agree.
- **Survives Redis outages.** Each rule picks what happens when Redis is down (see below).
- **Framework-agnostic.** Plain async Rust; use it from axum, actix, tonic, or anything else.
- **Metrics built in.** Allowed / denied / degraded counts and Redis latency, as Prometheus text.

## When Redis is down

Pick a policy per rule with `.on_store_error(...)`:

| Policy | What happens | Good for |
|---|---|---|
| `LocalFallback` (default) | Each server enforces its share of the limit in memory; the shares add up to exactly the limit | Most APIs |
| `Allow` | Let everything through | Limits that only protect capacity |
| `Deny` | Reject everything, with a short `Retry-After` (5 s by default, `deny_retry_after` to change) | Login, password reset, anything abuse-sensitive |

```rust
use plimsoll::OnStoreError;

let login = Rule::builder("login")
    .limit(5)
    .window(Duration::from_secs(60))
    .on_store_error(OnStoreError::Deny)
    .build()?;
```

`LocalFallback` needs to know how many servers share the limit and which one this is. Let them find each other through Redis:

```rust
use plimsoll::redis::Membership;
use std::sync::Arc;

let limiter = Arc::new(Limiter::new(store));
let membership = Membership::new(limiter.store(), "web-1"); // unique per server, e.g. the pod name
membership.clone().spawn(Arc::clone(&limiter), Duration::from_secs(5));
// on shutdown: membership.leave().await
```

When Redis comes back, requests the fallback admitted during the outage are written into the shared window, so the total across the outage stays within the limit.

Each server heartbeats every 5 s; one that stops is dropped after 15 s. The limit is then split exactly: with 10 over 3 servers they get 4, 3 and 3. While Redis is down the heartbeat can't run, so servers keep the last membership they saw, which is what the fallback needs. Without `Membership`, set `.instances(n)` and `.instance_index(i)` on the builder yourself.

Every degraded answer has `degraded = true` and `store_error` set.

## Several apps, one Redis

Give each deployment its own key prefix so two apps with a rule called `api` don't share a limit:

```rust
use plimsoll::redis::{RedisConfig, RedisStore};

let config = RedisConfig::default()
    .prefix("billing")                       // default: "plimsoll"
    .timeout(Duration::from_millis(100));    // the default; raise it for slow networks
let store = RedisStore::connect_with("redis://127.0.0.1/", config).await?;
```

## Running Redis for plimsoll

Limits live only in Redis. If Redis restarts without persistence, every window starts over. Turn on AOF (`appendonly yes`) or use a replica if that matters for you.

## Rule limits

`Rule::builder(...).build()` rejects configurations that can't be enforced correctly:

- `limit` must be between 1 and 2^53.
- `window` must be between 1 ms and 366 days.
- The name must not be empty or contain `{`, `}`, `:`, `"`, `\`, whitespace, or control characters. Use `v1.login` or `v1-login` rather than `v1:login`. (Keys can contain anything.)

## Metrics

```rust
use plimsoll::Metrics;
use std::sync::Arc;

let metrics = Arc::new(Metrics::new());
let limiter = Limiter::builder(store).observer(Arc::clone(&metrics)).build();

// In your GET /metrics handler:
let body = metrics.render_prometheus();
```

To send events somewhere else, implement the `plimsoll::Observer` trait instead.

## Without Redis

For a single server, or in tests, swap in the in-memory store. Everything else stays the same:

```rust
let limiter = Limiter::new(plimsoll::LocalStore::new());
```

## Try the demo

`examples/axum-demo` is a small web service. Docker runs two copies of it sharing one Redis and a limit of 10 requests per 60 seconds:

```sh
docker compose up --build

# Alternate between the two servers: ten 200s in total, then 429s from both.
for i in $(seq 1 6); do
  curl -si localhost:3001/ | head -1
  curl -si localhost:3002/ | head -1
done

docker compose stop redis    # now each server allows 5 on its own ("degraded": true)
docker compose start redis   # back to the shared limit
```

Requests are keyed by the `x-api-key` header (default `anonymous`). The demo also serves `GET /metrics`. Settings (`RATE_LIMIT`, `RATE_WINDOW_SECS`, `INSTANCES`, `PORT`, `REDIS_URL`) are environment variables, listed at the top of `examples/axum-demo/src/main.rs`.

## Crates

You normally depend on `plimsoll` only.

| Crate | Contents |
|---|---|
| `plimsoll` | Everything you need. Re-exports the other two; the `redis` feature (on by default) adds `plimsoll::redis::RedisStore`. |
| `plimsoll-core` | Rules, decisions, the `Store` trait, the limiter, and the in-memory store. No network code. |
| `plimsoll-redis` | The Redis store and its Lua script. |

## How it works

Every allowed request is recorded as a timestamp and a cost. Each `(rule, key)` pair uses three Redis keys:

| Key | Holds |
|---|---|
| `{plimsoll:<rule>:<key>}:ts` | timestamps, oldest first |
| `{plimsoll:<rule>:<key>}:cost` | the matching costs |
| `{plimsoll:<rule>:<key>}:sum` | running total of costs in the window |

On each check, one Lua script reads Redis's clock, drops entries older than the window, and allows the request if `sum + cost <= limit`. Denied requests are not recorded, so retrying doesn't make things worse. Keys expire on their own after one idle window. The `{...}` hash tag keeps all three keys together, ready for Redis Cluster.

## Performance

On a laptop with local Redis: ~100 µs per check, ~53 000 checks/s from one Redis, ~13 bytes of Redis memory per request in the window. Details and how to reproduce: [BENCHMARKS.md](BENCHMARKS.md).

## Testing

```sh
cargo test --workspace
```

The suite covers edge cases, randomized checks against a simple reference model, and golden-file regression tests over a dataset of recorded traffic patterns. The Redis tests replay the same data through the real Lua script and require identical results; they need a running Redis (`PLIMSOLL_REDIS_URL`, default `redis://127.0.0.1:6379/`) and skip otherwise. Details are in [TESTING.md](TESTING.md).

Requires Rust 1.88 or newer.

## Background

The design draws on:

- B. Guan, *Designing Scalable Rate Limiting Systems* (arXiv:2602.11741, 2026)
- B. Raghavan et al., *Cloud Control with Distributed Rate Limiting* (SIGCOMM 2007)

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option.
