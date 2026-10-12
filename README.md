# plimsoll

> The Plimsoll line marks the safe load limit on a ship's hull. **plimsoll** marks it for your API.

Distributed rate limiting for Rust. One global limit, enforced across every instance of your service, backed by Redis.

**Status:** pre-release (MVP in progress). See [PLAN.md](PLAN.md) for the roadmap.

## Why

A rate limiter running inside each instance can be bypassed by spreading requests across instances. A shared limit needs shared state, and shared state needs atomic updates. plimsoll keeps that state in Redis and does every check-and-update in a single Lua script, so concurrent requests can't race past the limit.

## Features (MVP)

- **Sliding window log**: exact counts over a rolling window, with no 2× burst at window edges like fixed windows have.
- **Weighted requests**: an expensive call can cost more than 1.
- **One clock**: time comes from Redis, so instances with skewed clocks still agree.
- **Graceful degradation**: if Redis is unreachable, each rule chooses to allow, deny, or fall back to a local limit of `limit / instances`.
- **HTTP-ready decisions**: `Retry-After`, `RateLimit` and `RateLimit-Policy` headers, and `Cache-Control: no-store` for 429s ([RFC 6585](https://www.rfc-editor.org/rfc/rfc6585#section-4)).
- **Framework-agnostic**: works with axum, actix, tonic, or anything async.

## Usage (planned API)

```rust
use plimsoll::{Limiter, Rule, OnStoreError};
use plimsoll::redis::RedisStore;
use std::time::Duration;

let store = RedisStore::connect("redis://127.0.0.1/").await?;
let limiter = Limiter::builder(store).instances(2).build();

let api = Rule::builder("api")
    .limit(100)
    .window(Duration::from_secs(60))
    .build()?;

let login = Rule::builder("login")
    .limit(5)
    .window(Duration::from_secs(60))
    .on_store_error(OnStoreError::Deny)
    .build()?;

let decision = limiter.check(&api, "user:123").await?;
if !decision.allowed {
    // respond 429 with decision.headers()
}
```

## How it works

Each `(rule, key)` pair has three Redis keys that share a hash tag:

| Key | Holds |
|---|---|
| `{plimsoll:<rule>:<key>}:ts` | deque of request timestamps (oldest first) |
| `{plimsoll:<rule>:<key>}:cost` | deque of request costs, kept in step with `ts` |
| `{plimsoll:<rule>:<key>}:sum` | running total of costs in the window |

On every check, one Lua script reads Redis `TIME`, drops expired entries from the front of the deques, compares `sum + cost` with the limit, and appends the request if it fits. Denied requests aren't recorded.

## Run the demo

```sh
docker compose up --build
curl -i localhost:3001/
curl -i localhost:3002/     # same limit, different instance
```

## Background

The design draws on:
- B. Guan, *Designing Scalable Rate Limiting Systems* (arXiv:2602.11741, 2026)
- B. Raghavan et al., *Cloud Control with Distributed Rate Limiting* (SIGCOMM 2007)

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option.
