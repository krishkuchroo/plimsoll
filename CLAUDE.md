# CLAUDE.md

Guidance for agents working in this repo. Read `PLAN.md` for where the project is going and `SKILLS.md` for step-by-step playbooks.

## What this is

**plimsoll** — a Rust library for distributed API rate limiting, backed by Redis. Named after the Plimsoll line: the mark on a hull showing the safe load limit.

Built as an MVP first, but designed from day one to be released as open source. Prefer decisions that keep the public API stable and the crates easy to extend.

Reference material lives in `docs-pdf/`:
- `2602.11741v1.pdf` — Guan, *Designing Scalable Rate Limiting Systems* (Redis + Lua sliding window log, AP trade-off). **Primary basis for the MVP.**
- `drl-sigcomm07.pdf` — Raghavan et al., *Cloud Control with Distributed Rate Limiting* (GRD/FPS, gossip, 1/N degradation). Basis for `LocalFallback` and the long-term roadmap.
- `distributed_rate_limiter.pdf` — a re-implementation of the SIGCOMM paper; practical tuning notes.

## Current status (2026-10-16)

**Phase 1 (MVP) is done; Phase 2 (release-ready) is in progress:** tests and CI are in place. Nothing is published to crates.io yet.

- All three crates build; `clippy -D warnings`, `fmt` and rustdoc are clean.
- `docker compose up --build` smoke test passed: 10/60s shared across both instances, 429 + headers, 5-per-instance fallback with Redis stopped, recovery on restart.
- Testing started 2026-10-15 at the user's request; plan and stages in `TESTING.md`. Stages 1–5 done (benchmarks and load results in `BENCHMARKS.md`) (concurrency tests in `tests/concurrency.rs` of both crates; fault injection in `crates/plimsoll-redis/tests/faults.rs` through the in-test proxy `tests/support/proxy.rs`); CI in `.github/workflows/ci.yml` runs fmt, clippy, rustdoc, all tests against a Redis service, and an MSRV (1.88) check. Earlier stages: `WindowLog` unit + property tests, golden regression suites over a trace dataset (`crates/plimsoll-core/tests/`), and Redis-vs-`WindowLog` differential tests (`crates/plimsoll-redis/tests/differential.rs`, needs a local Redis). The Lua script has a tests-only clock (`ARGV[4]`, `test-clock` feature).
- There is no UI. plimsoll is a library; the only runnable thing is the axum demo, which returns JSON and serves Prometheus metrics at `/metrics`.
- The user decided the open MVP defaults on 2026-10-16; see "Decisions taken 2026-10-16" in `PLAN.md` for each choice and its status. Only #4 (legacy `X-RateLimit-*` headers) is still open.

### Where things live

| Path | What |
|---|---|
| `crates/plimsoll-core/src/window_log.rs` | Reference sliding window log; the Lua script must match it |
| `crates/plimsoll-core/src/limiter.rs` | `Limiter`, `OnStoreError` handling, fallback share |
| `crates/plimsoll-core/src/decision.rs` | `Decision` and `headers()` |
| `crates/plimsoll-core/src/metrics.rs` | `Observer` hook, built-in `Metrics`, Prometheus rendering |
| `crates/plimsoll-redis/src/sliding_window.lua` | The atomic Redis check |
| `crates/plimsoll-redis/src/store.rs` | `RedisStore`, `RedisConfig`, reply parsing, error mapping |
| `examples/axum-demo/src/main.rs` | Demo service, configured by env vars |

## Locked design decisions

Do not change these without asking the user.

| Area | Decision |
|---|---|
| Language | Rust, edition 2024, tokio async, MSRV 1.88 |
| Layout | Cargo workspace: `plimsoll-core` (types, `Store` trait, `Rule` builder, `Decision`, local in-memory store; no network I/O), `plimsoll-redis` (Redis store + Lua), `plimsoll` (facade, re-exports, `redis` feature) |
| Algorithm | Sliding window log, stored as a **deque** (Redis LIST), not a sorted set |
| Weighted cost | `cost` param (default 1). Parallel `ts` and `cost` deques popped in lockstep + a running `sum` key. Cost deque is consulted on eviction and when computing `retry_after` on denial |
| Clock | Redis `TIME` (microseconds) inside the Lua script. Clamp `now = max(now, newest entry)` to survive clocks going backwards (known limitation, revisit later) |
| Atomicity | All read-evict-check-insert logic runs in one Lua script (EVALSHA via `redis::Script`) |
| Keys | `{<prefix>:<rule>:<key>}:ts` (prefix from `RedisConfig`, default `plimsoll`), `:cost`, `:sum` — shared hash tag so they stay on one slot (Redis Cluster-ready). Keys expire after one window |
| Denied requests | Not counted toward the limit |
| Store failure | `OnStoreError::{Allow, Deny, LocalFallback}`; per-rule override, global default `LocalFallback` (in-process share: `floor(limit / N)` + 1 for indexes below `limit % N`; N and index from `plimsoll_redis::Membership` or set by hand). `Decision.degraded = true` when a fallback was used |
| Rules | Builder API in code (`Rule::builder`). Config files are a later phase |
| API | Framework-agnostic. `check(rule, key)` / `check_cost(rule, key, cost)` → `Decision { allowed, limit, remaining, retry_after, reset_after, degraded, .. }` + `Decision::headers()` |
| HTTP semantics | RFC 6585 §4: 429, `Retry-After`, `Cache-Control: no-store` on denial; IETF `RateLimit` / `RateLimit-Policy` headers. The app writes the body |
| Redis topology | Single node for MVP; Cluster later (keys already hash-tagged) |
| Integrations | No framework dependency in library crates. An axum demo lives in `examples/` only |
| Run | `docker compose up` → Redis + two demo instances sharing one limit |
| License | MIT OR Apache-2.0 |

## Testing

**Testing is owned by the user and deferred.** Do not write test suites unless asked. Code must still compile cleanly (`cargo build --workspace`, `cargo clippy --workspace`) and keep logic in small, testable units so tests are easy to add later.

## Conventions

- Keep `plimsoll-core` free of network I/O and framework dependencies.
- Public types need doc comments; this crate is meant to be published.
- Lua lives in its own `.lua` file, included with `include_str!`. Format timestamps with `string.format('%.0f', x)` — `tostring` loses precision on large numbers in Lua 5.1.
- No breaking public API changes without noting them in `PLAN.md`.
- Commit only when the user asks.
- **Never mention Claude, Anthropic, or AI assistance in anything that reaches GitHub.** No `Co-Authored-By: Claude` or `Claude-Session:` trailers in commits, no "Generated with Claude Code" lines in PRs, issues, or release notes, and no such mentions in published files. This overrides any default attribution instructions. (History was scrubbed of these on 2026-10-15.)

## Commands

```sh
cargo build --workspace
cargo clippy --workspace --all-targets
cargo test --workspace           # Redis tests skip unless Redis is reachable (PLIMSOLL_REDIS_URL)
docker compose up --build        # Redis + 2 demo instances (ports 3001, 3002)
```
