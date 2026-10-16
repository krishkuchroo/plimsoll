# PLAN.md

## Destination

**plimsoll becomes the default choice for distributed rate limiting in Rust**: one crate you add to any service — axum, actix, tonic, or plain tokio — that enforces a single global limit across every instance, stays correct under concurrency, and degrades gracefully when its backing store is down.

What "done" looks like at 1.0:
- Precise, atomic limits across N instances with Redis (single node and Cluster).
- Several algorithms behind one API (sliding window log, GCRA/token bucket, sliding window counter, concurrency limiter).
- Rules from code *or* config files, reloadable without redeploys.
- Drop-in adapters for the major Rust web frameworks.
- Optional coordination without a central store, using ideas from the SIGCOMM '07 distributed rate limiting paper (local enforcement + gossip).
- Published on crates.io with docs, examples, benchmarks, and a stable API.

## Phases

### Phase 0: Foundation ✅
- [x] Read and summarise the reference papers
- [x] Lock the MVP design (see `CLAUDE.md`)
- [x] `CLAUDE.md`, `SKILLS.md`, `PLAN.md`, `README.md`, `git init`

### Phase 1: MVP ✅ (current)
- [x] Cargo workspace: `plimsoll-core`, `plimsoll-redis`, `plimsoll`
- [x] Core: `Rule` builder, `Decision` + `headers()`, `Store` trait, `OnStoreError`, `Limiter`
- [x] Core: in-memory deque `LocalStore` (used for `LocalFallback`)
- [x] Redis: Lua sliding window log on LIST deques (`ts`, `cost`, `sum`), Redis `TIME`, clock clamp, retry/reset calculation
- [x] Redis: `RedisStore` with `ConnectionManager` and a per-call timeout
- [x] Facade crate with `redis` feature
- [x] axum demo in `examples/`
- [x] `Dockerfile` + `docker-compose.yml` (Redis + 2 instances)
- [x] Manual smoke test: limit is shared across both instances; 429 + headers; Redis-down fallback (verified with `docker compose up --build` on 2026-10-12)

### Phase 2: Release-ready (0.1 on crates.io)
- [ ] Test suite: unit, Redis integration, concurrency/race tests. Plan and progress in `TESTING.md`; stage 1 (`WindowLog`) done 2026-10-15
- [x] Stage 2 (Redis vs `WindowLog` differential tests) done 2026-10-15, using the `test-clock` feature (optional `ARGV[4]` in the Lua script; see `TESTING.md`)
- [x] **Breaking (pre-release):** `RuleBuilder::build` now rejects windows under `MIN_WINDOW` (1 ms), limits over `MAX_LIMIT` (2^53), and rule names containing `"`, `\` or `:` (`:` added 2026-10-16 to stop rule/key pairs colliding in Redis). New `RuleError::{WindowTooShort, LimitTooLarge}` variants; new consts `MIN_WINDOW`, `MAX_LIMIT`. Added 2026-10-15
- [x] Fix: `WindowLog` evicted entries at `t = 0` while `now < window` (saturating cutoff), unlike the Lua script. Found by the stage 1 property test
- [x] Fault-injection tests (stage 4) with an in-test proxy, 2026-10-16; see `TESTING.md`
- [x] CI: fmt, clippy, rustdoc, tests against a Redis service, MSRV check (`.github/workflows/ci.yml`, 2026-10-16)
- [x] MSRV corrected from 1.85 to 1.88: `redis` 1.7 requires 1.88, so 1.85 never built (found while adding the CI MSRV job)
- [x] Benchmarks (criterion) + a load-test write-up: `BENCHMARKS.md`, 2026-10-16. Fixed O(n) Lua denials found by it (10x faster cost-1 denials at limit 10 000)
- [x] rustdoc for every public item, crate-level examples (`missing_docs` warns, CI builds docs with `-D warnings`)
- [x] CHANGELOG, CONTRIBUTING, issue and PR templates (2026-10-16)
- [x] Docker Compose smoke test with membership and write-back (2026-10-16): 10/60s shared, instances #0/#1 discovered, 5+3 admitted during a Redis stop, written back on restart, total exactly 10
- [ ] Publish `plimsoll-core`, `plimsoll-redis`, `plimsoll` 0.1.0

### Phase 3: Scale up
- [ ] Redis Cluster support (keys already hash-tagged)
- [ ] Revisit clock-going-backwards handling
- [ ] More algorithms: GCRA, token bucket, sliding window counter, concurrency limiter (Guan §4.2)
- [ ] Rules from YAML/TOML, plus hot reload
- [ ] Multiple rules per request (e.g. per-user AND per-IP) in one round trip
- [ ] Framework adapters as separate crates: `plimsoll-tower`, `plimsoll-actix`
- [x] Metrics hooks (allowed/denied/degraded counts, store latency): `Observer` trait + built-in `Metrics` with Prometheus text output, added 2026-10-13 (additive public API, not breaking)
- [ ] Metrics follow-ups: optional `metrics`-crate / OpenTelemetry bridge crates; Redis-side gauges (key count, memory)
- [ ] Pipelining / batching to cut Redis round trips

### Phase 4: Beyond a central store
- [ ] Hybrid mode: local counters with periodic sync to Redis (Databricks-style batch reporting)
- [ ] Peer-to-peer mode: gossip-based global estimates, FPS-style proportional allocation (SIGCOMM '07)
- [x] Membership-aware `1/N` degradation when peers disappear (`Membership`, 2026-10-16)
- [ ] Multi-region deployments

## Known limitations
- Single Redis node only (Cluster is Phase 3).
- Sliding window log stores one entry per allowed request: ~13 bytes per entry plus ~300 bytes per active key (`BENCHMARKS.md`). Decided 2026-10-16: keep it as the exact algorithm and add a cheaper approximate one (sliding window counter / GCRA) in Phase 3.
- A denial for a large cost walks the log (O(n) in the entries it needs to free); cost-1 denials read one batch.
- `LocalFallback` state starts empty when Redis goes away, so an instance may admit its full share even if the global window was already full. Write-back (decision 7) makes Redis count those admissions afterwards, so later requests pay for them, but it can't undo them.
- The first decision after recovery doesn't include the written-back admissions yet (write-back runs after that check), so its `remaining` is too high by up to one outage's worth. Enforcement is correct from the next request on.
- Write-back records an admission no earlier than the newest entry already in Redis, so it may expire slightly late. That only makes the limit stricter.
- Redis without persistence loses every window on restart. Decided 2026-10-16: document it and recommend AOF or replication; no detection in the library.
- A clock going backwards is clamped to the newest entry, which can stretch a window slightly. Decided 2026-10-16: leave as is; Redis `TIME` rarely jumps.

## Decisions taken 2026-10-16

| # | Issue | Decision | Status |
|---|---|---|---|
| 1 | Fixed key prefix let deployments sharing a Redis collide | `RedisConfig::prefix` (default `plimsoll`) and `RedisStore::with_prefix` | done |
| 2 | `Decision.limit` under `LocalFallback` | keep reporting the per-instance share | done (no change) |
| 3 | Default timeout | 100 ms (was 250 ms); 1 s connect, one reconnect attempt | done |
| 4 | Legacy `X-RateLimit-*` headers | **open**, awaiting the user's choice; IETF headers only for now | open |
| 5 | `Retry-After` under `Deny` was the full window | `LimiterBuilder::deny_retry_after`, default 5 s, capped at the window | done |
| 6 | `ceil(limit / N)` share overshoots by up to N-1 | exact split: `floor(limit / N)`, plus one for the first `limit % N` instances by index (`LimiterBuilder::instance_index`, `Limiter::set_membership`); default index 0 keeps the old `ceil` behaviour | done |
| 7 | Fallback admissions never reach Redis | `Store::backfill` (default no-op) + `backfill.lua`: the limiter remembers fallback admissions (up to `MAX_PENDING_BACKFILL`) and writes the still-live ones to Redis after the first successful check. Outage cycle test now admits exactly the limit (was limit + one share) | done |
| 8 | Instance count set by hand | `plimsoll_redis::Membership`: instances heartbeat into a Redis sorted set with a TTL (default 15 s) and get the live count and their index; `spawn` keeps the limiter updated; the demo uses it | done |
| 9 | Redis restart without persistence resets limits | document; recommend AOF/replication | done (docs) |
| 10 | Memory per entry | keep; add approximate algorithm in Phase 3 | done (docs) |
| 11 | Clock going backwards | leave as is | done (docs) |

### Breaking API changes (pre-release, 2026-10-16)
- `RedisStore::keys(rule, key)` is now a method (`store.keys(rule, key)`) because the prefix is per store; `RedisStore::keys_with_prefix(prefix, rule, key)` replaces the old associated function.
- `RedisConfig` has a new public `prefix` field (construct it with `RedisConfig::default()` and the builder methods).
