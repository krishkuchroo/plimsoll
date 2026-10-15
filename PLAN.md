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
- [x] **Breaking (pre-release):** `RuleBuilder::build` now rejects windows under `MIN_WINDOW` (1 ms), limits over `MAX_LIMIT` (2^53), and rule names containing `"` or `\`. New `RuleError::{WindowTooShort, LimitTooLarge}` variants; new consts `MIN_WINDOW`, `MAX_LIMIT`. Added 2026-10-15
- [x] Fix: `WindowLog` evicted entries at `t = 0` while `now < window` (saturating cutoff), unlike the Lua script. Found by the stage 1 property test
- [ ] CI: fmt, clippy, build, tests against Redis
- [ ] Benchmarks (criterion) + a load-test write-up
- [ ] rustdoc for every public item, crate-level examples
- [ ] CHANGELOG, CONTRIBUTING, issue templates
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
- [ ] Membership-aware `1/N` degradation when peers disappear
- [ ] Multi-region deployments

## Known limitations (MVP)
- Clock going backwards is handled by clamping to the newest entry, not fully solved.
- Single Redis node only.
- Sliding window log stores one entry per allowed request, so memory grows with the limit.
- `LocalFallback` needs the instance count configured by hand. Its per-instance share is `ceil(limit / instances)`, so N instances may admit up to N-1 extra requests when `limit` is not divisible by `instances`.
- Under `OnStoreError::Deny`, `Retry-After` is the full window because the real usage is unknown.
- `LocalFallback` state starts empty when Redis goes away, so an instance may briefly admit its full share even if the global window was already full. Likewise, requests admitted by the fallback are never written to Redis, so the global count under-reports by that amount once Redis is back (observed: ~1 s of fallback traffic during reconnect after `docker compose start redis`).

## Open decisions (defaults chosen during the MVP build, not yet confirmed)
- Under `LocalFallback`, `Decision.limit` reports the per-instance share (e.g. 5), not the rule's limit.
- The Redis key prefix is fixed to `plimsoll`; two deployments sharing one Redis and rule names would collide. A `prefix` option on `RedisConfig` would fix it.
- `RedisConfig` defaults: 250 ms per-call timeout, 1 s connect timeout, one reconnect attempt per call.
- Rule names may contain `:`, so rule `api:a` + key `b` collides with rule `api` + key `a:b` in Redis. Forbid `:` in rule names, or escape/length-prefix the rule in the key? (found by testing, 2026-10-15)
- Only the structured-field IETF `RateLimit` / `RateLimit-Policy` headers are emitted; no legacy `X-RateLimit-*`.
