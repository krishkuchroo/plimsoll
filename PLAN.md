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
- [x] Manual smoke test: limit is shared across both instances; 429 + headers; Redis-down fallback (verified against a local Redis; `docker compose up` run still pending)

### Phase 2: Release-ready (0.1 on crates.io)
- [ ] Test suite (owned by the user): unit, Redis integration, concurrency/race tests
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
- [ ] Metrics hooks (allowed/denied/degraded counts, store latency)
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
- `LocalFallback` state starts empty when Redis goes away, so an instance may briefly admit its full share even if the global window was already full.
