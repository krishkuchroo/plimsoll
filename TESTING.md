# TESTING.md

How we check that plimsoll is correct. Kept short on purpose; grow it as stages land.

## The oracle

`plimsoll_core::WindowLog` is the reference. Every store must give the same
decision as `WindowLog` for the same sequence of `(now, cost)` checks. Tests
compare against it, or against a naive brute-force model, instead of
hard-coding expected numbers where possible.

## Invariants every test can assert

1. **No over-admission:** for any `now`, the total cost of allowed requests in
   `(now - window, now]` is `<= limit`.
2. **No false denial:** a request is denied only if admitting it would break (1).
3. **Denials are free:** a denied check records nothing (it may still evict
   expired entries).
4. **`retry_after` is exact:** retrying at `now + retry_after` is allowed,
   retrying 1 µs earlier is not (when nothing else arrives).
5. **Degraded is bounded:** under `LocalFallback`, total admitted across N
   instances is `<= ceil(limit / N) * N`.

## Stages

| # | Stage | Where | Needs | Status |
|---|---|---|---|---|
| 1 | Edge-case unit tests + property test of `WindowLog` vs a brute-force model | `crates/plimsoll-core/tests/window_log.rs` | nothing | done; found and fixed the t = 0 eviction bug (2026-10-15) |
| 2 | Redis vs `WindowLog` differential test: same traces + 64 random sequences, plus real-clock TTL check and odd keys | `crates/plimsoll-redis/tests/differential.rs` | local Redis | done 2026-10-15; verified it catches an injected off-by-one in the Lua eviction |
| 3 | Concurrency: 4 Redis connections x 8 threads (exact count, serialised `used` 1..=N, weighted costs, 50 isolated keys); `LocalStore` and `LocalFallback` share under 8 OS threads | `crates/plimsoll-redis/tests/concurrency.rs`, `crates/plimsoll-core/tests/concurrency.rs` | local Redis | done 2026-10-16 |
| 4 | Faults via an in-test TCP proxy (`tests/support/proxy.rs`, Toxiproxy-style: refuse, latency, blackhole, cut): each policy when Redis is down, latency below/above the timeout, reconnect after cuts, full outage cycle (invariant 5), `SCRIPT FLUSH`, lost keys | `crates/plimsoll-redis/tests/faults.rs` | local Redis | done 2026-10-16; verified it catches a broken fallback share and a missing timeout |
| 5 | Benchmarks (criterion, both crates) and a load generator with a Redis memory report; results in `BENCHMARKS.md` | `crates/*/benches/`, `crates/plimsoll-redis/examples/load.rs` | local Redis | done 2026-10-16; found and fixed O(n) Redis denials |

## Regression suite

Fixed inputs, saved outputs. Any change in behaviour fails the run until it
is reviewed and accepted with `PLIMSOLL_BLESS=1` (then check `git diff`).

| Suite | Inputs | Golden output |
|---|---|---|
| `tests/regression_traces.rs` | `tests/data/traces/*.trace`: 6 hand-written edge cases (`hand-*`) + 8 generated traffic shapes (`gen-*`, seeded, regenerate with `-- --ignored`) | `tests/golden/traces/` |
| `tests/regression_limiter.rs` | scripted store: healthy outcomes, 3 errors x 3 policies x 5 limit/instance combos, per-rule overrides, invalid costs, 16 rule-builder inputs | `tests/golden/limiter/` |

Trace replay also checks the brute-force model on every step, so a wrong
result cannot be blessed silently. The `.trace` files are the dataset stage 2
will replay against Redis.

## Findings

Fixed 2026-10-15 (in `RuleBuilder::build`, goldens updated):
- Windows under 1 µs rounded to 0 and limited nothing (1000/1000 admitted
  with `limit(1)`). Now `MIN_WINDOW = 1 ms`, error `RuleError::WindowTooShort`.
- Rule names with `"` or `\` broke the `RateLimit` headers. Now rejected as
  `RuleError::InvalidName`.
- Limits above 2^53 lost precision in Lua. Now `MAX_LIMIT = 2^53`, error
  `RuleError::LimitTooLarge`.
- `WindowLog` evicted `t = 0` entries early (stage 1).
- Redis denials copied the whole log (1.23 ms at limit 10 000); now batched,
  ~120 µs (stage 5, `BENCHMARKS.md`). New trace `hand-large-log` covers the
  batch boundaries.

- Rule `api:a` + key `b` and rule `api` + key `a:b` shared one Redis key.
  Fixed 2026-10-16 by forbidding `:` in rule names (keys come last, so they
  may contain anything).

### What stage 4 showed

- A slow (500 ms) or silent Redis turns into a `Timeout` decision in under
  2x the configured timeout; requests never stall.
- After connections are cut the limiter is back on Redis within 3 checks.
- Outage cycle, limit 20 over 4 instances: 8 from Redis, 5 from fallback,
  then 7 more from Redis = exactly 20, because fallback admissions are
  written back on recovery (was 25 before write-back, 2026-10-16).
  Admissions that expired during the outage are not written back.
- Deleted keys reset the window (documented limitation; Redis without
  persistence behaves the same after a restart).

## Traffic shapes (for stages 1–3, generated in-test)

Steady at/above limit · random gaps · bursts aligned to window edges ·
mixed costs (incl. `cost == remaining`, `cost > limit`) · clock going backwards ·
timestamps near zero.

## Edge-case checklist

- [x] Entry exactly at `ts + window` is evicted; at `ts + window - 1` it is not
- [x] `cost == remaining` allowed; `cost > limit` denied with `retry_after = window`
- [x] Denied check records nothing
- [x] `retry_after` exact (invariant 4)
- [x] Clock going backwards is clamped to the newest entry
- [x] Timestamps smaller than `window` (fresh `LocalStore`) evict nothing early
- [x] Keys with `{`, `}`, `:`, unicode, empty, NUL, 10 KB (each isolated)
- [x] Large µs timestamps survive `%.0f` formatting in Lua (`hand-large-timestamps` in stage 2)
- [x] `ceil(limit / N)` overshoot under `LocalFallback` (golden: limit 10, N=3 admits 4 each)
- [x] `Decision::headers()` values on allow and deny

## Test clock

The Lua script takes an optional `ARGV[4] = now_us`. When present it replaces
Redis `TIME` and skips key expiry. Only `RedisStore::check_at` sends it, and
that method exists only with the `test-clock` feature, which the crate's own
dev-dependency turns on. Production builds never send it.

## Run

```sh
cargo test -p plimsoll-core                                  # everything
PLIMSOLL_BLESS=1 cargo test -p plimsoll-core                 # accept intended output changes
cargo test -p plimsoll-core --test regression_traces -- --ignored   # regenerate gen-* traces

# Stage 2 (Redis). Skips if Redis is not reachable unless PLIMSOLL_REQUIRE_REDIS is set.
redis-server --port 6390 --save '' --daemonize yes
PLIMSOLL_REDIS_URL=redis://127.0.0.1:6390/ PLIMSOLL_REQUIRE_REDIS=1 cargo test -p plimsoll-redis
```
