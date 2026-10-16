# Changelog

All notable changes to plimsoll are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased] — first release, 0.1.0

### Added
- Sliding window log rate limiting with one atomic Redis Lua script per check; weighted requests (`check_cost`).
- `Rule` builder with validation: limit 1..=2^53, window 1 ms..=366 days, rule names without `{`, `}`, `:`, `"`, `\`, whitespace or control characters.
- `Decision` with `allowed`, `remaining`, `retry_after`, `reset_after`, `degraded`, and `headers()` producing IETF `RateLimit` / `RateLimit-Policy`, plus `Retry-After` and `Cache-Control: no-store` on denial.
- Store failure policies per rule: `LocalFallback` (default), `Allow`, `Deny`.
- `LocalFallback` splits the limit exactly across instances by index (`instance_index`, `set_membership`).
- `plimsoll_redis::Membership`: instances discover each other through Redis and keep the limiter's instance count and index current.
- Fallback admissions are written back to Redis after an outage (`Store::backfill`), so the total across an outage stays within the limit.
- `RedisConfig::prefix` so deployments sharing a Redis keep separate limits.
- `LimiterBuilder::deny_retry_after` (default 5 s) for `Retry-After` under `Deny`.
- `LocalStore` for single-instance services and tests.
- `Observer` hook and built-in `Metrics` with Prometheus text output.
- axum demo with Docker Compose: two instances sharing one limit.
- Tests: unit, property, golden regression over a trace dataset, Redis-vs-reference differential, concurrency, fault injection. Benchmarks and a load generator (`BENCHMARKS.md`).

### Defaults
- Redis per-call timeout 100 ms, connect timeout 1 s.
- Minimum supported Rust version 1.88.
