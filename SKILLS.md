# SKILLS.md

Playbooks for recurring work in this repo. Follow the steps; check `CLAUDE.md` for the locked decisions first.

## Change the Redis Lua script
1. Edit the `.lua` file in `crates/plimsoll-redis/src/`.
2. Keep it to the three keys (`ts`, `cost`, `sum`) under one hash tag. Never touch a key not passed in `KEYS`, or Cluster support breaks.
3. Read time only via `redis.call('TIME')`. Format numbers with `string.format('%.0f', x)`.
4. Keep `ts` and `cost` deques in lockstep: every push/pop on one must happen on the other, and `sum` must be updated in the same branch.
5. If the return tuple changes, update the parser in the Redis store at the same time.
6. Keep the `ARGV[4]` test-clock branch working: it must only replace `TIME` and skip expiry, never change decisions.
7. Run the differential tests against a local Redis (see `TESTING.md`), then smoke test with `docker compose up --build`.
8. If the change could affect speed, rerun `cargo bench -p plimsoll-redis` and update `BENCHMARKS.md`.

## Add a new algorithm
1. Write down its state, memory per key, and accuracy trade-off in `PLAN.md` before coding.
2. Add it as a variant of the rule's algorithm setting in `plimsoll-core`; keep existing defaults unchanged.
3. Implement it in every store (`LocalStore` and Redis). The local store must match Redis semantics so `LocalFallback` behaves the same way.
4. Use a separate key namespace so algorithms can't read each other's state.
5. Document it in the README's algorithm table.

## Add a new store backend
1. Create a new crate `crates/plimsoll-<backend>` depending only on `plimsoll-core`.
2. Implement the `Store` trait. All read-check-write logic must be atomic on the backend side.
3. Map backend errors into `StoreError` so the limiter's `OnStoreError` policy handles them.
4. Re-export it from the `plimsoll` facade behind a feature flag.

## Add a framework adapter
1. New crate `crates/plimsoll-<framework>`. Never add the framework as a dependency of `plimsoll-core`.
2. Extract the key with a user-supplied closure; don't hard-code IP or header logic.
3. On denial: 429, `Decision::headers()`, and a short body explaining the limit (RFC 6585 §4).

## Run the demo
```sh
docker compose up --build
for i in $(seq 1 8); do curl -si localhost:3001/ | head -1; curl -si localhost:3002/ | head -1; done
docker compose stop redis   # watch Decision.degraded / fallback behaviour
```

## Prepare a release
1. `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings`
2. Bump versions in all three crates together; update CHANGELOG.
3. `cargo publish -p plimsoll-core`, then `plimsoll-redis`, then `plimsoll`.
