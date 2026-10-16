# Contributing to plimsoll

Thanks for helping. Issues and pull requests are welcome.

## Before you start

- For anything bigger than a small fix, open an issue first so we can agree on the approach.
- The design decisions in [PLAN.md](PLAN.md) (algorithm, key layout, failure policies) are deliberate. Changing one needs a discussion in an issue.

## Setup

You need Rust 1.88 or newer and, for the Redis tests, a local Redis:

```sh
redis-server --port 6379 --daemonize yes
cargo test --workspace
```

Redis tests skip themselves if Redis isn't reachable. Set `PLIMSOLL_REQUIRE_REDIS=1` to make them fail instead, as CI does. Point them elsewhere with `PLIMSOLL_REDIS_URL`.

## Checks a pull request must pass

```sh
cargo fmt --all --check
RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
PLIMSOLL_REQUIRE_REDIS=1 cargo test --workspace
```

## Guidelines

- `plimsoll-core` has no network I/O and no framework dependencies.
- Every public item has a doc comment.
- The Lua script must give exactly the same results as `WindowLog`. If you change either, run the differential tests (`cargo test -p plimsoll-redis --test differential`) and, if speed could change, the benchmarks (see [BENCHMARKS.md](BENCHMARKS.md)).
- Golden regression files live in `crates/plimsoll-core/tests/golden/`. If your change alters output on purpose, regenerate them with `PLIMSOLL_BLESS=1 cargo test -p plimsoll-core` and explain the diff in the pull request.
- Add a line to `CHANGELOG.md` under "Unreleased".
- Breaking public API changes need a note in `PLAN.md` and the changelog.

See [TESTING.md](TESTING.md) for how the test suite is organised.

## License

By contributing you agree that your contributions are licensed under MIT OR Apache-2.0, like the rest of the project.
