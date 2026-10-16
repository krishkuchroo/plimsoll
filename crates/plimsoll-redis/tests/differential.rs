//! Stage 2 of TESTING.md: the Lua script against the reference `WindowLog`.
//!
//! Replays the shared trace dataset (`plimsoll-core/tests/data/traces/`) and
//! random sequences through both, with the script's clock pinned by
//! `RedisStore::check_at`, and requires identical outcomes at every step.
//!
//! Needs a Redis server; see `support/mod.rs` for how it is found. Every test uses its own unique rule names and deletes its keys afterwards.

#[path = "../../plimsoll-core/tests/common/mod.rs"]
mod common;
mod support;

use std::path::Path;
use std::time::Duration;

use common::Trace;
use plimsoll_core::{CheckOutcome, CheckRequest, Store, WindowLog};
use plimsoll_redis::RedisStore;
use plimsoll_redis::redis::{self, AsyncCommands};
use proptest::prelude::*;
use support::{connect, delete_keys, redis_url, unique_rule};

/// Run `steps` through Redis and `WindowLog` side by side. Returns the first
/// mismatch as a readable message.
async fn compare(
    store: &RedisStore,
    rule: &str,
    window_us: u64,
    limit: u64,
    steps: &[(u64, u64)],
) -> Result<(), String> {
    let mut log = WindowLog::new();
    let request = |cost| CheckRequest {
        rule,
        key: "k",
        limit,
        window: Duration::from_micros(window_us),
        cost,
    };
    let mut result = Ok(());
    for (i, &(now, cost)) in steps.iter().enumerate() {
        let expected = log.check(now, window_us, limit, cost);
        let actual: CheckOutcome = store
            .check_at(request(cost), now)
            .await
            .map_err(|e| format!("step {}: store error {e}", i + 1))?;
        if actual != expected {
            result = Err(format!(
                "step {} (now={now}, cost={cost}):\n  WindowLog: {expected:?}\n  Redis:     {actual:?}",
                i + 1
            ));
            break;
        }
    }
    delete_keys(rule, "k").await;
    result
}

#[tokio::test]
async fn traces_match_window_log() {
    let Some(store) = connect().await else { return };
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../plimsoll-core/tests/data/traces");
    let mut paths: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "trace"))
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no traces in {}", dir.display());

    let mut failures = Vec::new();
    for path in &paths {
        let name = path.file_stem().unwrap().to_string_lossy();
        let trace = Trace::parse(&name, &std::fs::read_to_string(path).unwrap());
        let rule = unique_rule(&trace.name);
        if let Err(msg) = compare(&store, &rule, trace.window, trace.limit, &trace.steps).await {
            failures.push(format!("{}: {msg}", trace.name));
        }
    }
    assert!(
        failures.is_empty(),
        "Redis differs from WindowLog:\n{}",
        failures.join("\n")
    );
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, failure_persistence: None, ..ProptestConfig::default() })]

    /// Random traffic, including clock steps backwards, near-zero and
    /// epoch-sized timestamps, and costs above the limit.
    #[test]
    fn random_sequences_match_window_log(
        start in prop_oneof![Just(0u64), Just(1_792_000_000_000_000u64)],
        limit in 1u64..=8,
        steps in prop::collection::vec((-200i64..=600, 1u64..=9), 1..80),
    ) {
        const WINDOW: u64 = 1_000;
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let outcome = rt.block_on(async {
            let Some(store) = connect().await else { return Ok(()) };
            let mut now = start;
            let steps: Vec<(u64, u64)> = steps
                .iter()
                .map(|&(dt, cost)| {
                    now = now.saturating_add_signed(dt);
                    (now, cost)
                })
                .collect();
            compare(&store, &unique_rule("prop"), WINDOW, limit, &steps).await
        });
        prop_assert!(outcome.is_ok(), "{}", outcome.unwrap_err());
    }
}

/// The production path: Redis `TIME` and real key expiry.
#[tokio::test]
async fn real_clock_limits_and_sets_ttl() {
    let Some(store) = connect().await else { return };
    let rule = unique_rule("real");
    let request = CheckRequest {
        rule: &rule,
        key: "k",
        limit: 3,
        window: Duration::from_secs(60),
        cost: 1,
    };
    for i in 0..3 {
        assert!(store.check(request).await.unwrap().allowed, "request {i}");
    }
    let denied = store.check(request).await.unwrap();
    assert!(!denied.allowed);
    let retry = denied.retry_after.unwrap();
    assert!(
        retry > Duration::ZERO && retry <= Duration::from_secs(60),
        "{retry:?}"
    );

    let client = redis::Client::open(redis_url()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    for key in RedisStore::keys(&rule, "k") {
        let ttl: i64 = conn.pttl(&key).await.unwrap();
        assert!((1..=60_000).contains(&ttl), "{key} has PTTL {ttl}");
    }
    delete_keys(&rule, "k").await;
}

/// Awkward caller keys must each get their own window.
#[tokio::test]
async fn odd_keys_are_isolated() {
    let Some(store) = connect().await else { return };
    let rule = unique_rule("keys");
    let long = "x".repeat(10_000);
    let keys = [
        "",
        "a",
        "a:b",
        "{",
        "}",
        "{a}",
        "}{",
        "ゲート",
        "a\0b",
        long.as_str(),
    ];
    for key in keys {
        let request = CheckRequest {
            rule: &rule,
            key,
            limit: 1,
            window: Duration::from_secs(1),
            cost: 1,
        };
        let first = store.check_at(request, 1_000_000).await.unwrap();
        let second = store.check_at(request, 1_000_001).await.unwrap();
        assert!(first.allowed, "first request for key {key:?} denied");
        assert!(!second.allowed, "second request for key {key:?} allowed");
    }
    for key in keys {
        delete_keys(&rule, key).await;
    }
}
