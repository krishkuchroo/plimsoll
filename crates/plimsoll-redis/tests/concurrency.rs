//! Stage 3 of TESTING.md: concurrent checks against Redis must never
//! over-admit, and must not under-admit either.
//!
//! Each test opens several independent connections ("instances"), lines up
//! hundreds of tasks behind a barrier, and releases them at once on a
//! multi-threaded runtime. Uses the production path (Redis `TIME`, real
//! expiry) with a 60 s window, so nothing expires mid-test.

mod support;

use std::sync::Arc;
use std::time::Duration;

use plimsoll_core::{Limiter, OnStoreError, Rule};
use plimsoll_redis::RedisStore;
use support::{connect, delete_keys, unique_rule};
use tokio::sync::Barrier;

const INSTANCES: usize = 4;

/// One limiter per simulated instance, each with its own Redis connection.
async fn instances() -> Option<Vec<Arc<Limiter<RedisStore>>>> {
    let mut out = Vec::new();
    for _ in 0..INSTANCES {
        let store = connect().await?;
        out.push(Arc::new(
            Limiter::builder(store)
                .instances(INSTANCES as u32)
                // A store error here is a test failure, not something to absorb.
                .on_store_error(OnStoreError::Deny)
                .build(),
        ));
    }
    Some(out)
}

fn rule(name: &str, limit: u64) -> Rule {
    Rule::builder(name)
        .limit(limit)
        .window(Duration::from_secs(60))
        .build()
        .unwrap()
}

/// Fire `requests` (key, cost) checks at once, spread round-robin over the
/// instances. Returns `(key, cost, used_after, allowed)` per request.
async fn burst(
    limiters: &[Arc<Limiter<RedisStore>>],
    rule: &Rule,
    requests: Vec<(String, u64)>,
) -> Vec<(String, u64, u64, bool)> {
    let barrier = Arc::new(Barrier::new(requests.len()));
    let mut handles = Vec::new();
    for (i, (key, cost)) in requests.into_iter().enumerate() {
        let limiter = Arc::clone(&limiters[i % limiters.len()]);
        let rule = rule.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let d = limiter.check_cost(&rule, &key, cost).await.unwrap();
            assert!(!d.degraded, "store failed: {:?}", d.store_error);
            (key, cost, d.limit - d.remaining, d.allowed)
        }));
    }
    let mut out = Vec::new();
    for h in handles {
        out.push(h.await.unwrap());
    }
    out
}

/// 400 unit requests, limit 100: exactly 100 admitted, and the admitted
/// requests saw `used` = 1, 2, ..., 100 exactly once each, which shows every
/// check was serialised.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn one_key_exact_under_contention() {
    let Some(limiters) = instances().await else {
        return;
    };
    let name = unique_rule("one-key");
    let r = rule(&name, 100);

    let results = burst(&limiters, &r, vec![("k".to_owned(), 1); 400]).await;
    delete_keys(&name, "k").await;

    let mut used: Vec<u64> = results.iter().filter(|x| x.3).map(|x| x.2).collect();
    used.sort_unstable();
    assert_eq!(used, (1..=100).collect::<Vec<_>>());
    assert!(results.iter().filter(|x| !x.3).all(|x| x.2 == 100));
}

/// Weighted costs: admitted cost never exceeds the limit, and every denied
/// request really did not fit at the end (no false denials).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn weighted_costs_never_exceed_limit() {
    let Some(limiters) = instances().await else {
        return;
    };
    let name = unique_rule("weighted");
    let limit = 100;
    let r = rule(&name, limit);
    // Deterministic spread of costs 1..=10.
    let requests: Vec<_> = (0..300u64)
        .map(|i| ("k".to_owned(), 1 + (i * 7) % 10))
        .collect();

    let results = burst(&limiters, &r, requests).await;
    delete_keys(&name, "k").await;

    let admitted: u64 = results.iter().filter(|x| x.3).map(|x| x.1).sum();
    assert!(
        admitted <= limit,
        "admitted cost {admitted} > limit {limit}"
    );
    for (_, cost, _, allowed) in &results {
        if !allowed {
            assert!(
                admitted + cost > limit,
                "a cost-{cost} request was denied but {admitted} + {cost} fits in {limit}"
            );
        }
    }
}

/// 50 keys hammered at once: each gets exactly its own limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn many_keys_stay_isolated() {
    let Some(limiters) = instances().await else {
        return;
    };
    let name = unique_rule("many-keys");
    let r = rule(&name, 10);
    let keys: Vec<String> = (0..50).map(|i| format!("user-{i}")).collect();
    let requests: Vec<_> = (0..30)
        .flat_map(|_| keys.iter().map(|k| (k.clone(), 1)))
        .collect();

    let results = burst(&limiters, &r, requests).await;
    for key in &keys {
        delete_keys(&name, key).await;
    }

    for key in &keys {
        let n = results.iter().filter(|x| &x.0 == key && x.3).count();
        assert_eq!(n, 10, "key {key} admitted {n}");
    }
}
