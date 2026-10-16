//! Stage 4 of TESTING.md: what the limiter does when Redis misbehaves.
//!
//! Every test puts a `FaultProxy` between `RedisStore` and Redis and breaks
//! the link in a different way: refused connections, latency above and below
//! the timeout, a blackhole, cut connections, a flushed script cache, lost
//! state. Checks that each `OnStoreError` policy is applied, that slow Redis
//! never stalls a request much past the timeout, that `LocalFallback` stays
//! within its share, and that the limiter recovers once Redis is back.

mod support;

use std::time::{Duration, Instant};

use plimsoll_core::{Decision, Limiter, OnStoreError, Rule, StoreError};
use plimsoll_redis::redis::{self, AsyncCommands};
use plimsoll_redis::{DEFAULT_PREFIX, RedisConfig, RedisStore};
use support::proxy::{FaultProxy, Mode, upstream_of};
use support::{connect, delete_keys, redis_url, unique_rule};

/// Per-call timeout for these tests. Latency faults are chosen well clear of
/// it (50 ms below, 500 ms above) so timing noise can't flip a result.
const TIMEOUT: Duration = Duration::from_millis(200);
const WINDOW: Duration = Duration::from_secs(60);

struct Setup {
    proxy: FaultProxy,
    limiter: Limiter<RedisStore>,
}

/// Proxy + limiter, or `None` when there is no Redis to proxy to.
async fn setup(instances: u32, policy: OnStoreError) -> Option<Setup> {
    connect().await?;
    let proxy = FaultProxy::start(upstream_of(&redis_url())).await;
    let config = RedisConfig::default()
        .timeout(TIMEOUT)
        .connect_timeout(TIMEOUT)
        .lazy(true);
    let store = RedisStore::connect_with(proxy.url(), config).await.unwrap();
    let limiter = Limiter::builder(store)
        .instances(instances)
        .on_store_error(policy)
        .build();
    Some(Setup { proxy, limiter })
}

fn rule(name: &str, limit: u64, policy: Option<OnStoreError>) -> Rule {
    let mut b = Rule::builder(name).limit(limit).window(WINDOW);
    if let Some(p) = policy {
        b = b.on_store_error(p);
    }
    b.build().unwrap()
}

async fn check(limiter: &Limiter<RedisStore>, rule: &Rule) -> (Decision, Duration) {
    let started = Instant::now();
    let d = limiter.check(rule, "k").await.unwrap();
    (d, started.elapsed())
}

/// Keep checking until a decision comes from Redis again. Returns how many
/// checks it took; panics if it doesn't recover within ~3 s.
async fn wait_for_recovery(limiter: &Limiter<RedisStore>, rule: &Rule) -> u32 {
    for attempt in 1..=30 {
        let (d, _) = check(limiter, rule).await;
        if !d.degraded {
            return attempt;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("limiter did not recover from the outage within 30 checks");
}

/// Redis unreachable from the first request: each policy does what it says.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unreachable_redis_applies_each_policy() {
    let Some(s) = setup(4, OnStoreError::LocalFallback).await else {
        return;
    };
    s.proxy.set(Mode::Refuse);
    let name = unique_rule("down");

    // Allow: everything through.
    let allow = rule(&format!("{name}-allow"), 20, Some(OnStoreError::Allow));
    for _ in 0..25 {
        let (d, _) = check(&s.limiter, &allow).await;
        assert!(d.allowed && d.degraded);
        assert!(
            matches!(
                d.store_error,
                Some(StoreError::Unavailable(_) | StoreError::Timeout(_))
            ),
            "{:?}",
            d.store_error
        );
    }

    // Deny: nothing through, retry after the short default (5 s), not the window.
    let deny = rule(&format!("{name}-deny"), 20, Some(OnStoreError::Deny));
    for _ in 0..5 {
        let (d, _) = check(&s.limiter, &deny).await;
        assert!(!d.allowed && d.degraded);
        assert_eq!(d.retry_after, Some(plimsoll_core::DEFAULT_DENY_RETRY_AFTER));
    }

    // LocalFallback: exactly this instance's share, ceil(20 / 4) = 5.
    let fallback = rule(&format!("{name}-fallback"), 20, None);
    let mut admitted = 0;
    for _ in 0..20 {
        let (d, _) = check(&s.limiter, &fallback).await;
        assert!(d.degraded);
        assert_eq!(d.limit, 5, "fallback reports the per-instance share");
        admitted += u32::from(d.allowed);
    }
    assert_eq!(admitted, 5);
}

/// Latency under the timeout is just slower, not a failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn latency_below_timeout_is_not_degraded() {
    let Some(s) = setup(1, OnStoreError::Deny).await else {
        return;
    };
    let name = unique_rule("slow-ok");
    let r = rule(&name, 5, None);
    s.proxy.set(Mode::Latency(Duration::from_millis(50)));
    for _ in 0..3 {
        let (d, elapsed) = check(&s.limiter, &r).await;
        assert!(!d.degraded, "{:?}", d.store_error);
        assert!(elapsed >= Duration::from_millis(50));
    }
    delete_keys(&name, "k").await;
}

/// Latency over the timeout, and a blackhole, both turn into a quick
/// `Timeout` decision instead of a stalled request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_or_silent_redis_times_out_quickly() {
    let Some(s) = setup(1, OnStoreError::Deny).await else {
        return;
    };
    let name = unique_rule("slow-bad");
    let r = rule(&name, 5, None);
    // Warm the connection so we measure request timeouts, not connects.
    assert!(!check(&s.limiter, &r).await.0.degraded);

    for mode in [Mode::Latency(Duration::from_millis(500)), Mode::Blackhole] {
        s.proxy.set(mode);
        let (d, elapsed) = check(&s.limiter, &r).await;
        assert!(d.degraded && !d.allowed, "{mode:?}");
        assert!(
            matches!(d.store_error, Some(StoreError::Timeout(_))),
            "{mode:?}: {:?}",
            d.store_error
        );
        assert!(
            elapsed < TIMEOUT * 2,
            "{mode:?}: request took {elapsed:?} with a {TIMEOUT:?} timeout"
        );
        // A blackholed or late reply leaves the connection in an unknown
        // state; start the next mode from a fresh one.
        s.proxy.set(Mode::Pass);
        s.proxy.cut();
        wait_for_recovery(&s.limiter, &r).await;
    }
    delete_keys(&name, "k").await;
}

/// Connections dropped under a healthy Redis: the limiter reconnects on its
/// own within a few checks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovers_after_connections_are_cut() {
    let Some(s) = setup(1, OnStoreError::Deny).await else {
        return;
    };
    let name = unique_rule("cut");
    let r = rule(&name, 1_000, None);
    assert!(!check(&s.limiter, &r).await.0.degraded);

    for _ in 0..3 {
        s.proxy.cut();
        let attempts = wait_for_recovery(&s.limiter, &r).await;
        assert!(attempts <= 3, "took {attempts} checks to reconnect");
    }
    delete_keys(&name, "k").await;
}

/// Full outage cycle with `LocalFallback`, limit 20 across 4 instances:
///
/// 1. healthy: 8 admitted by Redis
/// 2. outage: this instance admits its share of 5 from memory
/// 3. recovered: Redis still holds 8 (fallback requests were never written
///    to it), so 12 more are admitted before the shared limit bites
///
/// Total 25 = limit + one instance's share. That is the documented bound
/// on fallback overshoot (PLAN.md, Known limitations).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outage_cycle_overshoot_is_bounded() {
    let Some(s) = setup(4, OnStoreError::LocalFallback).await else {
        return;
    };
    let name = unique_rule("cycle");
    let r = rule(&name, 20, None);

    let mut healthy = 0;
    for _ in 0..8 {
        let (d, _) = check(&s.limiter, &r).await;
        assert!(!d.degraded && d.allowed);
        healthy += 1;
    }

    s.proxy.set(Mode::Refuse);
    s.proxy.cut();
    let mut during = 0;
    for _ in 0..20 {
        let (d, _) = check(&s.limiter, &r).await;
        assert!(d.degraded);
        during += u32::from(d.allowed);
    }
    assert_eq!(during, 5);

    s.proxy.set(Mode::Pass);
    // The check that ends the wait is the first healthy one, and it is
    // admitted (Redis holds 8 of 20). Degraded checks while waiting are
    // denied because the fallback share is already used up.
    wait_for_recovery(&s.limiter, &r).await;
    let mut after = 1;
    for _ in 0..30 {
        let (d, _) = check(&s.limiter, &r).await;
        assert!(!d.degraded, "{:?}", d.store_error);
        after += u32::from(d.allowed);
    }
    delete_keys(&name, "k").await;

    assert_eq!(healthy, 8);
    assert_eq!(after, 12, "Redis should continue from its own count of 8");
    assert!(healthy + during + after <= 20 + 5);
}

/// `SCRIPT FLUSH` (e.g. after a Redis restart with a different config) is
/// invisible: the script is reloaded and the count continues.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn script_flush_is_transparent() {
    let Some(s) = setup(1, OnStoreError::Deny).await else {
        return;
    };
    let name = unique_rule("flush");
    let r = rule(&name, 5, None);
    assert_eq!(check(&s.limiter, &r).await.0.remaining, 4);

    let client = redis::Client::open(redis_url()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("SCRIPT")
        .arg("FLUSH")
        .query_async(&mut conn)
        .await
        .unwrap();

    let (d, _) = check(&s.limiter, &r).await;
    assert!(!d.degraded, "{:?}", d.store_error);
    assert_eq!(d.remaining, 3);
    delete_keys(&name, "k").await;
}

/// Losing the keys (Redis restarted without persistence, or evicted them
/// under memory pressure) resets the window. Documents current behaviour.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_state_resets_the_window() {
    let Some(s) = setup(1, OnStoreError::Deny).await else {
        return;
    };
    let name = unique_rule("lost");
    let r = rule(&name, 3, None);
    for _ in 0..3 {
        assert!(check(&s.limiter, &r).await.0.allowed);
    }
    assert!(!check(&s.limiter, &r).await.0.allowed);

    let client = redis::Client::open(redis_url()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = conn
        .del(&RedisStore::keys_with_prefix(DEFAULT_PREFIX, &name, "k"))
        .await
        .unwrap();

    let (d, _) = check(&s.limiter, &r).await;
    assert!(d.allowed && !d.degraded);
    assert_eq!(d.remaining, 2);
    delete_keys(&name, "k").await;
}
