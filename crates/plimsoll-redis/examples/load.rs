//! Load test and memory report for `RedisStore`.
//!
//! Drives `Limiter::check` from many concurrent tasks over several
//! connections for a fixed time, then reports throughput and latency
//! percentiles. Afterwards it measures Redis memory per key at a few limit
//! sizes with `MEMORY USAGE`.
//!
//! ```sh
//! cargo run --release -p plimsoll-redis --example load
//! ```
//!
//! | Variable | Default | Meaning |
//! |---|---|---|
//! | `PLIMSOLL_REDIS_URL` | `redis://127.0.0.1:6379/` | Redis to test against |
//! | `CONCURRENCY` | `64` | concurrent tasks |
//! | `CONNECTIONS` | `4` | Redis connections (simulated instances) |
//! | `SECONDS` | `10` | test duration |
//! | `KEYS` | `1000` | distinct keys, picked round-robin |
//! | `LIMIT` | `100` | requests per key per window |
//! | `WINDOW_SECS` | `60` | window length |
//!
//! Writes only keys under the rule `load-<pid>` and deletes them afterwards.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use plimsoll_core::{CheckRequest, Limiter, OnStoreError, Rule, Store as _};
use plimsoll_redis::RedisStore;
use plimsoll_redis::redis::{self, AsyncCommands};

fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env("PLIMSOLL_REDIS_URL", "redis://127.0.0.1:6379/".to_owned());
    let concurrency: usize = env("CONCURRENCY", 64);
    let connections: usize = env("CONNECTIONS", 4);
    let seconds: u64 = env("SECONDS", 10);
    let keys: usize = env("KEYS", 1000);
    let limit: u64 = env("LIMIT", 100);
    let window = Duration::from_secs(env("WINDOW_SECS", 60));
    let rule_name = format!("load-{}", std::process::id());
    let rule = Rule::builder(&rule_name)
        .limit(limit)
        .window(window)
        .on_store_error(OnStoreError::Deny)
        .build()?;

    let mut limiters = Vec::new();
    for _ in 0..connections {
        let store = RedisStore::connect(url.as_str()).await?;
        limiters.push(Arc::new(Limiter::new(store)));
    }
    let key_names: Arc<Vec<String>> = Arc::new((0..keys).map(|i| format!("user-{i}")).collect());
    let next_key = Arc::new(AtomicU64::new(0));
    let (allowed, denied, degraded) = (
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );

    println!(
        "load: {concurrency} tasks, {connections} connections, {keys} keys, \
         limit {limit}/{window:?}, {seconds}s"
    );
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let started = Instant::now();
    let mut tasks = Vec::new();
    for t in 0..concurrency {
        let limiter = Arc::clone(&limiters[t % connections]);
        let (rule, key_names, next_key) =
            (rule.clone(), Arc::clone(&key_names), Arc::clone(&next_key));
        let (allowed, denied, degraded) = (
            Arc::clone(&allowed),
            Arc::clone(&denied),
            Arc::clone(&degraded),
        );
        tasks.push(tokio::spawn(async move {
            let mut latencies = Vec::with_capacity(100_000);
            while Instant::now() < deadline {
                let i = next_key.fetch_add(1, Ordering::Relaxed) as usize % key_names.len();
                let t0 = Instant::now();
                let d = limiter.check(&rule, &key_names[i]).await.unwrap();
                latencies.push(t0.elapsed());
                let counter = if d.degraded {
                    &degraded
                } else if d.allowed {
                    &allowed
                } else {
                    &denied
                };
                counter.fetch_add(1, Ordering::Relaxed);
            }
            latencies
        }));
    }
    let mut latencies = Vec::new();
    for task in tasks {
        latencies.extend(task.await?);
    }
    let elapsed = started.elapsed();
    latencies.sort_unstable();

    let total = latencies.len();
    let pct = |p: f64| latencies[((total as f64 * p) as usize).min(total - 1)];
    println!(
        "checks: {total} in {elapsed:.1?} = {:.0}/s",
        total as f64 / elapsed.as_secs_f64()
    );
    println!(
        "allowed {}, denied {}, degraded {}",
        allowed.load(Ordering::Relaxed),
        denied.load(Ordering::Relaxed),
        degraded.load(Ordering::Relaxed)
    );
    println!(
        "latency p50 {:?}  p90 {:?}  p99 {:?}  p99.9 {:?}  max {:?}",
        pct(0.50),
        pct(0.90),
        pct(0.99),
        pct(0.999),
        latencies[total - 1]
    );

    let client = redis::Client::open(url.as_str())?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    for key in key_names.iter() {
        let _: () = conn.del(&RedisStore::keys(&rule_name, key)).await?;
    }

    println!("\nmemory per (rule, key), all three Redis keys:");
    let store = limiters[0].store();
    for n in [10u64, 100, 1_000, 10_000] {
        let name = format!("{rule_name}-mem-{n}");
        let request = CheckRequest {
            rule: &name,
            key: "k",
            limit: n,
            window: Duration::from_secs(3600),
            cost: 1,
        };
        for _ in 0..n {
            store.check(request).await?;
        }
        let mut bytes = 0u64;
        for k in RedisStore::keys(&name, "k") {
            let used: Option<u64> = redis::cmd("MEMORY")
                .arg("USAGE")
                .arg(&k)
                .query_async(&mut conn)
                .await?;
            bytes += used.unwrap_or(0);
        }
        let _: () = conn.del(&RedisStore::keys(&name, "k")).await?;
        println!(
            "  limit {n:>6}: {bytes:>8} bytes  ({:.1} bytes per entry)",
            bytes as f64 / n as f64
        );
    }
    Ok(())
}
