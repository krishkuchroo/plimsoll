//! Cost of one check against a real Redis: allowed, and denied at several
//! limit sizes (a denial reads the key's whole log to compute `retry_after`).
//!
//! ```sh
//! PLIMSOLL_REDIS_URL=redis://127.0.0.1:6379/ cargo bench -p plimsoll-redis
//! ```
//!
//! Prints a message and exits if Redis is not reachable. Uses the test clock
//! (`check_at`) to build large windows quickly, and deletes its keys after.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use plimsoll_core::{CheckRequest, Store};
use plimsoll_redis::redis::{self, AsyncCommands};
use plimsoll_redis::{DEFAULT_PREFIX, RedisStore};

fn url() -> String {
    std::env::var("PLIMSOLL_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into())
}

fn request<'a>(rule: &'a str, limit: u64, cost: u64) -> CheckRequest<'a> {
    CheckRequest {
        rule,
        key: "k",
        limit,
        window: Duration::from_secs(3600),
        cost,
    }
}

async fn cleanup(rule: &str) {
    let client = redis::Client::open(url()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = conn
        .del(&RedisStore::keys_with_prefix(DEFAULT_PREFIX, rule, "k"))
        .await
        .unwrap();
}

fn redis_check(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let Ok(store) = rt.block_on(RedisStore::connect(url())) else {
        eprintln!("SKIPPED: Redis not reachable at {}", url());
        return;
    };

    let mut g = c.benchmark_group("redis");
    g.bench_function("allow", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                store
                    .check(request("bench-allow", 1 << 50, 1))
                    .await
                    .unwrap(),
            )
        });
    });
    rt.block_on(cleanup("bench-allow"));

    for n in [10u64, 1_000, 10_000] {
        let rule = format!("bench-deny-{n}");
        // Fill the window with the test clock so all n entries stay inside it.
        rt.block_on(async {
            for i in 0..n {
                store
                    .check_at(request(&rule, n, 1), 1_000_000 + i)
                    .await
                    .unwrap();
            }
        });
        let now = 1_000_000 + n;
        for (label, cost) in [("deny_cost_1", 1), ("deny_cost_limit", n)] {
            g.bench_with_input(BenchmarkId::new(label, n), &n, |b, &n| {
                b.to_async(&rt).iter(|| async {
                    let out = store.check_at(request(&rule, n, cost), now).await.unwrap();
                    assert!(!out.allowed);
                    black_box(out)
                });
            });
        }
        rt.block_on(cleanup(&rule));
    }
    g.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3));
    targets = redis_check
}
criterion_main!(benches);
