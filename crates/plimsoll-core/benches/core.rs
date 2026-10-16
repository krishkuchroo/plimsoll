//! In-process costs: the reference algorithm, the local store, the limiter
//! wrapper, and header rendering. No I/O.
//!
//! ```sh
//! cargo bench -p plimsoll-core
//! ```

use std::future::Future;
use std::hint::black_box;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use plimsoll_core::{CheckRequest, Limiter, LocalStore, Rule, WindowLog};

const WINDOW: u64 = 60_000_000;

/// A log already holding `n` unit entries, all inside the window.
fn full_log(n: u64) -> WindowLog {
    let mut log = WindowLog::new();
    for i in 0..n {
        log.check(i, WINDOW, n, 1);
    }
    log
}

fn window_log(c: &mut Criterion) {
    let mut g = c.benchmark_group("window_log");
    for n in [10u64, 1_000, 100_000] {
        // Allowed: steady state where one entry expires and one is added.
        g.bench_with_input(BenchmarkId::new("allow_steady", n), &n, |b, &n| {
            let mut log = full_log(n);
            let mut now = WINDOW;
            b.iter(|| {
                now += 1;
                black_box(log.check(now, WINDOW, n, 1))
            });
        });
        // Denied with cost 1: retry_after only needs the oldest entry.
        g.bench_with_input(BenchmarkId::new("deny_cost_1", n), &n, |b, &n| {
            let mut log = full_log(n);
            b.iter(|| black_box(log.check(n, WINDOW, n, 1)));
        });
        // Denied with cost = limit: retry_after walks the whole log.
        g.bench_with_input(BenchmarkId::new("deny_cost_limit", n), &n, |b, &n| {
            let mut log = full_log(n);
            b.iter(|| black_box(log.check(n, WINDOW, n, n)));
        });
    }
    g.finish();
}

fn local_store(c: &mut Criterion) {
    let mut g = c.benchmark_group("local_store");
    let window = Duration::from_secs(60);
    g.bench_function("one_key", |b| {
        let store = LocalStore::new();
        b.iter(|| {
            black_box(store.check_sync(CheckRequest {
                rule: "api",
                key: "user-1",
                limit: u64::MAX >> 11,
                window,
                cost: 1,
            }))
        });
    });
    g.bench_function("10k_keys_round_robin", |b| {
        let store = LocalStore::new();
        let keys: Vec<String> = (0..10_000).map(|i| format!("user-{i}")).collect();
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % keys.len();
            black_box(store.check_sync(CheckRequest {
                rule: "api",
                key: &keys[i],
                limit: 1_000_000,
                window,
                cost: 1,
            }))
        });
    });
    g.finish();
}

fn limiter(c: &mut Criterion) {
    let rule = Rule::builder("api")
        .limit(1 << 40)
        .window(Duration::from_secs(60))
        .build()
        .unwrap();
    let limiter = Arc::new(Limiter::new(LocalStore::new()));
    c.bench_function("limiter/check_local", |b| {
        b.iter(|| black_box(ready(limiter.check(&rule, "user-1")).unwrap()));
    });
    let decision = ready(limiter.check(&rule, "user-1")).unwrap();
    c.bench_function("decision/headers", |b| {
        b.iter(|| black_box(decision.headers()));
    });
}

/// Poll a future that `LocalStore` always completes immediately, without
/// the cost of building an async runtime per call.
fn ready<F: Future>(fut: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    match std::pin::pin!(fut).poll(&mut cx) {
        Poll::Ready(out) => out,
        Poll::Pending => panic!("LocalStore future was not ready"),
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));
    targets = window_log, local_store, limiter
}
criterion_main!(benches);
