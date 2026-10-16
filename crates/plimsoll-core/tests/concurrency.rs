//! Stage 3 of TESTING.md, in-process half: `LocalStore` and the
//! `LocalFallback` path under many OS threads.

use std::future::Future;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use plimsoll_core::{
    CheckOutcome, CheckRequest, Limiter, LocalStore, OnStoreError, Rule, Store, StoreError,
};

const THREADS: usize = 8;

/// Run `per_thread` calls of `f` on each of `THREADS` threads, all released
/// together, and collect the results.
fn hammer<T: Send + 'static>(
    per_thread: usize,
    f: impl Fn() -> T + Send + Sync + 'static,
) -> Vec<T> {
    let f = Arc::new(f);
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let (f, barrier) = (Arc::clone(&f), Arc::clone(&barrier));
            thread::spawn(move || {
                barrier.wait();
                (0..per_thread).map(|_| f()).collect::<Vec<_>>()
            })
        })
        .collect();
    handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect()
}

/// 8000 checks, limit 1000: exactly 1000 admitted, each seeing a distinct
/// `used` from 1 to 1000.
#[test]
fn local_store_exact_under_contention() {
    let store = Arc::new(LocalStore::new());
    let results = hammer(1000, move || {
        store.check_sync(CheckRequest {
            rule: "api",
            key: "k",
            limit: 1000,
            window: Duration::from_secs(60),
            cost: 1,
        })
    });
    let mut used: Vec<u64> = results
        .iter()
        .filter(|o| o.allowed)
        .map(|o| o.used)
        .collect();
    used.sort_unstable();
    assert_eq!(used, (1..=1000).collect::<Vec<_>>());
}

struct Down;

impl Store for Down {
    fn check(
        &self,
        _request: CheckRequest<'_>,
    ) -> impl Future<Output = Result<CheckOutcome, StoreError>> + Send {
        std::future::ready(Err(StoreError::Unavailable("down".into())))
    }
}

/// Store down, 4 instances, limit 100: this instance admits exactly its
/// share, `ceil(100 / 4) = 25`, no matter how many threads race.
#[test]
fn local_fallback_admits_exact_share() {
    let limiter = Arc::new(
        Limiter::builder(Down)
            .instances(4)
            .on_store_error(OnStoreError::LocalFallback)
            .build(),
    );
    let rule = Rule::builder("api")
        .limit(100)
        .window(Duration::from_secs(60))
        .build()
        .unwrap();
    let results = hammer(50, move || {
        let d = tokio_test::block_on(limiter.check(&rule, "k")).unwrap();
        assert!(d.degraded);
        d.allowed
    });
    assert_eq!(results.iter().filter(|&&a| a).count(), 25);
}
