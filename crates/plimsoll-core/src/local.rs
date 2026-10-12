use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;
use std::time::Instant;

use crate::{CheckOutcome, CheckRequest, Store, StoreError, WindowLog};

/// How many checks happen between sweeps of fully expired keys.
const SWEEP_EVERY: u64 = 1024;

/// In-process sliding window store.
///
/// Suitable on its own for a single-instance service, and used by
/// [`Limiter`](crate::Limiter) to enforce a per-instance share of the limit
/// when the primary store is unreachable
/// ([`OnStoreError::LocalFallback`](crate::OnStoreError::LocalFallback)).
///
/// Time comes from a monotonic clock, so it never goes backwards. Expired
/// keys are swept periodically so memory is bounded by the number of keys
/// active inside one window.
#[derive(Debug)]
pub struct LocalStore {
    epoch: Instant,
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    logs: HashMap<String, Entry>,
    checks: u64,
}

#[derive(Debug)]
struct Entry {
    log: WindowLog,
    window: u64,
}

impl Default for LocalStore {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Number of `(rule, key)` pairs currently held, including ones whose
    /// entries have all expired but have not been swept yet.
    pub fn len(&self) -> usize {
        self.lock().logs.len()
    }

    /// Whether no keys are held.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drop every key. Useful when the primary store comes back.
    pub fn clear(&self) {
        self.lock().logs.clear();
    }

    /// Synchronous check; the async [`Store`] impl just wraps this.
    pub fn check_sync(&self, request: CheckRequest<'_>) -> CheckOutcome {
        let now = self.now_micros();
        let window = request.window_micros();
        let id = format!("{}\u{0}{}", request.rule, request.key);

        let mut inner = self.lock();
        inner.checks = inner.checks.wrapping_add(1);
        if inner.checks % SWEEP_EVERY == 0 {
            inner.sweep(now);
        }
        let entry = inner.logs.entry(id).or_insert_with(|| Entry {
            log: WindowLog::new(),
            window,
        });
        entry.window = window;
        entry.log.check(now, window, request.limit, request.cost)
    }

    fn now_micros(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_micros()).unwrap_or(u64::MAX)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // The data stays consistent even if a holder panicked between
        // statements, because every mutation is a single WindowLog::check.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Inner {
    fn sweep(&mut self, now: u64) {
        self.logs.retain(|_, entry| match entry.log.newest() {
            Some(newest) => newest + entry.window > now,
            None => false,
        });
    }
}

impl Store for LocalStore {
    fn check(
        &self,
        request: CheckRequest<'_>,
    ) -> impl Future<Output = Result<CheckOutcome, StoreError>> + Send {
        std::future::ready(Ok(self.check_sync(request)))
    }
}
