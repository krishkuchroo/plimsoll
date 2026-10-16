use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// One check handed to a [`Store`].
///
/// The limiter resolves the rule into plain numbers so a store never needs to
/// know about [`Rule`](crate::Rule) or fallback policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckRequest<'a> {
    /// Rule name; part of the store key.
    pub rule: &'a str,
    /// Caller-supplied key (user id, API key, IP, ...); part of the store key.
    pub key: &'a str,
    /// Maximum total cost inside one window.
    pub limit: u64,
    /// Sliding window length.
    pub window: Duration,
    /// Cost of this request. Always at least 1 and at most `limit` when it
    /// comes from a [`Limiter`](crate::Limiter).
    pub cost: u64,
}

impl CheckRequest<'_> {
    /// Window length in whole microseconds, the unit stores work in.
    pub fn window_micros(&self) -> u64 {
        u64::try_from(self.window.as_micros()).unwrap_or(u64::MAX)
    }
}

/// What a [`Store`] reports back for one check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckOutcome {
    /// Whether the request was recorded and may proceed.
    pub allowed: bool,
    /// Total cost currently inside the window, including this request when
    /// it was allowed.
    pub used: u64,
    /// When denied, how long until enough cost has expired for a request of
    /// this cost to fit. `None` when allowed.
    pub retry_after: Option<Duration>,
    /// How long until the oldest recorded entry leaves the window, i.e. when
    /// `remaining` next increases. Zero when the window is empty.
    pub reset_after: Duration,
}

/// A request admitted by
/// [`OnStoreError::LocalFallback`](crate::OnStoreError::LocalFallback) while
/// the store was unreachable. The [`Limiter`](crate::Limiter) keeps these and
/// hands them to [`Store::backfill`] once the store answers again, so the
/// shared window counts them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Admitted {
    /// When it was admitted, in microseconds since the Unix epoch.
    pub at_unix_micros: u64,
    /// Its cost.
    pub cost: u64,
}

/// Why a store could not answer a check.
///
/// Backends map their own errors into these variants so that
/// [`OnStoreError`](crate::OnStoreError) can act on them uniformly. Variants
/// hold strings rather than source errors so decisions stay `Clone`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    /// The backend could not be reached or the connection dropped.
    #[error("store unavailable: {0}")]
    Unavailable(String),
    /// The backend did not answer within the configured time.
    #[error("store call timed out after {0:?}")]
    Timeout(Duration),
    /// The backend answered with something the store could not interpret.
    #[error("store returned an unexpected reply: {0}")]
    Protocol(String),
    /// Any other backend error.
    #[error("store error: {0}")]
    Other(String),
}

/// A backend that holds sliding window state.
///
/// Implementations must perform the whole read-evict-check-insert sequence
/// atomically with respect to other callers, so that concurrent checks cannot
/// race past the limit. They must also match the semantics of
/// [`WindowLog`](crate::WindowLog) exactly, so that
/// [`OnStoreError::LocalFallback`](crate::OnStoreError::LocalFallback)
/// behaves the same way as the primary store.
pub trait Store: Send + Sync {
    /// Evict expired entries, decide, and record the request if allowed.
    fn check(
        &self,
        request: CheckRequest<'_>,
    ) -> impl Future<Output = Result<CheckOutcome, StoreError>> + Send;

    /// Record requests that were admitted by the in-process fallback while
    /// this store was unreachable, so the shared window counts them.
    ///
    /// `entries` are in admission order. Implementations should skip
    /// entries already outside `window`, never record one later than their
    /// own "now", and keep the log ordered (recording an entry slightly late
    /// is acceptable: it only makes the limit stricter). The default does
    /// nothing, which suits stores that are never shared.
    fn backfill(
        &self,
        rule: &str,
        key: &str,
        window: Duration,
        entries: &[Admitted],
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        let _ = (rule, key, window, entries);
        std::future::ready(Ok(()))
    }
}

impl<S: Store + ?Sized> Store for Arc<S> {
    fn check(
        &self,
        request: CheckRequest<'_>,
    ) -> impl Future<Output = Result<CheckOutcome, StoreError>> + Send {
        (**self).check(request)
    }

    fn backfill(
        &self,
        rule: &str,
        key: &str,
        window: Duration,
        entries: &[Admitted],
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        (**self).backfill(rule, key, window, entries)
    }
}

impl<S: Store + ?Sized> Store for &S {
    fn check(
        &self,
        request: CheckRequest<'_>,
    ) -> impl Future<Output = Result<CheckOutcome, StoreError>> + Send {
        (**self).check(request)
    }

    fn backfill(
        &self,
        rule: &str,
        key: &str,
        window: Duration,
        entries: &[Admitted],
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        (**self).backfill(rule, key, window, entries)
    }
}
