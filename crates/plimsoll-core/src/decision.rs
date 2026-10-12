use std::sync::Arc;
use std::time::Duration;

use crate::{CheckOutcome, StoreError};

/// The result of checking a request against a rule.
///
/// Carries everything an HTTP layer needs: whether to proceed, how much
/// quota is left, when to retry, and whether the answer came from a
/// degraded path. [`Decision::headers`] renders the standard headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    /// Whether the request may proceed.
    pub allowed: bool,
    /// Name of the rule that produced this decision.
    pub rule: Arc<str>,
    /// The limit that was enforced. Equals the rule's limit, except under
    /// [`OnStoreError::LocalFallback`](crate::OnStoreError::LocalFallback)
    /// where it is this instance's share of the limit.
    pub limit: u64,
    /// Cost of the request that was checked.
    pub cost: u64,
    /// Length of the rule's window.
    pub window: Duration,
    /// Cost still available inside the current window.
    pub remaining: u64,
    /// When denied, how long until a request of this cost would fit.
    /// `None` when allowed.
    pub retry_after: Option<Duration>,
    /// How long until the oldest entry leaves the window.
    pub reset_after: Duration,
    /// `true` when the store failed and an [`OnStoreError`](crate::OnStoreError)
    /// policy produced this decision instead.
    pub degraded: bool,
    /// The store failure that caused degradation, if any.
    pub store_error: Option<StoreError>,
}

impl Decision {
    /// Build a decision from a store outcome.
    pub(crate) fn from_outcome(
        rule: Arc<str>,
        limit: u64,
        cost: u64,
        window: Duration,
        outcome: CheckOutcome,
    ) -> Self {
        Self {
            allowed: outcome.allowed,
            rule,
            limit,
            cost,
            window,
            remaining: limit.saturating_sub(outcome.used),
            retry_after: outcome.retry_after,
            reset_after: outcome.reset_after,
            degraded: false,
            store_error: None,
        }
    }

    /// Whether the request was denied. Convenience for `!allowed`.
    pub fn denied(&self) -> bool {
        !self.allowed
    }

    /// `Retry-After` value in whole seconds, rounded up and never below 1.
    /// `None` when the request was allowed.
    pub fn retry_after_secs(&self) -> Option<u64> {
        self.retry_after.map(|d| ceil_secs(d).max(1))
    }

    /// HTTP headers describing this decision.
    ///
    /// Always includes the IETF `RateLimit-Policy` and `RateLimit` fields
    /// (draft-ietf-httpapi-ratelimit-headers, structured-field form):
    ///
    /// ```text
    /// RateLimit-Policy: "api";q=100;w=60
    /// RateLimit: "api";r=42;t=17
    /// ```
    ///
    /// On denial it also includes `Retry-After` and `Cache-Control: no-store`
    /// as RFC 6585 §4 recommends for a 429 response. The application still
    /// chooses the status code and writes the body.
    pub fn headers(&self) -> Vec<(&'static str, String)> {
        let mut headers = Vec::with_capacity(4);
        headers.push((
            "RateLimit-Policy",
            format!(
                "\"{}\";q={};w={}",
                self.rule,
                self.limit,
                ceil_secs(self.window).max(1)
            ),
        ));
        headers.push((
            "RateLimit",
            format!(
                "\"{}\";r={};t={}",
                self.rule,
                self.remaining,
                ceil_secs(self.reset_after)
            ),
        ));
        if let Some(secs) = self.retry_after_secs() {
            headers.push(("Retry-After", secs.to_string()));
            headers.push(("Cache-Control", "no-store".to_owned()));
        }
        headers
    }
}

fn ceil_secs(d: Duration) -> u64 {
    let secs = d.as_secs();
    if d.subsec_nanos() > 0 { secs + 1 } else { secs }
}
