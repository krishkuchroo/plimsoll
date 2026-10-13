//! Observing limiter decisions.
//!
//! [`Limiter`](crate::Limiter) calls an [`Observer`] once per check, after the
//! decision is made. Implement [`Observer`] to feed your own metrics system,
//! or use the built-in [`Metrics`] collector, which keeps per-rule counters
//! and a store latency histogram and renders them in the Prometheus text
//! format. Neither needs a metrics crate dependency.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::{OnStoreError, StoreError};

/// What happened on one check. Passed to [`Observer::on_check`].
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct CheckEvent<'a> {
    /// Rule name.
    pub rule: &'a str,
    /// Caller-supplied key. Do not use it as a metric label: it is
    /// unbounded and may identify a person.
    pub key: &'a str,
    /// Cost of the request.
    pub cost: u64,
    /// Whether the request was allowed.
    pub allowed: bool,
    /// The fallback policy that produced the decision, or `None` when the
    /// store answered normally.
    pub fallback: Option<OnStoreError>,
    /// The store failure, if the store did not answer.
    pub store_error: Option<&'a StoreError>,
    /// Time spent waiting on the store, including failed calls.
    pub store_latency: Duration,
}

impl CheckEvent<'_> {
    /// Whether a fallback policy produced the decision.
    pub fn degraded(&self) -> bool {
        self.fallback.is_some()
    }
}

/// Receives one [`CheckEvent`] per check.
///
/// Called on the request path, so implementations must be cheap and must
/// not block. Panics propagate to the caller of `check`.
pub trait Observer: Send + Sync + 'static {
    /// Record one check.
    fn on_check(&self, event: &CheckEvent<'_>);
}

impl<O: Observer + ?Sized> Observer for Arc<O> {
    fn on_check(&self, event: &CheckEvent<'_>) {
        (**self).on_check(event)
    }
}

/// Upper bounds of the store latency histogram buckets, in seconds.
/// Chosen around the default 250 ms store timeout.
pub const LATENCY_BUCKETS: [f64; 11] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
];

/// Built-in [`Observer`] that counts decisions per rule.
///
/// Share it between the limiter and your `/metrics` endpoint:
///
/// ```
/// use plimsoll_core::{Limiter, LocalStore, Metrics};
/// use std::sync::Arc;
///
/// let metrics = Arc::new(Metrics::new());
/// let limiter = Limiter::builder(LocalStore::new())
///     .observer(Arc::clone(&metrics))
///     .build();
/// let text = metrics.render_prometheus();
/// # let _ = (limiter, text);
/// ```
///
/// Counters are lock-free atomics; a rule's entry is created under a write
/// lock the first time that rule is seen.
#[derive(Debug, Default)]
pub struct Metrics {
    rules: RwLock<HashMap<Arc<str>, Arc<RuleCounters>>>,
}

#[derive(Debug, Default)]
struct RuleCounters {
    allowed: AtomicU64,
    denied: AtomicU64,
    cost_allowed: AtomicU64,
    fallback_allow: AtomicU64,
    fallback_deny: AtomicU64,
    fallback_local: AtomicU64,
    err_unavailable: AtomicU64,
    err_timeout: AtomicU64,
    err_protocol: AtomicU64,
    err_other: AtomicU64,
    /// Non-cumulative counts per bucket; the last slot is +Inf.
    latency_buckets: [AtomicU64; LATENCY_BUCKETS.len() + 1],
    latency_sum_micros: AtomicU64,
    latency_count: AtomicU64,
}

/// Point-in-time counters for one rule, from [`Metrics::snapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct RuleSnapshot {
    /// Rule name.
    pub rule: String,
    /// Allowed checks.
    pub allowed: u64,
    /// Denied checks.
    pub denied: u64,
    /// Total cost of allowed checks.
    pub cost_allowed: u64,
    /// Checks decided by a fallback policy, by policy:
    /// `[Allow, Deny, LocalFallback]`.
    pub fallback: [u64; 3],
    /// Store failures by kind: `[unavailable, timeout, protocol, other]`.
    pub store_errors: [u64; 4],
    /// Store calls observed.
    pub latency_count: u64,
    /// Sum of store latency.
    pub latency_sum: Duration,
    /// Cumulative counts per [`LATENCY_BUCKETS`] bound, then +Inf.
    pub latency_buckets: Vec<u64>,
}

impl RuleSnapshot {
    /// Checks decided by any fallback policy.
    pub fn degraded(&self) -> u64 {
        self.fallback.iter().sum()
    }
}

impl Metrics {
    /// An empty collector.
    pub fn new() -> Self {
        Self::default()
    }

    fn counters(&self, rule: &str) -> Arc<RuleCounters> {
        if let Some(c) = self.read().get(rule) {
            return Arc::clone(c);
        }
        let mut rules = self.rules.write().unwrap_or_else(|e| e.into_inner());
        Arc::clone(rules.entry(Arc::from(rule)).or_default())
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<Arc<str>, Arc<RuleCounters>>> {
        self.rules.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Current counters for every rule seen so far, sorted by rule name.
    pub fn snapshot(&self) -> Vec<RuleSnapshot> {
        let mut out: Vec<RuleSnapshot> = self
            .read()
            .iter()
            .map(|(rule, c)| c.snapshot(rule))
            .collect();
        out.sort_by(|a, b| a.rule.cmp(&b.rule));
        out
    }

    /// Render every counter in the Prometheus text exposition format
    /// (version 0.0.4). Serve it with content type
    /// `text/plain; version=0.0.4`.
    ///
    /// | Metric | Type | Labels |
    /// |---|---|---|
    /// | `plimsoll_checks_total` | counter | `rule`, `outcome` = `allowed` \| `denied` |
    /// | `plimsoll_cost_allowed_total` | counter | `rule` |
    /// | `plimsoll_degraded_total` | counter | `rule`, `policy` = `allow` \| `deny` \| `local_fallback` |
    /// | `plimsoll_store_errors_total` | counter | `rule`, `kind` = `unavailable` \| `timeout` \| `protocol` \| `other` |
    /// | `plimsoll_store_latency_seconds` | histogram | `rule` |
    pub fn render_prometheus(&self) -> String {
        let snaps = self.snapshot();
        let mut s = String::new();
        let mut family = |name: &str, kind: &str, help: &str, body: &dyn Fn(&mut String)| {
            let _ = writeln!(s, "# HELP {name} {help}");
            let _ = writeln!(s, "# TYPE {name} {kind}");
            body(&mut s);
        };

        family(
            "plimsoll_checks_total",
            "counter",
            "Rate limit checks by outcome.",
            &|s| {
                for r in &snaps {
                    let rule = escape(&r.rule);
                    let _ = writeln!(
                        s,
                        "plimsoll_checks_total{{rule=\"{rule}\",outcome=\"allowed\"}} {}",
                        r.allowed
                    );
                    let _ = writeln!(
                        s,
                        "plimsoll_checks_total{{rule=\"{rule}\",outcome=\"denied\"}} {}",
                        r.denied
                    );
                }
            },
        );
        family(
            "plimsoll_cost_allowed_total",
            "counter",
            "Total cost of allowed requests.",
            &|s| {
                for r in &snaps {
                    let _ = writeln!(
                        s,
                        "plimsoll_cost_allowed_total{{rule=\"{}\"}} {}",
                        escape(&r.rule),
                        r.cost_allowed
                    );
                }
            },
        );
        family(
            "plimsoll_degraded_total",
            "counter",
            "Checks decided by a store-error fallback policy.",
            &|s| {
                for r in &snaps {
                    let rule = escape(&r.rule);
                    for (policy, n) in ["allow", "deny", "local_fallback"].iter().zip(r.fallback) {
                        let _ = writeln!(
                            s,
                            "plimsoll_degraded_total{{rule=\"{rule}\",policy=\"{policy}\"}} {n}"
                        );
                    }
                }
            },
        );
        family(
            "plimsoll_store_errors_total",
            "counter",
            "Store failures by kind.",
            &|s| {
                for r in &snaps {
                    let rule = escape(&r.rule);
                    for (kind, n) in ["unavailable", "timeout", "protocol", "other"]
                        .iter()
                        .zip(r.store_errors)
                    {
                        let _ = writeln!(
                            s,
                            "plimsoll_store_errors_total{{rule=\"{rule}\",kind=\"{kind}\"}} {n}"
                        );
                    }
                }
            },
        );
        family(
            "plimsoll_store_latency_seconds",
            "histogram",
            "Time spent waiting on the store per check.",
            &|s| {
                for r in &snaps {
                    let rule = escape(&r.rule);
                    for (bound, n) in LATENCY_BUCKETS.iter().zip(&r.latency_buckets) {
                        let _ = writeln!(
                            s,
                            "plimsoll_store_latency_seconds_bucket{{rule=\"{rule}\",le=\"{bound}\"}} {n}"
                        );
                    }
                    let _ = writeln!(
                        s,
                        "plimsoll_store_latency_seconds_bucket{{rule=\"{rule}\",le=\"+Inf\"}} {}",
                        r.latency_count
                    );
                    let _ = writeln!(
                        s,
                        "plimsoll_store_latency_seconds_sum{{rule=\"{rule}\"}} {}",
                        r.latency_sum.as_secs_f64()
                    );
                    let _ = writeln!(
                        s,
                        "plimsoll_store_latency_seconds_count{{rule=\"{rule}\"}} {}",
                        r.latency_count
                    );
                }
            },
        );
        s
    }
}

impl Observer for Metrics {
    fn on_check(&self, event: &CheckEvent<'_>) {
        self.counters(event.rule).record(event);
    }
}

impl RuleCounters {
    fn record(&self, e: &CheckEvent<'_>) {
        let inc = |c: &AtomicU64, n: u64| {
            c.fetch_add(n, Ordering::Relaxed);
        };
        if e.allowed {
            inc(&self.allowed, 1);
            inc(&self.cost_allowed, e.cost);
        } else {
            inc(&self.denied, 1);
        }
        match e.fallback {
            Some(OnStoreError::Allow) => inc(&self.fallback_allow, 1),
            Some(OnStoreError::Deny) => inc(&self.fallback_deny, 1),
            Some(OnStoreError::LocalFallback) => inc(&self.fallback_local, 1),
            None => {}
        }
        match e.store_error {
            Some(StoreError::Unavailable(_)) => inc(&self.err_unavailable, 1),
            Some(StoreError::Timeout(_)) => inc(&self.err_timeout, 1),
            Some(StoreError::Protocol(_)) => inc(&self.err_protocol, 1),
            Some(StoreError::Other(_)) => inc(&self.err_other, 1),
            None => {}
        }
        let secs = e.store_latency.as_secs_f64();
        let slot = LATENCY_BUCKETS
            .iter()
            .position(|&b| secs <= b)
            .unwrap_or(LATENCY_BUCKETS.len());
        inc(&self.latency_buckets[slot], 1);
        inc(
            &self.latency_sum_micros,
            u64::try_from(e.store_latency.as_micros()).unwrap_or(u64::MAX),
        );
        inc(&self.latency_count, 1);
    }

    fn snapshot(&self, rule: &str) -> RuleSnapshot {
        let get = |c: &AtomicU64| c.load(Ordering::Relaxed);
        let mut running = 0;
        let latency_buckets = self.latency_buckets[..LATENCY_BUCKETS.len()]
            .iter()
            .map(|b| {
                running += get(b);
                running
            })
            .collect();
        RuleSnapshot {
            rule: rule.to_owned(),
            allowed: get(&self.allowed),
            denied: get(&self.denied),
            cost_allowed: get(&self.cost_allowed),
            fallback: [
                get(&self.fallback_allow),
                get(&self.fallback_deny),
                get(&self.fallback_local),
            ],
            store_errors: [
                get(&self.err_unavailable),
                get(&self.err_timeout),
                get(&self.err_protocol),
                get(&self.err_other),
            ],
            latency_count: get(&self.latency_count),
            latency_sum: Duration::from_micros(get(&self.latency_sum_micros)),
            latency_buckets,
        }
    }
}

/// Escape a Prometheus label value.
fn escape(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
