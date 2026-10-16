//! Regression suite for `Limiter`, `Decision::headers` and `Rule` validation.
//!
//! A scripted store returns fixed outcomes or errors, so every combination of
//! fallback policy, store error, instance count and rule shape produces
//! deterministic output. The rendered table is compared with
//! `tests/golden/limiter/*.txt`.
//!
//! ```sh
//! PLIMSOLL_BLESS=1 cargo test -p plimsoll-core --test regression_limiter   # accept new output
//! ```

mod common;

use std::fmt::Write as _;
use std::future::Future;
use std::time::Duration;

use common::assert_golden;
use plimsoll_core::{
    CheckOutcome, CheckRequest, Decision, Limiter, OnStoreError, Rule, Store, StoreError,
};

/// Store that always answers with the same result.
struct Scripted(Result<CheckOutcome, StoreError>);

impl Store for Scripted {
    fn check(
        &self,
        _request: CheckRequest<'_>,
    ) -> impl Future<Output = Result<CheckOutcome, StoreError>> + Send {
        std::future::ready(self.0.clone())
    }
}

fn rule(name: &str, limit: u64, window: Duration, policy: Option<OnStoreError>) -> Rule {
    let mut b = Rule::builder(name).limit(limit).window(window);
    if let Some(p) = policy {
        b = b.on_store_error(p);
    }
    b.build().unwrap()
}

fn render(d: &Decision) -> String {
    let headers: Vec<String> = d
        .headers()
        .iter()
        .map(|(k, v)| format!("{k}: {v}"))
        .collect();
    format!(
        "allowed={} degraded={} limit={} remaining={} retry_s={} err={}\n    {}",
        d.allowed,
        d.degraded,
        d.limit,
        d.remaining,
        d.retry_after_secs().map_or("-".into(), |s| s.to_string()),
        d.store_error.as_ref().map_or("-".into(), |e| e.to_string()),
        headers.join(" | "),
    )
}

fn ok(
    allowed: bool,
    used: u64,
    retry_ms: Option<u64>,
    reset_ms: u64,
) -> Result<CheckOutcome, StoreError> {
    Ok(CheckOutcome {
        allowed,
        used,
        retry_after: retry_ms.map(Duration::from_millis),
        reset_after: Duration::from_millis(reset_ms),
    })
}

/// Healthy store: header rendering across outcome shapes and rule names.
#[test]
fn healthy_store_decisions() {
    let w60 = Duration::from_secs(60);
    let cases: Vec<(&str, Rule, Result<CheckOutcome, StoreError>)> = vec![
        (
            "first request",
            rule("api", 10, w60, None),
            ok(true, 1, None, 60_000),
        ),
        (
            "last slot",
            rule("api", 10, w60, None),
            ok(true, 10, None, 1_500),
        ),
        (
            "denied, retry 1.2s",
            rule("api", 10, w60, None),
            ok(false, 10, Some(1_200), 1_200),
        ),
        (
            "denied, retry <1ms rounds to 1s",
            rule("api", 10, w60, None),
            ok(false, 10, Some(0), 0),
        ),
        (
            "denied, retry exactly 2s",
            rule("api", 10, w60, None),
            ok(false, 10, Some(2_000), 2_000),
        ),
        (
            "window 500ms reports w=1",
            rule("fast", 3, Duration::from_millis(500), None),
            ok(true, 1, None, 500),
        ),
        (
            "window 1 year",
            rule("yearly", 1, Duration::from_secs(365 * 86_400), None),
            ok(true, 1, None, 365 * 86_400_000),
        ),
        (
            "used > limit is clamped",
            rule("api", 10, w60, None),
            ok(false, 12, Some(100), 100),
        ),
        (
            "rule name with colon",
            rule("v1.login", 5, w60, None),
            ok(true, 1, None, 60_000),
        ),
        (
            "rule name with semicolon",
            rule("a;q=999", 5, w60, None),
            ok(true, 1, None, 60_000),
        ),
        (
            "rule name unicode",
            rule("ゲート", 5, w60, None),
            ok(true, 1, None, 60_000),
        ),
    ];

    let mut out = String::new();
    for (label, rule, result) in cases {
        let limiter = Limiter::new(Scripted(result));
        let d = tokio_test::block_on(limiter.check(&rule, "user:1")).unwrap();
        writeln!(out, "{label}\n    {}", render(&d)).unwrap();
    }
    assert_golden("limiter/healthy", &out);
}

/// Store down: every policy x error kind x instance count.
#[test]
fn degraded_store_decisions() {
    let errors = [
        StoreError::Unavailable("connection refused".into()),
        StoreError::Timeout(Duration::from_millis(250)),
        StoreError::Protocol("bad reply".into()),
    ];
    let policies = [
        OnStoreError::Allow,
        OnStoreError::Deny,
        OnStoreError::LocalFallback,
    ];
    let window = Duration::from_secs(60);

    let mut out = String::new();
    for err in &errors {
        for policy in policies {
            for (limit, instances) in [(10, 1), (10, 2), (10, 3), (2, 5), (1, 1)] {
                let limiter = Limiter::builder(Scripted(Err(err.clone())))
                    .instances(instances)
                    .on_store_error(policy)
                    .build();
                let r = rule("api", limit, window, None);
                // Send one more than the limit so the fallback share is exhausted.
                let mut admitted = 0;
                let mut last = None;
                for _ in 0..=limit {
                    let d = tokio_test::block_on(limiter.check(&r, "k")).unwrap();
                    admitted += u64::from(d.allowed);
                    last = Some(d);
                }
                writeln!(
                    out,
                    "{err:?} {policy:?} limit={limit} instances={instances}: admitted {admitted}/{}\n    last: {}",
                    limit + 1,
                    render(&last.unwrap())
                )
                .unwrap();
            }
        }
    }
    assert_golden("limiter/degraded", &out);
}

/// A per-rule policy overrides the limiter default.
#[test]
fn per_rule_policy_override() {
    let window = Duration::from_secs(60);
    let limiter = Limiter::builder(Scripted(Err(StoreError::Timeout(Duration::from_millis(
        250,
    )))))
    .on_store_error(OnStoreError::Allow)
    .build();
    let mut out = String::new();
    for policy in [
        None,
        Some(OnStoreError::Deny),
        Some(OnStoreError::LocalFallback),
    ] {
        let r = rule("api", 4, window, policy);
        let d = tokio_test::block_on(limiter.check(&r, "k")).unwrap();
        writeln!(out, "rule policy {policy:?}\n    {}", render(&d)).unwrap();
    }
    assert_golden("limiter/override", &out);
}

/// Invalid costs never reach the store.
#[test]
fn invalid_costs() {
    let limiter = Limiter::new(Scripted(ok(true, 1, None, 0)));
    let r = rule("api", 5, Duration::from_secs(1), None);
    let mut out = String::new();
    for cost in [0, 1, 5, 6, u64::MAX] {
        let res = tokio_test::block_on(limiter.check_cost(&r, "k", cost));
        let shown = match res {
            Ok(d) => format!("ok allowed={}", d.allowed),
            Err(e) => format!("err {e}"),
        };
        writeln!(out, "cost {cost}: {shown}").unwrap();
    }
    assert_golden("limiter/invalid_cost", &out);
}

/// Rule builder validation across odd inputs.
#[test]
fn rule_validation() {
    let cases: Vec<(&str, Option<u64>, Option<Duration>)> = vec![
        ("api", Some(1), Some(Duration::from_secs(1))),
        ("  padded  ", Some(1), Some(Duration::from_secs(1))),
        ("", Some(1), Some(Duration::from_secs(1))),
        ("   ", Some(1), Some(Duration::from_secs(1))),
        ("has space", Some(1), Some(Duration::from_secs(1))),
        ("{tag}", Some(1), Some(Duration::from_secs(1))),
        ("v1:login", Some(1), Some(Duration::from_secs(1))),
        ("tab\there", Some(1), Some(Duration::from_secs(1))),
        ("nul\0", Some(1), Some(Duration::from_secs(1))),
        ("api", Some(0), Some(Duration::from_secs(1))),
        ("api", None, Some(Duration::from_secs(1))),
        ("api", Some(1), None),
        ("api", Some(1), Some(Duration::ZERO)),
        ("say\"hi", Some(1), Some(Duration::from_secs(1))),
        ("back\\slash", Some(1), Some(Duration::from_secs(1))),
        ("api", Some(1), Some(Duration::from_nanos(1))),
        ("api", Some(1), Some(Duration::from_micros(999))),
        ("api", Some(1), Some(plimsoll_core::MIN_WINDOW)),
        (
            "api",
            Some(plimsoll_core::MAX_LIMIT),
            Some(Duration::from_secs(1)),
        ),
        (
            "api",
            Some(plimsoll_core::MAX_LIMIT + 1),
            Some(Duration::from_secs(1)),
        ),
        ("api", Some(u64::MAX), Some(Duration::from_secs(1))),
        ("api", Some(1), Some(plimsoll_core::MAX_WINDOW)),
        (
            "api",
            Some(1),
            Some(plimsoll_core::MAX_WINDOW + Duration::from_nanos(1)),
        ),
    ];
    let mut out = String::new();
    for (name, limit, window) in cases {
        let mut b = Rule::builder(name);
        if let Some(l) = limit {
            b = b.limit(l);
        }
        if let Some(w) = window {
            b = b.window(w);
        }
        let shown = match b.build() {
            Ok(r) => format!(
                "ok name={:?} limit={} window={:?}",
                r.name(),
                r.limit(),
                r.window()
            ),
            Err(e) => format!("err {e}"),
        };
        writeln!(out, "{name:?} {limit:?} {window:?}: {shown}").unwrap();
    }
    assert_golden("limiter/rule_validation", &out);
}

/// `Retry-After` under `Deny`: the default, a custom value, and the cap at
/// the rule's window.
#[test]
fn deny_retry_after() {
    let down = || Scripted(Err(StoreError::Unavailable("down".into())));
    let mut out = String::new();
    for (label, custom, window) in [
        ("default, 60s window", None, Duration::from_secs(60)),
        (
            "custom 2s, 60s window",
            Some(Duration::from_secs(2)),
            Duration::from_secs(60),
        ),
        ("default capped by 3s window", None, Duration::from_secs(3)),
        (
            "custom 1500ms rounds up",
            Some(Duration::from_millis(1500)),
            Duration::from_secs(60),
        ),
    ] {
        let mut b = Limiter::builder(down()).on_store_error(OnStoreError::Deny);
        if let Some(d) = custom {
            b = b.deny_retry_after(d);
        }
        let limiter = b.build();
        let d = tokio_test::block_on(limiter.check(&rule("api", 5, window, None), "k")).unwrap();
        writeln!(out, "{label}\n    {}", render(&d)).unwrap();
    }
    assert_golden("limiter/deny_retry_after", &out);
}
