//! Stage 1 of TESTING.md: `WindowLog` edge cases and a property test against
//! a brute-force model.

use std::time::Duration;

use plimsoll_core::WindowLog;
use proptest::prelude::*;

const WINDOW: u64 = 1_000;
const LIMIT: u64 = 5;

/// Start well past the window so early-timestamp effects don't leak into
/// tests that aren't about them.
const T0: u64 = 1_000_000;

fn full_log(at: u64) -> WindowLog {
    let mut log = WindowLog::new();
    for _ in 0..LIMIT {
        assert!(log.check(at, WINDOW, LIMIT, 1).allowed);
    }
    log
}

#[test]
fn allows_up_to_limit_then_denies() {
    let mut log = full_log(T0);
    let out = log.check(T0, WINDOW, LIMIT, 1);
    assert!(!out.allowed);
    assert_eq!(out.used, LIMIT);
}

#[test]
fn denied_check_leaves_state_unchanged() {
    let mut log = full_log(T0);
    let before = log.clone();
    assert!(!log.check(T0 + 10, WINDOW, LIMIT, 1).allowed);
    assert!(!log.check(T0 + 10, WINDOW, LIMIT, LIMIT + 1).allowed);
    assert_eq!(log, before);
}

#[test]
fn entry_evicted_exactly_at_window_end() {
    let mut log = full_log(T0);
    assert!(!log.clone().check(T0 + WINDOW - 1, WINDOW, LIMIT, 1).allowed);
    assert!(log.check(T0 + WINDOW, WINDOW, LIMIT, 1).allowed);
}

#[test]
fn cost_equal_to_remaining_is_allowed() {
    let mut log = WindowLog::new();
    assert!(log.check(T0, WINDOW, LIMIT, 2).allowed);
    assert!(log.check(T0, WINDOW, LIMIT, LIMIT - 2).allowed);
    assert!(!log.check(T0, WINDOW, LIMIT, 1).allowed);
}

#[test]
fn cost_above_limit_is_denied_with_full_window() {
    let mut log = WindowLog::new();
    let out = log.check(T0, WINDOW, LIMIT, LIMIT + 1);
    assert!(!out.allowed);
    assert_eq!(out.retry_after, Some(Duration::from_micros(WINDOW)));
    assert!(log.is_empty());
}

#[test]
fn retry_after_is_exact() {
    let mut log = WindowLog::new();
    for (dt, cost) in [(0, 2), (100, 2), (200, 1)] {
        assert!(log.check(T0 + dt, WINDOW, LIMIT, cost).allowed);
    }
    // Needs 3 freed: the first two entries (2 + 2) must expire.
    let now = T0 + 300;
    let retry = log
        .clone()
        .check(now, WINDOW, LIMIT, 3)
        .retry_after
        .unwrap();
    assert_eq!(retry, Duration::from_micros(T0 + 100 + WINDOW - now));

    let at = now + retry.as_micros() as u64;
    assert!(!log.clone().check(at - 1, WINDOW, LIMIT, 3).allowed);
    assert!(log.check(at, WINDOW, LIMIT, 3).allowed);
}

#[test]
fn reset_after_tracks_oldest_entry() {
    let mut log = WindowLog::new();
    log.check(T0, WINDOW, LIMIT, 1);
    let out = log.check(T0 + 300, WINDOW, LIMIT, 1);
    assert_eq!(out.reset_after, Duration::from_micros(WINDOW - 300));
}

#[test]
fn clock_going_backwards_is_clamped() {
    let mut log = WindowLog::new();
    log.check(T0, WINDOW, LIMIT, 1);
    log.check(T0 - 500, WINDOW, LIMIT, 1);
    assert_eq!(log.newest(), Some(T0));
}

/// A fresh `LocalStore` starts its clock at 0, so `now < window` is normal.
/// An entry at `t = 0` must still count for the whole first window.
#[test]
fn timestamps_near_zero_do_not_evict_early() {
    let mut log = WindowLog::new();
    for _ in 0..LIMIT {
        assert!(log.check(0, WINDOW, LIMIT, 1).allowed);
    }
    assert!(!log.check(10, WINDOW, LIMIT, 1).allowed);
}

/// Brute-force model: keep every allowed `(t, cost)` forever and sum the ones
/// inside `(now - window, now]` on each check, using signed maths.
#[derive(Default)]
struct Model {
    allowed: Vec<(u64, u64)>,
}

impl Model {
    fn used(&self, now: u64, window: u64) -> u64 {
        self.allowed
            .iter()
            .filter(|&&(t, _)| (t as i128) > now as i128 - window as i128)
            .map(|&(_, c)| c)
            .sum()
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Invariants 1–4 of TESTING.md over random traffic. Steps go forwards
    /// only; clock clamping is covered by its own test.
    #[test]
    fn matches_brute_force_model(
        start in prop_oneof![Just(0u64), 0u64..WINDOW, Just(T0)],
        steps in prop::collection::vec((0u64..=WINDOW / 2, 1u64..=LIMIT + 1), 1..200),
    ) {
        let mut log = WindowLog::new();
        let mut model = Model::default();
        let mut now = start;

        for (dt, cost) in steps {
            now += dt;
            let before = log.clone();
            let out = log.check(now, WINDOW, LIMIT, cost);
            let used = model.used(now, WINDOW);

            prop_assert_eq!(out.allowed, used + cost <= LIMIT, "now={} cost={} used={}", now, cost, used);
            if out.allowed {
                model.allowed.push((now, cost));
            } else {
                // A denial may evict expired entries but never records one.
                prop_assert!(log.is_empty() || log.newest() == before.newest());
                prop_assert!(log.len() <= before.len());
                prop_assert!(log.sum() <= before.sum());
                // retry_after is exact when nothing else arrives.
                let retry = out.retry_after.unwrap().as_micros() as u64;
                if cost <= LIMIT {
                    prop_assert!(!before.clone().check(now + retry - 1, WINDOW, LIMIT, cost).allowed);
                    prop_assert!(before.clone().check(now + retry, WINDOW, LIMIT, cost).allowed);
                }
            }
            prop_assert_eq!(out.used, model.used(now, WINDOW));
            prop_assert!(out.used <= LIMIT);
        }
    }
}
