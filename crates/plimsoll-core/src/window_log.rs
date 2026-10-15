use std::collections::VecDeque;
use std::time::Duration;

use crate::CheckOutcome;

/// Reference sliding window log for one `(rule, key)` pair.
///
/// Two deques, `ts` and `cost`, are kept in lockstep (entry `i` of one
/// describes the same request as entry `i` of the other) together with a
/// running `sum` of the costs. This is exactly the shape of the three Redis
/// keys, and the Lua script must keep the same semantics as [`WindowLog::check`].
///
/// Timestamps are microseconds on whatever clock the caller uses; the log only
/// compares them with each other.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WindowLog {
    ts: VecDeque<u64>,
    cost: VecDeque<u64>,
    sum: u64,
}

impl WindowLog {
    /// An empty log.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether no entries are recorded.
    pub fn is_empty(&self) -> bool {
        self.ts.is_empty()
    }

    /// Number of recorded entries.
    pub fn len(&self) -> usize {
        self.ts.len()
    }

    /// Total cost of recorded entries (before any eviction).
    pub fn sum(&self) -> u64 {
        self.sum
    }

    /// Timestamp of the newest entry, if any.
    pub fn newest(&self) -> Option<u64> {
        self.ts.back().copied()
    }

    /// Run one check at time `now` (microseconds) for a window of
    /// `window` microseconds.
    ///
    /// Steps, in order:
    /// 1. Clamp `now` to the newest entry so a clock that went backwards cannot
    ///    produce out-of-order entries.
    /// 2. Evict entries with `ts <= now - window` from the front of both deques
    ///    and subtract their cost from `sum`.
    /// 3. Allow when `sum + cost <= limit`; if so, push `(now, cost)` and add to
    ///    `sum`. Denied requests are not recorded.
    /// 4. On denial, walk from the oldest entry accumulating cost until enough
    ///    would have expired for `cost` to fit; `retry_after` is that entry's
    ///    expiry minus `now`. If the request can never fit (`cost > limit`),
    ///    `retry_after` is the full window.
    pub fn check(&mut self, now: u64, window: u64, limit: u64, cost: u64) -> CheckOutcome {
        let now = match self.ts.back() {
            Some(&newest) if newest > now => newest,
            _ => now,
        };
        self.evict(now, window);

        let allowed = self.sum.saturating_add(cost) <= limit;
        let retry_after = if allowed {
            self.ts.push_back(now);
            self.cost.push_back(cost);
            self.sum += cost;
            None
        } else {
            Some(Duration::from_micros(
                self.retry_after(now, window, limit, cost),
            ))
        };

        let reset_after = self
            .ts
            .front()
            .map(|&oldest| (oldest + window).saturating_sub(now))
            .unwrap_or(0);

        CheckOutcome {
            allowed,
            used: self.sum,
            retry_after,
            reset_after: Duration::from_micros(reset_after),
        }
    }

    fn evict(&mut self, now: u64, window: u64) {
        // Before `now` reaches `window` nothing can have expired. Saturating
        // to 0 here would wrongly evict entries recorded at t = 0.
        let Some(cutoff) = now.checked_sub(window) else {
            return;
        };
        while let Some(&oldest) = self.ts.front() {
            if oldest > cutoff {
                break;
            }
            self.ts.pop_front();
            let c = self.cost.pop_front().unwrap_or(0);
            self.sum = self.sum.saturating_sub(c);
        }
        if self.ts.is_empty() {
            self.sum = 0;
        }
    }

    /// Microseconds until a request of `cost` would fit, assuming no new
    /// entries arrive in between.
    fn retry_after(&self, now: u64, window: u64, limit: u64, cost: u64) -> u64 {
        let need = (self.sum + cost).saturating_sub(limit);
        let mut freed = 0u64;
        for (&ts, &c) in self.ts.iter().zip(self.cost.iter()) {
            freed += c;
            if freed >= need {
                return (ts + window).saturating_sub(now);
            }
        }
        window
    }
}
