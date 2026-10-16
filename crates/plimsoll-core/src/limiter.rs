use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::{
    CheckEvent, CheckRequest, Decision, LocalStore, Observer, OnStoreError, Rule, Store, StoreError,
};

/// Checks requests against rules using a [`Store`], with a fallback policy
/// for when the store cannot answer.
///
/// Create one with [`Limiter::builder`] (or [`Limiter::new`] for defaults)
/// and share it across your handlers, typically behind an `Arc` or inside
/// framework state.
pub struct Limiter<S> {
    store: S,
    local: LocalStore,
    /// Instance count (high 32 bits) and this instance's index (low 32
    /// bits), packed so both always change together.
    membership: AtomicU64,
    on_store_error: OnStoreError,
    deny_retry_after: Duration,
    observer: Option<Arc<dyn Observer>>,
}

/// Default `Retry-After` for requests denied by [`OnStoreError::Deny`]: short,
/// so clients come back soon after the store recovers.
pub const DEFAULT_DENY_RETRY_AFTER: Duration = Duration::from_secs(5);

impl<S: std::fmt::Debug> std::fmt::Debug for Limiter<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Limiter")
            .field("store", &self.store)
            .field(
                "membership",
                &unpack(self.membership.load(Ordering::Relaxed)),
            )
            .field("on_store_error", &self.on_store_error)
            .field("deny_retry_after", &self.deny_retry_after)
            .field("observer", &self.observer.is_some())
            .finish_non_exhaustive()
    }
}

impl<S: Store> Limiter<S> {
    /// Start building a limiter around `store`.
    pub fn builder(store: S) -> LimiterBuilder<S> {
        LimiterBuilder {
            store,
            instances: 1,
            instance_index: 0,
            on_store_error: OnStoreError::default(),
            deny_retry_after: DEFAULT_DENY_RETRY_AFTER,
            observer: None,
        }
    }

    /// A limiter with defaults: one instance, [`OnStoreError::LocalFallback`].
    pub fn new(store: S) -> Self {
        Self::builder(store).build()
    }

    /// The primary store.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// The in-process store used for [`OnStoreError::LocalFallback`].
    pub fn local_store(&self) -> &LocalStore {
        &self.local
    }

    /// Number of instances this limiter assumes share each rule.
    pub fn instances(&self) -> u32 {
        unpack(self.membership.load(Ordering::Relaxed)).0
    }

    /// This instance's position among them, from `0` to `instances() - 1`.
    pub fn instance_index(&self) -> u32 {
        unpack(self.membership.load(Ordering::Relaxed)).1
    }

    /// Update the instance count and this instance's index while running,
    /// for example from a membership heartbeat. A count below 1 is treated
    /// as 1 and the index is clamped to `instances - 1`. Only affects
    /// [`OnStoreError::LocalFallback`] shares.
    pub fn set_membership(&self, instances: u32, index: u32) {
        self.membership
            .store(pack(instances, index), Ordering::Relaxed);
    }

    /// Check a request of cost 1 for `key` under `rule`.
    pub async fn check(&self, rule: &Rule, key: &str) -> Result<Decision, LimitError> {
        self.check_cost(rule, key, 1).await
    }

    /// Check a request of the given cost for `key` under `rule`.
    ///
    /// `cost` must be between 1 and the rule's limit; anything else is a
    /// programming error and returns [`LimitError::InvalidCost`] because such
    /// a request could never be admitted.
    pub async fn check_cost(
        &self,
        rule: &Rule,
        key: &str,
        cost: u64,
    ) -> Result<Decision, LimitError> {
        let limit = rule.limit();
        if cost == 0 || cost > limit {
            return Err(LimitError::InvalidCost { cost, limit });
        }
        let request = CheckRequest {
            rule: rule.name(),
            key,
            limit,
            window: rule.window(),
            cost,
        };
        let started = Instant::now();
        let result = self.store.check(request).await;
        let store_latency = started.elapsed();
        let (decision, fallback) = match result {
            Ok(outcome) => (
                Decision::from_outcome(rule.name_arc(), limit, cost, rule.window(), outcome),
                None,
            ),
            Err(err) => {
                let policy = rule.on_store_error().unwrap_or(self.on_store_error);
                (self.degrade(rule, policy, request, err), Some(policy))
            }
        };
        if let Some(observer) = &self.observer {
            observer.on_check(&CheckEvent {
                rule: rule.name(),
                key,
                cost,
                allowed: decision.allowed,
                fallback,
                store_error: decision.store_error.as_ref(),
                store_latency,
            });
        }
        Ok(decision)
    }

    fn degrade(
        &self,
        rule: &Rule,
        policy: OnStoreError,
        request: CheckRequest<'_>,
        err: StoreError,
    ) -> Decision {
        let mut decision = match policy {
            OnStoreError::Allow => Decision {
                allowed: true,
                rule: rule.name_arc(),
                limit: request.limit,
                cost: request.cost,
                window: request.window,
                remaining: request.limit.saturating_sub(request.cost),
                retry_after: None,
                reset_after: request.window,
                degraded: true,
                store_error: None,
            },
            OnStoreError::Deny => Decision {
                allowed: false,
                rule: rule.name_arc(),
                limit: request.limit,
                cost: request.cost,
                window: request.window,
                remaining: 0,
                retry_after: Some(self.deny_retry_after.min(request.window)),
                reset_after: request.window,
                degraded: true,
                store_error: None,
            },
            OnStoreError::LocalFallback => {
                let (instances, index) = unpack(self.membership.load(Ordering::Relaxed));
                let share = local_share(request.limit, instances, index);
                if share == 0 {
                    // This instance's share of a small limit is nothing:
                    // other instances hold the whole limit while degraded.
                    return Decision {
                        allowed: false,
                        rule: rule.name_arc(),
                        limit: 0,
                        cost: request.cost,
                        window: request.window,
                        remaining: 0,
                        retry_after: Some(self.deny_retry_after.min(request.window)),
                        reset_after: request.window,
                        degraded: true,
                        store_error: Some(err),
                    };
                }
                let local_request = CheckRequest {
                    limit: share,
                    cost: request.cost.min(share),
                    ..request
                };
                let outcome = self.local.check_sync(local_request);
                let mut d = Decision::from_outcome(
                    rule.name_arc(),
                    share,
                    request.cost,
                    request.window,
                    outcome,
                );
                d.degraded = true;
                d
            }
        };
        decision.store_error = Some(err);
        decision
    }
}

/// This instance's share of a global limit. The limit is split exactly:
/// every instance gets `floor(limit / instances)`, and the instances with
/// index below `limit % instances` get one more, so the shares add up to
/// `limit` when every instance has a distinct index. An instance whose share
/// is 0 admits nothing while degraded.
///
/// With the default index of 0 on every instance this is `ceil(limit / n)`
/// each, which can overshoot by up to `n - 1` in total.
pub(crate) fn local_share(limit: u64, instances: u32, index: u32) -> u64 {
    let n = u64::from(instances.max(1));
    let index = u64::from(index).min(n - 1);
    limit / n + u64::from(index < limit % n)
}

fn pack(instances: u32, index: u32) -> u64 {
    let instances = instances.max(1);
    (u64::from(instances) << 32) | u64::from(index.min(instances - 1))
}

fn unpack(packed: u64) -> (u32, u32) {
    ((packed >> 32) as u32, packed as u32)
}

/// Builder for [`Limiter`]. Created by [`Limiter::builder`].
pub struct LimiterBuilder<S> {
    store: S,
    instances: u32,
    instance_index: u32,
    on_store_error: OnStoreError,
    deny_retry_after: Duration,
    observer: Option<Arc<dyn Observer>>,
}

impl<S: std::fmt::Debug> std::fmt::Debug for LimiterBuilder<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LimiterBuilder")
            .field("store", &self.store)
            .field("instances", &self.instances)
            .field("instance_index", &self.instance_index)
            .field("on_store_error", &self.on_store_error)
            .field("deny_retry_after", &self.deny_retry_after)
            .field("observer", &self.observer.is_some())
            .finish()
    }
}

impl<S: Store> LimiterBuilder<S> {
    /// How many instances of the service share each rule. Used to size the
    /// per-instance limit under [`OnStoreError::LocalFallback`]. Values
    /// below 1 are treated as 1.
    pub fn instances(mut self, instances: u32) -> Self {
        self.instances = instances.max(1);
        self
    }

    /// This instance's position among the instances, `0..instances`. Give
    /// every instance a distinct index and the `LocalFallback` shares add
    /// up to exactly the rule's limit; leave it at the default `0` on all of
    /// them and each gets `ceil(limit / instances)`. With Redis, a
    /// membership heartbeat (`plimsoll_redis::Membership`) can set both
    /// values automatically.
    pub fn instance_index(mut self, index: u32) -> Self {
        self.instance_index = index;
        self
    }

    /// Default policy when the store fails, for rules that do not set their
    /// own. Defaults to [`OnStoreError::LocalFallback`].
    pub fn on_store_error(mut self, policy: OnStoreError) -> Self {
        self.on_store_error = policy;
        self
    }

    /// `Retry-After` for requests denied by [`OnStoreError::Deny`] while the
    /// store is down. The real usage is unknown then, so this is just how
    /// soon clients should try again; it is capped at the rule's window.
    /// Defaults to [`DEFAULT_DENY_RETRY_AFTER`] (5 s).
    pub fn deny_retry_after(mut self, retry_after: Duration) -> Self {
        self.deny_retry_after = retry_after;
        self
    }

    /// Call `observer` once after every check, for metrics or logging.
    /// See [`Metrics`](crate::Metrics) for a ready-made collector.
    /// Replaces any observer set earlier.
    pub fn observer(mut self, observer: impl Observer) -> Self {
        self.observer = Some(Arc::new(observer));
        self
    }

    /// Build the limiter.
    pub fn build(self) -> Limiter<S> {
        Limiter {
            store: self.store,
            local: LocalStore::new(),
            membership: AtomicU64::new(pack(self.instances, self.instance_index)),
            on_store_error: self.on_store_error,
            deny_retry_after: self.deny_retry_after,
            observer: self.observer,
        }
    }
}

/// Why a check could not be performed at all.
///
/// Store failures are not errors here; they are handled by the rule's
/// [`OnStoreError`] policy and surface as [`Decision::degraded`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LimitError {
    /// `cost` was zero or larger than the rule's limit.
    #[error("cost {cost} is outside 1..={limit}")]
    InvalidCost {
        /// The cost that was requested.
        cost: u64,
        /// The rule's limit.
        limit: u64,
    },
}

#[allow(dead_code)]
fn _assert_send_sync<S: Store>() {
    fn check<T: Send + Sync>() {}
    check::<Limiter<S>>();
    check::<Arc<Limiter<S>>>();
}
