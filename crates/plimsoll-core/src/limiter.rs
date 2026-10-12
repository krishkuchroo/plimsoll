use std::sync::Arc;

use crate::{CheckRequest, Decision, LocalStore, OnStoreError, Rule, Store, StoreError};

/// Checks requests against rules using a [`Store`], with a fallback policy
/// for when the store cannot answer.
///
/// Create one with [`Limiter::builder`] (or [`Limiter::new`] for defaults)
/// and share it across your handlers, typically behind an `Arc` or inside
/// framework state.
#[derive(Debug)]
pub struct Limiter<S> {
    store: S,
    local: LocalStore,
    instances: u32,
    on_store_error: OnStoreError,
}

impl<S: Store> Limiter<S> {
    /// Start building a limiter around `store`.
    pub fn builder(store: S) -> LimiterBuilder<S> {
        LimiterBuilder {
            store,
            instances: 1,
            on_store_error: OnStoreError::default(),
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
        self.instances
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
        match self.store.check(request).await {
            Ok(outcome) => Ok(Decision::from_outcome(
                rule.name_arc(),
                limit,
                cost,
                rule.window(),
                outcome,
            )),
            Err(err) => Ok(self.degrade(rule, request, err)),
        }
    }

    fn degrade(&self, rule: &Rule, request: CheckRequest<'_>, err: StoreError) -> Decision {
        let policy = rule.on_store_error().unwrap_or(self.on_store_error);
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
                retry_after: Some(request.window),
                reset_after: request.window,
                degraded: true,
                store_error: None,
            },
            OnStoreError::LocalFallback => {
                let share = local_share(request.limit, self.instances);
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

/// This instance's share of a global limit: `ceil(limit / instances)`, so a
/// limit smaller than the instance count still admits one request per
/// instance rather than none.
fn local_share(limit: u64, instances: u32) -> u64 {
    let n = u64::from(instances.max(1));
    limit.div_ceil(n).max(1)
}

/// Builder for [`Limiter`]. Created by [`Limiter::builder`].
#[derive(Debug)]
pub struct LimiterBuilder<S> {
    store: S,
    instances: u32,
    on_store_error: OnStoreError,
}

impl<S: Store> LimiterBuilder<S> {
    /// How many instances of the service share each rule. Used to size the
    /// per-instance limit under [`OnStoreError::LocalFallback`]. Values
    /// below 1 are treated as 1.
    pub fn instances(mut self, instances: u32) -> Self {
        self.instances = instances.max(1);
        self
    }

    /// Default policy when the store fails, for rules that do not set their
    /// own. Defaults to [`OnStoreError::LocalFallback`].
    pub fn on_store_error(mut self, policy: OnStoreError) -> Self {
        self.on_store_error = policy;
        self
    }

    /// Build the limiter.
    pub fn build(self) -> Limiter<S> {
        Limiter {
            store: self.store,
            local: LocalStore::new(),
            instances: self.instances,
            on_store_error: self.on_store_error,
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
