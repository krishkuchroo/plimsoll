use std::sync::Arc;
use std::time::Duration;

use plimsoll_core::{Limiter, Store, StoreError};
use redis::Script;
use redis::aio::ConnectionManager;

use crate::RedisStore;
use crate::store::map_error;

const MEMBERS_LUA: &str = include_str!("members.lua");

/// Default lifetime of a registration without a heartbeat.
pub const DEFAULT_MEMBERSHIP_TTL: Duration = Duration::from_secs(15);

/// Lets instances of a service discover each other through Redis, so
/// [`OnStoreError::LocalFallback`](plimsoll_core::OnStoreError::LocalFallback)
/// can split each limit exactly without the instance count being configured
/// by hand.
///
/// Each instance registers under a unique id with a time-to-live and
/// refreshes it with [`heartbeat`](Membership::heartbeat). Every heartbeat
/// returns the live instances: their count, and this instance's position in
/// sorted id order. [`spawn`](Membership::spawn) runs the heartbeat in the
/// background and feeds the result to
/// [`Limiter::set_membership`](plimsoll_core::Limiter::set_membership).
///
/// While Redis is down the heartbeat fails and the limiter keeps the last
/// known membership, which is exactly what the fallback needs.
///
/// ```no_run
/// # use std::sync::Arc;
/// # use std::time::Duration;
/// # use plimsoll_core::Limiter;
/// # use plimsoll_redis::{Membership, RedisStore};
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let store = RedisStore::connect("redis://127.0.0.1/").await?;
/// let membership = Membership::new(&store, "web-1"); // unique per instance, e.g. the pod name
/// let limiter = Arc::new(Limiter::new(store));
/// membership.spawn(Arc::clone(&limiter), Duration::from_secs(5));
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct Membership {
    conn: ConnectionManager,
    script: Script,
    key: String,
    id: String,
    ttl: Duration,
    timeout: Duration,
}

impl std::fmt::Debug for Membership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Membership")
            .field("key", &self.key)
            .field("id", &self.id)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

/// The live instances as seen by one heartbeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Members {
    /// Number of live instances, including this one.
    pub count: u32,
    /// This instance's position among them in sorted id order.
    pub index: u32,
}

impl Membership {
    /// Membership for the instance `id`, sharing the store's connection
    /// and key prefix. Ids must be unique across the deployment.
    pub fn new(store: &RedisStore, id: impl Into<String>) -> Self {
        Self {
            conn: store.connection(),
            script: Script::new(MEMBERS_LUA),
            key: format!("{{{}}}:instances", store.prefix()),
            id: id.into(),
            ttl: DEFAULT_MEMBERSHIP_TTL,
            timeout: store.timeout(),
        }
    }

    /// How long a registration lives without a heartbeat. Heartbeat at
    /// least two or three times per TTL. Default: 15 s.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// This instance's id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Register or refresh this instance and return the live membership.
    pub async fn heartbeat(&self) -> Result<Members, StoreError> {
        let ids = self.run(false).await?;
        let mut ids: Vec<String> = ids;
        ids.sort_unstable();
        let index = ids.iter().position(|i| i == &self.id).ok_or_else(|| {
            StoreError::Protocol("instance missing from its own heartbeat".into())
        })?;
        Ok(Members {
            count: u32::try_from(ids.len()).unwrap_or(u32::MAX),
            index: u32::try_from(index).unwrap_or(u32::MAX),
        })
    }

    /// Remove this instance right away, e.g. on graceful shutdown, so the
    /// others don't wait for the TTL to notice.
    pub async fn leave(&self) -> Result<(), StoreError> {
        self.run(true).await.map(|_| ())
    }

    /// Heartbeat every `every` in a background task and apply the result to
    /// `limiter`. Failed heartbeats are skipped; the limiter keeps the last
    /// known membership. Abort the returned handle to stop.
    pub fn spawn<S>(self, limiter: Arc<Limiter<S>>, every: Duration) -> tokio::task::JoinHandle<()>
    where
        S: Store + 'static,
    {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(every);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if let Ok(m) = self.heartbeat().await {
                    limiter.set_membership(m.count, m.index);
                }
            }
        })
    }

    async fn run(&self, leave: bool) -> Result<Vec<String>, StoreError> {
        let mut conn = self.conn.clone();
        let ttl_ms = u64::try_from(self.ttl.as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        let mut invocation = self.script.prepare_invoke();
        invocation
            .key(&self.key)
            .arg(&self.id)
            .arg(ttl_ms)
            .arg(if leave { "1" } else { "0" });
        tokio::time::timeout(self.timeout, invocation.invoke_async(&mut conn))
            .await
            .map_err(|_| StoreError::Timeout(self.timeout))?
            .map_err(map_error)
    }
}
