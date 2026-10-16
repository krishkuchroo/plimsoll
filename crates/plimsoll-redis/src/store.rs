use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use plimsoll_core::{Admitted, CheckOutcome, CheckRequest, Store, StoreError};
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::{Client, IntoConnectionInfo, RedisError, Script};

/// Lua source, compiled once per process and loaded into Redis on first use.
const SLIDING_WINDOW_LUA: &str = include_str!("sliding_window.lua");
/// Writes fallback admissions back after an outage; see [`Store::backfill`].
const BACKFILL_LUA: &str = include_str!("backfill.lua");

/// Default key prefix: keys look like `{plimsoll:<rule>:<key>}:ts`.
pub const DEFAULT_PREFIX: &str = "plimsoll";

/// Connection and timeout settings for [`RedisStore`].
#[derive(Debug, Clone)]
pub struct RedisConfig {
    /// Upper bound on one check, including any reconnect attempt. When it
    /// elapses the limiter sees [`StoreError::Timeout`] and applies the
    /// rule's `OnStoreError` policy. Default: 100 ms, far above a healthy
    /// Redis round trip (well under 1 ms locally, a few ms across a
    /// network) but short enough that a struggling Redis adds little delay.
    pub timeout: Duration,
    /// Time allowed to establish a TCP connection. Default: 1 s.
    pub connect_timeout: Duration,
    /// Reconnect attempts the connection manager makes inside one failed
    /// call before giving up. Keep this small; `timeout` is the real bound.
    /// Default: 1.
    pub reconnect_retries: usize,
    /// When `true`, [`RedisStore::connect_with`] returns immediately and the
    /// first check opens the connection. When `false` (the default) it
    /// connects eagerly and fails fast if Redis is unreachable.
    pub lazy: bool,
    /// First segment of every Redis key. Give each deployment that shares a
    /// Redis its own prefix so rules with the same name don't share limits.
    /// Must be non-empty and must not contain `{`, `}`, `:`, whitespace or
    /// control characters. Default: [`DEFAULT_PREFIX`].
    pub prefix: String,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_millis(100),
            connect_timeout: Duration::from_secs(1),
            reconnect_retries: 1,
            lazy: false,
            prefix: DEFAULT_PREFIX.to_owned(),
        }
    }
}

impl RedisConfig {
    /// Set the per-check timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the connect timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Connect lazily on first use instead of eagerly.
    pub fn lazy(mut self, lazy: bool) -> Self {
        self.lazy = lazy;
        self
    }

    /// Set the key prefix. See [`RedisConfig::prefix`](#structfield.prefix).
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    fn manager_config(&self) -> ConnectionManagerConfig {
        ConnectionManagerConfig::new()
            .set_connection_timeout(Some(self.connect_timeout))
            .set_response_timeout(Some(self.timeout))
            .set_number_of_retries(self.reconnect_retries)
            .set_max_delay(self.timeout)
    }
}

/// A [`Store`] backed by a single Redis node.
///
/// Cloning is cheap; the underlying [`ConnectionManager`] is a multiplexed
/// connection that reconnects automatically after a drop.
#[derive(Clone)]
pub struct RedisStore {
    conn: ConnectionManager,
    script: Script,
    backfill_script: Script,
    timeout: Duration,
    prefix: Arc<str>,
}

impl std::fmt::Debug for RedisStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStore")
            .field("timeout", &self.timeout)
            .field("prefix", &self.prefix)
            .field("script_sha", &self.script.get_hash())
            .finish_non_exhaustive()
    }
}

impl RedisStore {
    /// Connect with [`RedisConfig::default`].
    ///
    /// `url` is anything the `redis` crate accepts, for example
    /// `redis://127.0.0.1/` or `redis://:password@host:6379/0`.
    pub async fn connect(url: impl IntoConnectionInfo) -> Result<Self, StoreError> {
        Self::connect_with(url, RedisConfig::default()).await
    }

    /// Connect with explicit settings.
    pub async fn connect_with(
        url: impl IntoConnectionInfo,
        config: RedisConfig,
    ) -> Result<Self, StoreError> {
        validate_prefix(&config.prefix)?;
        let client = Client::open(url).map_err(map_error)?;
        let manager_config = config.manager_config();
        let conn = if config.lazy {
            ConnectionManager::new_lazy_with_config(client, manager_config).map_err(map_error)?
        } else {
            tokio::time::timeout(
                config.connect_timeout + config.timeout,
                ConnectionManager::new_with_config(client, manager_config),
            )
            .await
            .map_err(|_| StoreError::Timeout(config.connect_timeout + config.timeout))?
            .map_err(map_error)?
        };
        let mut store = Self::from_manager(conn, config.timeout);
        store.prefix = Arc::from(config.prefix.as_str());
        Ok(store)
    }

    /// Wrap an existing connection manager. Use this to share a connection
    /// with the rest of your application. Uses [`DEFAULT_PREFIX`]; see
    /// [`RedisStore::with_prefix`] to change it.
    pub fn from_manager(conn: ConnectionManager, timeout: Duration) -> Self {
        Self {
            conn,
            script: Script::new(SLIDING_WINDOW_LUA),
            backfill_script: Script::new(BACKFILL_LUA),
            timeout,
            prefix: Arc::from(DEFAULT_PREFIX),
        }
    }

    /// Use a different key prefix. Fails if the prefix is empty or contains
    /// `{`, `}`, `:`, whitespace or control characters.
    pub fn with_prefix(mut self, prefix: &str) -> Result<Self, StoreError> {
        validate_prefix(prefix)?;
        self.prefix = Arc::from(prefix);
        Ok(self)
    }

    /// A handle to the shared connection.
    pub(crate) fn connection(&self) -> ConnectionManager {
        self.conn.clone()
    }

    /// The key prefix in use.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The per-check timeout.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// The three Redis keys this store uses for a `(rule, key)` pair, in
    /// `KEYS` order: `ts`, `cost`, `sum`.
    pub fn keys(&self, rule: &str, key: &str) -> [String; 3] {
        Self::keys_with_prefix(&self.prefix, rule, key)
    }

    /// The three Redis keys for a `(rule, key)` pair under `prefix`.
    pub fn keys_with_prefix(prefix: &str, rule: &str, key: &str) -> [String; 3] {
        let tag = format!("{{{prefix}:{rule}:{key}}}");
        [
            format!("{tag}:ts"),
            format!("{tag}:cost"),
            format!("{tag}:sum"),
        ]
    }

    /// Run one check with the script's clock pinned to `now_us` instead of
    /// Redis `TIME`, and with key expiry turned off. For deterministic tests
    /// only: never use it in production, keys written this way never expire.
    #[cfg(feature = "test-clock")]
    pub async fn check_at(
        &self,
        request: CheckRequest<'_>,
        now_us: u64,
    ) -> Result<CheckOutcome, StoreError> {
        self.run(request, Some(now_us)).await
    }

    async fn run(
        &self,
        request: CheckRequest<'_>,
        now_us: Option<u64>,
    ) -> Result<CheckOutcome, StoreError> {
        let [ts, cost, sum] = self.keys(request.rule, request.key);
        let mut conn = self.conn.clone();
        let mut invocation = self.script.prepare_invoke();
        invocation
            .key(ts)
            .key(cost)
            .key(sum)
            .arg(request.limit)
            .arg(request.window_micros())
            .arg(request.cost);
        if let Some(now) = now_us {
            invocation.arg(now);
        }

        let reply: (i64, i64, i64, i64) =
            tokio::time::timeout(self.timeout, invocation.invoke_async(&mut conn))
                .await
                .map_err(|_| StoreError::Timeout(self.timeout))?
                .map_err(map_error)?;

        parse_reply(reply)
    }
}

impl Store for RedisStore {
    fn check(
        &self,
        request: CheckRequest<'_>,
    ) -> impl Future<Output = Result<CheckOutcome, StoreError>> + Send {
        self.run(request, None)
    }

    fn backfill(
        &self,
        rule: &str,
        key: &str,
        window: Duration,
        entries: &[Admitted],
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        let [ts, cost, sum] = self.keys(rule, key);
        let mut invocation = self.backfill_script.prepare_invoke();
        invocation
            .key(ts)
            .key(cost)
            .key(sum)
            .arg(u64::try_from(window.as_micros()).unwrap_or(u64::MAX));
        for e in entries {
            invocation.arg(e.at_unix_micros).arg(e.cost);
        }
        let mut conn = self.conn.clone();
        let timeout = self.timeout;
        async move {
            let _recorded: i64 = tokio::time::timeout(timeout, invocation.invoke_async(&mut conn))
                .await
                .map_err(|_| StoreError::Timeout(timeout))?
                .map_err(map_error)?;
            Ok(())
        }
    }
}

/// Convert the script's `{allowed, used, retry_after_us, reset_after_us}`
/// reply into a [`CheckOutcome`]. Kept separate from I/O so the contract
/// with the Lua script is easy to test.
fn parse_reply(
    (allowed, used, retry_after, reset_after): (i64, i64, i64, i64),
) -> Result<CheckOutcome, StoreError> {
    let allowed = match allowed {
        0 => false,
        1 => true,
        other => return Err(StoreError::Protocol(format!("allowed flag was {other}"))),
    };
    let micros = |name: &str, v: i64| {
        u64::try_from(v).map_err(|_| StoreError::Protocol(format!("{name} was negative: {v}")))
    };
    Ok(CheckOutcome {
        allowed,
        used: micros("used", used)?,
        retry_after: if allowed {
            None
        } else {
            Some(Duration::from_micros(micros("retry_after", retry_after)?))
        },
        reset_after: Duration::from_micros(micros("reset_after", reset_after)?),
    })
}

/// A prefix ends up inside the `{...}` hash tag and before the rule name, so
/// braces would break the tag and a `:` would let two prefixes collide.
fn validate_prefix(prefix: &str) -> Result<(), StoreError> {
    let bad = prefix.is_empty()
        || prefix
            .chars()
            .any(|c| matches!(c, '{' | '}' | ':') || c.is_whitespace() || c.is_control());
    if bad {
        return Err(StoreError::Other(format!(
            "invalid key prefix {prefix:?}: must be non-empty without braces, colons, whitespace or control characters"
        )));
    }
    Ok(())
}

pub(crate) fn map_error(err: RedisError) -> StoreError {
    if err.is_timeout() {
        StoreError::Timeout(Duration::ZERO)
    } else if err.is_io_error() || err.is_connection_refusal() || err.is_connection_dropped() {
        StoreError::Unavailable(err.to_string())
    } else if matches!(
        err.kind(),
        redis::ErrorKind::Parse | redis::ErrorKind::UnexpectedReturnType
    ) {
        StoreError::Protocol(err.to_string())
    } else {
        StoreError::Other(err.to_string())
    }
}
