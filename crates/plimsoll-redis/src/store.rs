use std::future::Future;
use std::time::Duration;

use plimsoll_core::{CheckOutcome, CheckRequest, Store, StoreError};
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::{Client, IntoConnectionInfo, RedisError, Script};

/// Lua source, compiled once per process and loaded into Redis on first use.
const SLIDING_WINDOW_LUA: &str = include_str!("sliding_window.lua");

/// Key prefix shared by every plimsoll key.
const KEY_PREFIX: &str = "plimsoll";

/// Connection and timeout settings for [`RedisStore`].
#[derive(Debug, Clone)]
pub struct RedisConfig {
    /// Upper bound on one check, including any reconnect attempt. When it
    /// elapses the limiter sees [`StoreError::Timeout`] and applies the
    /// rule's `OnStoreError` policy. Default: 250 ms.
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
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_millis(250),
            connect_timeout: Duration::from_secs(1),
            reconnect_retries: 1,
            lazy: false,
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
    timeout: Duration,
}

impl std::fmt::Debug for RedisStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStore")
            .field("timeout", &self.timeout)
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
        Ok(Self::from_manager(conn, config.timeout))
    }

    /// Wrap an existing connection manager. Use this to share a connection
    /// with the rest of your application.
    pub fn from_manager(conn: ConnectionManager, timeout: Duration) -> Self {
        Self {
            conn,
            script: Script::new(SLIDING_WINDOW_LUA),
            timeout,
        }
    }

    /// The per-check timeout.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// The three keys used for a `(rule, key)` pair, in `KEYS` order.
    pub fn keys(rule: &str, key: &str) -> [String; 3] {
        let tag = format!("{{{KEY_PREFIX}:{rule}:{key}}}");
        [
            format!("{tag}:ts"),
            format!("{tag}:cost"),
            format!("{tag}:sum"),
        ]
    }

    async fn run(&self, request: CheckRequest<'_>) -> Result<CheckOutcome, StoreError> {
        let [ts, cost, sum] = Self::keys(request.rule, request.key);
        let mut conn = self.conn.clone();
        let mut invocation = self.script.prepare_invoke();
        invocation
            .key(ts)
            .key(cost)
            .key(sum)
            .arg(request.limit)
            .arg(request.window_micros())
            .arg(request.cost);

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
        self.run(request)
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

fn map_error(err: RedisError) -> StoreError {
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
