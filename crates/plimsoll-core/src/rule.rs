use std::sync::Arc;
use std::time::Duration;

/// Longest window a rule may use. Timestamps are kept in microseconds and
/// must stay exactly representable as a Lua double; a year is far inside that
/// bound and anything longer is almost certainly a configuration mistake.
pub const MAX_WINDOW: Duration = Duration::from_secs(366 * 24 * 60 * 60);

/// Shortest window a rule may use. Stores work in whole microseconds and
/// Redis expires keys in whole milliseconds, so a shorter window would round
/// down and stop limiting anything.
pub const MIN_WINDOW: Duration = Duration::from_millis(1);

/// Largest limit a rule may use: 2^53, the largest integer a Lua double
/// (used by the Redis script) represents exactly.
pub const MAX_LIMIT: u64 = 1 << 53;

/// What a [`Limiter`](crate::Limiter) does when the store cannot answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum OnStoreError {
    /// Fail open: allow the request. Use for limits that protect capacity
    /// rather than security.
    Allow,
    /// Fail closed: deny the request. Use for abuse-sensitive endpoints such
    /// as login or password reset.
    Deny,
    /// Enforce an in-process limit of `limit / instances` until the store is
    /// back. This is the global default. Each instance stays within its share
    /// of the global limit, so the sum across instances never exceeds it,
    /// at the cost of accuracy when traffic is uneven across instances.
    #[default]
    LocalFallback,
}

/// A named rate limit: at most `limit` units of cost per `window`.
///
/// Build one with [`Rule::builder`]. Rules are cheap to clone and are meant
/// to be created once at startup and shared.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Rule {
    name: Arc<str>,
    limit: u64,
    window: Duration,
    on_store_error: Option<OnStoreError>,
}

impl Rule {
    /// Start building a rule with the given name.
    ///
    /// The name becomes part of the store key (`{plimsoll:<name>:<key>}`),
    /// which is why it may not contain `:`: otherwise rule `a:b` with key `c`
    /// and rule `a` with key `b:c` would share one window. Keys may contain
    /// anything because they come last.
    /// so two rules with the same name share state.
    pub fn builder(name: impl Into<String>) -> RuleBuilder {
        RuleBuilder {
            name: name.into(),
            limit: None,
            window: None,
            on_store_error: None,
        }
    }

    /// The rule's name, also used as the policy name in `RateLimit` headers.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Shared handle to the name, used when a decision needs to carry it.
    pub(crate) fn name_arc(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }

    /// Maximum total cost allowed inside one window.
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Length of the sliding window.
    pub fn window(&self) -> Duration {
        self.window
    }

    /// Per-rule store failure policy, if one overrides the limiter default.
    pub fn on_store_error(&self) -> Option<OnStoreError> {
        self.on_store_error
    }
}

/// Builder for [`Rule`]. Created by [`Rule::builder`].
#[derive(Debug, Clone)]
pub struct RuleBuilder {
    name: String,
    limit: Option<u64>,
    window: Option<Duration>,
    on_store_error: Option<OnStoreError>,
}

impl RuleBuilder {
    /// Maximum total cost per window. Required, at least 1 and at most
    /// [`MAX_LIMIT`].
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Window length. Required, between [`MIN_WINDOW`] and [`MAX_WINDOW`].
    pub fn window(mut self, window: Duration) -> Self {
        self.window = Some(window);
        self
    }

    /// Override the limiter's default [`OnStoreError`] policy for this rule.
    pub fn on_store_error(mut self, policy: OnStoreError) -> Self {
        self.on_store_error = Some(policy);
        self
    }

    /// Validate and build the rule.
    pub fn build(self) -> Result<Rule, RuleError> {
        let name = self.name.trim();
        if name.is_empty() {
            return Err(RuleError::EmptyName);
        }
        if name.chars().any(|c| {
            matches!(c, '{' | '}' | ':' | '"' | '\\') || c.is_whitespace() || c.is_control()
        }) {
            return Err(RuleError::InvalidName(name.to_owned()));
        }
        let limit = match self.limit {
            Some(0) | None => return Err(RuleError::ZeroLimit),
            Some(limit) if limit > MAX_LIMIT => return Err(RuleError::LimitTooLarge(limit)),
            Some(limit) => limit,
        };
        let window = match self.window {
            None => return Err(RuleError::ZeroWindow),
            Some(window) if window.is_zero() => return Err(RuleError::ZeroWindow),
            Some(window) if window < MIN_WINDOW => return Err(RuleError::WindowTooShort(window)),
            Some(window) if window > MAX_WINDOW => return Err(RuleError::WindowTooLong(window)),
            Some(window) => window,
        };
        Ok(Rule {
            name: Arc::from(name),
            limit,
            window,
            on_store_error: self.on_store_error,
        })
    }
}

/// Why a [`RuleBuilder`] refused to build.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuleError {
    /// The name was empty or only whitespace.
    #[error("rule name must not be empty")]
    EmptyName,
    /// The name contained braces, colons, quotes, backslashes, whitespace or
    /// control characters. Braces would interfere with the Redis Cluster hash
    /// tag; colons would let two rule/key pairs map to the same store key;
    /// quotes and backslashes would break the `RateLimit` header strings.
    #[error(
        "rule name {0:?} must not contain braces, colons, quotes, backslashes, whitespace or control characters"
    )]
    InvalidName(String),
    /// `limit` was missing or zero.
    #[error("rule limit must be at least 1")]
    ZeroLimit,
    /// `limit` exceeded [`MAX_LIMIT`].
    #[error("rule limit {0} exceeds the maximum of {MAX_LIMIT}")]
    LimitTooLarge(u64),
    /// `window` was missing or zero.
    #[error("rule window must be longer than zero")]
    ZeroWindow,
    /// `window` was shorter than [`MIN_WINDOW`].
    #[error("rule window {0:?} is shorter than the minimum of {MIN_WINDOW:?}")]
    WindowTooShort(Duration),
    /// `window` exceeded [`MAX_WINDOW`].
    #[error("rule window {0:?} exceeds the maximum of {MAX_WINDOW:?}")]
    WindowTooLong(Duration),
}
