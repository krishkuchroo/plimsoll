//! Redis store for [plimsoll](https://github.com/krishkuchroo/plimsoll).
//!
//! [`RedisStore`] implements [`plimsoll_core::Store`] with a sliding window
//! log kept in three Redis keys per `(rule, key)` pair:
//!
//! | Key | Type | Holds |
//! |---|---|---|
//! | `{plimsoll:<rule>:<key>}:ts` | LIST | request timestamps in microseconds, oldest first |
//! | `{plimsoll:<rule>:<key>}:cost` | LIST | request costs, in lockstep with `ts` |
//! | `{plimsoll:<rule>:<key>}:sum` | STRING | running total of costs in the window |
//!
//! The whole read-evict-check-insert sequence runs inside one Lua script
//! (`EVALSHA`, loaded on first use), so concurrent checks from any number of
//! instances cannot race past the limit. Time is read with Redis `TIME`, so
//! instances with skewed clocks still agree. The three keys share one hash
//! tag and the script only touches keys passed in `KEYS`, which keeps it
//! Redis Cluster compatible.
//!
//! # Example
//!
//! ```no_run
//! use plimsoll_core::{Limiter, Rule};
//! use plimsoll_redis::RedisStore;
//! use std::time::Duration;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let store = RedisStore::connect("redis://127.0.0.1/").await?;
//! let limiter = Limiter::builder(store).instances(2).build();
//! let rule = Rule::builder("api").limit(100).window(Duration::from_secs(60)).build()?;
//! let decision = limiter.check(&rule, "user:123").await?;
//! # Ok(()) }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod membership;
mod store;

pub use membership::{DEFAULT_MEMBERSHIP_TTL, Members, Membership};
pub use store::{DEFAULT_PREFIX, RedisConfig, RedisStore};

/// Re-export of the `redis` crate this store is built on, so callers can
/// build a [`redis::Client`] or [`redis::aio::ConnectionManager`] themselves.
pub use redis;
