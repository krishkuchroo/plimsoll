//! # plimsoll
//!
//! Distributed API rate limiting for Rust, backed by Redis.
//!
//! The Plimsoll line marks the safe load limit on a ship's hull. plimsoll
//! marks it for your API: one global limit, enforced atomically across every
//! instance of your service, with graceful degradation when the store is
//! down.
//!
//! This crate is a facade. Everything in [`plimsoll_core`] is re-exported at
//! the root, and with the `redis` feature (on by default) the Redis store is
//! available as [`redis::RedisStore`].
//!
//! # Quick start
//!
//! ```no_run
//! use plimsoll::redis::RedisStore;
//! use plimsoll::{Limiter, OnStoreError, Rule};
//! use std::time::Duration;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let store = RedisStore::connect("redis://127.0.0.1/").await?;
//! let limiter = Limiter::builder(store).instances(2).build();
//!
//! let api = Rule::builder("api")
//!     .limit(100)
//!     .window(Duration::from_secs(60))
//!     .build()?;
//!
//! let login = Rule::builder("login")
//!     .limit(5)
//!     .window(Duration::from_secs(60))
//!     .on_store_error(OnStoreError::Deny)
//!     .build()?;
//!
//! let decision = limiter.check(&api, "user:123").await?;
//! if !decision.allowed {
//!     // respond 429 with decision.headers()
//! }
//! # let _ = login;
//! # Ok(()) }
//! ```
//!
//! # Features
//!
//! - `redis` (default): enables [`redis`], the Redis-backed store.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub use plimsoll_core::*;

/// Redis-backed store. Enabled by the `redis` feature.
#[cfg(feature = "redis")]
pub mod redis {
    pub use plimsoll_redis::{RedisConfig, RedisStore};

    /// The underlying `redis` client crate.
    pub use plimsoll_redis::redis as client;
}
