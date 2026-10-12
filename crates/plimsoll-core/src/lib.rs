//! Core types for [plimsoll](https://github.com/krishkuchroo/plimsoll), a
//! distributed rate limiter for Rust.
//!
//! This crate has no network I/O and no framework dependencies. It defines:
//!
//! - [`Rule`]: a named limit (`limit` requests per `window`) built with
//!   [`Rule::builder`].
//! - [`Decision`]: the result of a check, with everything needed to answer an
//!   HTTP request, including [`Decision::headers`].
//! - [`Store`]: the trait a backend implements. All read-evict-check-insert
//!   logic must be atomic on the backend side.
//! - [`LocalStore`]: an in-process store used on its own for single-instance
//!   services and by [`Limiter`] as the [`OnStoreError::LocalFallback`] path.
//! - [`Limiter`]: ties a store, a fallback policy and rules together.
//!
//! The algorithm is a sliding window log: every allowed request is recorded
//! with its timestamp and cost, entries older than the window are evicted on
//! each check, and a request is allowed when the sum of costs in the window
//! plus the request's cost fits under the limit. Denied requests are not
//! recorded. See [`WindowLog`] for the reference implementation that the
//! Redis Lua script mirrors.
//!
//! # Example
//!
//! ```
//! use plimsoll_core::{Limiter, LocalStore, Rule};
//! use std::time::Duration;
//!
//! # tokio_test::block_on(async {
//! let limiter = Limiter::new(LocalStore::new());
//! let rule = Rule::builder("api")
//!     .limit(2)
//!     .window(Duration::from_secs(60))
//!     .build()
//!     .unwrap();
//!
//! assert!(limiter.check(&rule, "user:1").await.unwrap().allowed);
//! assert!(limiter.check(&rule, "user:1").await.unwrap().allowed);
//! let denied = limiter.check(&rule, "user:1").await.unwrap();
//! assert!(!denied.allowed);
//! assert!(denied.retry_after.is_some());
//! # });
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod decision;
mod limiter;
mod local;
mod rule;
mod store;
mod window_log;

pub use decision::Decision;
pub use limiter::{LimitError, Limiter, LimiterBuilder};
pub use local::LocalStore;
pub use rule::{OnStoreError, Rule, RuleBuilder, RuleError};
pub use store::{CheckOutcome, CheckRequest, Store, StoreError};
pub use window_log::WindowLog;
