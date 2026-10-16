//! Helpers shared by the Redis integration tests.
//!
//! Redis URL from `PLIMSOLL_REDIS_URL`, default `redis://127.0.0.1:6379/`.
//! Tests skip with a message when Redis is not reachable, unless
//! `PLIMSOLL_REQUIRE_REDIS` is set (use that in CI).

#![allow(dead_code)]

pub mod proxy;

use std::sync::atomic::{AtomicU64, Ordering};

use plimsoll_redis::redis::{self, AsyncCommands};
use plimsoll_redis::{DEFAULT_PREFIX, RedisStore};

pub fn redis_url() -> String {
    std::env::var("PLIMSOLL_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into())
}

/// Connect, or return `None` (and say why) when Redis is not available.
/// A failed attempt is remembered so the property test doesn't retry it
/// for every case.
pub async fn connect() -> Option<RedisStore> {
    static UNREACHABLE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    if UNREACHABLE.get().is_some() {
        return None;
    }
    match RedisStore::connect(redis_url()).await {
        Ok(store) => Some(store),
        Err(e) if std::env::var_os("PLIMSOLL_REQUIRE_REDIS").is_some() => {
            panic!("Redis required but not reachable at {}: {e}", redis_url())
        }
        Err(e) => {
            eprintln!("SKIPPED: Redis not reachable at {} ({e})", redis_url());
            let _ = UNREACHABLE.set(());
            None
        }
    }
}

/// A rule name no other test run will use.
pub fn unique_rule(tag: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "test-{}-{nanos}-{}-{tag}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    )
}

pub async fn delete_keys(rule: &str, key: &str) {
    let client = redis::Client::open(redis_url()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = conn
        .del(&RedisStore::keys_with_prefix(DEFAULT_PREFIX, rule, key))
        .await
        .unwrap();
}
