//! Instance discovery through Redis: count, index, leaving, expiry, and the
//! background task feeding the limiter.

mod support;

use std::sync::Arc;
use std::time::Duration;

use plimsoll_core::Limiter;
use plimsoll_redis::{Members, Membership, RedisConfig, RedisStore};
use support::{connect, redis_url, unique_rule};

/// A store with a prefix no other test uses, so membership sets don't mix.
async fn isolated_store() -> Option<RedisStore> {
    connect().await?;
    let prefix = unique_rule("members");
    let config = RedisConfig::default().prefix(prefix);
    Some(RedisStore::connect_with(redis_url(), config).await.unwrap())
}

#[tokio::test]
async fn count_and_index_follow_sorted_ids() {
    let Some(store) = isolated_store().await else {
        return;
    };
    let c = Membership::new(&store, "c");
    let a = Membership::new(&store, "a");
    let b = Membership::new(&store, "b");

    assert_eq!(c.heartbeat().await.unwrap(), Members { count: 1, index: 0 });
    a.heartbeat().await.unwrap();
    b.heartbeat().await.unwrap();
    // Sorted ids: a, b, c.
    assert_eq!(a.heartbeat().await.unwrap(), Members { count: 3, index: 0 });
    assert_eq!(b.heartbeat().await.unwrap(), Members { count: 3, index: 1 });
    assert_eq!(c.heartbeat().await.unwrap(), Members { count: 3, index: 2 });

    b.leave().await.unwrap();
    assert_eq!(c.heartbeat().await.unwrap(), Members { count: 2, index: 1 });
    for m in [&a, &c] {
        m.leave().await.unwrap();
    }
}

#[tokio::test]
async fn silent_instances_expire() {
    let Some(store) = isolated_store().await else {
        return;
    };
    let ttl = Duration::from_millis(300);
    let alive = Membership::new(&store, "alive").ttl(ttl);
    let silent = Membership::new(&store, "silent").ttl(ttl);

    silent.heartbeat().await.unwrap();
    assert_eq!(alive.heartbeat().await.unwrap().count, 2);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        alive.heartbeat().await.unwrap(),
        Members { count: 1, index: 0 }
    );
    alive.leave().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_keeps_the_limiter_up_to_date() {
    let Some(store) = isolated_store().await else {
        return;
    };
    let every = Duration::from_millis(50);
    let x = Membership::new(&store, "x");
    let y = Membership::new(&store, "y");
    let lx = Arc::new(Limiter::new(store.clone()));
    let ly = Arc::new(Limiter::new(store.clone()));
    let tx = x.clone().spawn(Arc::clone(&lx), every);
    let ty = y.clone().spawn(Arc::clone(&ly), every);

    let mut seen = (0, 0, 0, 0);
    for _ in 0..40 {
        tokio::time::sleep(every).await;
        seen = (
            lx.instances(),
            lx.instance_index(),
            ly.instances(),
            ly.instance_index(),
        );
        if seen == (2, 0, 2, 1) {
            break;
        }
    }
    tx.abort();
    ty.abort();
    x.leave().await.unwrap();
    y.leave().await.unwrap();
    assert_eq!(seen, (2, 0, 2, 1), "(x count, x index, y count, y index)");
}
