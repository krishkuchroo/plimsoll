//! Two copies of this binary share one limit through Redis.
//!
//! Configuration is by environment variable:
//!
//! | Variable | Default | Meaning |
//! |---|---|---|
//! | `REDIS_URL` | `redis://127.0.0.1/` | Redis connection URL |
//! | `PORT` | `3000` | listen port |
//! | `INSTANCE_NAME` | `instance` | reported in responses and logs |
//! | `INSTANCES` | `2` | starting guess for how many instances share the limit; replaced by the live count from the Redis membership heartbeat |
//! | `RATE_LIMIT` | `10` | requests per window |
//! | `RATE_WINDOW_SECS` | `60` | window length |
//!
//! Requests are keyed by the `x-api-key` header, falling back to `anonymous`,
//! so a plain `curl` against both instances hits the same bucket.
//!
//! `GET /metrics` returns this instance's counters in Prometheus text format.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use plimsoll::redis::{Membership, RedisConfig, RedisStore};
use plimsoll::{Decision, Limiter, Metrics, Rule};
use serde_json::json;
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppState {
    limiter: Arc<Limiter<RedisStore>>,
    metrics: Arc<Metrics>,
    rule: Rule,
    instance: Arc<str>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let redis_url = env_or("REDIS_URL", "redis://127.0.0.1/");
    let port: u16 = env_or("PORT", "3000").parse()?;
    let instance: Arc<str> = env_or("INSTANCE_NAME", "instance").into();
    let instances: u32 = env_or("INSTANCES", "2").parse()?;
    let limit: u64 = env_or("RATE_LIMIT", "10").parse()?;
    let window_secs: u64 = env_or("RATE_WINDOW_SECS", "60").parse()?;

    let rule = Rule::builder("demo")
        .limit(limit)
        .window(Duration::from_secs(window_secs))
        .build()?;

    // Lazy so the instance starts even if Redis is still coming up; the first
    // request opens the connection and any failure goes through the
    // OnStoreError policy (LocalFallback here).
    let store =
        RedisStore::connect_with(redis_url.as_str(), RedisConfig::default().lazy(true)).await?;
    let metrics = Arc::new(Metrics::new());
    let limiter = Limiter::builder(store)
        .instances(instances)
        .observer(Arc::clone(&metrics))
        .build();

    // Instances find each other through Redis every 5 s, so the fallback
    // splits the limit exactly (by instance index) without hand-tuned config.
    let limiter = Arc::new(limiter);
    let membership = Membership::new(limiter.store(), &*instance);
    membership
        .clone()
        .spawn(Arc::clone(&limiter), Duration::from_secs(5));

    let state = AppState {
        limiter: Arc::clone(&limiter),
        metrics,
        rule,
        instance: Arc::clone(&instance),
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/healthz", get(|| async { "ok" }))
        .route("/metrics", get(metrics_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(%instance, port, limit, window_secs, instances, %redis_url, "listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    // Tell the other instances right away instead of waiting for the TTL.
    let _ = membership.leave().await;
    Ok(())
}

async fn index(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let key = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_owned();

    let decision = match state.limiter.check(&state.rule, &key).await {
        Ok(decision) => decision,
        Err(err) => {
            tracing::error!(%err, "rate limit check failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response();
        }
    };

    if decision.degraded {
        tracing::warn!(
            instance = %state.instance,
            error = ?decision.store_error,
            limit = decision.limit,
            "store unavailable, decision came from fallback"
        );
    }

    let body = json!({
        "instance": &*state.instance,
        "key": key,
        "allowed": decision.allowed,
        "limit": decision.limit,
        "remaining": decision.remaining,
        "instances": state.limiter.instances(),
        "instance_index": state.limiter.instance_index(),
        "reset_after_secs": decision.reset_after.as_secs_f64(),
        "retry_after_secs": decision.retry_after_secs(),
        "degraded": decision.degraded,
    });

    let status = if decision.allowed {
        StatusCode::OK
    } else {
        StatusCode::TOO_MANY_REQUESTS
    };
    let mut response = (status, axum::Json(body)).into_response();
    apply_headers(&mut response, &decision);
    response
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.render_prometheus(),
    )
}

/// Copy `Decision::headers()` onto an HTTP response.
fn apply_headers(response: &mut Response, decision: &Decision) {
    let headers = response.headers_mut();
    for (name, value) in decision.headers() {
        if let (Ok(name), Ok(value)) = (name.parse::<HeaderName>(), HeaderValue::from_str(&value)) {
            headers.insert(name, value);
        }
    }
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
    tracing::info!("shutting down");
}
