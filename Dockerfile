# syntax=docker/dockerfile:1.7
# Builds the axum demo. Two copies of the resulting image share one limit
# through Redis; see docker-compose.yml.

FROM rust:1-slim-bookworm AS builder
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release -p axum-demo \
    && cp target/release/axum-demo /usr/local/bin/axum-demo

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/axum-demo /usr/local/bin/axum-demo
ENV PORT=3000
EXPOSE 3000
HEALTHCHECK --interval=5s --timeout=2s --retries=3 \
    CMD curl -fsS http://127.0.0.1:3000/healthz || exit 1
CMD ["axum-demo"]
