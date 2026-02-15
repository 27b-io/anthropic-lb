FROM rust:1.85-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/anthropic-lb /usr/local/bin/
EXPOSE 8082
ENTRYPOINT ["anthropic-lb"]
CMD ["--config", "/etc/anthropic-lb/config.toml"]
