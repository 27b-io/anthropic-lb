FROM rust:1.85-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 1000 app && useradd -u 1000 -g 1000 -s /sbin/nologin app
COPY --from=builder /app/target/release/anthropic-lb /usr/local/bin/
USER 1000
EXPOSE 8082
ENTRYPOINT ["anthropic-lb"]
CMD ["/etc/anthropic-lb/config.toml"]
