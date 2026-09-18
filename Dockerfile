FROM rust:bookworm AS builder
WORKDIR /app
# rust-toolchain.toml must ride along: rustup in the builder reads it, keeping
# the image on the pinned compiler. Drop it and the build silently floats on
# whatever stable the base image ships — untested by CI.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
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
