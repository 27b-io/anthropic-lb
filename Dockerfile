FROM rust:bookworm@sha256:9181b56c2a21ab1d5e73cffe04fc762426d30aa7fac3c91141dd033f49a7fc8b AS builder
WORKDIR /app
# rust-toolchain.toml must ride along: rustup in the builder reads it, keeping
# the image on the pinned compiler. Drop it and the build silently floats on
# whatever stable the base image ships — untested by CI.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src/ src/
# GIT_SHA feeds anthropic_lb_info{revision} on /metrics; defaults to "unknown"
# so a plain `docker build .` still works.
ARG GIT_SHA=unknown
RUN ANTHROPIC_LB_GIT_SHA=$GIT_SHA cargo build --release --locked

FROM debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 1000 app && useradd -u 1000 -g 1000 -s /sbin/nologin app
COPY --from=builder /app/target/release/anthropic-lb /usr/local/bin/
USER 1000
EXPOSE 8082
ENTRYPOINT ["anthropic-lb"]
CMD ["/etc/anthropic-lb/config.toml"]
