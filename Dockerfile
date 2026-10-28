# cloudflared ships in the image for deployments that reach Redis through a
# Cloudflare Access TCP tunnel: the deploy's entrypoint runs `cloudflared access
# tcp` beside the proxy, which then connects to 127.0.0.1. Copied from
# Cloudflare's own image, pinned by digest so Renovate bumps tag and digest
# together; the binary is static, so the bookworm base needs nothing extra.
# Inert unless an entrypoint starts it. Run it at `--loglevel info` or quieter:
# at debug it logs the WebSocket request headers, Access client secret included.
# cloudflared is Apache-2.0; the runtime base already ships that license text at
# /usr/share/common-licenses/Apache-2.0, and cloudflared publishes no NOTICE file.
FROM cloudflare/cloudflared:2026.10.5@sha256:1b1d34d0a9c0bfca9de72db8460d328db3f19edd2b96169114e3e893a97bbe53 AS cloudflared

FROM rust:bookworm AS builder
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

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 1000 app && useradd -u 1000 -g 1000 -s /sbin/nologin app
COPY --from=builder /app/target/release/anthropic-lb /usr/local/bin/
COPY --from=cloudflared --chown=0:0 /usr/local/bin/cloudflared /usr/local/bin/
USER 1000
EXPOSE 8082
ENTRYPOINT ["anthropic-lb"]
CMD ["/etc/anthropic-lb/config.toml"]
