## What

`x-forwarded-for`, `x-real-ip`, `forwarded` and `true-client-ip` exist for the proxy's own `resolve_client_ip`. Behind cloudflared or the Cloudflare Worker they carry the caller's real edge IP, so relaying them upstream made every pooled-account request correlatable to the individual caller behind the proxy. Both forward paths — `forward_anthropic` and the OpenAI-compat → Anthropic translation — now drop them after client-IP resolution.

New top-level knob **`forward_client_ip`** (default `false`) restores relaying for operators who want the caller IP visible upstream. Edge-added `cf-*` headers remain the ingress's job per #166.

Ruling: Ray, 2026-09-06 — "strip by default, env flagged". The knob is a TOML key rather than an environment variable because the binary's only configuration input is the TOML file (every other operator knob lives there); switching to an env var is a five-line change if preferred.

## Tests (written first, watched fail)

- `messages_path_strips_caller_identity_headers_upstream_by_default` — failed red on `x-forwarded-for` reaching a header-capturing mock upstream
- `chat_completions_path_strips_caller_identity_headers_upstream_by_default` — same, on the translation path
- `forward_client_ip_true_relays_caller_identity_headers_on_messages_path` / `…_on_chat_completions_path` — knob wiring on each call site
- Unrelated headers (`x-custom-trace`) asserted still forwarded

Full suite: 635 + 18 + 12 pass; `cargo fmt --check` and `cargo clippy --all-targets -D warnings` clean.

## Rollout

Phase-2 gate of the Cloudflare Containers rollout: gates Phase 3 traffic, not the Phase 2 deploy. Ships in the same release as #169; Phase 2 pins that image.

Closes #168
Closes LAB-3030
