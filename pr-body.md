## Problem

`operators` is one bit meaning two things: *may read `/_stats` and `/metrics`*, and *bypasses every request policy*. `pre_request_gate` returns early for an operator, above the per-client model allow-list, the daily token budget, the utilization ceiling and the emergency brake.

The consequence is that the only credential you can hand to a metrics scrape, an uptime check or a dashboard datasource is also unmetered authority to spend against every configured account — and a `client_budgets` entry written for that name is dead config the gate never reaches.

## Change

A second role, `readers`, carrying the read bit alone:

- `authorize_admin` admits it to `/_stats` and `/metrics`, alongside `operators`.
- `pre_request_gate` refuses it with `403` on every proxied surface. The refusal happens before endpoint selection, so there is no upstream call and no usage or budget record.
- `operators` semantics are untouched.

One placement covers the whole proxied surface: the router is `/_stats`, `/metrics`, `/v1/chat/completions` and a `.fallback(...)` catch-all, so `/v1/messages`, `/v1/models` and the `/upstream/<name>/...` forwards all reach the same gate.

The deny is checked *above* the operator bypass. The roles are disjoint by boot validation, so the ordering cannot matter for any config that boots; putting it first means that if that validation is ever weakened, an overlap resolves to the denial rather than to the wider grant.

### Startup validation

- `readers` joins the existing cross-check chain — every name must match a configured `[[clients]]` entry, like `client_budgets` / `operators` / `[response_cache].clients`.
- A name in both `readers` and `operators` is a boot error naming the offender.
- `readers` without `[[clients]]` is rejected. Under a single shared `proxy_key` the key holder is the operator by construction and proxy-path client ids are caller-asserted, so the role could not be enforced — it would read as scoping something while scoping nothing. There are no existing configs carrying the key, so nothing regresses.

## Tests

Nine tests extending the existing auth coverage rather than a parallel harness:

- Read-only principal: `200` on both read surfaces; `403` on `POST /v1/messages`, `POST /v1/chat/completions` and `GET /v1/models`. The upstream in that test counts its hits, so *no upstream call* is asserted, not assumed, and the usage and budget maps are asserted empty.
- The other two principal classes unchanged — the pre-existing operator/plain/unauthenticated matrix test is untouched and still passes, plus a gate test that a plain client keeps its allow-list treatment.
- Gate-level: the role is denied; the denial beats the operator bypass when a name holds both roles (the state boot validation rejects — pinning the fail-closed ordering as behaviour rather than a comment).
- Startup validation: unknown name, both-lists overlap, and the missing-`[[clients]]` rejection, plus a disjoint-roles happy path.

Each of the three load-bearing arms was mutation-checked — disabling the gate deny, reverting `authorize_admin`, and moving the deny below the bypass each produce a failing test.

`cargo fmt --check`, `RUSTFLAGS="-Dwarnings" cargo clippy --all-targets` and the full suite (718 tests) are green locally.

## Docs

`config.toml.example`, the config reference table, the security-posture table, and a short "give a scrape `readers`, not `operators`" note in the security section.
