# NeuralMimicry Relay

Durable enterprise ingestion and Conductor-approved Refiner actions.

Relay provides the first shared non-AI connector and delivery boundary over Refiner's existing MCP adapters. It does not replace Gail provider transport, neural event bridges, OctoBot execution or Continuum infrastructure.

POST /v1/ingest accepts a bounded batch, expected_checkpoint, new checkpoint and ordered changes. A database transaction writes all events, current source metadata and checkpoint together. A per-source advisory lock and compare-and-swap permit independent sources to proceed concurrently. Identical current-batch replay succeeds; conflicting reuse fails. Repeated old event IDs cannot roll current permissions back. Tombstones contain null data. GET /v1/changes?after=N filters through current permissions and returns a continuation cursor, including across fully denied pages. Readers receive tombstones for previously visible events. GET /v1/sources/{source}/{object}?purpose=... supplies the current grant to other products.

Source metadata must come from a trusted control-level adapter and contains explicit readers, allowed purposes, expiry (at most 24 hours), observation time and independent training permission. A control-level source publisher is a trust boundary. The initial source change adapter is the push contract; cron/webhook/native-source connectors can feed it without implementing another transport stack.

POST /v1/connectors/refiner/read forwards to the configured Refiner MCP resource endpoint under the caller's bearer. POST /v1/actions supports only refiner_mcp operations. It checks live Customers control access, current source execute rights, and live Conductor execution_approved plus exact plan.relay_operation JSON. Argument, version, target or source changes invalidate approval.

Before dispatch it inserts a unique durable receipt scoped by tenant/idempotency_key. Another process or a retry returns the original receipt. Crashes, timeouts, tool errors and ambiguous results remain reconciliation_required; Relay never automatically resubmits a write. MCP isError=false permits a completed transport receipt, not a claim that every downstream business effect was independently verified. GET /v1/receipts/{id} exposes the actor's receipt. Automatic reconciliation and compensating actions require a target-specific adapter and are not implemented. There is no universal exactly-once guarantee and no unapproved arbitrary URL, shell, trading or infrastructure executor.

## Build and run

Rust 1.98.1 or newer, PostgreSQL 16 or newer. Run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test` (add `--workspace` in Verity). Lockfiles are committed. The shared kernel is a pinned Git dependency from neuralmimicry/verity. CI receives a read-only Verity deploy key through VERITY_READ_KEY. Never replace it with an unrestricted personal access token.

Set DATABASE_URL to a dedicated least-privilege product database, CUSTOMERS_URL to Customers, NM_TENANT to the single configured identity realm (default neuralmimicry), and NM_LISTEN (default 0.0.0.0:8080). Service URLs use <SERVICE>_URL; GAIL_TOKEN and CONDUCTOR_TOKEN are scoped upstream credentials. Caller bearer tokens are only forwarded to Customers, Relay, Verity and Refiner as required. TLS certificate verification is enabled and redirects are disabled. PostgreSQL uses an internal network connection; require cluster network isolation or a trusted TLS proxy for databases across trust boundaries.

`cargo run --release` starts the service. GET /healthz checks the process; GET /readyz checks its database. Business APIs require a live Customers observe/use/control grant for this service. Source operations also require Relay observe access and current source-purpose rights. The service never trusts forwarded identity headers or treats service-account tokens as human users. Each deployment is a single tenant realm; use separate databases and deployments for different identity realms.

HTTP bodies are limited to 1 MiB, upstream responses to 2 MiB, concurrent requests to 64, the database pool to eight and CPU evaluations to four. Requests have a 30-second timeout; CPU jobs retain their permits after caller cancellation. Product databases store mutable governed content; do not put their raw payloads in an immutable ledger. Logs avoid tokens and request bodies.

## Operations and acceptance

The deployment owner is SwarmHPC. Use `swarmhpc/ansible/continuum_tenant_relay_site.yml`, or `continuum_tenant_business_suite_site.yml` for all four. See [operations](docs/operations.md) and [QA](docs/qa.md). Production evidence must record source commit, image digest, rollout/readiness, functional checks and limitations. A healthy pod does not qualify a model or prove an external business effect.

