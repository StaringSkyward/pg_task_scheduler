# pg_task_scheduler

`pg_task_scheduler` is an embeddable Rust crate for cron-like distributed job scheduling backed by PostgreSQL. It is designed to slot into an existing Rust application that already uses PostgreSQL, Diesel, and Tokio, without introducing external infrastructure such as Redis or a workflow engine.

## Execution guarantee

The scheduler provides **leased at-least-once execution per scheduled occurrence**. Each occurrence is a durable row in PostgreSQL. A worker claims it using `FOR UPDATE SKIP LOCKED` and a fencing token; if the worker crashes or exceeds its lease, another worker reclaims the row after the lease expires. Because a handler may therefore run more than once for the same occurrence, **handlers must be idempotent** — use `ctx.run_id` or `ctx.scheduled_for` as idempotency keys.

## Data model

Run state is decomposed across three relations so that invalid combinations are unrepresentable. `scheduler_runs` is the immutable occurrence (created once; carries no status, lease, or outcome columns). `scheduler_run_leases` exists *if and only if* the run is currently claimed: its presence means "running" and its absence means "pending". `scheduler_run_outcomes` exists *if and only if* the run is terminal (completed or failed). Derived status — `Pending`, `Running`, `Completed`, `Failed` — is a Rust sum type computed from the presence or absence of these rows, never stored as a column. Two database triggers enforce the "lease XOR outcome" invariant at the schema level. See [`SchedulerDesign.md`](SchedulerDesign.md) for the full design.

## Applying migrations

The crate ships raw SQL files under `migrations/0001_create_scheduler_tables/{up.sql,down.sql}`. It does **not** embed or auto-run them. Copy them into your application's Diesel migration set (or apply with the Diesel CLI) before starting the scheduler.

## Usage

```rust
use std::time::Duration;
use pg_task_scheduler::{Scheduler, WorkerId, JobContext, JobError};
use tokio_util::sync::CancellationToken;

#[derive(serde::Deserialize)]
struct DigestArgs { user_id: uuid::Uuid }

async fn send_digest(ctx: JobContext, _args: DigestArgs) -> Result<(), JobError> {
    // ctx.run_id and ctx.scheduled_for are stable across retries —
    // use them as idempotency keys so re-execution is safe.
    let _ = ctx.run_id;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `pool` is a diesel_async deadpool (or bb8/mobc) Pool<AsyncPgConnection>.
    let scheduler = Scheduler::builder(pool, WorkerId::try_from("api-1")?)
        .poll_interval(Duration::from_secs(1))
        .register::<DigestArgs, _, _>("send_digest_email", send_digest)?
        .build()?;
    scheduler.run_until_shutdown(CancellationToken::new()).await?;
    Ok(())
}
```

`build()` errors if no handler has been registered. `run_until_shutdown` drains in-flight handlers on cancellation before returning.

## Feature flags

| Feature      | Default | Description                                                          |
|--------------|---------|----------------------------------------------------------------------|
| `deadpool`   | yes     | `SchedulerPool` impl for `diesel-async`'s deadpool integration       |
| `bb8`        | no      | `SchedulerPool` impl for `diesel-async`'s bb8 integration            |
| `mobc`       | no      | `SchedulerPool` impl for `diesel-async`'s mobc integration           |
| `metrics`    | no      | Emit counters via `gnort` (runs materialized, claimed, completed, failed, reaped) |
| `axum`       | no      | Admin routes for listing, pausing, resuming, and inspecting jobs     |

Enable exactly one pool feature. `deadpool` is selected by default.

## Running the tests

Integration tests require a PostgreSQL 13+ instance. Set `DATABASE_URL` and run:

```sh
DATABASE_URL=postgres://postgres:postgres@localhost:5432/mydb cargo test
```

Each test creates its own randomly-named schema and tears it down after, so they are safe to run against a shared development database.

## Design reference

See [`SchedulerDesign.md`](docs/SchedulerDesign.md) for the full architecture, data model, execution semantics, and Rust stack.
