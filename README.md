# pg_task_scheduler

> **v2 status:** this README describes the queue specified in
> [docs/postgresql_queue_design.md](docs/postgresql_queue_design.md). The
> implementation is tracked by
> [the v2 plan](docs/superpowers/plans/2026-11-10-postgresql-queue-design.md).

`pg_task_scheduler` is an embeddable PostgreSQL task queue for Rust applications
using Diesel, `diesel-async`, and Tokio. PostgreSQL is the system of record for
task state, scheduling, retries, ownership, and completion. Immediate tasks,
scheduled tasks, and retries all use the same `due_at` mechanism.

## Guarantees

- **Idempotent enqueue:** each producer-supplied `task_key` maps to exactly one
  logical task for the idempotency horizon. A retried enqueue returns the
  existing task. The same key with different content is rejected as an
  idempotency conflict.
- **Atomic enqueue with domain state:** enqueue on the caller's connection, and
  the task commits or rolls back with the caller's own writes.
- **One valid owner per attempt:** ownership is the task id, `locked_by`, a
  monotonically increasing `lock_token`, and an unexpired `locked_until`. Every
  worker transition checks all four.
- **Hard leases:** once `locked_until` passes, the old worker can no longer
  heartbeat, complete, retry, or fail the task, even if the reaper has not run.
- **Exactly-once PostgreSQL-contained effects:** business writes and the `done`
  transition can share one transaction.
- **At-least-once external effects:** a worker can perform a remote effect and
  crash before recording completion. External effects are effectively
  exactly-once only when the destination deduplicates by `task_key` (or fences
  by `lock_token`).
- **Best-effort priority:** `FOR UPDATE SKIP LOCKED` can skip a locked
  high-priority task and run a lower one.

See [Concurrency Model](docs/Concurrency.md) for races, fencing, and scaling,
and [Scheduler Design](docs/SchedulerDesign.md) for the data model and state
machine.

## Database Schema

The crate ships forward-only SQL. It does not install schema automatically.
Include it in the host application's migrations before starting workers:

| File | Contents |
| --- | --- |
| `migrations/0001_create_queue_tables/up.sql` | `queue_tasks`, `queue_task_attempts`, `queue_outbox`, `queue_task_replays` |
| `migrations/0002_create_schedule_tables/up.sql` | `scheduler_jobs` (only with the `cron` feature) |

## Enqueue

Define a task once so producers and handlers share the name and payload type:

```rust,no_run
use pg_task_scheduler::{EnqueueOptions, Task, TaskKey, enqueue};

struct SendEmail;

impl Task for SendEmail {
    const NAME: &'static str = "send-email";
    type Args = SendEmailArgs;
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SendEmailArgs {
    user_id: uuid::Uuid,
}

# async fn example(conn: &mut diesel_async::AsyncPgConnection, user_id: uuid::Uuid) -> Result<(), Box<dyn std::error::Error>> {
// A retry of the same logical request MUST reuse the same key.
let key = TaskKey::try_from(format!("user:{user_id}:welcome-email"))?;
let task = enqueue::<SendEmail>(conn, key, SendEmailArgs { user_id }, EnqueueOptions::immediate()).await?;
if !task.inserted {
    // A duplicate request: `task` describes the existing logical task.
}
# Ok(())
# }
```

`EnqueueOptions::at(timestamp)` schedules the task for later. `.priority(..)`
(higher runs first) and `.max_attempts(..)` (default 3) tune it. When the
connection is inside a transaction, the task and its `queue_available`
notification commit with the caller's data. Run producers at `READ COMMITTED`.

**Idempotency horizon:** a `task_key` stays reserved while its row exists. Rows
are kept for at least `Retention::idempotency_horizon` (default 7 days), and
workers refuse a retention shorter than that. Producers must not reuse a key
within the horizon for a different logical task.

## Worker

```rust,no_run
use std::time::Duration;
use pg_task_scheduler::{Scheduler, TaskError};
use tokio_util::sync::CancellationToken;
# use pg_task_scheduler::Task;
# struct SendEmail;
# impl Task for SendEmail { const NAME: &'static str = "send-email"; type Args = serde_json::Value; }
# async fn deliver(_idempotency_key: &str, _args: serde_json::Value) -> Result<(), std::io::Error> { Ok(()) }
# async fn example(pool: diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>) -> Result<(), Box<dyn std::error::Error>> {
let scheduler = Scheduler::builder(pool)
    .lease_duration(Duration::from_secs(60))
    .heartbeat_interval(Duration::from_secs(20))
    .register_task::<SendEmail, _, _>(|ctx, args| async move {
        // task_key is stable across retries: use it as the remote idempotency key.
        deliver(ctx.task_key.as_str(), args)
            .await
            .map_err(|e| TaskError::retry(e.to_string()))
    })?
    .build()?;

scheduler.run_until_shutdown(CancellationToken::new()).await?;
# Ok(())
# }
```

Each scheduler generates a unique UUID `worker_id` at startup. It claims due
tasks for its registered names, heartbeats every `heartbeat_interval`, and
completes or fails each task with ownership-guarded statements. If a heartbeat
proves ownership was lost, it cancels `ctx.cancellation` and drops the handler.
The handler must then stop issuing new non-idempotent side effects.

Handlers classify every failure:

- `TaskError::retry(..)` requeues with exponential backoff and jitter until
  `max_attempts` is exhausted, then marks the task `dead`.
- `TaskError::permanent(..)`, and any error converted with `?`, marks the task
  `dead` immediately.

### PostgreSQL-contained work

When a task's whole effect is database writes, do the writes and the completion
in one transaction. Either both commit or neither does:

```rust,no_run
use diesel_async::{AsyncConnection, RunQueryDsl};
use pg_task_scheduler::{SchedulerError, TaskContext, TaskError, store};

async fn ship_order(
    pool: diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>,
    ctx: TaskContext,
    order_id: i64,
) -> Result<(), TaskError> {
    let mut conn = pool.get().await.map_err(|e| TaskError::retry(e.to_string()))?;
    conn.transaction::<_, SchedulerError, _>(async |c| {
        // Lock and validate ownership first; the reaper skips a locked row.
        store::lock_owned(c, &ctx.lease).await?;
        diesel::sql_query("UPDATE orders SET shipped = true WHERE id = $1")
            .bind::<diesel::sql_types::BigInt, _>(order_id)
            .execute(c)
            .await?;
        // Zero rows (lease lost or expired) is an error, so the transaction rolls back.
        store::complete(c, &ctx.lease).await
    })
    .await
    .map_err(|e| TaskError::retry(e.to_string()))
}
```

The lease must outlast the transaction. A transaction that finishes after
`locked_until` cannot complete the task, and it rolls back.

### Transactional outbox

To emit a command for later delivery, complete with
`store::complete_with_outbox`. The outbox row is inserted only if the
ownership-guarded completion succeeds:

```rust,no_run
# use pg_task_scheduler::{OutboxCommand, SchedulerError, TaskContext, store};
# async fn example(conn: &mut diesel_async::AsyncPgConnection, ctx: &TaskContext) -> Result<(), SchedulerError> {
let command = OutboxCommand {
    effect_name: "order-shipped".into(),
    // Any unambiguous, stable encoding of (task_key, effect_name).
    command_key: serde_json::to_string(&(ctx.task_key.as_str(), "order-shipped"))?,
    topic: "orders".into(),
    payload: serde_json::json!({ "order": 12345 }),
};
let inserted = store::complete_with_outbox(conn, &ctx.lease, &command).await?;
# let _ = inserted;
# Ok(())
# }
```

Publishing `queue_outbox` rows is the application's job. The relay must set
`published_at`, and receivers must deduplicate by `command_key`.

## Operations

- **Reaper:** every worker runs it every `reaper_interval`. Expired leases go
  back to `queued` after `crash_retry_delay`, or to `dead` when no attempts
  remain. Running it on many instances is safe.
- **Retention:** the same maintenance task deletes `done` and `dead` rows and
  published outbox rows older than their `Retention` periods, in bounded
  batches. Attempt history is deleted with its task.
- **Replay:** `queue::replay(conn, task_id, operator, reason)` moves a `dead`
  task back to `queued` with the same `task_key`, resets `attempts`, and bumps
  `lock_token`. Each replay is recorded in `queue_task_replays`. Enqueue a new
  `task_key` instead if you want a new external effect.
- **Inspection:** `store::task`, `store::attempts`, and `store::queue_stats`
  (counts by status, due count, oldest due age, depth by name and priority, and
  outbox backlog).
- **Health:** `Scheduler::health()` reports starting, healthy, degraded, or
  stopped.
- **Timeouts:** set `statement_timeout` and `lock_timeout` on pool connections.
  Queue statements are short.
- **Connections:** each running scheduler holds one pool connection for
  `LISTEN queue_available`, and briefly takes others for claims, heartbeats,
  finalization, and maintenance. Handlers hold no connection unless they take
  one.

### Observability

The crate emits signals through the standard facades and configures neither of
them:

- **Logs and spans** use [`tracing`](https://docs.rs/tracing). Each task runs
  inside a `task` span with `task_id`, `task_key`, `task_name`, `attempt_no`,
  `worker_id`, and `lock_token`. Failure events add `classification`
  (`retryable` or `permanent`) and, for retries, `retry_due_at`. Install any
  subscriber you like.
- **Metrics** use [`metrics`](https://docs.rs/metrics). Install a recorder or
  exporter (Prometheus, StatsD, OpenTelemetry, …) in the application. Without
  one, every metric call is a no-op.

| Metric | Kind | Labels |
| --- | --- | --- |
| `pg_task_scheduler_tasks_claimed_total` | counter | `task_name` |
| `pg_task_scheduler_tasks_done_total` | counter | `task_name` |
| `pg_task_scheduler_tasks_retried_total` | counter | `task_name` |
| `pg_task_scheduler_tasks_dead_total` | counter | `task_name` |
| `pg_task_scheduler_leases_lost_total` | counter | `task_name` |
| `pg_task_scheduler_heartbeat_failures_total` | counter | `task_name` |
| `pg_task_scheduler_leases_expired_total` | counter | (none; reaper) |
| `pg_task_scheduler_cleanup_deleted_total` | counter | `kind` = `task` or `outbox` |
| `pg_task_scheduler_claim_latency_seconds` | histogram | `task_name` (claim time − `due_at`, database clock) |
| `pg_task_scheduler_task_duration_seconds` | histogram | `task_name` |
| `pg_task_scheduler_db_query_seconds` | histogram | `operation` = `claim`, `heartbeat`, `complete`, `retry`, `fail`, `reap` or `cleanup` |

Queue-level gauges (counts by status, due count, oldest due age, depth by name
and priority, outbox backlog) come from `store::queue_stats`. They are not
emitted automatically because that query scans retained rows. Poll it at an
interval that suits you and publish the values as gauges.

### Admin router

The optional Axum router (`axum` feature) serves `GET /tasks/:id` and
`POST /tasks/:id/replay` (plus schedule routes with `cron`). The host must put
authentication in front of it.

## Recurring Schedules

With the `cron` feature, `jobs::create`, `jobs::ensure_job`, pause/resume, and
reschedule manage rows in `scheduler_jobs`. Each due occurrence becomes an
ordinary queue task with `task_key = "schedule:{id}:{scheduled_for}"`, so a
re-materialized occurrence never creates a second task. Deleting or editing a
schedule does not change tasks it already created. After downtime, one
occurrence is created for the missed slot (`run_once`).

## Feature Flags

| Feature | Default | Purpose |
| --- | --- | --- |
| `deadpool` | yes | Deadpool implementation of `SchedulerPool` |
| `bb8` | no | bb8 implementation of `SchedulerPool` |
| `mobc` | no | mobc implementation of `SchedulerPool` |
| `axum` | no | Administration router |
| `cron` | no | Recurring schedules as a queue producer |

## Tests

```sh
cp .env.example .env
docker compose up -d db
cargo test --all-features
```

Every integration test installs the schema into an isolated PostgreSQL schema.
