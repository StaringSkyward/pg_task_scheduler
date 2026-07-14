# pg_task_scheduler

`pg_task_scheduler` is an embeddable PostgreSQL work queue for Rust applications
using Diesel, `diesel-async`, and Tokio. Immediate, delayed, and recurring work
all become durable task rows and execute through the same concurrent worker.

## Guarantees

- Durable at-least-once execution after a task transaction commits.
- Concurrent batch claim with `FOR UPDATE SKIP LOCKED`.
- Token-fenced completion, failure, cancellation, and lease renewal.
- Automatic recovery after worker death or lease expiry.
- Atomic recurring materialization: create the task and advance the schedule
  cursor in the same transaction.
- Immutable execution snapshots: payload, retry policy, and lease policy are
  copied onto the task when it is created.

Exactly-once external side effects are not possible. Handlers must use the stable
`ctx.run_id` as an idempotency key.

See [Concurrency Model](docs/Concurrency.md) for multi-worker behavior, fencing,
failure races, scaling limits, and operational guidance.

## Database Schema

The crate ships its complete, forward-only schema in
`migrations/0001_create_scheduler_tables/up.sql`. It does not embed or install
the schema automatically. Include that SQL in the host application's database
setup before starting workers.

No `down.sql` is provided. Applications that need to remove the queue should
define teardown SQL appropriate to their own data-retention and deployment
requirements.

See [Scheduler Design](docs/SchedulerDesign.md) for the data model, state
transitions, Rust boundaries, and performance design.

## Immediate Work

Define a task once so enqueueing and handler registration share the same payload
type and stable name:

```rust,no_run
use pg_task_scheduler::{Task, EnqueueOptions, enqueue};

struct SendEmail;

impl Task for SendEmail {
    const NAME: &'static str = "send-email";
    type Args = SendEmailArgs;
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SendEmailArgs {
    user_id: uuid::Uuid,
}

# async fn example(conn: &mut diesel_async::AsyncPgConnection) -> Result<(), pg_task_scheduler::SchedulerError> {
let task = enqueue::<SendEmail>(
    conn,
    SendEmailArgs { user_id: uuid::Uuid::new_v4() },
    EnqueueOptions::immediate(),
).await?;
# let _ = task;
# Ok(())
# }
```

`enqueue` accepts the caller's `&mut AsyncPgConnection`. When called inside an
existing Diesel transaction, application data and queued work commit or roll back
together. `EnqueueOptions::at(timestamp)` creates delayed one-off work. Options
also configure priority, lease duration, attempts, retry backoff, and an optional
deduplication key.

## Worker

```rust,no_run
use std::time::Duration;
use pg_task_scheduler::{JobError, Scheduler, WorkerId};
use tokio_util::sync::CancellationToken;
# use pg_task_scheduler::Task;
# struct SendEmail;
# impl Task for SendEmail { const NAME: &'static str = "send-email"; type Args = serde_json::Value; }
# async fn example(pool: diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>) -> Result<(), Box<dyn std::error::Error>> {
let scheduler = Scheduler::builder(pool, WorkerId::try_from("api-1")?)
    .poll_interval(Duration::from_millis(250))
    .register_task::<SendEmail, _, _>(|ctx, args| async move {
        // Stop optional work promptly if lease ownership is lost.
        if ctx.cancellation.is_cancelled() {
            return Err(JobError::retry("lease lost"));
        }
        let _ = args;
        Ok(())
    })?
    .build()?;

let health = scheduler.health();
let _ = health;
scheduler.run_until_shutdown(CancellationToken::new()).await?;
# Ok(())
# }
```

`JobError::retry` requeues work using the task's snapshotted backoff until
`max_attempts` is exhausted. `JobError::permanent`, `JobError::msg`, and ordinary
errors converted with `From` are terminal. Long-running handlers are protected by
automatic token-guarded lease renewal.

## Recurring Schedules

Recurring definitions remain available through `jobs::create`,
`jobs::ensure_job`, pause/resume, and reschedule. When a definition is due, the
materializer creates a normal queue task. Later edits or deletion of the schedule
cannot change or delete that materialized work.

The current misfire policy is `run_once`: after downtime, one occurrence is
created for the missed cursor and the next cursor moves to the next future time.

## Operations

- `store::run_state` inspects current state.
- `cancel` atomically cancels pending or running work and fences its worker.
- `prune_terminal` deletes bounded batches of old terminal history.
- `Scheduler::health` reports starting, healthy, degraded, or stopped state.
- The optional Axum router exposes schedule administration plus task state and
  cancellation routes.

The claim hot path is backed by a partial pending index; expired leases and
terminal retention have separate partial indexes. Attempt history is stored in
`scheduler_run_attempts` and cascades only when terminal history is explicitly
pruned.

## Feature Flags

| Feature | Default | Purpose |
| --- | --- | --- |
| `deadpool` | yes | Deadpool implementation of `SchedulerPool` |
| `bb8` | no | bb8 implementation of `SchedulerPool` |
| `mobc` | no | mobc implementation of `SchedulerPool` |
| `metrics` | no | Datadog counters through `gnort` |
| `axum` | no | Optional administration router |

## Tests

```sh
cp .env.example .env
docker compose up -d db
cargo test --all-features
```

Every integration test installs the complete schema into an isolated PostgreSQL
namespace.
