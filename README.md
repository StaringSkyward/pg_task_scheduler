# pg_task_scheduler

`pg_task_scheduler` is an embeddable PostgreSQL task queue for Rust applications
using Diesel, `diesel-async`, and Tokio. PostgreSQL is the system of record for
task state, scheduling, retries, ownership, and completion. Immediate tasks,
scheduled tasks, and retries all use the same `due_at` mechanism.

**Status: early alpha.** The API and the database schema may change between 0.x
releases.

Requires PostgreSQL 13 or later, Rust 1.88 or later, Tokio, Diesel 2.3 and
`diesel-async` 0.9.

Try it locally with the example app: `docker compose up` in
[example-app/](https://github.com/StaringSkyward/pg_task_scheduler/tree/main/example-app) starts a demo with a web page, two
workers and the admin UI.

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
  high-priority task and run a lower one. Priority applies within a queue; use
  queues to isolate work that must not starve.

See [Concurrency Model](https://github.com/StaringSkyward/pg_task_scheduler/blob/main/docs/Concurrency.md) for races, fencing, and
scaling, and [Scheduler Design](https://github.com/StaringSkyward/pg_task_scheduler/blob/main/docs/SchedulerDesign.md) for the data model
and state machine. The optional admin UI is the separate
[`pg_task_scheduler_admin`](https://github.com/StaringSkyward/pg_task_scheduler/blob/main/admin/README.md) crate, available on GitHub
(see [Admin UI](#admin-ui));
[Admin UI decisions](https://github.com/StaringSkyward/pg_task_scheduler/blob/main/docs/AdminUI.md) records what it, cancel, pause and the
worker registry do, and why.

## Database Schema

The crate ships forward-only SQL. It does not install schema automatically.
Include it in the host application's migrations before starting workers. The
files are in the published crate, and in the repository's
[`migrations/`](https://github.com/StaringSkyward/pg_task_scheduler/tree/v0.1.0/migrations) directory at the tag matching your
version:

| File | Contents |
| --- | --- |
| `migrations/0001_create_queue_tables/up.sql` | `queue_tasks`, `queue_task_attempts`, `queue_outbox` |
| `migrations/0002_create_schedule_tables/up.sql` | `scheduler_jobs` (only with the `cron` feature) |
| `migrations/0003_admin/up.sql` | `cancelled` task and attempt status; `queue_admin_actions`; `queue_pauses`; `queue_workers`; `queue_tasks.admin_action_id`; partial indexes on dead/cancelled tasks (none on `queue_task_attempts`, so closing an attempt can stay a HOT update) |

Every user needs `0003`, whether or not they run the admin UI: cancel, pause and
the worker registry are part of the core crate and always on.

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
(higher runs first) and `.max_attempts(..)` (default 3) tune it. A task goes to
`Task::QUEUE` (default `"default"`) unless `.queue(QueueName)` overrides it. The
queue is not part of idempotency equality: re-enqueueing a key with another queue
returns the existing task. When the
connection is inside a transaction, the task and its `queue_available`
notification commit with the caller's data. Run producers at `READ COMMITTED`.

**Idempotency horizon:** a `task_key` stays reserved while its row exists. Rows
are kept for at least `Retention::idempotency_horizon` (default 7 days), and
workers refuse a retention shorter than that. Producers must not reuse a key
within the horizon for a different logical task.

## Worker

```rust,no_run
use std::time::Duration;
use pg_task_scheduler::{CancellationToken, Scheduler, TaskError};
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
proves ownership was lost (the lease expired, or an operator cancelled the
task), it drops the handler future and then cancels `ctx.cancellation`. The
handler itself never sees that token fire; only work it spawned with a clone of
the token does, and that work must stop issuing new non-idempotent side
effects.

Handlers classify every failure:

- `TaskError::retry(..)` requeues with exponential backoff and jitter until
  `max_attempts` is exhausted, then marks the task `dead`.
- `TaskError::permanent(..)`, and any error converted with `?`, marks the task
  `dead` immediately.
- `TaskError::interrupted()` is for a handler that stopped early because
  shutdown began. During shutdown the task is released without counting the
  attempt. Outside shutdown it is retried like `TaskError::retry(..)`.

### Queues

Every task belongs to one queue. A worker serves the queues configured with
`.queue(name, limit)`, each with its own limit on running handlers:

```rust,no_run
use std::num::NonZeroUsize;
use pg_task_scheduler::{Scheduler, Task};
# struct SendEmail;
# impl Task for SendEmail { const NAME: &'static str = "send-email"; type Args = serde_json::Value; }

struct RebuildReport;

impl Task for RebuildReport {
    const NAME: &'static str = "rebuild-report";
    const QUEUE: &'static str = "bulk";
    type Args = serde_json::Value;
}

# async fn example(pool: diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>) -> Result<(), Box<dyn std::error::Error>> {
let scheduler = Scheduler::builder(pool)
    .queue("default", NonZeroUsize::new(16).unwrap())?
    .queue("bulk", NonZeroUsize::new(4).unwrap())?
    .register_task::<SendEmail, _, _>(|_ctx, _args| async { Ok(()) })?
    .register_task::<RebuildReport, _, _>(|_ctx, _args| async { Ok(()) })?
    .build()?;
# let _ = scheduler;
# Ok(())
# }
```

- **Routing:** a task goes to `Task::QUEUE` (default `"default"`).
  `EnqueueOptions::queue(..)` overrides it for one enqueue.
- **Priority** orders tasks only within a queue. A queue full of high-priority
  tasks cannot take another queue's handler slots.
- **Limits** apply per scheduler instance: three instances serving `bulk` at 4
  run up to 12 `bulk` handlers. Each claim takes up to `claim_batch_size` tasks
  from one queue.
- **Defaults:** with no `.queue(..)` call, a worker serves `default` with a
  limit of 16. Configuring any queue replaces that implicit default, so add
  `.queue("default", n)` to keep serving it. Configuring a queue again replaces
  its limit.
- **Unserved queues:** a queue that no running worker serves never runs; its
  tasks stay `queued`. `store::queue_stats` reports depth by queue.

### Shutdown

When the token passed to `run_until_shutdown` fires, the worker stops claiming
and cancels every running handler's `ctx.cancellation`. It then waits up to
`shutdown_timeout` for the handlers to finish.

A handler that stops early returns `TaskError::interrupted()`. The task goes back
to `queued`, due at once, and the attempt is not counted. Handlers still running
at the timeout are dropped and their tasks released the same way, for up to 5 s
more. A task whose release fails (for example, the database is unreachable)
stays `running` until the reaper recovers it after the lease, and that attempt
counts.

Releases do not count attempts and have no limit, so `max_attempts` does not
bound a task that hangs (for example, stuck on a remote call with no timeout):
it is released and reclaimed at every deploy and never reaches `dead`. Put
timeouts on a handler's own awaits.

With the `cron` feature, shutdown can also cut off a materializer transaction,
which stays open on its pooled connection holding its schedule row lock. The
transaction sets `idle_in_transaction_session_timeout` to 30 s, so the server
ends it after 30 s idle; until then, if the host keeps its pool alive after
`run_until_shutdown` returns, other workers skip that schedule.

```rust,no_run
use std::time::Duration;
use pg_task_scheduler::TaskError;
# use pg_task_scheduler::TaskContext;
# async fn work(_args: &serde_json::Value) -> Result<(), TaskError> { Ok(()) }
# async fn handler(ctx: TaskContext, args: serde_json::Value) -> Result<(), TaskError> {
tokio::select! {
    _ = ctx.cancellation.cancelled() => Err(TaskError::interrupted()),
    done = tokio::time::timeout(Duration::from_secs(300), work(&args)) => match done {
        Ok(result) => result,
        Err(_elapsed) => Err(TaskError::retry("work timed out")),
    },
}
# }
```

Set the orchestrator's grace period (for example Kubernetes
`terminationGracePeriodSeconds`) above `shutdown_timeout + 6 s`: up to 5 s of
releases, then up to 0.5 s to delete the worker's registry row. A released task
starts again from the beginning, so make long tasks resumable: keep their
progress keyed by `task_key`.

### Worker registry

Each worker keeps a row in `queue_workers`, for display only: claims, ownership
and health never read it. The row holds its label, status (`starting`,
`healthy`, `degraded` or `stopping`), last error, task names, and per queue the
limit, handlers in use, whether the queue is paused, and when one of its
dispatches there last ended. The worker writes it at start and every
`registry_interval`, marks it `stopping` when shutdown begins, and deletes it
once the drain ends. Each shutdown write is bounded by 0.5 s, so a stalled
database adds at most 1 s to shutdown. `store::workers` lists the rows, live
workers first. A row not written for 3 × `registry_interval` reads as `lost`
(for example, the process was killed), and every worker's maintenance deletes
rows not written for 24 h. A failed registry write is logged and counted in
`pg_task_scheduler_registry_failures_total`; it never affects claiming or
health.

- `.label("eu-1")` names the worker in the registry. It defaults to the
  `HOSTNAME` environment variable when that is set and non-empty, and otherwise
  the worker id is shown.
- `.registry_interval(d)` sets how often the row is written (default 15 s;
  `build()` rejects zero and anything over 5 years).

Since the row is a periodic snapshot, it lags: a new worker reads `starting`
until its second write, a draining worker's row is not refreshed after its
`stopping` write (a drain longer than 3 × `registry_interval` reads as lost),
and a write cut off at shutdown can land after the delete and re-create the row
until it reads as lost and is pruned. See
[Admin UI decisions](https://github.com/StaringSkyward/pg_task_scheduler/blob/main/docs/AdminUI.md#6-worker-registry).

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

## Running Queues

Advice for choosing queues, sizing workers and deploying. The API is in
[Queues](#queues) and [Shutdown](#shutdown).

### Organising tasks into queues

- Split queues by how long their work may wait and by what it uses, not by
  priority level. Priority orders tasks within a queue; queues give capacity
  that other work cannot take. For example:
  - `default`: user-facing work that should start within seconds;
  - `bulk`: exports and backfills;
  - a queue per slow or rate-limited external dependency, so its slow handlers
    or retry storms cannot fill every slot.
- Put tasks that run longer than `shutdown_timeout` in their own queue, so a
  separate deployment with a longer grace period can serve them.
- Keep to a handful of queues. Each one adds a claim statement per worker loop
  and splits capacity.
- Set the queue on the task type (`Task::QUEUE`). Use `EnqueueOptions::queue`
  only to route a single call, such as a backfill of a task type that normally
  runs live. Cron schedules set it with `CreateJob::queue`.
- Queue names should be short, lowercase and stable, and describe the kind of
  work rather than the team that owns it.
- To rename a queue without stranding tasks:
  1. deploy workers that serve both names;
  2. switch producers to the new name;
  3. stop serving the old name once its depth is zero.
- A task runs only on a worker that serves its queue **and** has a handler
  registered for its task name.

### Pausing a queue

`store::pause_queue(conn, &queue, operator, reason)` stops every worker, on
every instance, claiming from that queue. It is a `queue_pauses` row, so it
survives restarts and deploys. Running tasks finish; a task that fails, is
released or is reaped while paused goes back to `queued` and waits. Producers
and cron keep enqueuing. A claim that started before the pause committed may
still take tasks, and idle workers stop waking for the paused queue's due
tasks. `store::resume_queue` deletes the row and sends `NOTIFY
queue_available`, so workers claim again at once. Both return whether the state
changed, and write a `queue_admin_actions` row only when it did. The queue needs
no tasks, only a valid name. `store::paused_queues` lists the pauses.

### Sizing workers

- The handlers needed across all instances are about arrival rate × average
  handler duration (Little's law). Set each instance's limit to that number
  divided by the number of instances, times 1.5 to 2 to leave room for bursts
  and retries.
- Run separate deployments for queues that scale differently. Each process
  configures only its own queues.
- Size the pool at the sum of the queue limits + 2 per scheduler, plus whatever
  handlers take (see Connections under [Operations](#operations)). Keep
  (instances × pool size) under PostgreSQL's `max_connections`, with room to
  spare.
- A `claim_batch_size` above a queue's limit gains nothing.

### Keeping queues healthy

Poll `store::queue_stats` on a timer and publish the values as gauges (see
[Observability](#observability)).

| Signal | Meaning | Action |
| --- | --- | --- |
| `QueueDepth.oldest_due_age` above the queue's latency target | under capacity, or no worker serves the queue | check a worker serves it and registers a handler for the task name, then add capacity |
| `due` growing steadily | arrivals exceed capacity | add instances or raise the limit |
| p95 of `claim_latency_seconds{queue}` | the worker-side view of the same thing | same as above |
| `tasks_dead_total`, `leases_lost_total`, `leases_expired_total` | handler bugs, or a lease too short for the work | fix the handler or raise `lease_duration` |
| `tasks_released_total` | rises at deploys, which is expected | outside deploys, look for restart loops |
| one task name released at every deploy, with `running` above zero between deploys | probably hung: releases never count attempts | add a timeout in the handler |
| `tasks_cancelled_total` | an operator cancelled running tasks | expected after a cancel; `queue_admin_actions` says who and why |

The admin UI's overview turns the same signals into a status per queue, without
scanning retained rows (see
[admin/README.md](https://github.com/StaringSkyward/pg_task_scheduler/blob/main/admin/README.md#queue-status-what-it-means-and-what-to-do)).

### Deploys and shutdown

- Make the orchestrator's grace period longer than `shutdown_timeout` + 6 s.
- Set `shutdown_timeout` at or above a typical task's duration in the queues
  that process serves.
- Long handlers should watch `ctx.cancellation`, save progress keyed by
  `task_key`, and return `TaskError::interrupted()`.
- Releases do not count attempts and have no limit, so `max_attempts` does not
  bound a task that hangs. Handlers must put timeouts on their own awaits (for
  example `tokio::time::timeout`).
- **Returning `Ok(())` after cancellation marks the task done.** Do not swallow
  cancellation.
- A task that has not stopped by the deadline is dropped mid-await. Anything it
  had already started outside PostgreSQL may have happened, so the external
  effect must be idempotent by `task_key`.
- A dropped DB-contained handler can leave its pooled connection idle in
  transaction, holding the task's row lock. If the host keeps the pool alive
  after `run_until_shutdown` returns, peers' reapers (`SKIP LOCKED`) skip that
  task until the connection is recycled or the process exits. Exit the process
  (or drop the pool) after shutdown.
- A release sends no notification, so another worker picks the task up within
  `max_poll_interval`.

## Operations

- **Reaper:** every worker runs it every `reaper_interval`. Expired leases go
  back to `queued` after `crash_retry_delay`, or to `dead` when no attempts
  remain. Running it on many instances is safe.
- **Retention:** the same maintenance task deletes `done`, `dead` and
  `cancelled` rows and published outbox rows older than their `Retention`
  periods, in bounded batches. `cancelled` rows are kept for `Retention.dead`,
  and the three task deletes share one `batch_size` budget, `done` first.
  Attempt history is deleted with its task. Each round reaps one batch before
  it deletes one, so a retention backlog never delays lease recovery.
- **Replay:** `queue::replay(conn, task_id, operator, reason)` moves a `dead`
  or `cancelled` task back to `queued`, due now, with the same `task_key`. It
  resets `attempts`, clears `last_error`, bumps `lock_token`, and wakes workers.
  Each replay is recorded in `queue_admin_actions` and linked from the task's
  `admin_action_id`. Enqueue a new `task_key` instead if you want a new external
  effect.
- **Cancel:** `store::cancel(conn, task_id, operator, reason)` moves a `queued`
  or `running` task to `cancelled`, recorded in `queue_admin_actions`. A running
  task's attempt closes with outcome `cancelled`. Its worker's next heartbeat
  (within `heartbeat_interval`) finds the lease gone and drops the handler;
  anything it already did stays done. The worker counts this in
  `tasks_cancelled_total`, not `leases_lost_total`. A handler that finishes
  first gets `LostOwnership` on completion, and that is counted as a
  cancellation too. The call never waits on a row lock: it returns
  `NotCancellable` for a terminal or missing task, and `Busy` when another
  transaction (for example a handler inside `lock_owned`) still holds the row
  after 2 s of retries every 100 ms. A cancelled task keeps its `task_key` until
  retention deletes it (after `Retention.dead`), so enqueueing that key again
  returns the cancelled task and does not run it; replay does.
- **Bulk:** `store::cancel_matching` (queued and running) and
  `store::replay_matching` (dead and cancelled, optionally only those with an
  exact `last_error`) act on a task name, and optionally one queue. They work in
  batches of `BULK_BATCH` (500), each one statement and transaction, until a
  batch comes back short, or until the budget (`BULK_BUDGET`, 10 s) has run out
  after a full batch. Then `more_remain` is `true`: call again, which is a new
  action. Each call records one `queue_admin_actions` row, with its `affected`
  count and `max_task_id`, and touches only tasks with an id up to the highest
  one when it started, so it ends even while producers keep enqueuing. Rows
  locked elsewhere are skipped, so concurrent calls split the work; for the same
  reason `more_remain = false` does not prove that nothing matching is left.
  `count_cancellable` and `count_replayable` count up to a cap. Cancel by name
  reads every queued and running row through their partial indexes (never the
  retained done, dead and cancelled rows) and filters by name: an index on the
  name there would tax every enqueue.
- **Inspection:** `store::task`, `store::attempts`, and `store::queue_stats`
  (counts of queued, running, done and dead tasks, due count, oldest due age,
  depth by queue, name and priority with due count and oldest due age, and
  outbox backlog). The admin UI's read APIs (`store::overview`,
  `store::queued_page`, `store::running_page`, `store::done_page`,
  `store::failed_page`, `store::task_detail`, `store::workers`,
  `store::admin_actions`) are bounded by page size, a count cap or the number of
  queues and workers, so unlike `queue_stats` they are cheap to poll.
- **Health:** `Scheduler::health()` reports starting, healthy, degraded, or
  stopped. Claim errors (and, with `cron`, materialize errors) set degraded;
  heartbeat and maintenance failures do not affect health. A worker with no free
  handler slot in any queue runs no claim cycle, so it keeps its last status.
  `consecutive_failures` counts failed cycles; a materialize failure counts once
  until the next materialization, however many claim cycles run meanwhile.
  Status becomes stopped when `run_until_shutdown` returns or its future is
  dropped.
- **Timeouts:** set `statement_timeout` and `lock_timeout` on pool connections.
  Queue statements are short.
- **Connections:** each running scheduler holds one pool connection for
  `LISTEN queue_available`, and briefly takes others for claims, heartbeats,
  finalization, and maintenance. Handlers hold no connection unless they take
  one. Per scheduler sharing a pool, that is 1 for `LISTEN`, 1 for claims and
  maintenance, and up to the sum of queue limits for heartbeats and
  finalization, plus one per handler that takes its own. So set the pool's
  `max_size` to at least the sum of queue limits + 2 per scheduler, plus whatever
  handlers take. A smaller pool starves heartbeats until local deadlines drop
  handlers. Deadpool's default `max_size` is 2 × the logical CPU count (4 on a
  2-vCPU host), well below the 16 + 2 = 18 that the default queue's limit of 16
  needs.

### Observability

The crate emits signals through the standard facades and configures neither of
them:

- **Logs and spans** use [`tracing`](https://docs.rs/tracing). Each task runs
  inside a `task` span with `task_id`, `task_key`, `task_name`, `queue`,
  `attempt_no`, `worker_id`, and `lock_token`. Failure events add
  `classification` (`retryable` or `permanent`) and, for retries,
  `retry_due_at`. Install any subscriber you like.
- **Metrics** use [`metrics`](https://docs.rs/metrics) 0.24. Install a recorder
  or exporter (Prometheus, StatsD, OpenTelemetry, …) in the application. Without
  one, every metric call is a no-op. The recorder must be built for `metrics`
  0.24: one built for another 0.x version receives nothing.

| Metric | Kind | Labels |
| --- | --- | --- |
| `pg_task_scheduler_tasks_claimed_total` | counter | `task_name`, `queue` |
| `pg_task_scheduler_tasks_done_total` | counter | `task_name` |
| `pg_task_scheduler_tasks_retried_total` | counter | `task_name` |
| `pg_task_scheduler_tasks_dead_total` | counter | `task_name` |
| `pg_task_scheduler_tasks_released_total` | counter | `task_name` (released at shutdown; the attempt is not counted) |
| `pg_task_scheduler_tasks_cancelled_total` | counter | `task_name` (an operator cancelled the running task; seen by its worker) |
| `pg_task_scheduler_leases_lost_total` | counter | `task_name` |
| `pg_task_scheduler_heartbeat_failures_total` | counter | `task_name` |
| `pg_task_scheduler_leases_expired_total` | counter | (none; reaper) |
| `pg_task_scheduler_tasks_materialized_total` | counter | (none; `cron` feature only) |
| `pg_task_scheduler_cleanup_deleted_total` | counter | `kind` = `task` or `outbox` |
| `pg_task_scheduler_registry_failures_total` | counter | (none; a failed worker registry write or prune) |
| `pg_task_scheduler_claim_latency_seconds` | histogram | `task_name`, `queue` (claim time − `due_at`, database clock) |
| `pg_task_scheduler_task_duration_seconds` | histogram | `task_name` |
| `pg_task_scheduler_db_query_seconds` | histogram | `operation` = `claim`, `heartbeat`, `complete`, `retry`, `fail`, `release`, `reap`, `cleanup` or `materialize` (`materialize` is `cron` only) |

Tasks the reaper marks dead are counted only in
`pg_task_scheduler_leases_expired_total`, not in `pg_task_scheduler_tasks_dead_total`.
`pg_task_scheduler_tasks_materialized_total` counts tasks inserted; an occurrence
that already exists as a task counts nothing.

Queue-level gauges (counts by status, due count, oldest due age, depth by queue,
name and priority with due count and oldest due age per queue, outbox backlog)
come from `store::queue_stats`. They are not emitted automatically because that
query scans retained rows. Poll it at an interval that suits you and publish the
values as gauges.

### Admin UI

The core crate has no HTTP dependencies. The admin UI, its JSON API and the
standalone `pg-task-admin` server are in the separate `pg_task_scheduler_admin`
crate: see [admin/README.md](https://github.com/StaringSkyward/pg_task_scheduler/blob/main/admin/README.md) for setup and
[Admin UI decisions](https://github.com/StaringSkyward/pg_task_scheduler/blob/main/docs/AdminUI.md) for what it does and why.

The admin crate is available on GitHub but is not currently published to
crates.io, pending feedback from users on its value and features. To embed it,
take both crates from the same git tag, so the application builds one copy of
the core crate:

```toml
[dependencies]
pg_task_scheduler = { git = "https://github.com/StaringSkyward/pg_task_scheduler", tag = "v0.1.0" }
pg_task_scheduler_admin = { git = "https://github.com/StaringSkyward/pg_task_scheduler", tag = "v0.1.0" }
```

To install the standalone server:
`cargo install --locked --git https://github.com/StaringSkyward/pg_task_scheduler --tag v0.1.0 pg_task_scheduler_admin`.

Feedback on whether the admin UI is useful, and what it should do, is welcome
in [GitHub issues](https://github.com/StaringSkyward/pg_task_scheduler/issues).

## Recurring Schedules

With the `cron` feature, `jobs::create`, `jobs::ensure_job`, pause/resume, and
reschedule manage rows in `scheduler_jobs`. Each due occurrence becomes an
ordinary queue task with `task_key = "schedule:{id}:{scheduled_for}"`, so a
re-materialized occurrence never creates a second task. Every running scheduler
materializes due schedules at most once per `max_poll_interval`, so an occurrence
becomes a task within `max_poll_interval` of its time. Deleting or editing a
schedule does not change tasks it already created. After downtime, one
occurrence is created for the missed slot (`run_once`). `CreateJob::queue(..)`
picks the queue for the tasks a schedule produces (default `default`).

## Feature Flags

| Feature | Default | Purpose |
| --- | --- | --- |
| `deadpool` | yes | Deadpool implementation of `SchedulerPool` |
| `bb8` | no | bb8 implementation of `SchedulerPool` |
| `mobc` | no | mobc implementation of `SchedulerPool` |
| `cron` | no | Recurring schedules as a queue producer |

## Tests

```sh
cp .env.example .env
docker compose up -d db
cargo test --workspace --all-features
```

Every integration test installs the schema into an isolated PostgreSQL schema.
