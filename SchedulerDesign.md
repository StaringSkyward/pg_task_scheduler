# PG Task Scheduler - The Simple Rust Scheduler

PG Task Scheduler is a Rust crate for cron-like distributed job scheduling backed by PostgreSQL.

It is designed to dovetail into an existing Rust application or workspace that already uses
PostgreSQL, Diesel, and Tokio. Axum integration is optional and should be treated as an add-on for
admin routes and operational tooling rather than a core requirement.

Running scheduled work in modern deployments is harder than traditional single-host cron. Kubernetes
pods are rotated, deployments interrupt long-running processes, and multiple application replicas may
be active at the same time. This crate aims to provide a small, embeddable scheduler that handles
those realities without introducing external infrastructure such as Redis, RabbitMQ, or a workflow
engine.

## Goals

- Provide cron-like scheduling for Rust applications.
- Use PostgreSQL as the durable coordination and recovery mechanism.
- Allow multiple workers to run safely across many pods or hosts.
- Recover scheduled work after node crashes, pod rotation, or deployment interruption.
- Keep the core crate simple enough to embed in an existing service.

## Non-Goals

- Exactly-once side effects.
- Hard real-time scheduling.
- Sub-minute schedule precision.
- Replacing a full workflow engine.
- Requiring Axum or any specific web framework.

## Execution Guarantee

PG Task Scheduler provides **leased at-least-once execution per scheduled occurrence**.

A scheduled occurrence is a durable row in PostgreSQL representing one intended run of a job at a
specific time. Workers claim occurrences using a database transaction, a lease token, and
`FOR UPDATE SKIP LOCKED`.

The scheduler guarantees that once an occurrence has been materialized in PostgreSQL, a worker crash
or pod rotation will not silently lose it. If a worker disappears before finalizing the occurrence,
another worker may reclaim it after the lease expires.

The scheduler does **not** guarantee exactly-once side effects. A job handler may run more than once
for the same scheduled occurrence if:

- the worker is killed after performing side effects but before marking the occurrence completed;
- the worker exceeds its lease timeout;
- PostgreSQL is unavailable during finalization;
- the process panics or is force-killed.

Job handlers must therefore be idempotent. The crate should pass a stable `run_id` and
`scheduled_for` timestamp to the handler so application code can use them as idempotency keys.

Given the above, you should consider architecting your jobs to complete quickly where possible. Even
if that means running the job more frequently.

## Scheduling Model

The scheduler separates durable job definitions from concrete scheduled occurrences.

- `scheduler_jobs` stores the cron configuration.
- `scheduler_runs` stores claimable occurrences and execution history.

Workers do not run directly from `scheduler_jobs`. Instead, due jobs first materialize a
`scheduler_runs` row. That row remains visible to the system until it is completed, failed, or
explicitly abandoned according to policy.

The key invariant is:

```text
Create the durable run row and advance the job's next_run_at in the same transaction.
```

That transaction is what prevents pod death from turning into a missed run.

The crate enforces this at the API boundary rather than by caller discipline:
only the materializer (inside that transaction) and the explicit
`jobs::reschedule` operation may advance a job's `next_run_at` past a due
occurrence. Strict `jobs::create` (which errors on a duplicate name) and
idempotent `jobs::ensure_job` never move an existing cursor — on a name conflict
`ensure_job` reconciles configuration but
leaves `next_run_at` (and `is_paused`) untouched — so registering a job at
startup can never skip a pending slot. All four operations compute the cursor
from the database clock (`now()` in-transaction), the same source the
materializer uses.

## Data Model

The scheduler decomposes run state so that invalid combinations are unrepresentable: separate
relations for the occurrence, its lease, and its outcome, plus a `run_outcome` enum and two triggers
that keep the one cross-table invariant in the schema rather than in caller discipline.

### `scheduler_jobs` — durable job definitions

| Column          | Type        | Description                                           |
| --------------- | ----------- | ----------------------------------------------------- |
| id              | UUID        | Primary key (`JobId`)                                 |
| name            | TEXT        | Unique job name (`JobName`)                           |
| cron_expression | TEXT        | Five-field cron, stored from a validated `CronExpression` |
| job_args        | JSONB       | Arguments passed to the handler                       |
| next_run_at     | TIMESTAMPTZ | Next occurrence to materialize                        |
| lease_duration  | INTERVAL    | Max time a worker may own an occurrence without renew |
| max_attempts    | INTEGER     | Max crash/timeout attempts (`CHECK >= 1`)            |
| is_paused       | BOOLEAN     | Prevents materialization                             |
| created_at / updated_at | TIMESTAMPTZ | Bookkeeping                                   |

### `scheduler_runs` — the immutable occurrence

Created once; only `attempt_count` ever changes. It carries no status, lease, or outcome columns —
those are separate facts.

| Column        | Type        | Description                               |
| ------------- | ----------- | ----------------------------------------- |
| id            | UUID        | Primary key (`RunId`)                     |
| job_id        | UUID        | FK → `scheduler_jobs` (ON DELETE CASCADE) |
| scheduled_for | TIMESTAMPTZ | The schedule time this row represents     |
| attempt_count | INTEGER     | Times claimed (`CHECK >= 0`)             |
| created_at / updated_at | TIMESTAMPTZ | Bookkeeping                     |

```sql
CREATE UNIQUE INDEX scheduler_runs_job_id_scheduled_for_idx
ON scheduler_runs (job_id, scheduled_for);
```

The unique `(job_id, scheduled_for)` index makes the scheduled occurrence the unit of deduplication.

### `scheduler_run_leases` — exists iff the run is claimed

A row exists exactly while a run is leased ("running"). Every column is `NOT NULL`, so a lease is
always a *complete* set of facts; the nullable co-varying trio of a single-table design is gone. The
`run_id` primary key makes "at most one lease per run" structural.

| Column           | Type        | Description                      |
| ---------------- | ----------- | -------------------------------- |
| run_id           | UUID        | PK, FK → `scheduler_runs`        |
| worker_id        | TEXT        | Lease owner (`WorkerId`)         |
| lease_token      | UUID        | Fencing token (`LeaseToken`)     |
| lease_expires_at | TIMESTAMPTZ | Reclaimable after this time      |
| started_at       | TIMESTAMPTZ | When the current attempt started |

### `scheduler_run_outcomes` — exists iff the run is terminal

| Column      | Type        | Description                         |
| ----------- | ----------- | ----------------------------------- |
| run_id      | UUID        | PK, FK → `scheduler_runs`           |
| outcome     | run_outcome | `completed` \| `failed`             |
| finished_at | TIMESTAMPTZ | Terminal time                       |
| last_error  | TEXT        | `NOT NULL` iff `outcome = 'failed'` |

```sql
CHECK ((outcome = 'failed') = (last_error IS NOT NULL))
```

### Derived status

Status is not stored. It is derived from the presence of lease/outcome rows and surfaced in Rust as
a sum type:

```text
no lease, no outcome   → Pending
lease present          → Running(Lease)
outcome present        → Completed | Failed { error }
```

A read-model view `scheduler_runs_status` LEFT JOINs the three relations for inspection and the admin
routes.

### Enforcing "lease XOR outcome"

The one invariant not expressible as a single-table constraint — a run must not simultaneously have a
lease and an outcome — is enforced by two triggers, keeping it in the schema:

- `AFTER INSERT ON scheduler_run_outcomes`: delete the lease for that `run_id`, so finalizing a run
  atomically clears its lease.
- `BEFORE INSERT ON scheduler_run_leases`: raise if an outcome already exists for that `run_id`, so a
  terminal run can never be re-leased.

### Type model

Identity, units, and security-sensitive values are newtypes, not raw primitives: `JobId`, `RunId`,
`JobName`, `LeaseToken`, `WorkerId`, `MaxAttempts(NonZeroU32)`, `LeaseDuration` (checked
`TryFrom<Duration>`, microsecond-exact, converted to `INTERVAL` without `as`), and `CronExpression`
(parse-don't-validate at the
job-creation boundary). The `run_outcome` enum maps to a Rust enum via `diesel-derive-enum` whose
`FromSql` rejects unknown labels — no stringly status, no partial `Option` decode.

### Migrations

Migrations ship as raw SQL files under `migrations/0001_create_scheduler_tables/{up.sql,down.sql}`.
The crate does not embed or auto-run them. A consuming application copies them into its own Diesel
migration set (or applies them with the Diesel CLI), keeping full control over when schema changes
run. The crate's `src/schema.rs` mirrors these tables for Diesel query building.

## Cron Expression Syntax

PG Task Scheduler starts with five-field cron syntax:

```text
# .---------------- minute (0 - 59)
# |  .------------- hour (0 - 23)
# |  |  .---------- day of month (1 - 31)
# |  |  |  .------- month (1 - 12) OR jan,feb,mar,apr ...
# |  |  |  |  .---- day of week (0 - 6) (Sunday=0 or 7) OR sun,mon,tue,wed,thu,fri,sat
# |  |  |  |  |
# *  *  *  *  *
17 * * * *
```

The initial implementation should evaluate schedules in UTC. Timezone-aware scheduling and DST
behavior can be added later as explicit features.

Parsing and next-occurrence calculation use the `croner` crate, which supports standard five-field
POSIX cron (including `0`/`7` for Sunday and month/day names) and computes the next occurrence
strictly after a given `chrono::DateTime<Utc>`. The `cron` crate was the original choice but requires
a seconds field, so it cannot parse standard five-field expressions like `17 * * * *`.

## Architecture

### Materializing Runs

A scheduler task periodically finds due jobs, inserts one durable occurrence, and advances
`next_run_at` to the next future occurrence (`run_once` misfire) in the same transaction — one
transaction per job so a single bad row cannot block the rest:

```sql
-- 1. lock one due job
SELECT id, next_run_at, cron_expression, now() AS db_now
FROM scheduler_jobs
WHERE next_run_at <= now() AND is_paused = false
ORDER BY next_run_at ASC
FOR UPDATE SKIP LOCKED
LIMIT 1;

-- 2. (Rust) next := CronExpression::next_after(db_now)

-- 3. create the occurrence for the missed slot
INSERT INTO scheduler_runs (job_id, scheduled_for)
VALUES ($job_id, $old_next_run_at)
ON CONFLICT (job_id, scheduled_for) DO NOTHING;

-- 4. advance the job
UPDATE scheduler_jobs SET next_run_at = $next, updated_at = now() WHERE id = $job_id;
```

The next-run calculation lives in Rust (`croner`); occurrence creation and the `next_run_at` advance
stay atomic. If the stored cron fails to re-parse (corruption — it was validated on create) the job
is paused and the error logged, never silently advanced.

### Claiming Runs

A worker claims the oldest runnable occurrence *for jobs it has handlers for*. "Runnable" = no
outcome and (no lease, or an expired lease still under `max_attempts`). The run row is the mutex via
`FOR UPDATE SKIP LOCKED`; the claim runs in one transaction:

```sql
-- 1. lock a candidate (only jobs whose names this worker registered)
SELECT r.id AS run_id, r.job_id, j.name AS job_name, j.job_args,
       r.scheduled_for, r.attempt_count + 1 AS attempt
FROM scheduler_runs r
JOIN scheduler_jobs j ON j.id = r.job_id
LEFT JOIN scheduler_run_leases   l ON l.run_id = r.id
LEFT JOIN scheduler_run_outcomes o ON o.run_id = r.id
WHERE o.run_id IS NULL
  AND j.name = ANY($registered_names)
  AND (l.run_id IS NULL
       OR (l.lease_expires_at <= now() AND r.attempt_count < j.max_attempts))
ORDER BY r.scheduled_for ASC
FOR UPDATE OF r SKIP LOCKED
LIMIT 1;

-- 2. bump the attempt counter
UPDATE scheduler_runs SET attempt_count = attempt_count + 1, updated_at = now()
WHERE id = $run_id;

-- 3. take (or reclaim) the lease, deriving the deadline from the job
INSERT INTO scheduler_run_leases (run_id, worker_id, lease_token, lease_expires_at)
SELECT $run_id, $worker_id, $lease_token, now() + j.lease_duration
FROM scheduler_jobs j JOIN scheduler_runs r ON r.job_id = j.id
WHERE r.id = $run_id
ON CONFLICT (run_id) DO UPDATE
  SET worker_id = EXCLUDED.worker_id,
      lease_token = EXCLUDED.lease_token,
      lease_expires_at = EXCLUDED.lease_expires_at,
      started_at = now()
RETURNING lease_token, lease_expires_at;
```

Filtering by registered job names means a worker never claims work it cannot run, so there is no
"unknown handler" runtime branch. `claim_one` returns a value that always carries a complete lease.

### Finalizing Runs

Completion is fenced by the lease token: the outcome row is inserted only if the caller still holds
the lease. The `AFTER INSERT` trigger then clears the lease, so finalization is atomic and an expired
worker that returns later inserts nothing.

```sql
INSERT INTO scheduler_run_outcomes (run_id, outcome, last_error)
SELECT $run_id, $outcome, $last_error
FROM scheduler_run_leases
WHERE run_id = $run_id AND lease_token = $lease_token
RETURNING run_id;
```

Zero rows returned means the finalization was fenced out (the lease was lost). `$outcome` is
`completed` with a `NULL` error, or `failed` with the handler's error message.

## Lease Duration

The initial implementation can treat `lease_duration` as the maximum allowed runtime for a single
attempt. A worker does not need to renew the lease while the handler is running.

This keeps the recovery model simple, but it has an important consequence: if a handler runs longer
than `lease_duration`, another worker may reclaim and execute the same scheduled occurrence. Job
owners should configure `lease_duration` above the expected worst-case runtime and keep handlers
idempotent.

Lease renewal can be added later as an optimization for long-running jobs. Renewal must use the same
`run_id` and `lease_token` fencing rule as finalization.

## Failure Semantics

Recommended default behavior:

| Event                                      | Behavior                                                              |
| ------------------------------------------ | --------------------------------------------------------------------- |
| Handler returns `Ok(())`                   | Mark the occurrence `completed`                                       |
| Handler returns `Err(_)`                   | Mark the occurrence `failed`; do not retry until next schedule        |
| Worker crashes or pod is killed            | Reclaim the occurrence after `lease_expires_at`                       |
| Worker exceeds lease duration              | Reclaim until `max_attempts`; then mark `failed`                      |
| Worker loses lease but later returns       | Finalization is ignored because the lease token no longer matches     |
| PostgreSQL is unavailable during execution | Handler may complete, but finalization may fail; occurrence can retry |

This gives crash recovery without turning every application-level error into an automatic retry
loop.

## Graceful Shutdown

On SIGTERM or application shutdown, a worker should:

1. Stop claiming new runs.
2. Allow in-flight handlers to finish until the configured shutdown deadline.
3. Finalize completed handlers while their leases are still valid.
4. Optionally release unfinished leases back to `pending`.

If Kubernetes force-kills the pod before shutdown completes, lease expiry handles recovery.

## Misfires and Downtime

The initial misfire policy should be simple:

```text
run_once
```

If the scheduler was down while one or more schedules elapsed, it should create one immediate
catch-up occurrence for the missed job and then advance `next_run_at` to the next future occurrence.

Future policies can be added later:

- `skip`: skip missed occurrences and resume at the next future time.
- `catch_up`: enqueue every missed occurrence up to a configured limit.

## Resolution and Precision

The minimum schedule resolution is one minute, like traditional cron. The worker polling interval can
default to one second, and the lease reaper can run every 20 seconds.

These intervals are not hard real-time guarantees. Tokio and PostgreSQL do not constitute a
real-time operating system.

## Rust Stack

Core dependencies (always compiled):

- `tokio` for async runtime support.
- `diesel` and `diesel-async` for PostgreSQL access.
- `croner` for parsing five-field cron expressions and calculating future occurrences.
- `chrono` for `TIMESTAMPTZ` mapping (`DateTime<Utc>`).
- `serde` and `serde_json` for job arguments.
- `uuid` for primary keys and lease/fencing tokens.
- `tracing` for structured logs and spans to Datadog.
- `thiserror` for error types.
- `tokio-util` for `CancellationToken`-based shutdown.

Optional integrations (off by default, behind Cargo features):

- `metrics` feature → `gnort` for metrics to Datadog. Without the feature, metric calls compile to
  no-ops.
- `axum` feature → `axum` for admin routes.
- Connection-pool features `deadpool` (default), `bb8`, and `mobc` select which pool's
  `SchedulerPool` implementation is compiled in.

## Intended Public API

The crate should expose an embeddable scheduler rather than requiring a standalone service.

Example shape:

```rust
// worker_id is a constructor argument, so a scheduler cannot be built without one.
let scheduler = Scheduler::builder(pool, WorkerId::try_from("api-1")?)
    .poll_interval(Duration::from_secs(1))
    .reaper_interval(Duration::from_secs(20))
    .shutdown_timeout(Duration::from_secs(25))
    .register::<DigestArgs, _, _>("send_digest_email", send_digest_email)?
    .register::<SyncArgs, _, _>("sync_accounts", sync_accounts)?
    .build()?; // errors only if no handler was registered

scheduler.run_until_shutdown(cancel_token).await?; // tokio_util::sync::CancellationToken
```

Handlers should receive execution context as well as JSON arguments:

```rust
// Handlers receive a domain type deserialized from job_args at the registry boundary,
// not a raw serde_json::Value. A deserialize failure becomes an explicit failed outcome.
#[derive(serde::Deserialize)]
struct DigestArgs { user_id: Uuid }

async fn send_digest_email(ctx: JobContext, args: DigestArgs) -> Result<(), JobError> {
    let idempotency_key = ctx.run_id;       // stable across retries
    let scheduled_for = ctx.scheduled_for;

    // Application code performs idempotent work here.

    Ok(())
}
```

## Crate Architecture

### Connection pool abstraction

The builder is generic over the connection pool so the crate slots into whatever pooling the host
application already uses:

```rust
pub trait SchedulerPool: Clone + Send + Sync + 'static {
    type Conn: DerefMut<Target = AsyncPgConnection> + Send;
    // named `acquire`, not `get`, to avoid colliding with the pools' inherent `get`
    fn acquire(&self) -> impl Future<Output = Result<Self::Conn, PoolError>> + Send;
}
```

Feature-gated implementations are provided for `deadpool` (default), `bb8`, and `mobc`. An
application with a bespoke setup can implement the trait itself. Return-position `impl Trait` in
traits (stable on the project's toolchain) avoids an `async-trait` dependency.

### Module layout

```text
src/
  lib.rs            crate docs, re-exports, feature wiring
  error.rs          SchedulerError, JobError
  pool.rs           SchedulerPool trait + feature-gated impls
  schema.rs         Diesel table! definitions
  models.rs         internal Diesel row structs + domain Job projection, RunState/Outcome
  cron.rs           parse + next-occurrence calc (UTC, croner)
  jobs.rs           programmatic job CRUD (create/ensure_job/reschedule/pause/resume/list)
  store/            one focused, independently testable fn per SQL operation
    materialize.rs  atomic: insert run + advance next_run_at (run_once misfire)
    claim.rs        FOR UPDATE SKIP LOCKED claim of pending/expired runs
    finalize.rs     fenced complete/fail (lease_token guard)
    reap.rs         lease-expiry recovery + max_attempts -> failed
  runtime/
    builder.rs      SchedulerBuilder
    registry.rs     name -> boxed async handler
    context.rs      JobContext
    worker.rs       poll loop, materialize tick, dispatch
    shutdown.rs     graceful drain
  metrics.rs        feature = "metrics" (gnort); no-op shims otherwise
  admin.rs          feature = "axum" admin routes
migrations/
  0001_create_scheduler_tables/{up.sql, down.sql}
```

### Job management API

Creating, pausing, resuming, and listing job definitions is available programmatically (in
`jobs.rs`), independent of the optional Axum routes, which are a thin wrapper over it.

The job CRUD functions (`create`, `ensure_job`, `reschedule`, `get`, `list`) return a domain `Job`
projection, not the internal `scheduler_jobs` Diesel row. Each storage primitive is parsed into a
domain type at the read boundary (`cron_expression` → `CronExpression`, `lease_duration` →
`LeaseDuration`, `max_attempts` → `MaxAttempts`, `is_paused` → `JobLifecycle`); a row whose stored
values violate those invariants (e.g. edited directly in SQL) surfaces as `SchedulerError::CorruptJob`,
not a silent or mis-typed success. `CreateJob::new` accepts a typed `args: impl Serialize` and a
`JobLifecycle`, so neither a raw JSON value nor a bare bool crosses the creation boundary.

### Testing

Integration tests require a PostgreSQL instance reachable via the `DATABASE_URL` environment
variable. Each test creates its own randomly-named schema, applies `migrations/up.sql` into it, and
sets `search_path`, giving isolation on a shared database without cross-test interference. Cron
next-occurrence logic is covered by pure unit tests with no database. Implementation proceeds
test-first.
