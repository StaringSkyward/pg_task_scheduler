# PostgreSQL Work Queue Design

## Scope

The crate provides one durable execution path for three producers:

- immediate application work;
- delayed one-off work;
- occurrences materialized from recurring cron schedules.

It is an embeddable Diesel/Tokio crate, not a standalone service or workflow
engine. Its execution guarantee is leased at-least-once. External side effects
must be idempotent.

## Data Model

### `scheduler_jobs`

Recurring schedule definitions contain the cron expression, materialization
cursor, payload template, pause state, and default execution policy. The table is
not the executable queue.

### `scheduler_runs`

This is the hot queue table. A row snapshots everything required to execute:

- stable task name and JSON payload;
- optional originating schedule id and scheduled time;
- availability, priority, and optional deduplication key;
- maximum attempts, retry backoff, and lease duration;
- current state, lease, and terminal facts.

State is a PostgreSQL enum. The intended current-row product type is:

```text
Pending   = no lease, no terminal facts, attempts < max_attempts
Running   = complete lease, no terminal facts
Completed = no lease, finished_at, no error
Failed    = no lease, finished_at, error
Cancelled = no lease, finished_at, no error
```

Two local constraints enforce the state/column nullability combinations and the
attempt-count bounds. The normal claim path also guarantees that a running task
has a positive attempt count and exactly one corresponding open attempt row.
Those cross-table facts cannot be expressed by a row-local `CHECK`; the writable
claim CTE and transactional transitions maintain them.

Keeping current state on one row is deliberate. PostgreSQL can enforce its
current-row invariants locally, every transition contends on the same row lock,
and partial indexes can isolate hot pending/running rows from history.

### `scheduler_run_attempts`

Each claim inserts an attempt containing worker id, fencing token, start, and
lease deadline. Completion, failure, expiry, or cancellation closes that row.
This preserves execution history without putting historical attempts on the
claim path.

## Transitions

### Enqueue

Immediate and delayed enqueue insert a pending `scheduler_runs` row using the
caller's connection, then request a PostgreSQL notification. If the caller has
opened a transaction, the task and notification commit together. In autocommit
mode they are separate statements, so notification failure can be reported after
the task has already committed. Polling remains the correctness mechanism. A
partial unique index on `(job_name, deduplication_key)` provides optional
producer idempotency.

### Materialize

A materializer locks one due schedule with `FOR UPDATE SKIP LOCKED`, inserts the
immutable task snapshot, and advances `next_run_at` in the same transaction. A
partial unique index on `(job_id, scheduled_for)` prevents duplicate occurrences.

### Claim

A single writable CTE:

1. selects up to available worker capacity from pending tasks using
   `FOR UPDATE SKIP LOCKED`;
2. changes them to running and generates database fencing tokens;
3. inserts matching attempt rows;
4. returns complete typed claims.

Candidate selection orders by priority descending, then availability and id
ascending. This is a preference rather than a strict global execution order; see
the [Concurrency Model](Concurrency.md).

### Complete, Fail, Renew

Every worker-owned transition is an `UPDATE scheduler_runs ... WHERE id = ? AND
state = 'running' AND lease_token = ?`. PostgreSQL row locking serializes that
predicate with cancellation and recovery. After a concurrent token rotation, the
waiting update rechecks its predicate and affects no row.

Completion and failure also require an unexpired lease. Retryable failures move
back to pending only while attempts remain; otherwise they become failed.
Renewal extends both the task lease and current attempt in one transaction.

### Recover

Recovery selects expired running rows in bounded `SKIP LOCKED` batches. It closes
the attempt as expired, then either requeues the task or fails it when attempts
are exhausted. Claiming only pending rows keeps the hot query simple and
indexable.

### Cancel and Retain

Cancellation locks the task and atomically closes a running attempt before
moving the task to cancelled. A stale worker is then fenced. Retention deletes
only terminal rows older than a caller-supplied cutoff, in bounded locked batches.

## Rust Boundaries

`Task` associates a stable name with one serializable/deserializable argument
type. Both `enqueue::<T>` and `register_task::<T>` use it. Identifier, duration,
attempt, priority, lifecycle, state, failure, renewal, and mutation outcomes are
domain types rather than loosely related primitives.

The worker exposes health through a Tokio watch channel. Database errors degrade
health and are retried on subsequent cycles; they do not discard durable work.

## Performance

The important indexes are:

```text
scheduler_runs_pending_claim_idx
scheduler_runs_running_expiry_idx
scheduler_runs_terminal_retention_idx
scheduler_runs_schedule_occurrence_idx
scheduler_runs_deduplication_idx
scheduler_jobs_due_idx
scheduler_run_attempts_run_idx
```

Claiming is one database round trip per capacity batch. Handlers hold no database
connection while running; connections are acquired briefly for renewal and
finalization. Materialization and recovery are bounded so one tick cannot process
an unlimited backlog.

## Testing Contract

The PostgreSQL integration suite covers:

- two concurrent claimers cannot both own the same task;
- stale finalization blocked behind token rotation;
- concurrent expired-lease recovery;
- transactional enqueue rollback and deduplication;
- delayed availability and priority;
- retry exhaustion, cancellation, retention, and renewal;
- immutable schedule snapshots and schedule deletion;
- end-to-end short-lease execution with heartbeat;
- use of the pending partial index through `EXPLAIN`.
