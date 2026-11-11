# PostgreSQL Task Queue Design

This document describes how the crate implements
[the queue specification](postgresql_queue_design.md) (cited below as §N). The
specification is normative. This document records the Rust boundaries, the
configuration, and the places where the implementation deliberately differs.

## Scope

The crate is an embeddable Diesel/Tokio library. It is not a standalone service.
A single PostgreSQL database is the system of record. There is no clustering,
consensus, or multi-database coordination (§1).

Producers are:

- application code calling `enqueue` (immediate or scheduled);
- the optional `cron` materializer, which turns recurring schedules into queue
  tasks.

## Data Model

### `queue_tasks` (§4.1)

There is one row per logical task. It holds the identity (`id`, `task_key`), the
immutable content (`task_name`, `payload`), the logical `queue` (default
`'default'`; workers run each queue with its own concurrency), the scheduling fields (`priority`,
`due_at`), the attempt budget (`attempts`, `max_attempts`), the ownership fields
(`locked_by`, `lock_token`, `locked_until`), `last_error`, and timestamps.

`status` is `TEXT` with a CHECK for `queued | running | done | dead`. Two row
CHECKs enforce the state shape:

```text
running      ⇔ locked_by and locked_until are set
done | dead  ⇔ completed_at is set
```

`lock_token` is never cleared. It increments on every claim and on every replay,
so it identifies one ownership generation.

Indexes follow §4.2:

| Index | Serves |
| --- | --- |
| `queue_tasks_claim_idx (queue, priority DESC, due_at, id) WHERE queued` | claim |
| `queue_tasks_due_idx (queue, due_at, id) WHERE queued` | next-due wakeup, due counts |
| `queue_tasks_expired_lease_idx (locked_until, id) WHERE running` | reaper |
| `queue_tasks_terminal_cleanup_idx (completed_at, id) WHERE done/dead` | retention |

### `queue_task_attempts` (§4.3)

Each claim inserts one row with `attempt_no`, `lock_token`, `worker_id` and
`started_at` in the same statement as the claim. Completion, retry, permanent
failure, release, and the reaper close it with `finished_at`, an `outcome`
(`done | retry | dead | lease_expired | released`), and `error`. Rows cascade when their
task is deleted by retention.

### `queue_outbox` (§14)

Commands emitted by a successful completion. `(task_key, effect_name)` is unique,
so a retry cannot emit the same effect twice. `command_key` is globally unique, so
a cross-task key collision fails instead of silently deduplicating.

### `queue_task_replays`

This is the audit trail for administrative replay: task id and key, the new
`lock_token`, operator, reason, and timestamp. It has no foreign key, so the audit
outlives retention.

### `scheduler_jobs` (`cron` feature)

Recurring definitions: name (which is also the produced `task_name`), cron
expression, payload template, `max_attempts`, pause state, and the `next_run_at`
cursor. It is not part of the queue protocol.

### Deviations from the specification

| Spec | Implementation | Reason |
| --- | --- | --- |
| attempts PK `(task_id, attempt_no)` | PK `(task_id, lock_token)` plus an index on `(task_id, attempt_no)` | §18 replay resets `attempts`, so the next claim would reuse `attempt_no = 1` and violate the PK. `lock_token` never repeats. |
| claim selects any due task | adds `task_name = ANY($registered)` | a worker claims only names it can execute |
| §4.1 has no queue | `queue` column; claims filter one queue; per-queue worker limits | §16 "separate logical queues/worker pools" against starvation |
| replay "SHOULD be audited" | `queue_task_replays` table | durable audit with operator and reason |
| enqueue always notifies | notifies only when a row was inserted | a duplicate adds no work |
| no cancel transition | none provided | §5 defines no cancel transition |
| §5 lists five transitions | adds release (`running → queued`, `attempts − 1`) at worker shutdown | a deploy should neither consume an attempt nor leave the task until lease expiry |
| no recurring schedules | optional `cron` producer | existing feature, layered on enqueue semantics |

## State Machine (§5)

```text
              claim
    queued -------------> running
      ^ ^                 |  |  |
      | +-- release ------+  |  +---- success ----> done
      |     (shutdown)       |
      | retryable failure    |
      | or expired lease     +------- permanent / exhausted ---> dead
      +----------------------+                                   |
      ^                                                          |
      +-------------------- administrative replay ---------------+
```

Code never writes `status` outside these transitions.

## Transitions

Every function takes `&mut AsyncPgConnection`, so it composes with a caller's
transaction (nested transactions become savepoints).

| Operation | Function | Spec | Notes |
| --- | --- | --- | --- |
| Enqueue | `queue::enqueue::<T>(conn, key, args, opts)` | §6 | `INSERT … ON CONFLICT (task_key) DO NOTHING`, then a SELECT of the existing row with an SQL-side `task_name`/`payload` comparison (JSONB equality ignores key order). `queue` is not compared, like priority. A mismatch returns `SchedulerError::IdempotencyConflict`. |
| Claim | `store::claim(conn, worker, queue, names, limit, lease)` | §7.3–7.4 | One writable CTE: `SKIP LOCKED` candidates → `running`, `attempts + 1`, `lock_token + 1`, `locked_until = clock_timestamp() + lease` → attempt rows inserted → typed claims returned. |
| Heartbeat | `store::heartbeat(conn, lease, duration)` | §9 | Extends `locked_until` from `clock_timestamp()`. |
| Lock for DB work | `store::lock_owned(conn, lease)` | §10.2 | `SELECT … FOR UPDATE` with the ownership predicate. |
| Complete | `store::complete(conn, lease)` | §10.1 | Task → `done`, attempt → `done`. |
| Complete + outbox | `store::complete_with_outbox(conn, lease, cmd)` | §14 | Returns whether a new outbox row was inserted. |
| Retry | `store::retry(conn, lease, error, delay)` | §11.3 | `queued` at `now() + delay`, or `dead` if `attempts >= max_attempts`. |
| Permanent failure | `store::fail_permanently(conn, lease, error)` | §11.4 | Task and attempt → `dead`. |
| Release | `store::release(conn, lease)` | (none) | Worker shutdown only. `running → queued`, `attempts − 1`; `due_at` and `last_error` unchanged, so the task is due at once and keeps its place. Attempt → `released`. |
| Reap | `store::reap_expired(conn, batch, crash_retry_delay)` | §12 | Bounded `SKIP LOCKED` batch. Closes the attempt as `lease_expired` in the same statement. |
| Replay | `queue::replay(conn, id, operator, reason)` | §18 | `dead → queued`, `attempts = 0`, `lock_token + 1`, with an audit row. |
| Cleanup | `store::cleanup(conn, &retention)` | §20 | One bounded delete per terminal status (so `completed_at` is an index condition), `done` first, sharing one `batch_size` budget; then one of published outbox rows. |

Every worker-owned transition uses the full ownership predicate:

```sql
WHERE id = $task_id
  AND status = 'running'
  AND locked_by = $worker_id
  AND lock_token = $lock_token
  AND locked_until > clock_timestamp()
```

Zero rows returns `SchedulerError::LostOwnership(task_id)`. Inside a transaction,
that error rolls the transaction back. Two-statement transitions (task plus
attempt) run in one transaction and fail with an invariant error unless exactly
one attempt row closes.

## Worker Runtime (§24)

```text
startup:  generate UUID worker_id; LISTEN queue_available on one pooled connection;
          spawn maintenance (reaper + cleanup every reaper_interval)
loop:     [cron] materialize due schedules, at most once per max_poll_interval
          if no queue has a free permit: no claim cycle; wait as below, capped at
          max_poll_interval (a finishing handler, a notification or shutdown wakes it)
          for each queue with a free permit, in configuration order, on one connection:
            claim up to min(its free permits, claim_batch_size) from that queue
            spawn one dispatch per claim, holding one of that queue's permits
          if any batch was full: loop again immediately
          otherwise wait for the first of: notification, next due_at among queues
          that still have a free permit (clamped to 10 ms..max_poll_interval;
          max_poll_interval if none has), [cron] the next materialization,
          a handler finishing, shutdown
maintain: every reaper_interval, rounds of one reap batch then one cleanup batch
          until both run short
dispatch: run the handler and the lease keeper concurrently
          lease keeper: heartbeat every heartbeat_interval; on LostOwnership or local
                        deadline → cancel ctx.cancellation and drop the handler
          Ok      → complete   (LostOwnership after a handler-side completion is benign)
          retry   → retry with exponential backoff + jitter
          interrupted → release during shutdown, otherwise retry
          permanent → fail_permanently
shutdown: stop claiming and maintenance; cancel every ctx.cancellation; UNLISTEN;
          drain handlers until shutdown_timeout; then drop the rest and release
          their tasks, waiting up to 5 s (RELEASE_TIMEOUT); abort what remains
          (the reaper recovers those tasks after the lease)
dropped:  if the run future is dropped instead (aborted, or a failed try_join! sibling),
          maintenance stops, handlers are aborted, health becomes Stopped, and the
          LISTEN connection runs UNLISTEN on a task spawned on the worker's runtime
          (even if the drop happens off-runtime) before it returns to the pool
```

The lease keeper is a separate future, not a `select!` arm body. A DB-contained
handler holds its task row lock while the heartbeat waits on that lock, and the
handler must keep being polled so it can commit.

The local deadline is `Instant` at claim send + `lease_duration`, moved forward
on each successful heartbeat. It only gives up ownership early. PostgreSQL alone
decides whether a lease is valid.

Backoff for attempt `n` is
`min(base_retry_delay · 2^(n−1), max_retry_delay) + uniform(0..=retry_jitter)`.
PostgreSQL adds the delay to `now()` (§11.2).

A panicking handler is not finalized. Its attempt stays open until the reaper
recovers it after the lease expires.

## Configuration (§25)

| Builder method | Default |
| --- | --- |
| `queue(name, limit)` | `default`, 16 handlers per scheduler; configuring any queue replaces it |
| `claim_batch_size` | 16 per queue claim |
| `lease_duration` | 60 s |
| `heartbeat_interval` | 20 s (must be ≤ lease / 3) |
| `max_poll_interval` | 1 s |
| `base_retry_delay` / `max_retry_delay` / `retry_jitter` | 1 s / 5 min / 1 s |
| `reaper_interval` / `reaper_batch_size` | 20 s / 100 |
| `crash_retry_delay` | 10 s |
| `retention` | done 30 d, dead 30 d, idempotency horizon 7 d, published outbox 7 d, batch 1000 |
| `shutdown_timeout` | 25 s |

`build()` rejects zero intervals, `heartbeat_interval > lease_duration / 3`,
`base_retry_delay > max_retry_delay`, any `done` or `dead` retention shorter
than the idempotency horizon, and any duration over 5 years. `max_attempts` is
set per task at enqueue (default 3).

## Rust Boundaries

- `Task` ties a stable name to one `Serialize + DeserializeOwned` payload type
  and is used by both `enqueue::<T>` and `register_task::<T>`.
- Identifiers are validated newtypes: `TaskId`, `TaskKey`, `TaskName`,
  `QueueName`, `WorkerId`, `LockToken`.
- `TaskLease { task_id, worker_id, lock_token, attempt_no }` is the ownership
  proof every guarded transition takes. Handlers receive it as `ctx.lease`.
- `TaskContext` carries the lease, `task_key`, `task_name`, `max_attempts`,
  `locked_until` (as of claim; heartbeats do not update it), and a
  `CancellationToken`.
- `TaskError::{retry, permanent, interrupted}` is the handler's failure
  classification (§11.1).
- Observability (§23) goes through the `tracing` and `metrics` facades. The
  crate installs no subscriber or recorder. The metric names and labels are a
  public contract, listed in the [README](../README.md#observability).

## Performance

- Claiming is one round trip per batch. Handlers hold no connection while
  running.
- Heartbeat is a single `UPDATE`. Completion, retry, and failure are
  two-statement transactions.
- The reaper and cleanup loop over bounded batches on a separate task, so a large
  backlog never stalls claiming. A reap batch runs before every cleanup batch, so
  a cleanup backlog never delays lease recovery.
- `store::queue_stats` scans retained rows for its status counts. Poll it at a
  modest interval.
- The claim index is the spec's default. Benchmark it against realistic
  scheduled-task and priority distributions before tuning (§4.2).

## Testing Contract (§27)

| # | Requirement | Test |
| --- | --- | --- |
| 1 | duplicate enqueue | `enqueue::concurrent_duplicate_enqueue_creates_one_task` |
| 2 | conflicting duplicate | `enqueue::conflicting_duplicate_is_rejected` |
| 3 | concurrent claim | `claim::concurrent_claims_produce_one_owner` |
| 4 | bulk concurrency | `worker::bulk_workers_process_each_task_once` |
| 5 | scheduled timing | `claim::future_task_is_not_claimed_until_due` |
| 6 | priority | `claim::higher_priority_due_task_is_claimed_first` |
| 7 | retry | `transitions::retry_requeues_with_db_time_delay_then_dies`, `worker::retryable_error_requeues_with_backoff_then_dies` |
| 8 | hard lease | `transitions::every_transition_fails_after_hard_lease_expiry` |
| 9 | reaper | `maintenance::reaper_requeues_with_crash_retry_delay`, `maintenance::reaper_kills_exhausted_task` |
| 10 | stale ACK | `transitions::stale_token_cannot_complete_after_reclaim` |
| 11 | DB-only atomicity | `transitions::db_contained_failure_rolls_back_effect_and_completion` |
| 12 | external idempotency | `worker::crash_after_remote_accept_does_not_duplicate_external_effect` |
| 13 | outbox ownership | `transitions::stale_worker_cannot_emit_outbox_command` |
| 14 | transactional enqueue | `enqueue::enqueue_rolls_back_with_callers_transaction` |
| 15 | retention | `maintenance::cleanup_deletes_only_eligible_terminal_rows_in_batches` |

Process-kill fault injection (§27, SHOULD) is not automated yet. See
[Concurrency Model](Concurrency.md#validation-status).
