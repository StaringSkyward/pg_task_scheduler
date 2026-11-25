# PostgreSQL Task Queue Design

This document is the design of the crate's queue: its invariants, data model,
state machine, transitions, worker runtime and configuration, and the reason for
each choice. It is self-contained. [Concurrency Model](Concurrency.md) explains
the races, fencing and failure behaviour in detail, and
[Review decisions](ReviewDecisions.md) records what changed after the
pre-release reviews.

## Scope

The crate is an embeddable Diesel/Tokio library. It is not a standalone service.
The optional `pg_task_scheduler_admin` crate adds an admin UI that is either
mounted in the host application or run as its own `pg-task-admin` server; its
decisions are recorded in [Admin UI decisions](AdminUI.md). A single PostgreSQL
database is the system of record for task state, scheduling, retries, ownership
and completion. There is no clustering, consensus, or multi-database
coordination. PostgreSQL high availability can be added without changing the
queue protocol.

Producers are:

- application code calling `enqueue` (immediate or scheduled);
- the optional `cron` materializer, which turns recurring schedules into queue
  tasks.

## Invariants

Everything below rests on these rules:

1. PostgreSQL decides ownership and timing. Every persisted time is
   `TIMESTAMPTZ`, and a host clock never decides whether a task is due or a
   lease is valid.
2. `task_key` is stable across every retry and replay of one logical
   operation. It is the idempotency key for once-per-task external effects.
3. `lock_token` increments on every claim and every replay, and is never
   cleared, so it names one ownership generation.
4. Every worker-owned mutation first bounds its transaction by the lease left
   (`BOUND`, which is lost ownership at once for an expired lease), then locks
   its task row, then checks `status`, `locked_by`, `lock_token` and the hard
   lease in a new statement. A guarded
   statement that matches zero rows means lost ownership, never success.
5. Every transition out of `running` clears `locked_by` and `locked_until`.
6. PostgreSQL-contained effects can commit in the same transaction as the
   `done` transition, and an outbox row is written only by a guarded
   completion.
7. `LISTEN/NOTIFY` only reduces latency, and its notifications carry no
   payload. Polling is authoritative.
8. Claims and maintenance run as bounded statements and batches.
9. Terminal rows have an explicit retention, and a `task_key` is reserved for
   exactly as long as its row exists.
10. No network call is made while a transaction or task row lock is held. The
    exception is PostgreSQL-contained work, which is itself the transaction and
    must be short.

`READ COMMITTED` is enough, and the materializer requires it. Correctness comes
from row locks, `SKIP LOCKED`, unique constraints and the ownership predicate,
not from `SERIALIZABLE`.

## Data Model

`0001` first checks that the database is UTF8, and fails otherwise. It then
creates `queue_is_identifier(text)`, the identifier rule of
[Rust Boundaries](#rust-boundaries) in SQL. Every identifier column has a CHECK
that calls it, named `<table>_<column>_identifier`. Plain-text columns such as
`operator`, `reason`, `label` and `last_error` have none.

### `queue_tasks`

There is one row per logical task. It holds the identity (`id`, `task_key`), the
immutable content (`task_name`, `payload`), the logical `queue` (default
`'default'`; workers run each queue with its own concurrency), the scheduling
fields (`priority`, `due_at`), the attempt budget (`attempts`, and
`max_attempts`, at least 1), the ownership fields (`locked_by`, `lock_token`,
`locked_until`), `last_error`, and timestamps.

`status` is `TEXT` with a CHECK for `queued | running | done | dead |
cancelled`. Two row CHECKs enforce the state shape:

```text
running                  ⇔ locked_by and locked_until are set
done | dead | cancelled  ⇔ completed_at is set
```

`due_at` is `queue_timestamptz`, which is finite and below chrono's limit, so
PostgreSQL's `infinity` and `-infinity` never reach a claim. `task_key`,
`task_name`, `queue` and `locked_by` have identifier CHECKs
(`queue_tasks_task_key_identifier` and so on); `locked_by` may still be NULL.

`lock_token` is never cleared. It increments on every claim and on every replay,
so it identifies one ownership generation.

The claim and due indexes lead with `queue`:

| Index | Serves |
| --- | --- |
| `queue_tasks_claim_idx (queue, priority DESC, due_at, id) WHERE queued` | claim |
| `queue_tasks_due_idx (queue, due_at, id) WHERE queued` | next-due wakeup (the global due counts in `queue_stats` are a full scan) |
| `queue_tasks_expired_lease_idx (locked_until, id) WHERE running` | reaper |
| `queue_tasks_terminal_cleanup_idx (completed_at, id) WHERE done/dead/cancelled` | retention; the admin Completed list and overview done counts |
| `queue_tasks_failed_idx (status, completed_at DESC, id DESC) WHERE dead/cancelled` | admin Failed and Cancelled lists; overview dead counts and failure groups |
| `queue_tasks_failed_name_idx (task_name, status, completed_at DESC, id DESC) WHERE dead/cancelled` | bulk replay; Failed and Cancelled lists filtered by name |

The last two are partial on `dead`/`cancelled`, so enqueue, claim and completion
never maintain them. `queue_task_attempts` has no index beyond its primary key,
`queue_task_attempts_task_idx` and `queue_task_attempts_started_idx`: closing an
attempt sets `finished_at`, `outcome` and `error`, and an index on any of them
would make every close a non-HOT update that writes all of the table's indexes (`performance::attempt_close_stays_hot_eligible`). The overview's
failed-attempt groups read the newest attempts through
`queue_task_attempts_started_idx`.

### `queue_task_attempts`

Each claim inserts one row with `attempt_no`, `lock_token`, `worker_id` and
`started_at` in the same statement as the claim. Completion, retry, permanent
failure, release, the reaper and an operator's cancel close it with
`finished_at`, an `outcome` (`done | retry | dead | lease_expired | released |
cancelled`), and `error`. Rows cascade when their task is deleted by retention.

The primary key is `(task_id, lock_token)`, with an index on
`(task_id, attempt_no)`. Two CHECKs hold the row's shape: `attempt_no >= 1`, and
`(finished_at IS NULL) = (outcome IS NULL)`, so an attempt is open with neither
or closed with both. A third CHECK, `queue_task_attempts_error_on_failure`,
allows an `error` only on `retry`, `dead`, `lease_expired` and `cancelled`, so an
open, `done` or `released` attempt has none. Closing matches only an open
attempt, so an attempt closes once; closing a closed one is `Invariant`.
A task import must write a running task's open attempt; the reaper and cancel
repair a missing one. `worker_id` has the identifier CHECK
`queue_task_attempts_worker_id_identifier`.

### `queue_outbox`

Commands emitted by a successful completion. `(task_key, effect_name)` is unique,
so a retry cannot emit the same effect twice. `command_key` is globally unique, so
a cross-task key collision fails instead of silently deduplicating. `CommandKey`
allows at most 2048 bytes, a margin below PostgreSQL's B-tree entry limit of
about 2704 bytes, so the unique index can hold every key the type accepts, and
`queue_outbox_command_key_length` holds the column to 1 to 2048 bytes too.
`task_key`, `effect_name` and `topic` have identifier CHECKs.
`queue_outbox_unpublished_idx (id) WHERE published_at IS NULL` serves a relay, and
`queue_outbox_published_idx (published_at, id) WHERE published_at IS NOT NULL`
serves retention cleanup, so a backlog of unpublished rows never slows it.

### `queue_admin_actions`

The audit trail for administrative actions: one row per replay, cancel, pause or
resume, however many tasks it changed. It holds the action, the filter of a bulk
action (`task_name`, `queue`, `error_filter`, `last_error_filter`), `max_task_id` (the bulk id
bound), `operator`, `reason` and `at`. A row never changes after its insert, and
nothing deletes one. `UNIQUE (id, action)` is the target of
`queue_admin_action_tasks`' foreign key.

A CHECK fixes the shape of each action:

| Action | Carries |
| --- | --- |
| pause, resume | `queue` only |
| single replay or cancel | `task_name` and `queue`; its task is its one `queue_admin_action_tasks` row, which its one statement writes (a CHECK cannot see that) |
| bulk replay or cancel | `task_name` and `max_task_id`, optionally `queue`; a bulk replay always has `error_filter` (`any`, `missing` or `exact`), and `last_error_filter` exactly when it is `exact`; a bulk cancel has neither |

`task_name` and `queue` have identifier CHECKs, which a NULL passes.

### `queue_admin_action_tasks`

One row per task a replay or cancel changed: `action_id`, `action` and `task_id`,
with primary key `(action_id, task_id)`. The statement that changes the task
inserts it, single or bulk, so the audit cannot drift from what was applied. The
composite foreign key `(action_id, action)` to `queue_admin_actions (id, action)`
binds the row to a replay or cancel action (`action` is CHECKed to those two).
There is no foreign key to the task, so the audit outlives task retention.

A bulk batch skips the tasks its action already changed, so an action changes
each task at most once, and `store::resume_bulk` continues an action with the
same guarantee. An action's `affected` is its count of rows, read up to the
admin UI's count cap. `queue_admin_action_tasks_task_idx (task_id, action_id)`
lists the actions that changed a task.

### `queue_pauses`

One row per paused queue: `queue` (primary key) and the `action_id` of the
pause action that created it. A composite foreign key `(action_id, action,
queue)` to `queue_admin_actions (id, action, queue)`, with `action` fixed to
`'pause'`, ties the row to a pause action for the same queue. The operator,
reason and time are the action's, read by a join. Claims and the next-due wakeup
skip a queue with a row, reading it by `queue` alone. `queue` has the identifier
CHECK `queue_pauses_queue_identifier`.

### `queue_workers`

The display-only worker registry: one row per running scheduler, written by the
worker itself (see [Worker Runtime](#worker-runtime)). Nothing in claiming,
ownership or health reads it. It holds `worker_id`, `label`, `started_at`,
`last_seen_at`, `lost_after`, `status` and `last_error`. `lost_after` is
`statement_timestamp() + 3 × registry_interval` at each write, so a reader can tell a lost
worker without knowing the worker's configuration.

Two child tables, both cascading from `queue_workers`, hold what the worker
serves:

- `queue_worker_queues (worker_id, queue)`: `slot_limit ≥ 1`, `in_use` between 0
  and `slot_limit`, and `last_finished_at`;
- `queue_worker_task_names (worker_id, task_name)`: one row per task name the
  worker has a handler for.

Every `worker_id`, `queue` and `task_name` in the three tables has an identifier
CHECK.

Each write replaces the worker's rows in both. Whether a queue is paused is not
stored here: `store::workers` reads it from `queue_pauses` when it lists the
workers, so it is always current. `store::workers` is one statement: subqueries
aggregate each worker's queues as JSON and its task names as an array, so one
statement snapshot covers all three tables at any isolation level, inside a
caller's transaction too.

### `scheduler_jobs` (`cron` feature)

Recurring definitions: a unique schedule `name` (`ScheduleName`), the
`task_name` each occurrence runs (several schedules may share one), the cron
expression, the payload template (`job_args`), `max_attempts ≥ 1`, the queue,
and timestamps. `name`, `task_name` and `queue` have identifier CHECKs
(`scheduler_jobs_name_identifier` and so on). `updated_at` is changed by
`ensure_job` (on every call, even one that changes nothing) and definition
updates, never by the materializer or `reschedule`.

`scheduler_job_cursors (job_id, next_run_at)` holds each schedule's cursor: the
next occurrence to materialize. `next_run_at` is `queue_timestamptz`.
`scheduler_job_cursors_due_idx (next_run_at)` finds due schedules. A job
imported by SQL needs its `scheduler_job_cursors` row, or it is invisible to
`jobs::get` and `jobs::list` and never materialized.

`scheduler_job_pauses (job_id, paused_at)` holds one row per paused schedule.
Both cascade from `scheduler_jobs`, so deleting a schedule deletes its cursor
and its pause.

None of these tables is part of the queue protocol. A schedule only produces
`queue_tasks` rows (see [Recurring Schedules](#recurring-schedules)).

## Normal Form

The schema is in fifth normal form (5NF), with optional facts and multi-valued
facts in relations of their own. It is not in sixth normal form.

- An optional fact, one an entity may or may not have, is a row in its own
  relation, not a nullable column or a flag. A pause is a row in `queue_pauses`
  or `scheduler_job_pauses`.
- A multi-valued fact is a relation keyed so that a value cannot repeat, not
  JSONB or an array. A worker's queues are rows of `queue_worker_queues`, keyed
  `(worker_id, queue)`, and its task names are rows of
  `queue_worker_task_names`, keyed `(worker_id, task_name)`.

6NF would go further and split every attribute that can change on its own into
a relation of its own. The schema keeps the groups below together instead. Each
is an exception, renewed with its reason in
[entry 77](ReviewDecisions.md#77-three-relations-split-the-schedule-cursor-bulk-progress-and-the-pause).
A reason is **needed for a CHECK** or an **access cost**:

| Kept together | Reason |
| --- | --- |
| `queue_tasks` is one wide row, with nullable `locked_by`, `locked_until`, `completed_at` and `last_error` | Its state-shape CHECKs (running ⇔ owner and lease set; terminal ⇔ `completed_at` set) relate those columns, and a CHECK sees only one row. **Needed for a CHECK.** Each transition changes them in one guarded statement. `last_error` changes only in the statements that change the state. |
| `queue_task_attempts` keeps `finished_at`, `outcome` and `error` on the attempt row | They are written together when the attempt closes, and the CHECK `(finished_at IS NULL) = (outcome IS NULL)` keeps them a pair. `error` is a detail of the close. **Needed for a CHECK.** |
| `queue_outbox.published_at` is a nullable column (user decision) | A row is either published or not, so the column cannot contradict itself. The partial index `WHERE published_at IS NULL` keeps relay polling proportional to the pending rows; a separate table would make every poll an anti-join over every retained published row. **Access cost.** |
| `queue_admin_actions` is one audit table with nullable target columns: one durable row per action | Each action's shape CHECK decides which columns it carries, so the nulls are the action's shape, not independent facts. The row never changes after its insert: the tasks a replay or cancel changed are rows of `queue_admin_action_tasks`, and its `affected` is their count, so a role that runs admin actions needs only `INSERT` on both. **Needed for a CHECK** (its shape CHECK). |
| The worker registry keeps its observed values in its rows: `queue_workers.label`, `last_error`, `status`, `last_seen_at` and `lost_after`; `queue_worker_queues.in_use` and `last_finished_at` | They are display-only observations. Their one writer, the worker, rewrites them together in one transaction at every registry write, and nothing reads them for correctness. `in_use` also has a CHECK against `slot_limit` in the same row. **Needed for a CHECK** (`in_use` against `slot_limit`) for `in_use`; **access cost** for the rest, which are display-only observations. |

The schedule cursor, the pause's operator, reason and time, and a bulk action's
running count are not exceptions: each is a relation of its own or derived. A
schedule's cursor is a relation of its own, `scheduler_job_cursors`, so
advancing it leaves the definition and its `updated_at` alone. Every cursor
write happens under its job's row lock (`FOR UPDATE` on `scheduler_jobs`), from
the materializer, `jobs::reschedule`, or the statement in `jobs::create` and
`jobs::ensure_job` that inserts the job, so the lock order is job → cursor →
pause.

Every timestamp column is `queue_timestamptz`, a `TIMESTAMPTZ` domain that is
finite and below 262143-01-01, where chrono's range ends. PostgreSQL accepts
`infinity`, `-infinity` and years chrono cannot hold, so without the domain a
SQL import could write a row that fails to decode. With it, every stored
timestamp decodes, and the browse pages' `±infinity` first-page bounds still lie
outside it.

A stored timestamp is the time of the statement that wrote it
(`statement_timestamp()`). It becomes visible when its transaction commits, which
in a caller's transaction can be later. Lease expiries use `clock_timestamp()`.

## Design Choices

| Choice | Reason |
| --- | --- |
| Attempts are keyed by `(task_id, lock_token)`, with an index on `(task_id, attempt_no)` | Replay resets `attempts`, so the next claim reuses `attempt_no = 1`; `lock_token` never repeats. |
| A claim adds `task_name = ANY($registered)` | A worker claims only names it can execute. |
| A `queue` column; each claim reads one queue; per-queue handler limits; the claim and due indexes lead with `queue` | Priority alone lets sustained high-priority work starve the rest. Separate queues with their own limits keep one kind of work from taking another's handlers. |
| Enqueue notifies only when it inserted a row | A duplicate adds no work. |
| Cancel moves `queued` or `running` to `cancelled`, as an admin operation only; `cancelled` is terminal and replayable | Operators must be able to stop work, and fencing already removes a cancelled task from its worker. |
| Replay accepts `cancelled` as well as `dead` | It is the way back from a mistaken cancel. |
| A `queue_pauses` row stops claims from that queue on every worker | An operator can stop a queue's work without a deploy. |
| One `queue_admin_actions` table audits replay, cancel and pause | A durable audit with operator and reason, one row per action even in bulk. |
| A display-only worker registry | The admin UI shows which workers serve which queues; ownership still rests only on the lease. |
| Release (`running → queued`, `attempts − 1`) at worker shutdown | A deploy should neither use up an attempt nor leave the task until its lease expires. |
| Retention is the idempotency horizon | A key is reserved for exactly as long as its row exists, so there is one period per terminal status and nothing for two settings to disagree on. |
| An optional `cron` producer | Recurring work, layered on enqueue: each occurrence is an ordinary task with a deterministic key. |

## State Machine

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

    queued  --+
              +-- administrative cancel --> cancelled -- administrative replay --> queued
    running --+
```

A claim needs `due_at <= statement_timestamp()`. A retryable failure or an expired lease goes
back to `queued` while attempts remain, and to `dead` once `attempts` has
reached `max_attempts`. Two CHECKs on `queue_tasks` keep the budget honest:
`attempts <= max_attempts`, and `status <> 'queued' OR attempts < max_attempts`
(the claim's predicate depends on it; a queued row with no attempts left would
never run and never be cleaned up). Code never writes `status` outside these transitions.
Cancel and replay are admin operations; workers never cancel. A cancelled
running task's worker finds out at its next heartbeat, when the guarded
statement matches no row.

## Transitions

Every function takes `&mut AsyncPgConnection`, so it composes with a caller's
transaction (nested transactions become savepoints).

| Operation | Function | Notes |
| --- | --- | --- |
| Enqueue | `queue::enqueue::<T>(conn, key, args, opts)` | `INSERT … ON CONFLICT (task_key) DO NOTHING`, then a SELECT of the existing row. It is returned when the task name is the same and the stored payload, decoded as `T::Args` as the worker decodes it, equals `args` by `PartialEq` (equal `HashSet`s match in any serialization order; with `Args = Value`, `1` and `1.0` still conflict). `queue` is not compared, like priority. Anything else, including a stored payload that does not decode, returns `SchedulerError::IdempotencyConflict`. If that row is gone (cleanup deleted it in between), the insert runs again, for at most 3 passes, then `SchedulerError::Contention`. A key starting with `schedule:` is `SchedulerError::ReservedTaskKey`, and `args` whose JSON text the claim's parser rejects (nesting deeper than 128, for example) are `SchedulerError::Serde`, both before any SQL. Before the insert, `SELECT $1::jsonb::text` returns the payload as PostgreSQL will store it, and that text is decoded as `T::Args`, as the worker decodes it: a decode failure (a bare `f64` NaN, stored as `null`) is `Serde`, and a value that differs from the arguments (`T::Args: PartialEq`; JSONB normalizes numbers, so `1e18` comes back as an integer) is `SchedulerError::PayloadNotPreserved`. Before that, `queue::typed_payload` decodes the JSON the producer sends and rejects a value that differs the same way, so `Some(f64::INFINITY)` (written as `null`, decoded as `None`) is `PayloadNotPreserved` before any SQL. Nothing is written either way. |
| Claim | `store::claim(conn, worker, queue, names, limit, lease)` | One writable CTE: `SKIP LOCKED` candidates → `running`, `attempts + 1`, `lock_token + 1`, `locked_until = clock_timestamp() + lease` → attempt rows inserted → rows returned, `payload` as text. Each row is then converted on its own, and the call returns a `ClaimBatch`: `ready` holds the typed claims, and `poisoned` each row whose attempt number, `task_key`, payload or `max_attempts` fails, still leased and untouched. `claim` runs no dead-letter statement. Returns nothing while the queue has a `queue_pauses` row. |
| Dead-letter | `store::dead_letter(conn, &poisoned)` | Fails a `PoisonedClaim` permanently by its ownership key, through the same lock-then-check path as permanent failure, with its reason (for example `invalid payload: …`) as the error. `LostOwnership` once the lease has expired or moved on. The worker calls it on a task of its own after dispatching the batch's ready rows, one such task at a time per scheduler (rows claimed while it runs are left to the reaper); a row it misses stays `running` until the reaper requeues it, and a later claim poisons it again until its attempts run out. |
| Heartbeat | `store::heartbeat(conn, lease, duration)` | In one transaction, locks the row, then extends `locked_until` from `clock_timestamp()`. |
| Lock for DB work | `store::lock_owned(conn, lease)` | `SELECT … FOR UPDATE` with the ownership predicate. |
| Complete | `store::complete(conn, lease)` | Task → `done`, attempt → `done`. |
| Complete + outbox | `store::complete_with_outbox(conn, lease, cmd)` | Completion and the outbox insert in one guarded statement. Returns `OutboxWrite::Inserted`, or `AlreadyPresent` when the task already has this `effect_name` with the same `command_key`, `topic` and payload (canonical JSONB text: key order does not matter, numeric scale does). The same effect with different content is `SchedulerError::OutboxConflict`, and the completion rolls back. |
| Retry | `store::retry(conn, lease, error, delay)` | `queued` at `statement_timestamp() + delay`, or `dead` if `attempts >= max_attempts`. |
| Permanent failure | `store::fail_permanently(conn, lease, error)` | Task and attempt → `dead`. |
| Release | `store::release(conn, lease)` | Worker shutdown only. `running → queued`, `attempts − 1`; `due_at` and `last_error` unchanged, so the task is due at once and keeps its place. Attempt → `released`. |
| Reap | `store::reap_expired(conn, batch, crash_retry_delay)` | Bounded `SKIP LOCKED` batch of expired leases, back to `queued` at `statement_timestamp() + crash_retry_delay` or to `dead`. Closes the attempt as `lease_expired` in the same statement. |
| Replay | `queue::replay(conn, id, operator, reason)` | `dead` or `cancelled` → `queued`, due now, with the same `task_key`: `attempts = 0`, `lock_token + 1` (which fences a very late worker), `last_error` cleared, with an audit row and its membership row. One statement, which also sends `NOTIFY queue_available`. A genuinely new effect needs a new `task_key`, not a replay. |
| Cancel | `store::cancel(conn, id, operator, reason)` | `queued` or `running` → `cancelled`: lock fields cleared, `completed_at` set, open attempt closed as `cancelled`, audit row and membership row. One statement that locks the row `SKIP LOCKED`; a row held elsewhere is retried every 100 ms for 2 s, then `Busy`. Terminal or missing: `NotCancellable`. |
| Bulk cancel / replay | `store::cancel_matching`, `store::replay_matching` | By task name (and optionally queue; replay by an `ErrorFilter`: any `last_error`, none, or exactly a text, where `''` is not none). One audit row with `max_task_id = max(id)` at the start; then batches of 500 `SKIP LOCKED` rows with `id ≤ max_task_id` that the action has not changed yet, each one statement that also inserts a membership row per changed task; a replay batch sends one `NOTIFY`. So an action changes each task at most once. `BulkOutcome::stopped` says why it stopped: `ShortBatch` (a batch came back short, so the matches ran out or locked rows were skipped) or `BudgetSpent` (a full batch ended after the 10 s budget). |
| Resume a bulk action | `store::resume_bulk(conn, action_id, budget)` | More batches of that bulk replay or cancel, with its filter, bound, operator and reason, and no new audit row. `None` if no bulk action has that id. |
| Pause / resume | `store::pause_queue`, `store::resume_queue` | Pause inserts the audit action and then the `queue_pauses` row in one transaction, and rolls back when the queue is already paused, so an audit row exists only when the state changed. Resume deletes the row and writes its audit row. Resume sends `NOTIFY queue_available`. |
| Cleanup | `store::cleanup(conn, &retention)` | One bounded delete per terminal status (so `completed_at` is an index condition): `done`, then `dead`, then `cancelled` (kept for `Retention.dead`), each up to its own `batch_size`, so a backlog in one status never starves the others; then one of published outbox rows. `CleanupSummary` reports each count. A retention over 5 years is `SchedulerError::Config` before any delete. The deletes commit separately, so a database error mid-call leaves the earlier ones committed. |

Every worker-owned transition uses the full ownership predicate:

```sql
WHERE id = $task_id
  AND status = 'running'
  AND locked_by = $worker_id
  AND lock_token = $lock_token
  AND locked_until > clock_timestamp()
```

Each transition first locks the row in a statement of its own:

```sql
SELECT id FROM queue_tasks
WHERE id = $1 AND locked_by = $2 AND lock_token = $3
FOR UPDATE
```

It then runs the guarded statement as a new statement in the same
transaction. After a lock wait, PostgreSQL re-evaluates a statement's `WHERE`
only when the blocker updated the row, so a blocker that only locked it would
leave the scan's `clock_timestamp()` check stale. Under READ COMMITTED the new
statement takes a fresh snapshot and evaluates the expiry after the wait. The
lock statement matches the ownership identity without the expiry, so a stale or
fenced worker matches no row at once instead of waiting on the current owner.

The lease is checked when the transition runs. A caller that holds the row lock
and commits after `locked_until` still has exclusive ownership: the lock keeps
the reaper (`SKIP LOCKED`) and every other worker away until the commit.

Zero rows, from either statement, returns
`SchedulerError::LostOwnership(task_id)`. Inside a transaction, that error rolls
the transaction back. A transition that also closes the attempt does so in the
same transaction, and fails with an invariant error unless exactly one attempt
row closes.

## Recurring Schedules

With the `cron` feature, `jobs::create` and `jobs::ensure_job` write a schedule.
`CreateJob::for_task::<T>` takes the task name, queue and argument type from
`T: Task`; `CreateJob::new` takes a `TaskName` and any serializable arguments,
for a task whose type is not at hand. For a `for_task` spec, `create` and
`ensure_job` first check the arguments' stored form as `enqueue` does, and
return `SchedulerError::PayloadNotPreserved` without writing if it decodes to
different values.

- `ensure_job` inserts by name, or on conflict updates the task name, cron,
  arguments, `max_attempts` and queue. It never changes the pause state or the
  `next_run_at` cursor, so a redeploy cannot resume a schedule an operator
  paused. If a concurrent `jobs::delete` removes the row while the update waits
  on its lock, the update finds nothing and `ensure_job` inserts again, for at
  most 3 passes in its one transaction; if every pass finds the row gone, it
  returns `SchedulerError::Contention`.
- `jobs::pause` and `jobs::resume` lock the job row `FOR KEY SHARE`, so it
  cannot be deleted under them, then add or delete its `scheduler_job_pauses`
  row. They do not change `updated_at`.
- `jobs::reschedule` locks the job row `FOR UPDATE`, then moves its cursor. It
  does not change `updated_at`.
- `jobs::delete` deletes the schedule and, by cascade, its cursor and pause.

Every scheduler materializes due schedules at most once per
`max_poll_interval`, one schedule per transaction, on a task and connection of
its own. Each transaction runs at READ COMMITTED, sets
`idle_in_transaction_session_timeout`, `lock_timeout` and `statement_timeout` to
30 s (keeping a stricter host lock or statement timeout), finds the oldest due
unpaused cursor through `scheduler_job_cursors_due_idx`, locks its schedule's
row `FOR UPDATE SKIP LOCKED`, re-reads its cursor and pause in a new
statement, and parses its cron expression, the next occurrence after the
database's `statement_timestamp()`, its task name, queue and `max_attempts`. If any of them is
invalid, which only direct SQL can cause (for the task name or queue, only once
its CHECK is dropped), the schedule is paused and the error logged, and the
pass goes on. Otherwise the occurrence for the current `next_run_at` is
inserted with `task_key = "schedule:{id}:{next_run_at}"` (microsecond RFC 3339)
and `ON CONFLICT DO NOTHING`, and `next_run_at` moves to the next future
occurrence, in the same transaction. The insert copies `job_args` from the
schedule row in SQL (`INSERT … SELECT`), so the materializer never decodes them,
and the `queue_timestamptz` domain keeps `next_run_at` decodable. After
downtime, one occurrence is created for the missed slot.

The re-read needs READ COMMITTED. Neither a cursor write nor a pause changes the
job row, so locking the row rechecks neither and raises no serialization error.
Without the re-read, a reschedule committed between the scan's snapshot and its
lock would be materialized from the stale slot and then overwritten, and a pause
would get one more occurrence; under REPEATABLE READ the re-read would see the
same old snapshot as the scan. So
`store::materialize_due_jobs` runs each job's transaction at READ COMMITTED,
whatever the session default. On a connection already in a transaction it runs
each job in a savepoint, and if that transaction is not READ COMMITTED it
returns `SchedulerError::Config` and materializes nothing. diesel-async cannot
see a transaction opened with raw SQL, so it must not be called inside one.

Keys starting with `schedule:` are reserved for occurrences: `enqueue` rejects
one with `SchedulerError::ReservedTaskKey`. The first materialization of an
occurrence defines its content for as long as its task row is retained; a later
materialization of the same slot inserts nothing, and its content is not
compared.

## Worker Runtime

```text
startup:  generate UUID worker_id; the loop's first iteration spawns the LISTEN receiver,
          a task that holds one pooled connection, runs LISTEN queue_available, and reads
          notifications continuously, turning each queue_available into one stored wakeup
          (a burst is one wakeup; other channels are ignored);
          spawn maintenance (reaper + cleanup every reaper_interval);
          [cron] spawn the materializer (materialize due schedules now and every
          max_poll_interval, on its own connection; publish each run's result);
          spawn the registry task (write the queue_workers rows now and every
          registry_interval, then prune rows not written for 24 h; display only)
loop:     if the receiver has finished (never started, no connection, a failed LISTEN or a
          failed connection), spawn it again, and claim only once its LISTEN is in effect
          if no queue has a free permit: no claim cycle; wait as below, capped at
          max_poll_interval (a finishing handler, a notification or shutdown wakes it)
          for each queue with a free permit, in configuration order, on one connection:
            claim up to min(its free permits, claim_batch_size) from that queue
            spawn one dispatch per ready claim, holding one of that queue's permits
            then, if any rows were poisoned and the scheduler's one dead-letter slot is
            free, spawn a task that dead-letters them on a connection of its own
            (logged on failure; if the slot is busy the rows are logged at warn
            and left to the reaper)
          if any batch was full of ready claims: loop again immediately
          otherwise wait for the first of: notification, next due_at among queues
          that still have a free permit (clamped to 10 ms..max_poll_interval;
          max_poll_interval if none has, or if a due task could not be claimed
          because another transaction locks it), a dispatch or dead-letter task
          finishing, shutdown; [cron] while waiting, each materializer run
          reports health at once, with the last claim cycle's result
maintain: every reaper_interval, rounds of one reap batch then one cleanup batch
          until both run short
dispatch: if the local deadline has already passed (a slow claim response),
          start nothing: count a lost lease and leave the task to the reaper
          run the handler and the lease keeper concurrently
          lease keeper: heartbeat every heartbeat_interval: take a connection,
                        then run the heartbeat's transaction on a spawned task;
                        on LostOwnership or local deadline → drop the
                        handler, then cancel ctx.cancellation;
                        if the row under this lock_token is cancelled, count a
                        cancellation, otherwise a lost lease
          Ok      → complete   (LostOwnership after a handler-side completion or an
                                operator's cancel is benign and counted as such)
          retry   → retry with exponential backoff + jitter
          interrupted → release during shutdown, otherwise retry
          permanent → fail_permanently
shutdown: stop claiming, maintenance and the materializer, abandoning any pool or
          query wait in the loop;
          the registry task writes `stopping` once (bounded by 500 ms) and exits;
          cancel every ctx.cancellation; the receiver runs UNLISTEN queue_available on its
          own task (never delays the drain);
          drain handlers until shutdown_timeout; then drop the rest and release
          their tasks, waiting up to 5 s (RELEASE_TIMEOUT); abort what remains
          (a completion, retry, failure or release in flight runs on its own task
          and still commits or rolls back; the reaper recovers the other tasks
          after the lease); delete the registry row (bounded by 500 ms)
dropped:  if the run future is dropped instead (aborted, or a failed try_join! sibling),
          maintenance and the materializer stop, handlers are aborted, health
          becomes Stopped, and the receiver runs UNLISTEN queue_available on a task
          spawned on the worker's runtime
          (even if the drop happens off-runtime) before it returns to the pool
```

The lease keeper is a separate future, not a `select!` arm body. A DB-contained
handler holds its task row lock while the heartbeat waits on that lock, and the
handler must keep being polled so it can commit. Each heartbeat's transaction
runs on a spawned task, which owns its connection, so a lease keeper dropped
mid-heartbeat (its handler finished, or the local deadline passed) cannot leave
the heartbeat's transaction open, holding the row lock. The keeper takes the
connection itself, so a wait for one is dropped with it, and spawns nothing if
the deadline has passed by then. The spawned transaction first sets
`lock_timeout` and `statement_timeout` to the time left until the local
deadline (milliseconds rounded up, at least 1, at most `i32::MAX`, about 24.8
days), except that each keeps the session's value when that is non-zero and
smaller, so the heartbeat never raises a timeout the host set. `lock_timeout`
applies to each lock wait, and a statement can wait more than once, so
`statement_timeout` bounds each statement as a whole, and the lock statement
ends by the deadline. After the row lock it checks a token that the dispatch
cancels however it ends (finished, lease lost, released, or dropped with the
scheduler future, but not when graceful shutdown begins), and rolls back without
renewing if it is cancelled. A dispatch that ends between that check and the
update still gets that one renewal.

The local deadline is `Instant` at claim send + `lease_duration`, moved forward
on each successful heartbeat. It only gives up ownership early. PostgreSQL alone
decides whether a lease is valid.

Backoff for attempt `n` is exactly
`min(base_retry_delay · 2^(n−1), max_retry_delay) + uniform(0..=retry_jitter)`
for every `n`. The product is computed in nanoseconds as a saturating `u128`
with the doubling count clamped to 127; both limits lie far above the 5-year
cap on `max_retry_delay`, so neither changes the result. The worker computes
the delay, and PostgreSQL adds it to its `statement_timestamp()`, so a host clock never
decides when a retry is due.

A panicking handler is not finalized. Its attempt stays open until the reaper
recovers it after the lease expires.

## Configuration

| Builder method | Default |
| --- | --- |
| `queue(name, limit)` | `default`, 16 handlers per scheduler; configuring any queue replaces it |
| `claim_batch_size` | 16 per queue claim (`NonZeroU32`) |
| `lease_duration` | 60 s (checked as a `LeaseDuration`: 1 µs to 5 years) |
| `heartbeat_interval` | 20 s (must be ≤ lease / 3) |
| `max_poll_interval` | 1 s |
| `base_retry_delay` / `max_retry_delay` / `retry_jitter` | 1 s / 5 min / 1 s |
| `reaper_interval` / `reaper_batch_size` | 20 s / 100 (`NonZeroU32`) |
| `crash_retry_delay` | 10 s |
| `retention` | done 30 d, dead and cancelled 30 d, published outbox 7 d, batch 1000; a `task_key` stays reserved for as long as its row is kept |
| `shutdown_timeout` | 25 s |
| `registry_interval` | 15 s; the row reads as lost after 3 × this |
| `label` | `HOSTNAME` if set and non-empty, else none (the worker id is shown) |

`build()` rejects, checking the lease first and retention second:

- a `lease_duration` under 1 µs or over 5 years;
- a retention duration over 5 years (`store::cleanup` applies the same check);
- a scheduler with no handler registered;
- any other duration over 5 years;
- a heartbeat, poll, reaper or registry interval under 1 µs, the resolution
  PostgreSQL stores;
- a queue limit above `tokio::sync::Semaphore::MAX_PERMITS`;
- `heartbeat_interval > lease_duration / 3`;
- `base_retry_delay > max_retry_delay`.

`max_attempts` is set per task at enqueue (default 3).

## Rust Boundaries

- `Task` ties a stable name, a default queue and one
  `Serialize + DeserializeOwned + PartialEq` payload type together. `enqueue::<T>`,
  `register_task::<T>` and `CreateJob::for_task::<T>` all use it. `enqueue`
  and `CreateJob` parse the JSON text of the arguments back with the claim's
  parser, so they reject a payload the worker could not parse. The typed
  `enqueue::<T>`, and `jobs::create` and `jobs::ensure_job` for a
  `CreateJob::for_task::<T>` spec, first decode the JSON they send as `T::Args` (`queue::typed_payload`), then
  decode the payload as PostgreSQL stores it as `T::Args`, and reject arguments that come back as different values
  (`PayloadNotPreserved`). `store::task` returns the payload as a
  `TaskPayload`: `Json`, or `Unreadable { text, error }` for a stored value that
  does not parse (one written by direct SQL), and the key as a `StoredTaskKey`:
  `Valid`, or `Invalid { text, error }` for a stored key that breaks the
  identifier rule (possible only once its CHECK is dropped by hand). Either way
  its dead letter can still be read.
- Text identifiers are validated newtypes: `TaskKey`, `TaskName`, `QueueName`,
  `WorkerId`, `ScheduleName` (`cron`), `EffectName` and `Topic` (1 to 255
  characters, no leading or trailing whitespace, no NUL), and `CommandKey`
  (non-empty, no NUL, at most 2048 bytes). They are validated when constructed,
  by the CHECKs when stored, and again when decoded from the database. The
  identifier CHECKs call `queue_is_identifier`, whose whitespace class spells
  out what `str::trim` strips, because `[[:space:]]` follows the locale and
  misses U+0085 and U+00A0; it counts code points, so the migrations require a
  UTF8 database and fail otherwise. In a database whose CHECK was dropped by
  hand, a row with an invalid value is a decode error. The claim and
  `store::task` read `task_key` as text instead, so such a row can still be
  dead-lettered and opened. `TaskId`, `LockToken` and `JobId` carry no rule.
  `enqueue` also rejects a `TaskKey` with the reserved `schedule:` prefix.
- `TaskLease { task_id, worker_id, lock_token, attempt_no }` is the ownership
  proof every guarded transition takes. Handlers receive it as `ctx.lease`.
  `attempt_no` is an `AttemptNo`, which counts from 1; the attempt counts on
  `TaskView` and `TaskRow` are `u32`.
- Store inputs carry their own rules. `claim` and `next_due_in` take
  `&[TaskName]`. `claim` and `heartbeat` take a `LeaseDuration` (1 µs to 5
  years). Claim, reap and cleanup batch sizes are `NonZeroU32`. A delay that may
  be zero stays a `Duration`; one too large for a PostgreSQL interval is
  `SchedulerError::Config`, never saturated.
- `TaskContext` carries the lease, `task_key`, `task_name`, `max_attempts`
  (`MaxAttempts`), `locked_until` (as of claim; heartbeats do not update it),
  and a `CancellationToken`. On lease loss or cancel the handler future is
  dropped before the token fires, so only work the handler spawned observes it.
- Read models are sums that mirror the CHECKs. `TaskState` is `Queued`,
  `Running { locked_by, locked_until }` or `Finished { status, completed_at }`,
  and `AttemptState` is `Open` or `Closed { finished_at, outcome, error }`. A row the
  CHECKs forbid is `SchedulerError::Invariant`. An admin action's
  `AdminActionTarget` is `Task`, `Bulk` or `Queue`.
- Page limits and count caps are `NonZeroU32`. Cursor positions use checked
  arithmetic, and one that would overflow is `SchedulerError::InvalidCursor`.
- `TaskError::{retry, permanent, interrupted}` is the handler's failure
  classification: retryable when a later attempt may succeed, permanent when
  repeating the same operation cannot help. The error text is stored with each
  NUL escaped as `\0`, because PostgreSQL text cannot hold NUL, so the chosen
  outcome always lands.
- Observability goes through the `tracing` and `metrics` facades. The crate
  installs no subscriber or recorder. The metric names and labels are a public
  contract, listed in the [README](../README.md#observability).

## Performance

- Claiming is one round trip per batch. Handlers hold no connection while
  running.
- Heartbeat is a two-statement transaction: the row lock, then the guarded
  `UPDATE`. The worker's heartbeat sets its timeouts first, so it is three.
  Completion, retry, failure and release add the attempt close, so they are
  three.
- A typed `enqueue`, and a typed schedule's `create` or `ensure_job`, run one
  more query first, to check the arguments' stored form.
- The reaper and cleanup loop over bounded batches on a separate task, so a large
  backlog never stalls claiming. A reap batch runs before every cleanup batch, so
  a cleanup backlog never delays lease recovery.
- `store::queue_stats` scans retained rows for its status counts. Poll it at a
  modest interval. The admin read APIs are bounded instead, and each statement
  has an EXPLAIN test in `tests/performance.rs`.
- A pause adds one primary-key probe of `queue_pauses` to every claim. The
  registry adds one transaction of five statements per worker every
  `registry_interval`: upsert the worker row, delete and upsert its queue rows,
  delete and insert its task names. `store::workers` reads it back in one
  statement. Bulk cancel by name reads every queued and running row through
  their partial indexes (its status test is written as `OR`, which matches
  them; `IN` would scan every retained row) and filters by name, because an
  index on `task_name` there would tax every enqueue.
- With `cron`, `scheduler_job_cursors_due_idx` is not partial: a pause is a row
  of `scheduler_job_pauses`, so a paused schedule that is due stays at the
  front of the index. Each materializer statement that finds the next due
  schedule walks past every such schedule and probes the pause table for each.
  That is O(paused-and-due) per statement, which is cheap at realistic counts.
  The due cursor reaches its schedule by primary key. A schedule the
  materializer locks gets one more probe of its cursor and pause, which closes
  the race with a concurrent reschedule or pause.
- The claim index is a starting point, not an optimum. Benchmark it against
  realistic scheduled-task and priority distributions before tuning: very many
  future tasks or unusual priority mixes may justify bounded priority classes
  or priority-specific indexes.

## Testing Contract

Each requirement below has an automated test against PostgreSQL.

| # | Requirement | Test |
| --- | --- | --- |
| 1 | duplicate enqueue | `enqueue::concurrent_duplicate_enqueue_creates_one_task` |
| 2 | conflicting duplicate | `enqueue::conflicting_duplicate_is_rejected` |
| 3 | concurrent claim | `claim::concurrent_claims_produce_one_owner` |
| 4 | bulk concurrency | `worker::bulk_workers_process_each_task_once` |
| 5 | scheduled timing | `claim::future_task_is_not_claimed_until_due` |
| 6 | priority | `claim::higher_priority_due_task_is_claimed_first` |
| 7 | retry | `transitions::retry_requeues_with_db_time_delay_then_dies`, `worker::retryable_error_requeues_with_backoff_then_dies` |
| 8 | hard lease | `transitions::every_transition_fails_after_hard_lease_expiry`, `transitions::complete_waiting_on_a_lock_gives_up_at_expiry` |
| 9 | reaper | `maintenance::reaper_requeues_with_crash_retry_delay`, `maintenance::reaper_kills_exhausted_task` |
| 10 | stale ACK | `transitions::stale_token_cannot_complete_after_reclaim` |
| 11 | DB-only atomicity | `transitions::db_contained_failure_rolls_back_effect_and_completion` |
| 12 | external idempotency | `worker::crash_after_remote_accept_does_not_duplicate_external_effect` |
| 13 | outbox ownership | `transitions::stale_worker_cannot_emit_outbox_command` |
| 14 | transactional enqueue | `enqueue::enqueue_rolls_back_with_callers_transaction` |
| 15 | retention | `maintenance::cleanup_deletes_only_eligible_terminal_rows_in_batches` |

Process-kill fault injection at random points around claim, external effect,
completion and heartbeat is not automated yet. See
[Concurrency Model](Concurrency.md#validation-status).
