# Concurrency Model

## Summary

Many scheduler instances, each running many handlers, can share one PostgreSQL
database with no leader or external coordinator. PostgreSQL row locks decide who
claims a task. A hard lease plus a monotonic `lock_token` fences any worker that
no longer owns it.

Queue state stays consistent under any interleaving. Execution of external work
is **at-least-once**. A worker that lost its lease, or crashed after a remote call
succeeded, may overlap with or precede a retry. Effects that live entirely in
PostgreSQL can be made exactly-once by committing them in the same transaction as
completion.

## Ownership

A task attempt has at most one valid owner. Ownership is all four of:

```sql
id = $task_id
AND status = 'running'
AND locked_by = $worker_id          -- unique UUID per scheduler process
AND lock_token = $lock_token        -- +1 on every claim and every replay
AND locked_until > clock_timestamp()  -- hard lease, database time
```

Heartbeat, completion, completion with outbox, retry, permanent failure, release,
and the DB-contained `lock_owned` all use this predicate. Each first locks the row
in its own statement, in the same transaction:

```sql
SELECT id FROM queue_tasks
WHERE id = $1 AND locked_by = $2 AND lock_token = $3
FOR UPDATE
```

Only then does it run the guarded statement. The order matters: after a lock
wait PostgreSQL re-evaluates a statement's `WHERE` only if the blocker updated
the row, so a blocker that merely locked the row would leave the
`clock_timestamp()` check stale. The separate statement evaluates expiry after
the wait. The lock matches the ownership identity but not the expiry, so a
stale or fenced worker matches no row at once instead of queueing behind the
current owner. A guarded statement that matches zero rows means ownership is
lost (`SchedulerError::LostOwnership`). It never means success.

The lease is **hard**. Once `locked_until` passes, the old worker is stale even if
no reaper has touched the row yet. It can no longer heartbeat or finalize.

## Claiming Work

`store::claim` is a single statement:

1. Select due, claimable `queued` rows of one queue for the worker's registered
   task names, in `priority DESC, due_at, id` order, `FOR UPDATE SKIP LOCKED`, up
   to the batch limit.
2. Move them to `running`: `attempts + 1`, `lock_token + 1`,
   `locked_by = worker`, `locked_until = clock_timestamp() + lease`.
3. Insert one `queue_task_attempts` row per claim.
4. Return the claims.

The statement is atomic, so a claim and its attempt row commit together or not at
all. Concurrent claimers skip each other's locked rows, so a queued task gets one
owner.

Rust then converts each claimed row on its own. A row that fails (a key,
payload or count written by direct SQL that the types reject) comes back in
`ClaimBatch::poisoned`, still leased, instead of failing the call. The worker
dispatches the ready rows first, then dead-letters the poisoned ones with
`store::dead_letter` on a task and connection of their own, so a slow
dead-letter cannot spend a healthy row's lease. A poisoned row it misses stays
`running` until the reaper requeues it, and a later claim poisons it again
until its attempts run out.

A scheduler serves the queues configured with `.queue(name, limit)` (by default
`default` at 16), each with its own permits. Each cycle, it claims from every
queue with a free permit, in configuration order, at most
`min(that queue's free permits, claim_batch_size)` tasks per queue. Limits apply
to each instance separately, so ten instances serving `bulk` at 4 can run 40
`bulk` handlers.

Workers claim only task names they have handlers for, from queues they serve.
Worker groups with different registrations can share one queue.

## Races

| Race | Result |
| --- | --- |
| Two workers claim one task | One locks and claims it; the other skips it. |
| Two producers enqueue one `task_key` | `ON CONFLICT DO NOTHING` waits for the first insert to commit; the second returns the existing task. |
| Worker ACKs after its hard lease expired | The row is locked first (waiting out any other holder), then the predicate fails on `locked_until`; zero rows; the worker cannot complete. |
| Old generation ACKs after a reclaim | `lock_token` no longer matches; zero rows. |
| Heartbeat races the reaper | Either the heartbeat extends a still-valid lease, or the reaper requeues the expired row and the heartbeat matches zero rows. |
| Completion races the reaper | A transition starting after expiry matches zero rows; a transition that already holds the row lock keeps the row away from the reaper. |
| Release races lease expiry | A release starting after expiry matches zero rows; the reaper requeues the row and the attempt counts. |
| Reaper races a DB-contained transaction | The handler holds the task row via `lock_owned`; the reaper's `SKIP LOCKED` passes over it. |
| Two reapers select one expired task | One locks it; the other skips it. |
| Stale worker emits an outbox command | The outbox insert depends on the guarded completion CTE; no completed row means no outbox row. |
| Replay while a very late worker is still running | Replay bumps `lock_token`, so the late worker's token is fenced. |
| Operator cancels a running task | The cancel clears the lease; the worker's next heartbeat or transition matches zero rows. See [Admin Actions](#admin-actions). |
| Cancel races a claim of the same task | The cancel skips the row the claim holds, then retries and cancels the now running task. |
| Cancel races a DB-contained transaction | The cancel skips the `lock_owned` row and returns `Busy` after 2 s, or `NotCancellable` once the handler has committed `done`. |
| Pause races a claim already in flight | A claim whose snapshot predates the pause may still take tasks; every later claim skips the queue. |
| Two bulk actions on one task name | Each batch skips the other's locked rows, so they split the work and no task is counted twice. |

## Leases and Heartbeats

Handlers hold no database connection by default. Each dispatched task runs two
futures concurrently:

- the handler;
- a lease keeper that briefly takes a connection every `heartbeat_interval`
  (default 20 s, at most a third of `lease_duration`) to extend `locked_until`.

The lease keeper gives up, cancels `ctx.cancellation`, and drops the handler
future when either:

- a heartbeat matches zero rows; or
- a local monotonic deadline passes: claim send time plus `lease_duration`,
  advanced on each successful heartbeat. This covers heartbeats that keep failing
  or hang, for example during a database outage.

The local deadline only makes the worker give up early. It never extends
ownership, and host clocks never decide validity. A claim whose response
arrives after that deadline starts no handler: the dispatch counts a lost lease
and leaves the task to the reaper.

`ctx.cancellation` is cancelled when worker shutdown begins, when the lease
keeper gives up, or when the dispatch ends. When the lease keeper gives up, the
handler future is dropped first and the token fires after, so the handler never
observes it; only work it spawned with a clone of the token does. A handler that
stops early because
shutdown began should return `TaskError::interrupted()`; its task is then
released without counting the attempt. Shutdown drops handlers still running at
`shutdown_timeout` and releases their tasks the same way. A release does not
count the attempt and has no limit, so `max_attempts` does not bound a handler
that hangs: it is released at every deploy and never reaches `dead`. Handlers
must put timeouts on their own awaits.

Cancellation is cooperative. Async code stops when its future is dropped or when
it observes the token. Blocking code, independently spawned work, and remote
requests already in flight cannot be recalled.

A DB-contained handler that called `lock_owned` holds its row lock for its whole
transaction, so every heartbeat's lock statement waits on it and the lease
cannot be extended meanwhile. The lease keeper returns at the local deadline
(`sent + lease`, never after the database's `locked_until`), and the worker then
drops the handler future, abandoning its transaction. In the runtime the whole
transaction, including its commit, must therefore finish within the lease. The
lease is checked when a transition runs, after it has locked the row. A direct
store caller that commits after `locked_until` still holds exclusive ownership,
because its row lock kept the reaper and every other worker away; it does not
roll back. Each heartbeat's transaction runs
on a spawned task, so when the lease keeper is dropped mid-heartbeat (the
handler finished, or the local deadline passed) the heartbeat still commits or
rolls back, and never leaves an open transaction holding the row lock. The
spawned heartbeat is bounded by the lease: its `lock_timeout` and
`statement_timeout` are both the time left until the local deadline, except
that each keeps the session's value when the host set a smaller non-zero one,
so it never raises the host's timeouts. PostgreSQL applies `lock_timeout` to
each lock wait, and a statement can wait more than once, so `statement_timeout`
bounds each statement as a whole. Both accept at most `i32::MAX` milliseconds
(about 24.8 days), so for a longer lease the wait is capped there instead. Once
its dispatch has ended (finished, lease lost, released, or dropped with the
scheduler future), it renews nothing: after taking the row lock it sees the
dispatch's token cancelled and rolls back. The token is checked once, between
the lock and the update, so a dispatch that ends in that window still gets one
last renewal, which delays the reaper by at most one lease. Graceful shutdown
does not cancel that token, so handlers keep their leases while they drain.

## Admin Actions

Cancel, bulk actions and pause are admin operations. They never take ownership
of a task, and nothing in the worker trusts them beyond what the ownership
predicate already enforces.

**Cancel against a running handler.** `store::cancel` moves the row to
`cancelled`, clears `locked_by` and `locked_until`, and closes the open attempt
as `cancelled`, in one statement. The worker learns of it through the fence:

1. The lease keeper's next heartbeat, within `heartbeat_interval`, matches zero
   rows. The handler future is dropped and `ctx.cancellation` fires.
2. The worker reads the task's status under its own `lock_token`. `cancelled`
   means an operator cancelled it: the worker logs at info and counts
   `tasks_cancelled_total`, not `leases_lost_total`. Any other answer is a lost
   lease, as before.
3. A handler that returns before that heartbeat gets `LostOwnership` from its
   completion, retry or failure, and the same check counts a cancellation.

Anything the handler already did stays done. A cancel followed by a replay
before the old worker's next heartbeat bumps `lock_token`, so that worker sees a
different generation and reports a lost lease.

**Cancel against a claim.** The cancel locks the row with `SKIP LOCKED`, so it
never waits. If a claim (or a DB-contained handler inside `lock_owned`) holds the
row, the statement changes nothing, and a lock-free read in the same statement
sees the task still `queued` or `running`. `store::cancel` then retries every
100 ms, for up to 2 s, before returning `Busy`. A claim is one short statement,
so a retry normally finds the task `running` and cancels it. If the claim
committed after the cancel's snapshot but before its lock, the cancel's row lock
sees the claimed version, but the snapshot cannot see the new attempt row. The
attempt is therefore closed with `INSERT … ON CONFLICT (task_id, lock_token) DO
UPDATE`, which finds the committed row anyway. A `lock_owned` transaction that
outlasts the 2 s gives `Busy`, and one that commits `done` first gives
`NotCancellable`.

**Pause against an in-flight claim.** A claim checks for a `queue_pauses` row
with `NOT EXISTS` inside its own statement. A claim whose snapshot was taken
before the pause committed can still take tasks from the queue; every claim
after that skips it, and `next_due_in` stops waking idle workers for its due
tasks. Running tasks, the reaper and releases are unaffected: a task that fails
or is reaped goes back to `queued` and waits. Resume deletes the row and sends
`NOTIFY queue_available`, so idle workers claim again at once.

**Bulk actions: the id bound and skipped rows.** `cancel_matching` and
`replay_matching` first insert their audit row with `max_task_id`, the highest
committed task id at that moment. Every batch takes only rows with
`id ≤ max_task_id`, so a producer that keeps enqueuing the same name cannot keep
the action running forever; pause the queue or fix the producer to stop new
arrivals. A row with a lower id that commits later (a producer transaction that
was still open) can still be included. Each batch locks its rows
`FOR UPDATE SKIP LOCKED`, so two bulk actions on the same name split the work,
and a row locked by a handler or a claim is skipped. A batch can also come back
short when a row it found changed before it locked it. A short batch ends the
call with `BulkStop::ShortBatch`, which therefore does not prove that no
matching task is left: count again, or run the action again.

## Crash Recovery

A worker that dies leaves its task `running` until `locked_until`. Every scheduler
runs the reaper every `reaper_interval` in bounded `SKIP LOCKED` batches. It moves
expired rows back to `queued` with `due_at = now() + crash_retry_delay` (default
10 s, to avoid a tight crash loop), or to `dead` when `attempts` has reached
`max_attempts`. The open attempt row closes as `lease_expired`.

A panicking handler counts as a crash. The runtime does not finalize it, so the
reaper recovers it after the lease.

## External Effects

Fencing protects PostgreSQL state, not remote systems. This is a valid history:

1. Worker A calls a remote API, and the remote accepts.
2. Worker A crashes, or loses its lease, before recording completion.
3. The reaper requeues the task, and worker B makes the same call again.

So:

- a non-idempotent external effect needs deduplication on the receiving side,
  keyed to the logical effect. Pass `ctx.task_key` as the remote idempotency
  key, with the effect's name if a task has more than one effect. It is stable
  across retries and replay.
- fencing is a different guarantee. Where the remote supports ordered
  conditional writes, also pass `ctx.lease.lock_token` as a fencing token: a
  receiver that keeps the highest token it has seen rejects a stale write from
  an earlier owner. It does not deduplicate. Worker A's call with token 1 and
  worker B's retry with token 2 both pass the fence.
- `lock_token` is ordered only within one task row. A `task_key` can be reused
  once retention has deleted its row, and the new row's tokens start again, so
  scope a remote fence to the task id (`ctx.lease.task_id`), not to the
  `task_key`.
- without remote deduplication, duplicate external effects remain possible.

## PostgreSQL-Contained Effects and the Outbox

When all business effects are writes in the same database, the handler opens a
transaction, calls `store::lock_owned`, performs the writes, and calls
`store::complete` before committing. Either both commit or neither does. The
runtime's own completion attempt afterwards matches zero rows, sees that this
lease generation already completed the task, and does nothing.

To hand work to another system, `store::complete_with_outbox` inserts the outbox
row in the same guarded statement as completion. The application's relay
publishes at-least-once, and receivers deduplicate by `command_key`.

## Producers and the Idempotency Horizon

Producers can enqueue concurrently. `task_key` uniqueness gives one logical task
per key, and a duplicate returns the existing task. Enqueue inside the producer's
own transaction to make the domain change and the task atomic. Use
`READ COMMITTED`: the duplicate path re-reads the committed winner.

`READ COMMITTED` is a host requirement for every direct store caller. The
transitions' recheck after a lock wait, `ensure_job`'s recreate-on-delete and
claim all rely on it; under `REPEATABLE READ` they fail safely with a `40001`
serialization error rather than their documented outcome. The materializer
enforces `READ COMMITTED` itself.

A key stays reserved while its row exists, so retention is the idempotency
horizon: `Retention::done` for `done` tasks, and `Retention::dead` for `dead` and
`cancelled` ones (30 days each by default). A key can be reused once cleanup has
deleted its row.

With the `cron` feature, every scheduler also materializes due schedules, at
most once per `max_poll_interval`. It locks one schedule row with `SKIP LOCKED`,
inserts the occurrence with a deterministic `task_key`, and advances the cursor
in one transaction. That transaction sets `idle_in_transaction_session_timeout`
to 30 s, so one cut off by shutdown and left open on a pooled connection is
ended by the server instead of holding the schedule row, which other schedulers
skip, until the connection is recycled.

## Ordering and Fairness

The claim order (`priority DESC, due_at, id`) is a preference, not a guarantee.
`SKIP LOCKED` lets a worker take a lower-ranked row while a higher one is locked.
Handler durations reorder completion. Future tasks are never claimed before
`due_at`, measured in database time.

Priority orders tasks only within a queue. Sustained high-priority load can
starve lower priorities in the same queue. Queues handle starvation: each has its
own handler limit, so a busy queue cannot take another queue's slots. Put work
that must not starve in its own queue.

There is no priority ageing. An `ORDER BY` that depends on the current time
defeats the claim index, so every claim would sort all due rows. Periodically
promoting old rows instead rewrites them, and loses the priority the producer
chose.

## Wakeups and Polling

Polling is authoritative. `LISTEN/NOTIFY` only reduces latency:

- enqueue sends `NOTIFY queue_available` on commit when it inserts a task;
- an idle worker waits for the first of: a notification, the next `due_at`
  among its task names in queues with a free permit (database time), or
  `max_poll_interval` (default 1 s);
- scheduled tasks and retries become due with no notification, through the
  next-`due_at` wait;
- a due task that a claim skips because another transaction locks its row is
  retried after `max_poll_interval`, not every 10 ms;
- if the listening connection fails, the worker keeps polling and re-listens on
  the next loop.

## Capacity and Operations

- **Connection pool:** each scheduler permanently holds one connection for
  `LISTEN`. Claims, heartbeats, finalization, maintenance, and DB-contained
  handlers take short-lived connections. Each scheduler needs 1 connection for
  `LISTEN`, plus 1 for claims and maintenance, plus up to the sum of queue limits
  for heartbeats and finalization, plus one per handler that takes its own. So
  size `max_size ≥ sum of queue limits + 2` per scheduler sharing the pool, plus
  whatever handlers take. If DB-contained handlers share an undersized pool,
  heartbeats wait for connections until local deadlines drop the handlers.
  Deadpool's default `max_size` is 2 × the logical CPU count (4 on a 2-vCPU
  host), well below the 16 + 2 = 18 that the default queue's limit of 16 needs.
  While poisoned rows keep arriving, each claim cycle may start one dead-letter
  task per queue, and wakes from a NOTIFY or a finished dispatch can start
  another while one is still running, so budget about one more connection per
  queue, possibly more.
- **Sizing:** the handlers needed across all instances are about arrival rate ×
  average handler duration (Little's law). Each instance's limit is that number
  divided by the instance count, times 1.5 to 2 for bursts and retries. Keep
  (instances × pool `max_size`) under PostgreSQL's `max_connections`, with room
  to spare. Queue layout, health signals and deploy advice are in the README's
  [Running Queues](../README.md#running-queues).
- **Lease duration:** keep it well above pool wait plus database latency, and
  above the longest DB-contained transaction. A database outage longer than the
  lease lets another worker start the task.
- **Timeouts:** configure `statement_timeout` and `lock_timeout` on pool
  connections. Queue statements are short. The worker's heartbeat bounds its
  own transaction by the time left on its lease, but never raises a stricter
  value the host set.
- **Retry storms:** exponential backoff with jitter spreads correlated failures.
  `crash_retry_delay` does the same for crash loops.
- **Table health:** every claim, heartbeat, retry, and finalization updates the
  task row. Monitor autovacuum, dead tuples, index bloat, and cleanup throughput.
  Partitioning is a later, measured step.
- **Backlogs:** the reaper and cleanup loop in bounded batches on a separate task,
  so they never block claiming. Each round reaps one batch before it cleans one,
  so a cleanup backlog never delays lease recovery. Sustained input above
  capacity still grows `queued`. Watch `store::queue_stats`.

## Failure Semantics

| Failure | Result |
| --- | --- |
| Producer crashes before commit | Neither the task nor its domain changes are visible. |
| Producer times out after commit | Retrying with the same `task_key` returns the existing task. |
| Worker crashes before claim commit | The claim rolls back; the task stays `queued`. |
| Worker crashes after claim | The lease expires; the reaper requeues the task or marks it `dead`. |
| Claim committed but the response was lost | The task stays `running` with no live handler; the reaper recovers it after expiry. |
| Dead-lettering a poisoned row fails, or shutdown aborts it | The row stays `running`; the reaper requeues it after expiry, and a later claim poisons it again until its attempts run out. |
| Completion committed but the response was lost | The task is `done`; nothing re-runs it. |
| PostgreSQL unavailable | No transition happens; claim errors (and, with `cron`, materialize errors) set `Degraded`; heartbeat and maintenance failures do not affect health, and a worker with no free handler slot in any queue runs no claim cycle so it keeps its last status. Workers retry without inventing local state. |
| Graceful shutdown with handlers still running | Shutdown cancels every `ctx.cancellation` and drains for up to `shutdown_timeout`. A handler that returns `TaskError::interrupted()`, or is still running at the timeout and is dropped, has its task released: back to `queued`, due at once, the attempt not counted. A release not finished within 5 s more (the database is unreachable, or a dropped DB-contained transaction still holds the row lock) is aborted; the reaper recovers that task after the lease, and the attempt counts. A kept-alive pool can leave a dropped transaction's connection holding that lock, so exit the process after shutdown. |

## Validation Status

The integration suite covers every requirement of the
[testing contract](SchedulerDesign.md#testing-contract). It also covers:
heartbeats while a handler holds the row lock, replay followed by a reclaim,
notification and next-due wakeups, shutdown without detached work, and shutdown
release: an interrupted handler, a handler still running at `shutdown_timeout`,
and a release blocked behind a dropped transaction's row lock. For admin
actions it covers a cancel against a running worker
(`cancel::worker_drops_cancelled_handler_and_counts_cancellation`) and a held row
lock (`cancel::cancel_locked_row_is_busy`), bulk actions racing producers and
each other (`bulk::cancel_matching_ignores_tasks_enqueued_after_it_started`,
`bulk::concurrent_bulk_cancels_split_the_work`), and pause against claims,
retries and wakeups (`tests/pause.rs`). The race between a cancel's snapshot and
a claim's commit is argued above, not tested: it cannot be forced
deterministically.

Not yet automated:

- process-kill fault injection at random points around claim, effect,
  completion, and heartbeat;
- database outages longer than a lease;
- pool exhaustion;
- throughput and latency at production scale.

Run these with the host's PostgreSQL version, pool sizing, payload sizes, and
retention policy before relying on a capacity figure.
