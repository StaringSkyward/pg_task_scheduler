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

Heartbeat, completion, completion with outbox, retry, permanent failure, and the
DB-contained `lock_owned` all use this predicate. A guarded statement that
matches zero rows means ownership is lost (`SchedulerError::LostOwnership`). It
never means success.

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
| Worker ACKs after its hard lease expired | The predicate fails on `locked_until`; zero rows; the worker cannot complete. |
| Old generation ACKs after a reclaim | `lock_token` no longer matches; zero rows. |
| Heartbeat races the reaper | Either the heartbeat extends a still-valid lease, or the reaper requeues the expired row and the heartbeat matches zero rows. |
| Completion races the reaper | Completion needs an unexpired lease; once expired, only the reaper can move the row. |
| Release races lease expiry | Release needs an unexpired lease; after expiry only the reaper moves the row, and the attempt counts. |
| Reaper races a DB-contained transaction | The handler holds the task row via `lock_owned`; the reaper's `SKIP LOCKED` passes over it. |
| Two reapers select one expired task | One locks it; the other skips it. |
| Stale worker emits an outbox command | The outbox insert depends on the guarded completion CTE; no completed row means no outbox row. |
| Replay while a very late worker is still running | Replay bumps `lock_token`, so the late worker's token is fenced. |

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
ownership, and host clocks never decide validity.

`ctx.cancellation` is cancelled when worker shutdown begins, when the lease
keeper gives up, or when the dispatch ends. A handler that stops early because
shutdown began should return `TaskError::interrupted()`; its task is then
released without counting the attempt. Shutdown drops handlers still running at
`shutdown_timeout` and releases their tasks the same way.

Cancellation is cooperative. Async code stops when its future is dropped or when
it observes the token. Blocking code, independently spawned work, and remote
requests already in flight cannot be recalled.

A DB-contained handler holds its row lock while a heartbeat may be waiting on it.
Because the lease keeper is a separate future, the handler keeps being polled and
commits, and the heartbeat then sees a `done` row. The lease must still outlast
the database transaction. An expired lease makes the final guarded update match
zero rows, and the transaction rolls back.

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

- pass `ctx.task_key` as the remote idempotency key for every once-per-task
  effect. It is stable across retries and replay.
- where the remote supports ordered conditional writes, also pass
  `ctx.lease.lock_token` as a fencing token.
- without remote idempotency or fencing, duplicate external effects remain
  possible.

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

A key stays reserved while its row exists. Retention never deletes terminal rows
younger than `Retention::idempotency_horizon`, and the builder rejects a
retention configuration that would.

With the `cron` feature, every scheduler also materializes due schedules, at
most once per `max_poll_interval`. It locks one schedule row with `SKIP LOCKED`,
inserts the occurrence with a deterministic `task_key`, and advances the cursor
in one transaction.

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
  connections. Queue statements are short.
- **Retry storms:** exponential backoff with jitter spreads correlated failures.
  `crash_retry_delay` does the same for crash loops.
- **Table health:** every claim, heartbeat, retry, and finalization updates the
  task row. Monitor autovacuum, dead tuples, index bloat, and cleanup throughput.
  Partitioning is a later, measured step.
- **Backlogs:** the reaper and cleanup loop in bounded batches on a separate task,
  so they never block claiming. Each round reaps one batch before it cleans one,
  so a cleanup backlog never delays lease recovery. Sustained input above
  capacity still grows `queued`. Watch `store::queue_stats`.

## Failure Semantics (§22)

| Failure | Result |
| --- | --- |
| Producer crashes before commit | Neither the task nor its domain changes are visible. |
| Producer times out after commit | Retrying with the same `task_key` returns the existing task. |
| Worker crashes before claim commit | The claim rolls back; the task stays `queued`. |
| Worker crashes after claim | The lease expires; the reaper requeues the task or marks it `dead`. |
| Claim committed but the response was lost | The task stays `running` with no live handler; the reaper recovers it after expiry. |
| Completion committed but the response was lost | The task is `done`; nothing re-runs it. |
| PostgreSQL unavailable | No transition happens; claim errors (and, with `cron`, materialize errors) set `Degraded`; heartbeat and maintenance failures do not affect health, and a worker with no free handler slot in any queue runs no claim cycle so it keeps its last status. Workers retry without inventing local state. |
| Graceful shutdown with handlers still running | Shutdown cancels every `ctx.cancellation` and drains for up to `shutdown_timeout`. A handler that returns `TaskError::interrupted()`, or is still running at the timeout and is dropped, has its task released: back to `queued`, due at once, the attempt not counted. A release not finished within 5 s more (the database is unreachable, or a dropped DB-contained transaction still holds the row lock) is aborted; the reaper recovers that task after the lease, and the attempt counts. A kept-alive pool can leave a dropped transaction's connection holding that lock, so exit the process after shutdown. |

## Validation Status

The integration suite covers every §27 requirement. See the table in
[Scheduler Design](SchedulerDesign.md#testing-contract-27). It also covers:
heartbeats while a handler holds the row lock, replay followed by a reclaim,
notification and next-due wakeups, shutdown without detached work, and shutdown
release: an interrupted handler, a handler still running at `shutdown_timeout`,
and a release blocked behind a dropped transaction's row lock.

Not yet automated:

- process-kill fault injection at random points around claim, effect,
  completion, and heartbeat;
- database outages longer than a lease;
- pool exhaustion;
- throughput and latency at production scale.

Run these with the host's PostgreSQL version, pool sizing, payload sizes, and
retention policy before relying on a capacity figure.
