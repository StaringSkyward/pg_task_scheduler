# Concurrency Model

## Summary

The queue is designed to run multiple scheduler instances and multiple handlers
per instance against the same PostgreSQL database. It does not require a leader
or an external coordinator. PostgreSQL row locks choose task ownership, and a
per-attempt lease token fences workers that no longer own a task.

The execution guarantee is leased at-least-once, not exactly-once. Queue state
remains consistent under concurrency, but an expired worker and its replacement
can briefly execute the same task at the same time. Handlers must therefore make
external side effects idempotent.

## Claiming Work

`store::claim_batch` claims tasks with one writable CTE:

1. Select eligible pending rows in priority and availability order.
2. Lock them with `FOR UPDATE SKIP LOCKED`.
3. Change them to `running`, increment their attempt counts, and assign fresh
   lease tokens.
4. Insert their attempt-history rows.
5. Return complete claims to the worker.

The statement is atomic. If the task update or attempt insertion fails, none of
the claims commit. Concurrent workers skip rows already locked by another
claiming transaction, so a pending task cannot be successfully claimed by two
workers at once.

Each scheduler claims no more than its currently available Tokio semaphore
permits. `max_concurrency` is a per-instance limit, with a default of 16. For
example, ten instances configured with 16 permits can run up to 160 handlers,
subject to database, connection-pool, and application capacity.

Workers only claim task names for which they have registered handlers. Different
worker groups can consequently consume different task types from the same queue.

## Fencing and State Transitions

Completion, failure, and lease renewal update the task only when all of these
facts still hold:

```sql
id = $task_id
AND state = 'running'
AND lease_token = $lease_token
AND lease_expires_at > clock_timestamp()
```

Cancellation and recovery also lock and update the same `scheduler_runs` row.
PostgreSQL serializes competing transitions on that row. If an update waits for
another transaction, PostgreSQL rechecks its predicate after acquiring the lock.
A token rotated or cleared by the winning transaction therefore causes the stale
operation to affect no rows.

This resolves the main races as follows:

| Race | Result |
| --- | --- |
| Two workers claim one pending task | One locks and claims it; the other skips it. |
| Completion races with cancellation | One transition wins the row lock; the other observes terminal or fenced state. |
| Renewal races with expired-lease recovery | A valid renewal extends the lease, or recovery closes the attempt and fences the renewal. |
| Completion races with recovery | Completion succeeds only with the current unexpired token; otherwise recovery wins. |
| Two recovery processes select one expired task | One locks it; the other skips it. |
| An old attempt completes after a new claim | Its old token no longer matches, so finalization is fenced. |

`worker_id` is useful for attribution but is not the security boundary. Reusing a
worker id does not allow stale finalization because lease tokens, rather than
worker ids, prove ownership. Unique worker ids are still recommended for useful
logs and attempt history.

## Leases and Heartbeats

A running handler does not hold a database connection. The runtime acquires a
connection briefly to renew its lease every third of the lease duration and
again to record completion or failure. Successful renewal extends both the task
lease and its current attempt in one transaction.

If renewal is fenced, or the locally observed deadline passes after renewal
errors, the runtime cancels the handler's `CancellationToken` and drops the
handler future. Cancellation is cooperative:

- async code stops when its future is dropped or it observes the token;
- synchronous blocking work cannot be preempted while it is running;
- work spawned independently by a handler is not owned by the scheduler;
- an external request or side effect already issued cannot be undone.

If a worker process crashes, its task remains `running` until the lease expires.
Any scheduler instance may then recover it. Recovery closes the attempt as
expired and either returns the task to `pending` or marks it `failed` when its
attempt budget is exhausted.

## At-Least-Once Overlap

Fencing protects queue state, not arbitrary external systems. The following is a
valid execution:

1. Worker A starts a task and becomes unable to renew its lease.
2. The lease expires and another scheduler recovers the task.
3. Worker B claims and starts a new attempt.
4. Worker A has not yet observed lease loss and continues briefly.

Worker A cannot complete the queue row, but both handlers may have performed
external work. A handler may also finish an external side effect immediately
after its lease expires; its finalization will be fenced and the task may run
again.

Use the stable `JobContext::run_id` as the idempotency key across attempts. For
database effects, prefer a unique application-side idempotency record in the
same transaction as the effect. For remote services, pass the run id as the
remote idempotency key when supported. A random lease token identifies a single
attempt and should not replace the stable run id for task-level idempotency.

## Concurrent Producers

Recurring materialization is also safe to run on every scheduler instance. A
materializer locks a due schedule with `FOR UPDATE SKIP LOCKED`, inserts an
immutable task snapshot, and advances the schedule cursor in the same
transaction. The partial unique index on `(job_id, scheduled_for)` is a second
line of defense against duplicate occurrences.

Immediate and delayed producers may enqueue concurrently. When a deduplication
key is supplied, the partial unique index on `(job_name, deduplication_key)`
returns the existing task id to competing producers. That key remains occupied
while the task row exists, including after it becomes terminal, and becomes
available again only after retention deletes the row.

## Ordering and Fairness

Eligible tasks are considered by priority descending, then availability and id.
This is deterministic within a claim query, but it is not strict global
execution order. `SKIP LOCKED` deliberately allows a worker to take a lower
ranked row while a higher ranked row is locked or already executing. Handler
duration and process scheduling can also change completion order.

Use priority as a preference, not as a serialization mechanism. Tasks requiring
strict ordering or mutual exclusion need a separate domain-level mechanism.

## Capacity and Operations

Multiple workers scale the claim path without a central mutex, but PostgreSQL is
still the coordination point. Important operational limits include:

- **Connection pool:** handlers release connections while running, but claims,
  heartbeats, recovery, materialization, and finalization all need short-lived
  connections. Leave headroom beyond application traffic and account for many
  handlers renewing at similar times.
- **Lease duration:** choose a duration comfortably longer than normal pool and
  database latency. An outage lasting longer than the lease can cause another
  worker to start the same task.
- **Database maintenance:** the hot queue row is updated on every claim,
  heartbeat, retry, and terminal transition. Monitor dead tuples, autovacuum,
  index growth, and transaction age.
- **Retry storms:** retry backoff currently has no jitter. Large correlated
  failures can make many tasks eligible together and should be controlled by
  task-specific backoff and worker concurrency.
- **Polling:** polling is the correctness mechanism. Notifications emitted by
  enqueue are not required for correctness, and the current runtime's wake-up
  latency is bounded by `poll_interval`.
- **Backlogs:** materialization and recovery use bounded batches so one cycle
  cannot monopolize the database. Sustained input above processing capacity will
  still grow the pending table.

The partial pending, running-expiry, and terminal-retention indexes keep common
worker queries away from unrelated states. Retention should periodically remove
terminal history according to the application's audit requirements.

## Failure Semantics

PostgreSQL statement atomicity prevents partially committed claims and state
transitions. A lost client connection can make the result ambiguous to that
client, but it does not corrupt queue state:

- if a claim committed but its response was lost, the task remains leased and is
  recovered after expiry;
- if finalization committed but its response was lost, the terminal row prevents
  another claim;
- if a handler panics or is aborted, its unclosed attempt remains recoverable
  after lease expiry.

Graceful shutdown drains handlers until `shutdown_timeout` and then aborts the
remaining handler futures. Their leases are left for normal recovery.

## Validation Status

The integration suite currently covers:

- concurrent workers claiming disjoint tasks;
- a stale finalizer blocked behind token rotation;
- concurrent recovery of the same expired task;
- token-guarded renewal and cancellation;
- short-lease execution with heartbeat renewal;
- retry exhaustion and terminal retention;
- producer deduplication and transactional enqueue rollback;
- partial-index use for the pending claim query.

Before relying on a particular production capacity, add workload-specific tests
for many workers and large task sets. Fault tests should cover process death,
database outages longer than a lease, pool exhaustion, renewal-versus-recovery
races, slow or blocking handlers, and retry storms. Throughput and latency should
be measured with the same PostgreSQL version, pool sizing, payload distribution,
and retention policy used by the host application.
