# Review Decisions and Expected Behaviour

This records what was decided about each finding of the two whole-repository
reviews made before the first release, and what each decision looks like from
outside. The reviews themselves are kept out of the repository, so each entry
restates its finding.

- **Review 01** was run at `ee353a0` and triaged against `cade0a5`. Entries
  1–16 follow its findings in order; entries 17–20 cut across them.
- **Review 02** was run at `8cc77e3`, the head of PR #9. Entries 21–32 follow
  its findings 1–12 in order; entry 33 cuts across them.

It describes the code as shipped. Each entry has a source: **(user)** means the
user decided it; **(call)** means it was a judgement call, open to change.
**Ruling** marks a call made during implementation, recorded in the entry it
affects. Where review 02 changed a review 01 entry, that entry ends with a
**Changed by review 02** line that says what is different now and links to the
review 02 entry.

**Existing databases:** the migrations were edited in place, in both reviews
(entries 17 and 33). A database built by an earlier version of this repository
must be dropped and recreated.

## Review 01: findings

### 1. A stored schedule that cannot run pauses only itself

- **Finding:** `0 0 31 2 *` parses but never occurs. The materializer's error
  for the missing next occurrence rolled back without pausing the job and ended
  the pass, so one such row, stored by direct SQL, stopped every later schedule
  on every pass.
- **Source:** call.
- **Decision:** Before it writes anything, the materializer parses every
  job-local column of a due schedule: the cron expression, its next occurrence,
  the task name, the queue and `max_attempts`. Any failure pauses that job and
  is logged, and the pass continues. Only database errors end a pass.
- **Why:** One bad row must not block the other schedules. Each of these values
  is local to its job, and only direct SQL can make one invalid.
- **Expected behaviour:**
  - An unsatisfiable oldest job gets a `scheduler_job_pauses` row, and a later
    valid job materializes in the same pass
    (`materialize::unsatisfiable_cron_pauses_its_job_and_later_jobs_still_materialize`).
  - A due job with an invalid stored queue is paused, and the pass continues
    (`materialize::invalid_stored_queue_pauses_its_job_and_the_pass_continues`).
    Before, the bad queue was copied into a task row that then failed to load.
    An invalid task name or `max_attempts` is handled the same way; neither was
    checked here before.
  - The log is an error, `unschedulable job; pausing it`, with the job id and
    the defect (for example `queue: identifier must not be empty`).
- **Changed by review 02:** the materializer no longer reads `job_args` (the
  insert copies them in SQL), and a CHECK keeps `next_run_at` finite, so neither
  can end a pass
  ([entry 23](#23-the-materializer-decodes-nothing-a-stored-value-can-break)).

### 2. The schedule key prefix is reserved

- **Finding:** A producer can enqueue a task under a schedule occurrence's key.
  The materializer's `ON CONFLICT DO NOTHING` then inserts nothing and still
  advances the schedule, so the occurrence is lost without an error.
- **Source:** call (verdict: valid but overstated).
- **Decision:** No code change. The README documents `schedule:` as a reserved
  task-key prefix.
- **Why:** A collision needs a forged `schedule:<job uuid>:<timestamp to the
  microsecond>` key: another schedule's id and the exact time of one of its
  occurrences. No producer writes that by accident.
- **Expected behaviour:** The README's Recurring Schedules section says: "Task
  keys starting with `schedule:` are reserved for schedule occurrences; a
  producer that uses one can collide with an occurrence, which is then silently
  skipped." The materializer behaves as before.
- **Changed by review 02:** superseded. `enqueue` rejects the prefix with
  `SchedulerError::ReservedTaskKey`, and the README sentence quoted above was
  replaced ([entry 26](#26-enqueue-rejects-the-reserved-schedule-prefix)).

### 3. Retention is the idempotency horizon

- **Finding:** `store::cleanup` took any `Retention`, while only
  `SchedulerBuilder::build` checked that terminal retention was at least
  `Retention::idempotency_horizon`. A direct call with `done: Duration::ZERO`
  freed keys inside the advertised horizon.
- **Source:** call.
- **Decision:** `Retention::idempotency_horizon` is deleted. A `task_key` stays
  reserved for as long as its row exists: `Retention::done` for `done` tasks
  and `Retention::dead` for `dead` and `cancelled` ones.
- **Why:** With one period per terminal status there is nothing for `build`
  and `cleanup` to disagree on. Deleting the second setting fixes the gap
  without adding a check to keep in step.
- **Expected behaviour:**
  - The defaults are 30 days for both, so a key is reserved for 30 days after
    its task finishes.
  - `cleanup(&Retention { done: Duration::ZERO, .. })` deletes finished `done`
    tasks at once and frees their keys. That is now the stated horizon, not a
    bypass.
  - `build()` no longer has a horizon rule; it still rejects any duration over
    5 years.

### 4. Identifiers are checked when read from the database

- **Finding:** `TaskName`, `TaskKey`, `QueueName` and `WorkerId` validated in
  their constructors, but their Diesel decoding wrapped any database string, so
  a row written by direct SQL with `queue = ''` loaded as `QueueName("")`.
- **Source:** call. It reverses the earlier decision, in `ids.rs`, to trust
  values read back from the database.
- **Decision:** Every validated string newtype decodes through
  `TryFrom<String>`, using `#[diesel_newtype(try_from)]` from
  `diesel-derive-newtype` 2.1.3. That covers `TaskName`, `TaskKey`,
  `QueueName`, `WorkerId`, `ScheduleName`, `EffectName`, `Topic` and
  `CommandKey`. The materializer reads its columns as plain strings and parses
  them itself (entry 1), and so does the claim for `task_key`.
- **Why:** A value of a validated type must satisfy its rule wherever it came
  from.
- **Expected behaviour:** Reading a row with an invalid identifier is
  `SchedulerError::Database`, with a message naming the rule
  (`identifier_checks::decode_error_names_the_identifier_rule`). A read that
  includes such a row fails as a whole: an admin list containing it shows an
  error, and other lists keep working. `TaskId`, `LockToken` and `JobId` have
  no rule and decode as before. The claim is the exception: it commits in one
  statement before decoding, so it reads `task_key` as text and checks each
  row itself. A claimed row whose key breaks the rule (`queue_tasks` has no
  identifier CHECK, so a raw-SQL producer can write one) is logged at error
  level and marked `dead` through `fail_permanently`, with its attempt closed
  and `last_error` `invalid task_key: <rule>`. The rest of the batch is
  returned (`claim::claim_dead_letters_a_row_with_an_invalid_task_key_and_returns_the_rest`).
  `task_name` and `queue` cannot be invalid there, because the claim matches
  them against valid values.
- **Changed by review 02:** the claim also parses the payload, the attempt
  number and `max_attempts` per row, dead-letters by the ownership key instead
  of through `fail_permanently`, and logs and skips a row it cannot dead-letter
  instead of failing the call
  ([entry 22](#22-claim-dead-letters-any-row-that-fails-conversion)).

### 5. The schema is in targeted sixth normal form

- **Finding:** The schema was intentionally wide, with nested relations: a
  worker's queues in a JSONB column and its task names in an array, with no keys
  against repeats. The repository did not show these were needed.
- **Source:** user for the policy and the outbox exception; call for the other
  exceptions and the new CHECKs.
- **Decision:** A nullable fact, or a nested collection, that varies
  independently of its entity moves to a relation of its own. The NOT NULL
  attributes of one entity stay together.
  - `scheduler_jobs.is_paused` became `scheduler_job_pauses (job_id, paused_at)`,
    which cascades from its job, like `queue_pauses`.
  - `queue_workers.queues` became `queue_worker_queues (worker_id, queue,
    slot_limit, in_use, last_finished_at)`, keyed `(worker_id, queue)`.
  - `queue_workers.task_names` became `queue_worker_task_names (worker_id,
    task_name)`, keyed by both.
  - The registry no longer copies each queue's `paused` flag; `store::workers`
    reads it from `queue_pauses` when it lists the workers.
  - New CHECKs: on `queue_task_attempts`, `attempt_no >= 1` and
    `(finished_at IS NULL) = (outcome IS NULL)`; on `queue_admin_actions`, one
    shape per action (see the
    [design](SchedulerDesign.md#queue_admin_actions)).

  The documented exceptions are `queue_tasks`, `queue_outbox.published_at`
  (user), the attempt close columns, `queue_admin_actions`, and the registry's
  display-only nullable columns. Each reason is in
  [Normal Form](SchedulerDesign.md#normal-form).
- **Why:** The user asked for a 6NF schema. The exceptions are kept only where a
  state-shape CHECK, an action shape or a hot query needs the columns in one
  row.
- **Expected behaviour:**
  - **Schedule pause:** `jobs::pause` adds a pause row and `jobs::resume`
    deletes it. `ensure_job` adds one only when it creates a schedule from a
    `Paused` spec, and otherwise never touches it, so a redeploy keeps a
    schedule paused
    (`jobs::created_paused_job_is_paused_and_ensure_keeps_it_paused`).
    Deleting a schedule deletes its pause, so one recreated under the same
    name starts active (`jobs::recreated_schedule_is_not_paused`).
  - **`updated_at`:** pausing or resuming a schedule, and the materializer's
    own pause, no longer change `Job.updated_at`.
  - **Locking:** `jobs::pause` and `jobs::resume` lock the job row
    `FOR KEY SHARE`, so it cannot be deleted before the pause row references
    it. They wait for an in-flight materialization of that job, and the
    materializer (`FOR UPDATE SKIP LOCKED`) skips a job while one of them
    holds the lock. That lock alone does not close the race: `SELECT_DUE`'s
    pause anti-join reads the statement's snapshot, and a pause does not
    change the job row, so a pause that commits after that snapshot but
    before the materializer locks the row would not be re-checked, and one
    more occurrence would be inserted. So once it holds the row lock, the
    materializer re-reads `scheduler_job_pauses` in a new statement and
    skips the job if a pause is there. A pause that comes later blocks on
    that lock until the materializer commits.
  - **Admin role:** PostgreSQL requires UPDATE privilege for `FOR KEY SHARE`,
    so the admin role keeps `UPDATE` on `scheduler_jobs` and needs `SELECT`,
    `INSERT` and `DELETE` on `scheduler_job_pauses` (`admin/README.md`).
  - **Registry write:** one transaction of five statements per worker every
    `registry_interval`, not one upsert: upsert the worker row, delete the
    queues it no longer reports, upsert its queues, delete the task names it no
    longer has, insert the rest. A worker that re-registers with fewer queues
    loses the old rows
    (`registry::reregistering_with_fewer_queues_drops_the_old_ones`). Ruling:
    that test reuses one registration, because a fresh worker id each time
    would not exercise the delete.
  - **Registry reads:** a worker's queues are listed by queue name, not in
    configuration order. A pause shows on every serving worker as soon as it
    commits (`registry::paused_is_read_when_listed`). Removing a worker removes
    its queue and task-name rows (`registry::removing_a_worker_removes_its_queues_and_names`).
  - **CHECKs:** an attempt numbered 0, an open attempt with an outcome, and an
    admin action of the wrong shape are rejected
    (`schema_checks::attempt_number_starts_at_one`,
    `schema_checks::attempt_cannot_close_without_an_outcome`,
    `schema_checks::admin_action_shape_is_checked`).
- **Changed by review 02:** the policy is named 5NF, with optional and
  multi-valued facts in their own relations, not "targeted sixth normal form",
  and the approved exceptions now include the `scheduler_jobs` cursor and every
  observed registry value
  ([entry 24](#24-the-schema-is-in-5nf-with-approved-6nf-exceptions)). `due_at`
  and `next_run_at` have `isfinite` CHECKs (entries 22 and 23). The materializer
  enforces the READ COMMITTED its pause re-check needs
  ([entry 25](#25-the-materializer-runs-at-read-committed)), and
  `store::workers` reads the registry in one statement
  ([entry 31](#31-the-worker-registry-is-read-in-one-statement)).

### 6. A conflicting outbox effect is an error

- **Finding:** `complete_with_outbox` returned `Ok(false)` when the task's
  effect already existed, even with a different command key, topic or payload,
  so a completion could commit next to an older, incompatible command.
- **Source:** call. The review's trigger, cancel and replay, cannot happen,
  because a completed task is `done` and neither cancellable nor replayable.
  The real one is a `task_key` reused after cleanup while its outbox row, still
  unpublished or within its own retention, survives.
- **Decision:** `complete_with_outbox` returns `OutboxWrite::Inserted` or
  `OutboxWrite::AlreadyPresent`. An existing `(task_key, effect_name)` row with
  a different `command_key`, `topic` or `payload` is
  `SchedulerError::OutboxConflict`, and the completion rolls back.
- **Why:** A duplicate must mean the same command; anything else needs an error
  channel.
- **Expected behaviour:**
  - The same effect with the same content is `AlreadyPresent`
    (`transitions::same_effect_and_content_is_already_present`); payloads are
    compared as JSONB, so key order and whitespace do not matter
    (`transitions::outbox_payload_key_order_is_not_a_conflict`).
  - Different content is `OutboxConflict`, and the task stays `running` with
    its attempt open
    (`transitions::same_effect_with_different_content_conflicts_and_rolls_back`).
    A handler that propagates it with `?` fails the task permanently.
  - A `command_key` already used by another task still fails on its unique
    constraint (`transitions::command_key_collision_across_tasks_fails`).
  - The example app ignores the `OutboxWrite` value, and turns any error,
    including a conflict, into a retry.
- **Changed by review 02:** the example app fails the task permanently on an
  `OutboxConflict`
  ([entry 32](#32-ci-checks-the-minimum-rust-and-postgresql-18)).

### 7. A schedule has its own name and a typed constructor

- **Finding:** `CreateJob::new` took a `TaskName` and any `Serialize` value, so
  a schedule could pair a handler with the wrong argument type, and every
  occurrence then died at deserialization. The schedule's unique name was also
  its task name, so one handler could have only one schedule.
- **Source:** user (a separate schedule identity); call (the typed
  constructor).
- **Decision:**
  - `scheduler_jobs.name` is the schedule's own unique name (`ScheduleName`),
    and the new `scheduler_jobs.task_name` selects the handler.
  - `CreateJob::for_task::<T>(name, cron, max_attempts, lifecycle, args:
    T::Args)` takes `T::NAME` and `T::QUEUE`.
  - `CreateJob::new(name, task_name, …)` stays as the untyped boundary, for a
    task whose type is not at hand, with the `default` queue.
  - `.queue(..)` overrides the queue for either.
- **Why:** The schedule producer gets the same link between task and arguments
  as `enqueue::<T>`, and several schedules can run one handler.
- **Expected behaviour:**
  - Two schedules can run one task (`jobs::two_schedules_can_run_one_task`).
    An occurrence runs the task name, not the schedule name
    (`materialize::occurrence_runs_the_task_name_not_the_schedule_name`).
  - `for_task` takes the name and queue from the task
    (`jobs::for_task_takes_name_and_queue_from_the_task`).
  - `ensure_job` also reconciles `task_name` on conflict.
  - `GET /api/jobs` rows gain `task_name` (entry 18).
  - The example app builds its `digest` schedule with `for_task::<Digest>`.
- **Changed by review 02:** `ensure_job` inserts again when a concurrent delete
  removes the schedule it is reconciling
  ([entry 30](#30-ensure_job-survives-a-concurrent-delete)).

### 8. Public views are sums of the states the database allows

- **Finding:** `TaskView` and `TaskRow` carried a status beside optional owner,
  expiry and completion fields; `Attempt` optionalized its finish time and
  outcome; `AdminAction` combined a tag with optional targets. Shapes the CHECKs
  forbid were representable, and browse cursors filled missing fields with
  year-1 or year-9999 timestamps.
- **Source:** call.
- **Decision:**
  - `TaskState` is `Queued`, `Running { locked_by, locked_until }` or
    `Finished { status: TerminalStatus, completed_at }`. It replaces the status
    and three optional fields on `TaskView` and `TaskRow`, and
    `TaskState::status()` gives the old `TaskStatus`.
  - `AttemptState` is `Open` or `Closed { finished_at, outcome }`, on `Attempt`
    and in `TaskRow.attempt: Option<AttemptRun>`.
  - `max_attempts` is `MaxAttempts` on `TaskView`, `TaskRow`, `ClaimedTask` and
    `TaskContext`.
  - Browse cursors are built fallibly.
  - `AdminAction` carries an `AdminActionTarget` (`Task`, `Bulk` or `Queue`),
    and `AdminAction::kind()` is derived from it.
- **Why:** A consumer should not have to rediscover the state-shape rules, and
  a corrupt row should be an error, not a plausible timestamp.
- **Expected behaviour:**
  - Every shape the CHECKs allow decodes, and every other shape, or an unknown
    status or outcome, is `SchedulerError::Invariant`
    (`models::state_tests::task_state_accepts_each_checked_shape`,
    `models::state_tests::task_state_rejects_shapes_the_checks_forbid`,
    `models::state_tests::attempt_state_pairs_finish_and_outcome`).
  - A running or finished page given a row in another state returns
    `Invariant` instead of a substitute timestamp
    (`browse::tests::cursor_builders_reject_a_row_in_the_wrong_state`). The
    year-1 and year-9999 bounds remain only as the first page's starting point.
  - `TaskRow.attempt` is `None` when no attempt carries the task's current
    `lock_token`: never claimed, or replayed since. Ruling: this doc fix and the
    cursor test were folded into the work on entry 10, which touched the same
    code.
  - An admin action row whose columns do not fit its action is `Invariant`
    (`admin action row violates its shape`). The table's CHECK makes that
    unreachable for rows written since.
  - `claim` reads `max_attempts` through `MaxAttempts`; a stored value below 1,
    which the CHECK forbids, would be `Invariant`.
  - The admin JSON and pages show the same values as before (entry 18).
- **Changed by review 02:** `TaskLease.attempt_no` and `Attempt.attempt_no` are
  `AttemptNo`, and the attempt counts on `TaskView` and `TaskRow` are `u32`
  ([entry 29](#29-store-inputs-have-checked-types)). A claimed row whose
  `max_attempts` is below 1 is dead-lettered, not `Invariant`
  ([entry 22](#22-claim-dead-letters-any-row-that-fails-conversion)).

### 9. Intervals must be at least one microsecond

- **Finding:** Durations were checked at nanosecond resolution, then truncated
  to PostgreSQL's microseconds. A 3 ns lease passed `build()` and was stored as
  zero, so every completion lost ownership.
- **Source:** call.
- **Decision:** `build()` rejects a lease, heartbeat, poll, reaper or registry
  interval under 1 µs, the resolution PostgreSQL stores.
- **Why:** One rule at the builder covers the case the review demonstrated.
- **Expected behaviour:** A 3 ns lease fails to build with `lease_duration
  must be at least 1 microsecond`, and a 999 ns poll interval with `heartbeat,
  poll, reaper and registry intervals must be at least 1 microsecond`; 1 µs
  builds (`builder::tests::build_rejects_intervals_below_one_microsecond`).
  Retry delays, jitter, the crash-retry delay, the shutdown timeout and
  retention may still be zero. `store::claim` and `store::heartbeat` take a
  `LeaseDuration`, with the same bounds as the builder's lease. Store functions
  that take a delay as a `Duration`, such as `store::retry`, truncate it to
  microseconds, and one too large for a PostgreSQL interval is
  `SchedulerError::Config`.
- **Changed by review 02:** the store functions took any `Duration`, and
  `pg_interval` saturated a huge one. The lease is now a `LeaseDuration`,
  checked before the other settings, with its own messages; the shared message
  named the lease too ([entry 29](#29-store-inputs-have-checked-types)).

### 10. Limits and counts have types that fit their bounds

- **Finding:** A queue limit of `usize::MAX` built and then panicked when the
  semaphore was made. Page limits and count caps were `i64`, so
  `running_page(..., i64::MAX)` overflowed `limit + 1`, negative values reached
  SQL, and a deserialized cursor position could overflow.
- **Source:** call.
- **Decision:**
  - A queue limit above `tokio::sync::Semaphore::MAX_PERMITS` is a `Config`
    error at `build()`.
  - Page limits and count caps are `NonZeroU32`: the store's page functions,
    `admin_actions`, `count_cancellable`, `count_replayable`, the four
    `OverviewParams` caps, `AdminConfig.page_size`, and the standalone server's
    `count_cap`.
  - Cursor position arithmetic is checked, and an overflow is
    `SchedulerError::InvalidCursor`.
- **Why:** A function that returns `Result` should never panic on, or
  reinterpret, a value it accepted.
- **Expected behaviour:**
  - `MAX_PERMITS` builds and `MAX_PERMITS + 1` fails
    (`builder::tests::build_rejects_queue_limit_above_semaphore_max`).
  - The largest page limit works (`browse::largest_page_limit_does_not_overflow`).
  - A queued cursor at position `i64::MAX` is `InvalidCursor`
    (`browse::queued_cursor_at_max_position_is_invalid`).
  - The admin Queued list answers a forged cursor whose position would overflow
    with 400 `invalid cursor`.
  - `count_cap = 0` in the standalone TOML fails at load
    (`config::tests::rejects_zero_count_cap`).
- **Changed by review 02:** batch sizes are `NonZeroU32` too: `store::claim`,
  `store::reap_expired`, `Retention.batch_size`, and the builder's
  `claim_batch_size` and `reaper_batch_size`
  ([entry 29](#29-store-inputs-have-checked-types)).

### 11. Retry backoff doubles until it reaches the cap

- **Finding:** `retry_delay` clamped the exponent at 31, so with a 1 ns base and
  a 300 s cap, attempts from 33 on stayed at about 2.1 s.
- **Source:** call.
- **Decision:** The delay is exactly `min(base · 2^(n−1), max)`, plus jitter,
  for every attempt `n`. It is computed in nanoseconds as a `u128`, with the
  doubling count clamped to 127 and a saturating multiply.
- **Why:** The documented rule should hold for every accepted setting. Ruling:
  saturating arithmetic is allowed here despite the rule against silent
  defaults, because 2^127 ns is far above the 5-year limit on `max`, so neither
  the clamp nor the saturation can change the result.
- **Expected behaviour:** With a 1 ns base, attempt 34 waits 2^33 ns, and
  attempt 40 and `i32::MAX` wait exactly `max`
  (`worker::tests::retry_delay_keeps_doubling_past_attempt_32`). Attempt numbers
  below 1 count as attempt 1 (entry 19).
- **Changed by review 02:** `retry_delay` takes an `AttemptNo`, so no attempt
  number below 1 reaches it ([entry 29](#29-store-inputs-have-checked-types)).

### 12. A bulk result says why it stopped

- **Finding:** Bulk actions stop at a short `SKIP LOCKED` batch and then reported
  `more_remain = false`, though locked matching rows could remain; `true` after
  an exactly full last batch did not prove more existed either. The page hid
  its Continue button on `false`.
- **Source:** call.
- **Decision:** `BulkOutcome.stopped` is `BulkStop::BudgetSpent` (a full batch
  ended after the budget) or `BulkStop::ShortBatch` (a batch came back short:
  the matches ran out, or locked rows were skipped). It replaces `more_remain`.
  The admin result page offers a re-run after either.
- **Why:** The outcome should state what the call observed, not a conclusion it
  cannot draw.
- **Expected behaviour:**
  - With one matching row locked elsewhere, bulk cancel changes nothing and
    returns `ShortBatch` (`bulk::bulk_cancel_that_skips_a_locked_row_reports_short_batch`).
  - The result page always has a form that re-posts the same name, queue, error
    filter, typed name and reason as a new action
    (`actions::bulk_cancel_runs_and_offers_rerun`). After `BudgetSpent` its
    button is Continue, with `More tasks may match.`
    (`actions::tests::budget_spent_result_offers_continue`). After `ShortBatch`
    it is Run again, with `Finished, or some rows were locked by other work and
    skipped; run again to catch them.`
  - Ruling: that note was softened from "Rows locked by other work were
    skipped", because a short batch is also how a normal run finishes, and the
    page must not claim more than the call knows.

### 13. The design docs are self-contained

- **Finding:** `SchedulerDesign.md` called a specification normative that is not
  in the repository, and source comments cited its section numbers (`§18`,
  `§10.2`), which no reader could resolve.
- **Source:** user.
- **Decision:** `SchedulerDesign.md` is the design, with no outside
  specification: the invariants and rationale it relied on are written into it,
  and a "Design choices" table replaces the list of deviations. Every `§`
  reference in the code either describes its rule in place or names a
  `docs/AdminUI.md` entry. This record keeps the review's history apart from
  the design.
- **Why:** The crate's most important correctness claims must be checkable from
  the repository.
- **Expected behaviour:** `grep -rn "§" src admin/src` finds only
  `docs/AdminUI.md §N.M` references. No document in the repository links to a
  file outside it. No automated link check was added.

### 14. Manifest metadata needed no change

- **Finding:** The manifests lacked description, license, repository and
  `rust-version`.
- **Source:** call.
- **Decision:** No change: PR #8 had already added them (see
  [Publishing](Publishing.md)). A CI job on the minimum Rust version stays
  optional.
- **Expected behaviour:** Both crates declare `license = "MIT"`, a description,
  the repository and `rust-version = "1.88"`. CI builds and tests on the
  toolchain pinned in `rust-toolchain.toml` (1.100.0), and uses nightly only
  for the format check. Its `msrv` job checks the workspace, with every
  feature, on Rust 1.88
  ([Publishing, entry 10](Publishing.md#10-minimum-rust-version-is-188)).
- **Changed by review 02:** CI had no job on Rust 1.88, so a newer language
  feature could raise the real minimum unnoticed
  ([entry 32](#32-ci-checks-the-minimum-rust-and-postgresql-18)).

### 15. Example app integration tests are deferred

- **Finding:** The example's database guarantees (rollback of the first
  attempt, one order and one outbox row per task, publish after delivery) are
  checked by hand, not by tests.
- **Source:** call.
- **Decision:** Deferred until after 0.1.0.
- **Why:** The core crate's tests cover the primitives the example composes, and
  the example is not published.
- **Expected behaviour:** The example keeps its three pure unit tests; its
  walkthrough was checked from the command line
  ([Example app decisions, §6.3](ExampleApp.md#63-behavioural-checks-were-driven-from-the-command-line)).
- **Changed by review 02:** nothing; it stays deferred
  ([entry 32](#32-ci-checks-the-minimum-rust-and-postgresql-18)).

### 16. Outbox identities are typed and NUL is rejected

- **Finding:** `OutboxCommand` took plain strings for its effect, command key
  and topic, so empty or swapped values were accepted. Identifier validation
  accepted NUL, which PostgreSQL text cannot store, so the error came from the
  database instead.
- **Source:** call.
- **Decision:**
  - `EffectName` and `Topic` follow the identifier rule: 1 to 255 characters,
    no leading or trailing whitespace.
  - `CommandKey` is non-empty and at most 2048 bytes, a margin below the
    B-tree entry limit (about 2704 bytes) of the outbox's unique index; hash
    an over-long `(task_key, effect_name)` encoding.
  - Every identifier constructor rejects NUL (`IdentifierError::ContainsNul`).
- **Why:** These values identify and route a command, so they get the same
  boundary as the queue's own identifiers.
- **Expected behaviour:** `ids::identifier_tests::nul_is_rejected`,
  `ids::identifier_tests::effect_name_and_topic_follow_the_identifier_policy`
  and `ids::identifier_tests::command_key_allows_long_values_and_spaces`. The
  three types are exported from the crate root, and the README and the example
  app build commands with them.
- **Changed by review 02:** `CommandKey` had no length limit
  ([entry 28](#28-a-command-key-is-at-most-2048-bytes)).

## Review 01: cross-cutting decisions

### 17. The migrations were edited in place

- **Source:** user.
- **Decision:** `0001`–`0003` were rewritten into their final form, and no
  `0004` was added. `0001` creates `queue_tasks` and `queue_task_attempts` with
  the `cancelled` status, `admin_action_id` and the attempt CHECKs. `0003` no
  longer alters earlier tables: it creates its own tables and two partial
  indexes on `queue_tasks`. The old replay table, with `0003`'s copy and drop
  of it, is gone.
- **Why:** The first release should ship a schema with no upgrade history.
- **Expected behaviour:**
  - A database that ran the earlier migrations has different tables (for
    example `scheduler_jobs.is_paused` and the JSONB `queue_workers.queues`),
    and must be dropped and recreated. There is no upgrade path, and a
    migration runner that tracks applied versions will not re-run the edited
    files.
  - The example app needs `docker compose down -v` once.
  - The integration tests build a fresh schema for each test.
  - [Admin UI decisions §9](AdminUI.md#9-upgrade) and
    [Publishing, entry 12](Publishing.md#12-the-readme-describes-the-first-release-not-its-history)
    are marked accordingly.
- **Changed by review 02:** two CHECKs were added the same way
  ([entry 33](#33-the-migrations-were-edited-in-place-again)).

### 18. The admin JSON API keeps its wire fields

- **Source:** call.
- **Decision:** The JSON keeps `status`, `locked_by` and `completed_at` on
  tasks, and `is_paused` on jobs, derived from the new types. Jobs gain
  `task_name`.
- **Why:** A JSON client should not break because the Rust types changed shape.
- **Expected behaviour:** `GET /api/tasks/:id` returns the same fields as
  before; `max_attempts` is now serialized from a `u32`, which is the same JSON
  for every valid value. `GET /api/jobs` rows are `id`, `name`, `task_name`,
  `cron_expression` and `is_paused` (`api::list_jobs_returns_view`). Ruling: the
  names `is_paused` in `admin/src/api.rs` and `admin/tests/api.rs` are this
  wire field, kept on purpose.

### 19. Left out of scope

- **Source:** call.
- **Decision:** These stay as they are:
  - `TaskLease` keeps public fields. It is a fencing credential checked by
    PostgreSQL, and a private constructor would not replace that check.
  - Attempt numbers stay `i32`; a typed attempt number is deferred, so
    `retry_delay` counts an attempt number below 1 as 1.
  - `Count { value, capped }` is unchanged.
  - `OutboxCommand.command_key` stays caller-chosen.
- **Expected behaviour:** No change in these APIs.
- **Changed by review 02:** attempt numbers are now `AttemptNo`, so the second
  item no longer holds ([entry 29](#29-store-inputs-have-checked-types)). The
  other three are unchanged.

### 20. Every change is also linted with default features

- **Source:** call (Ruling).
- **Decision:** Each change was checked with
  `cargo clippy --workspace --all-targets -- -D warnings` (default features),
  as CI runs it, as well as with `just build::check`.
- **Why:** `just build::check` lints only with `--all-features`. A
  `#[cfg(feature = "cron")]` left on the wrong import broke the default build,
  and only the default-feature run caught it.
- **Expected behaviour:** Clippy passes with default features and with all
  features, and the library also builds with none
  (`cargo check --no-default-features --lib`).

## Review 02: findings

### 21. A transition checks the lease after it locks the row

- **Finding:** The ownership predicate compared `locked_until` with
  `clock_timestamp()` while the scan located the row. After a lock wait,
  PostgreSQL re-evaluates a statement's `WHERE` only when the blocker updated
  the row, so a transition that waited behind a transaction that only locked
  the row went ahead with no fresh expiry check: `complete` returned `Ok(())`
  after the lease had expired. The README also said a late transaction rolls
  back, which a caller that commits after `complete` can defeat.
- **Source:** user (fix both the SQL and the docs).
- **Decision:**
  - Heartbeat, `lock_owned`, `complete`, `complete_with_outbox`, `release`,
    `retry` and `fail_permanently` first lock the task row in a statement of
    their own, then run the ownership-guarded statement as a new statement in
    the same transaction. Under READ COMMITTED that statement takes a fresh
    snapshot and evaluates `clock_timestamp()` after the wait. Heartbeat is now
    a transaction.
  - Ruling: the lock statement matches the ownership identity,
    `SELECT id FROM queue_tasks WHERE id = $1 AND locked_by = $2 AND
    lock_token = $3 FOR UPDATE`, not the id alone as the spec said, and expiry
    stays in the second statement. A stale or fenced worker then fails at once
    instead of waiting on, or deadlocking with, the current owner's lock, and
    expiry is still checked after any wait. If wrong, revert to the id alone
    (one line).
  - The worker's lease keeper runs each heartbeat on a spawned task. When the
    handler finishes, or the local deadline passes, the lease keeper is
    dropped; a heartbeat dropped with it would leave its connection idle in a
    transaction, holding the row lock that the handler's completion then waits
    behind. The worker puts no timeout on its completion, retry and failure
    calls, so outside shutdown's final abort the heartbeat is the only
    transition it drops. A heartbeat task that panics is counted as a failed
    heartbeat.
  - The README and [Concurrency Model](Concurrency.md#ownership) say that the
    lease is checked when the transition runs, and that the row lock it takes
    keeps the reaper away until the caller's transaction ends. A caller that
    commits after `locked_until` still held exclusive ownership; the docs no
    longer say the late commit rolls back.
- **Why:** The hard-lease promise must hold when the check runs, after any
  wait. Exclusive ownership was never at risk, because the reaper skips locked
  rows; the expiry promise was.
- **Expected behaviour:**
  - A `complete` or heartbeat that starts before expiry, waits past
    `locked_until` on a row lock held by another transaction, and resumes when
    that transaction rolls back, returns `LostOwnership`
    (`transitions::complete_waiting_on_a_lock_past_expiry_loses_ownership`,
    `transitions::heartbeat_waiting_on_a_lock_past_expiry_loses_ownership`).
  - A worker whose `lock_token` was bumped by a replay or a reclaim gets
    `LostOwnership` without waiting on the new owner's lock.
  - After a DB-contained handler commits while a heartbeat waits on its lock,
    no connection is left idle in a transaction
    (`worker::db_contained_handler_survives_heartbeat_while_holding_row_lock`).
  - Each transition runs one more statement.

### 22. Claim dead-letters any row that fails conversion

- **Finding:** Claim commits its batch in one statement before Rust reads the
  rows. `payload` and `due_at` were decoded with the result, so a row
  PostgreSQL accepts but Rust cannot read (JSONB `{"x":1e500}`, or
  `due_at = '-infinity'`) failed the whole call after every row of the batch
  was `running`; the healthy rows then lost attempts to the reaper. The
  dead-letter path for an invalid `task_key` (entry 4) also used `?`, so an
  error there withheld the rest of the batch.
- **Source:** call.
- **Decision:**
  - `queue_tasks.due_at` has `CHECK (isfinite(due_at))`. A claimed row is due,
    so it is finite and at most `now()`, which chrono can represent.
  - Claim reads `payload` as text and parses it per row. A row whose attempt
    number, `task_key`, payload or `max_attempts` fails to convert is logged at
    error level (`claimed task is unusable; marking it dead`) and dead-lettered
    on its own: the task and its attempt become `dead`, and `last_error` names
    the field (for example `invalid payload: …`).
  - Dead-lettering is keyed by the ownership identity (task id, worker,
    `lock_token`) and takes the same lock-then-check path as
    `fail_permanently`. No `TaskLease` is built for a row that failed
    conversion.
  - An error while dead-lettering, such as lost ownership or a database error,
    is logged (`could not dead-letter the task; skipping it`) and that row is
    skipped. It never withholds the healthy rows.
- **Why:** The claim has already committed, so a defect in one row must stay in
  that row.
- **Expected behaviour:**
  - Inserting `due_at = '-infinity'` fails the CHECK
    (`schema_checks::infinite_due_at_is_rejected`).
  - From a batch of a `{"x":1e500}` row and a healthy one, claim returns the
    healthy one, and the other is `dead` with its attempt closed `dead`
    (`claim::claim_dead_letters_a_row_whose_payload_does_not_parse_and_returns_the_rest`).
    In a running worker the healthy task completes
    (`worker::healthy_task_in_a_batch_with_a_bad_payload_runs`).
  - A row whose `max_attempts` is below 1 is dead-lettered; it used to fail the
    claim with `Invariant` (entry 8). The `max_attempts` CHECK and the claim's
    `attempts + 1` make that branch and the attempt-number one unreachable
    through the schema; they guard the row anyway.
  - Unchanged: a task dead-lettered for its payload still fails to decode in
    `store::task`, the one view that reads the payload, as entry 4 accepts for
    reads.

### 23. The materializer decodes nothing a stored value can break

- **Finding:** The due-schedule query decoded `next_run_at` as a timestamp and
  `job_args` as JSON before the per-job quarantine of entry 1. An oldest
  schedule with `next_run_at = '-infinity'` failed every pass without being
  paused, so no later schedule ran.
- **Source:** call.
- **Decision:**
  - `scheduler_jobs.next_run_at` has `CHECK (isfinite(next_run_at))`. A due
    cursor is finite and at most `now()`, so it decodes.
  - The materializer no longer reads `job_args`. The occurrence's insert copies
    them from the schedule row in SQL
    (`INSERT … SELECT … FROM scheduler_jobs j WHERE j.id = $6`).
- **Why:** A value that direct SQL can make unreadable must not be decoded
  before the per-job boundary: a constraint rules it out, or the code never
  decodes it.
- **Expected behaviour:**
  - Inserting `next_run_at = '-infinity'` fails the CHECK
    (`schema_checks::infinite_next_run_at_is_rejected`).
  - A schedule whose `job_args` is `{"x":1e500}` materializes, its task's
    payload is the same JSONB, and the schedule is not paused
    (`materialize::job_args_that_serde_cannot_parse_are_copied_into_the_task`).

### 24. The schema is in 5NF, with approved 6NF exceptions

- **Finding:** The design called the schema "targeted sixth normal form", but
  its rule (give a nullable or nested fact that varies independently its own
  relation; keep an entity's NOT NULL attributes together) is narrower than
  6NF, whose test is the absence of nontrivial join dependencies.
  `scheduler_jobs`, for example, splits losslessly into its `next_run_at`
  cursor and the rest, and `queue_worker_queues.last_finished_at` is
  independent of the CHECK beside it.
- **Source:** user (option a: keep the schema, name the policy honestly, and
  approve each exception explicitly).
- **Decision:**
  - [Normal Form](SchedulerDesign.md#normal-form) no longer says "targeted
    sixth normal form". It names the policy, 5NF with optional facts and
    multi-valued facts in their own relations, and says plainly that this is
    not 6NF.
  - Each grouping that 6NF would split is an approved exception, with its
    reason in the design:
    - `queue_tasks` (its state-shape CHECKs);
    - `queue_task_attempts` (the CHECK that pairs finish and outcome);
    - `queue_outbox.published_at` (user; relay polling);
    - `queue_admin_actions` (an append-only audit with a shape CHECK);
    - the `scheduler_jobs` cursor and definition (`next_run_at` in the row with
      the definition);
    - the worker registry's observed values (`queue_workers.label`,
      `last_error`, `status`, `last_seen_at`, `lost_after`;
      `queue_worker_queues.in_use`, `last_finished_at`).
  - The section also names the two `isfinite` CHECKs (entries 22 and 23).
- **Why:** The schema keeps groups together where a constraint, a single-row
  write or a hot query needs them. Calling that 6NF overstated it. Naming the
  rule and approving each exception records the choice before the first schema
  is released.
- **Expected behaviour:** No schema change beyond the two CHECKs. Only this
  record still uses the old name, as history; entry 5 keeps its title so that
  links to it still work.

### 25. The materializer runs at READ COMMITTED

- **Finding:** The pause re-check of entry 5 closes the race with a concurrent
  pause only at READ COMMITTED. A pause does not change the job row, so locking
  it raises no serialization error, and under REPEATABLE READ the re-check reads
  the same old snapshot. The materializer used the session's default isolation,
  and could run inside a caller's transaction, so a pause committed before the
  lock could still get one more occurrence.
- **Source:** call.
- **Decision:**
  - `materialize_due_jobs` runs each job's transaction at READ COMMITTED,
    whatever the session's `default_transaction_isolation`.
  - On a connection already in a transaction, it reads `transaction_isolation`
    first. At any other level it returns `SchedulerError::Config`
    (`materialize_due_jobs needs READ COMMITTED; the caller's transaction is
    <level>`) and materializes nothing; at READ COMMITTED it runs each job in a
    savepoint.
  - Its doc says that diesel-async cannot see a transaction opened with raw SQL
    (a `BEGIN` sent through `batch_execute`), so it must not be called inside
    one. PostgreSQL then either switches that block to READ COMMITTED, if it
    has run no query yet, or rejects the materializer's `BEGIN`. The re-check
    never runs at REPEATABLE READ, but the materializer's commit or rollback
    ends the caller's block.
  - Ruling: the test of a REPEATABLE READ transaction opens it through
    diesel-async's tracked `begin_transaction_sql`, not the raw `BEGIN` the
    plan specified, which cannot produce the `Config` error. Both outcomes of a
    raw `BEGIN` are safe, so no probe for one was added. If wrong, a probe that
    sets a local setting and reads it back can be added later, at two more
    round trips per pass.
- **Why:** A guarantee that rests on an isolation level should establish that
  level, not inherit it.
- **Expected behaviour:**
  - With a session default of REPEATABLE READ, each job's transaction runs at
    READ COMMITTED
    (`materialize::materialize_runs_read_committed_under_a_repeatable_read_session`).
  - Inside a REPEATABLE READ transaction the call is a `Config` error
    (`materialize::materialize_inside_a_repeatable_read_transaction_is_a_config_error`);
    inside a READ COMMITTED one it materializes
    (`materialize::materialize_inside_a_read_committed_transaction_runs`).
  - The worker calls it on a pooled connection with no open transaction, so a
    role or database whose default is REPEATABLE READ now materializes at READ
    COMMITTED.
  - `SchedulerError::Config` is no longer only a builder error, and its doc
    says so.

### 26. Enqueue rejects the reserved schedule prefix

- **Finding:** Entry 2 reserved `schedule:` in prose only. `TaskKey` and
  `enqueue` accepted such keys, and the materializer treats any key conflict as
  an occurrence already created, even when its content differs.
- **Source:** call.
- **Decision:**
  - `enqueue` rejects a key starting with `schedule:` with
    `SchedulerError::ReservedTaskKey`, before any SQL. `TaskKey` still accepts
    the prefix, because occurrence keys and rows read back use it. One
    constant holds the prefix for both `enqueue` and the materializer.
  - The first materialization of an occurrence defines its content for as long
    as its task row is retained. This is documented, not compared.
- **Why:** The producer's boundary is where a reserved key should fail. An
  occurrence is identified by its schedule and slot and, like every task a
  schedule has already created, does not change when the schedule is edited.
- **Expected behaviour:**
  - `enqueue` with `schedule:imported-1` returns `ReservedTaskKey` and inserts
    nothing (`enqueue::enqueue_rejects_the_schedule_prefix`).
  - The README says so, and that re-pointing a schedule at a slot it already
    ran creates nothing new. A key written by direct SQL is not checked.
  - `SchedulerError` has a new variant.

### 27. Fencing is not deduplication

- **Finding:** The README offered fencing by `lock_token` as an alternative to
  deduplication for exactly-once external effects. A receiver that accepts only
  increasing tokens accepts worker A's call with token 1, then worker B's retry
  with token 2, so the effect happens twice.
- **Source:** call.
- **Decision:** The README's at-least-once guarantee and
  [Concurrency Model](Concurrency.md#external-effects) say that:
  - a non-idempotent external effect needs deduplication on the receiving
    side, keyed to the logical effect (`task_key`, which is stable across
    retries and replay, with the effect's name if a task has several);
  - fencing by `lock_token` only rejects stale writes from an earlier owner,
    and tokens are ordered only within one task row. A `task_key` can be reused
    once retention has deleted its row, and the new row's tokens start again,
    so a remote fence is scoped to the task id, not to the key.
- **Why:** The two mechanisms solve different problems, and only deduplication
  prevents a duplicate.
- **Expected behaviour:** Documentation only.

### 28. A command key is at most 2048 bytes

- **Finding:** `CommandKey` accepted any length, but `queue_outbox.command_key`
  has a unique B-tree index, whose entries PostgreSQL limits to about 2704
  bytes. A 9,600-byte key passed `CommandKey::try_from`, and
  `complete_with_outbox` then failed with an index-row size error.
- **Source:** call.
- **Decision:** `CommandKey` allows at most 2048 bytes, a margin below that
  limit, and rejects a longer key with `IdentifierError::CommandKeyTooLong`
  (`command key must be at most 2048 bytes`). The README's outbox example says
  the `(task_key, effect_name)` encoding must fit, and to hash it, for example
  as a hex SHA-256, if long keys could exceed it, as the JSON encoding of two
  near-maximum identifiers can.
- **Why:** A value the type accepts must be storable. The error belongs where
  the key is built, not at commit.
- **Expected behaviour:**
  - 2048 ASCII bytes and 512 four-byte characters are accepted; 2049 bytes and
    600 four-byte characters are `CommandKeyTooLong`
    (`ids::identifier_tests::command_key_byte_limit`).
  - An incompressible 2048-byte key of mixed one- to four-byte characters is
    stored by `complete_with_outbox`
    (`transitions::largest_command_key_is_stored`).
  - `IdentifierError` has a new variant.

### 29. Store inputs have checked types

- **Finding:** The builder checked its own settings, but direct store callers
  bypassed them. A 1 ns lease passed to `claim` or `heartbeat` was stored as
  zero, and `pg_interval` saturated a huge duration. `claim` took `&[String]`
  handler names and silently capped its batch size at `i64::MAX`. Attempt
  numbers were `i32`, and retry arithmetic treated a negative one as
  attempt 1.
- **Source:** call.
- **Decision:**
  - **Durations:** `LeaseDuration` (1 µs to 5 years; `LeaseDurationError` is
    `BelowOneMicrosecond` or `AboveFiveYears`) for `store::claim`,
    `store::heartbeat` and the scheduler's lease. `pg_interval` returns an
    error instead of saturating: a `Duration` above `i64::MAX` microseconds is
    `SchedulerError::Config` (`duration too large for a PostgreSQL interval`).
    Delays that may be zero (`store::retry`, the reaper's crash-retry delay,
    retention) stay `Duration`.
  - **Handler names:** `claim` and `next_due_in` take `&[TaskName]`.
  - **Batch sizes:** `claim`, `reap_expired` and `Retention.batch_size`, and the
    builder's `claim_batch_size` and `reaper_batch_size`, are `NonZeroU32`,
    bound exactly, with no `unwrap_or(i64::MAX)`. The registry write converts
    its slot counts exactly too.
  - **Attempts:** `AttemptNo` (positive; built with `TryFrom<u32>`, read with
    `get()`, and `AttemptNo::FIRST`) for `TaskLease.attempt_no` and
    `Attempt.attempt_no`, and the worker's retry backoff takes it. The attempt
    counts on `TaskView` and `TaskRow` are `u32`. A read that finds a stored
    value outside these types is `SchedulerError::Invariant`; the claim
    dead-letters such a row instead (entry 22).
  - **Builder:** `lease_duration` is stored as given and checked in `build()`
    with `LeaseDuration::try_from`, before the other rules. So a bad lease is
    now reported even when no handler is registered; before, the empty
    registry was reported first. The lease's messages are `lease_duration must
    be at least 1 microsecond` and `lease_duration must be at most 5 years`;
    the other four intervals share `heartbeat, poll, reaper and registry
    intervals must be at least 1 microsecond`.
  - **Deferred:** attaching the current attempt to `TaskState::Running`, and an
    `AttemptState::Open` that cannot carry an error, until they become part of
    the public contract.
- **Why:** The store functions are public. Each input should carry its rule in
  its type, not rely on the builder having run.
- **Expected behaviour:**
  - `LeaseDuration` rejects 999 ns and anything over 5 years, and accepts 1 µs
    (`models::task_enum_tests::lease_duration_bounds`); `pg_interval` rejects
    `Duration::MAX`
    (`models::task_enum_tests::pg_interval_rejects_what_it_cannot_represent`).
  - `AttemptNo` rejects 0 and anything above `i32::MAX`
    (`models::max_attempts_tests::attempt_no_rejects_zero_and_above_i32_max`),
    and the first attempt waits the base delay
    (`worker::tests::retry_delay_first_attempt_is_base`).
  - `build()` stores the checked lease
    (`builder::tests::build_stores_the_checked_lease`).
  - The admin JSON and pages show the same values as before: `attempts` and
    the attempt numbers now come from a `u32`.

### 30. `ensure_job` survives a concurrent delete

- **Finding:** `ensure_job` inserts with `ON CONFLICT DO NOTHING`, then updates
  by name. When `jobs::delete` committed while that update waited on the row
  lock, the update found no row, and `ensure_job` returned `Database(NotFound)`
  with no schedule in place.
- **Source:** call.
- **Decision:** When the reconcile update finds no row, `ensure_job` runs its
  insert-then-update pass again, for at most 3 passes in its one transaction.
  Each statement is new, so at READ COMMITTED it sees the delete. If all 3
  passes find the row gone, it returns `SchedulerError::Database(NotFound)`. Creating still adds the pause for a
  `Paused` spec, and reconciling still leaves the pause and the `next_run_at`
  cursor alone.
- **Why:** A delete racing an ensure is a normal interleaving of two public
  operations, and an ensure should end with the schedule in place.
- **Expected behaviour:** When the schedule's row is locked by a transaction
  that then deletes it and commits while `ensure_job` waits, `ensure_job`
  returns a schedule that exists
  (`jobs::ensure_job_survives_a_concurrent_delete`). It is a new row, with a
  new id and a cursor computed from the database's `now()`, and, like any
  schedule recreated under the same name, it does not inherit the deleted
  one's pause.

### 31. The worker registry is read in one statement

- **Finding:** `store::workers` read `queue_workers`, `queue_worker_queues` and
  `queue_worker_task_names` in three statements, in a REPEATABLE READ
  transaction of its own, or in the caller's transaction when one was open. The
  admin UI reads inside a transaction at the connection's default isolation,
  normally READ COMMITTED, so each statement took its own snapshot, and a
  registration change between them could pair a worker with empty or newer
  queues and raise false alerts.
- **Source:** call.
- **Decision:** `store::workers` is one SQL statement. Subqueries aggregate each
  worker's queues as JSON (with `paused` read from `queue_pauses`) and its task
  names as an array, so one statement snapshot covers all three tables at any
  isolation level. Its own REPEATABLE READ transaction is gone. The admin's
  read transaction stays as it is
  ([Admin UI decisions §3.9](AdminUI.md#39-overview-statements-see-slightly-different-moments)).
  Queue JSON that does not decode is `SchedulerError::Invariant`.
- **Why:** One statement gives one snapshot by construction, whatever the
  caller's transaction.
- **Expected behaviour:** Queues are still listed by name, task names in order,
  and `paused` is read live. Inside a READ COMMITTED transaction each worker
  comes back with its queues and task names
  (`registry::workers_reads_consistently_inside_a_read_committed_transaction`).
  That test covers the transaction path; the single statement is what makes
  the read consistent. The registry write is unchanged.

### 32. CI checks the minimum Rust and PostgreSQL 18

- **Finding:** CI built on the pinned 1.100 toolchain and ran its database
  suite only on PostgreSQL 13, and nothing checked the declared minimum, Rust
  1.88. The example used `postgres:13`, whose last upstream release was in
  November 2025. The default connection has no TLS, and no document said how
  to reach a database that requires it. The example retried an
  `OutboxConflict`, which fails the same way every time.
- **Source:** user (PostgreSQL 13 stays the documented minimum); call (the
  rest).
- **Decision:**
  - CI's `test` job runs against PostgreSQL 13 and 18, as a matrix that does
    not stop at the first failure. A separate `msrv` job runs
    `cargo +1.88 check --workspace --all-features --locked`.
  - The example uses `postgres:18`, with its data volume mounted at
    `/var/lib/postgresql`, as the 18 image requires.
  - The example's order handler fails the task permanently on an
    `OutboxConflict`, an identifier error or a JSON error; database and other
    scheduler errors are still retried.
  - The README's Operations section has a "Database TLS" note: build the pool
    with a TLS-capable connection through `ManagerConfig::custom_setup`.
  - The example app's integration test stays deferred (entry 15).
  - Ruling: installing the Rust 1.88 toolchain locally and starting a
    temporary `postgres:18` container, to verify this change, were in scope.
    The user approved a plan that named them, and both are local and
    reversible. If wrong, remove the toolchain or the container.
- **Why:** The release should be checked on the oldest Rust it claims and on a
  maintained PostgreSQL, and a demo that readers copy should not retry an error
  that cannot succeed.
- **Expected behaviour:**
  - `cargo +1.88 check --workspace --all-features --locked` passes.
  - The whole suite passes on PostgreSQL 18. One EXPLAIN test,
    `performance::replay_matching_uses_failed_name_index`, now seeds 5000 dead
    rows and runs `ANALYZE` first, like its sibling browse test: on an empty
    table PostgreSQL 18 chose a different index. The production SQL is
    unchanged.
  - An existing example database must be recreated with
    `docker compose down -v`: a PostgreSQL 13 data directory does not run on
    18, and the migrations changed (entry 33).

## Review 02: cross-cutting decisions

### 33. The migrations were edited in place again

- **Source:** call, following entry 17.
- **Decision:** The two `isfinite` CHECKs (entries 22 and 23) were added to
  `0001` and `0002` in place. There is no `0004`.
- **Why:** As in entry 17, the first release should ship a schema with no
  upgrade history.
- **Expected behaviour:** A database built before these changes lacks the
  CHECKs and must be dropped and recreated, as must the example's.
