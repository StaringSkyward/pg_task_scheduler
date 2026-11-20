# Review Decisions and Expected Behaviour

This records what was decided about each finding of the seven
whole-repository reviews made before the first release, and what each decision
looks like from outside. The reviews themselves are kept out of the repository,
so each entry restates its finding.

- **Review 01** was run at `ee353a0` and triaged against `cade0a5`. Entries
  1–16 follow its findings in order; entries 17–20 cut across them.
- **Review 02** was run at `8cc77e3`, the head of PR #9. Entries 21–32 follow
  its findings 1–12 in order; entry 33 cuts across them.
- **Review 03** was run at `1390560`, the head of PR #9. Entries 34–42 follow
  its findings 1–9 in order.
- **Review 04** was run at `a3ab225`, the head of PR #9. Entries 43–46 follow
  its findings 1–4 in order; entry 47, approved after its triage, cuts across
  them and earlier entries.
- **Review 05** was run at `6cd99f4`, the head of PR #9. Entries 48–49 follow
  its findings 1–2 in order.
- **Review 06** was run at `21da0b6`, the head of PR #9. Entries 50–52 follow
  its findings 1–3 in order.
- **Review 07** was run at `fecccd3`, the head of PR #9. Entries 53–55 follow
  its findings 1–3 in order.

It describes the code as shipped. Each entry has a source: **(user)** means the
user decided it; **(call)** means it was a judgement call, open to change.
**Ruling** marks a call made during implementation, recorded in the entry it
affects. Where a later review changed an entry, that entry ends with a
**Changed by review 02**, **Changed by review 03** or **Changed by review 04**
line that says what is different now and links to the later entry. An entry
that entry 47 changed has a **Changed by entry 47** line in the same way. A
**Changed by entry N** pointer (47 to 52, 54 or 55) says what that entry
changed; when it changes one bullet only, it may sit inside that bullet instead
of ending the entry.

**Existing databases:** the migrations were edited in place, in reviews 01 and
02 (entries 17 and 33), for entry 47 and in review 06 (entry 52's `isfinite`
CHECKs on `completed_at` and `locked_until`) and in review 07 (entry 55's
`queue_timestamptz` domain); reviews 03, 04 and 05's findings
changed none. A database built by an earlier version of this repository must be dropped
and recreated.

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
- **Changed by entry 47:** `scheduler_jobs.task_name` and `queue` have
  identifier CHECKs (`queue` had none), so direct SQL can store an invalid one
  only after dropping its CHECK by hand. The materializer still parses them and
  pauses such a job; the queue test drops `scheduler_jobs_queue_identifier`
  first
  ([entry 47](#47-identifier-columns-carry-the-identifier-rule-as-sql-checks)).

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
- **Changed by review 03:** `store::cleanup` applies the same 5-year cap before
  its first delete, so a retention it cannot apply deletes nothing
  ([entry 39](#39-cleanup-checks-retention-before-its-first-delete)).

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
- **Changed by review 04:** the claim returns a row with an invalid key in
  `ClaimBatch::poisoned` instead of dead-lettering it, and the worker
  dead-letters it after dispatching the rest; the test is now
  `claim::claim_poisons_a_row_with_an_invalid_task_key_and_returns_the_rest`
  ([entry 45](#45-claim-returns-poisoned-rows-for-the-caller-to-dead-letter)).
  `store::task` reads `task_key` as text too, so such a row can be opened, with
  `TaskView.task_key` as `StoredTaskKey::Invalid`
  ([entry 44](#44-a-dead-letter-with-an-invalid-task-key-can-be-opened)).
- **Changed by entry 47:** every identifier column, `queue_tasks.task_key`
  among them, now has an identifier CHECK, so a raw-SQL producer can no longer
  write an invalid key, name or queue. The decode checks and the claim's
  key check stay, for a database whose CHECK was dropped by hand; the decode
  and claim tests drop the named constraint first
  ([entry 47](#47-identifier-columns-carry-the-identifier-rule-as-sql-checks)).

### 5. The schema puts optional and multi-valued facts in their own relations

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
  multi-valued facts in their own relations, not a form of 6NF, and the
  approved exceptions now include the `scheduler_jobs` cursor and every
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
    compared as canonical JSONB text, so key order and whitespace do not matter
    (`transitions::outbox_payload_key_order_is_not_a_conflict`) but numeric
    scale does: `{"n":1}` against `{"n":1.0}` is `OutboxConflict`, because a
    receiver decodes them apart
    (`transitions::outbox_payload_scale_difference_is_a_conflict`).
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
    **Changed by entry 52:** a first page binds `NULL` for its timestamp and
    starts from `-infinity` or `infinity`; the year-1 and year-9999 bounds are
    gone
    ([entry 52](#52-browse-pages-start-from-infinity-not-year-1-or-9999)).
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
- **Changed by review 04:** the claim returns such a row as poisoned, and the
  worker dead-letters it
  ([entry 45](#45-claim-returns-poisoned-rows-for-the-caller-to-dead-letter)).

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
- **Changed by review 03:** `GET /api/tasks/:id` gains `payload`, tagged
  `json` or `unreadable`
  ([entry 36](#36-producers-reject-payloads-the-worker-cannot-parse)).
- **Changed by review 04:** `task_key` in `GET /api/tasks/:id` is no longer a
  string. It is tagged like `payload`: `{"valid": "<key>"}`, or
  `{"invalid": {"text", "error"}}` for a stored key that breaks the identifier
  rule ([entry 44](#44-a-dead-letter-with-an-invalid-task-key-can-be-opened)).

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
    keeps the reaper away until the caller's transaction ends. A direct store
    caller that commits after `locked_until` still holds exclusive ownership;
    the docs no longer say the late commit rolls back. For runtime handlers the
    whole transaction, including its commit, must finish within the lease: a
    handler holding the row lock (`lock_owned`) blocks every heartbeat, and the
    worker drops a handler whose lease deadline passes, abandoning its
    transaction.
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
- **Changed by review 03:** the lease keeper takes the connection itself and
  spawns only the transaction, which waits for the row lock at most until the
  local deadline and renews nothing once its dispatch has ended
  ([entry 35](#35-a-heartbeat-is-bounded-by-the-lease-and-ends-with-its-dispatch)).

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
    **Changed by entry 55:** the column is `queue_timestamptz`, which is finite
    and below chrono's limit, in place of the CHECK.
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
- **Changed by review 03:** `enqueue` and `CreateJob` reject a payload the claim
  could not parse, and `store::task` returns an unparseable payload as
  `TaskPayload::Unreadable` instead of failing, so the last item no longer
  holds ([entry 36](#36-producers-reject-payloads-the-worker-cannot-parse)).
  The dead letter's error text has each NUL escaped
  ([entry 37](#37-task-diagnostics-escape-nul)).
- **Changed by review 04:** `claim` no longer dead-letters. It returns a
  `ClaimBatch`: the typed claims in `ready`, and each row that fails conversion
  in `poisoned`, still leased and untouched, with the same reason text.
  `store::dead_letter` fails a poisoned row by the same ownership key and
  lock-then-check path. The worker dispatches the ready rows first, then logs
  and dead-letters the poisoned ones on a task of its own, with the same log
  messages, so a slow dead-letter cannot spend a healthy row's lease
  ([entry 45](#45-claim-returns-poisoned-rows-for-the-caller-to-dead-letter)).
  The payload test is now
  `claim::claim_poisons_a_row_whose_payload_does_not_parse_and_returns_the_rest`,
  and dead-letters each poisoned row itself.

### 23. The materializer decodes nothing a stored value can break

- **Finding:** The due-schedule query decoded `next_run_at` as a timestamp and
  `job_args` as JSON before the per-job quarantine of entry 1. An oldest
  schedule with `next_run_at = '-infinity'` failed every pass without being
  paused, so no later schedule ran.
- **Source:** call.
- **Decision:**
  - `scheduler_jobs.next_run_at` has `CHECK (isfinite(next_run_at))`. A due
    cursor is finite and at most `now()`, so it decodes. **Changed by entry
    55:** the column is `queue_timestamptz` in place of the CHECK.
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
  - [Normal Form](SchedulerDesign.md#normal-form) no longer uses that name.
    It names the policy, 5NF with optional facts and multi-valued facts in
    their own relations, and says plainly that this is not 6NF.
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
- **Expected behaviour:** No schema change beyond the two CHECKs. Entry 5's
  title, which used the old name, was renamed, and its links updated.
- **Changed by review 03:** `queue_admin_actions` is described as one durable
  row per action with a running `affected` count, not as append-only
  ([entry 42](#42-queue_admin_actions-is-one-durable-row-per-action)).

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
    savepoint. `read uncommitted` is accepted, since PostgreSQL runs it as READ
    COMMITTED. Nested, it skips the 30 s `idle_in_transaction_session_timeout`
    guard, which would outlive the savepoint and apply to the caller's
    transaction.
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
- **Changed by review 03:** the doc of `materialize_due_jobs` says it runs one
  bounded pass of at most 100 due schedules, not that it materializes all due
  jobs
  ([entry 41](#41-max_poll_interval-is-a-wake-cadence-not-a-latency-bound)).

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
- **Changed by review 03:** `build()` checks retention second, also before the
  empty registry
  ([entry 39](#39-cleanup-checks-retention-before-its-first-delete)).
- **Changed by review 04:** the claim returns a row with an out-of-range stored
  value as poisoned, and the worker dead-letters it
  ([entry 45](#45-claim-returns-poisoned-rows-for-the-caller-to-dead-letter)).

### 30. `ensure_job` survives a concurrent delete

- **Finding:** `ensure_job` inserts with `ON CONFLICT DO NOTHING`, then updates
  by name. When `jobs::delete` committed while that update waited on the row
  lock, the update found no row, and `ensure_job` returned `Database(NotFound)`
  with no schedule in place.
- **Source:** call.
- **Decision:** When the reconcile update finds no row, `ensure_job` runs its
  insert-then-update pass again, for at most 3 passes in its one transaction.
  Each statement is new, so at READ COMMITTED it sees the delete. If all 3
  passes find the row gone, it returns `SchedulerError::Database(NotFound)`.
  Creating still adds the pause for a `Paused` spec, and reconciling still
  leaves the pause and the `next_run_at` cursor alone.
- **Why:** A delete racing an ensure is a normal interleaving of two public
  operations, and an ensure should end with the schedule in place.
- **Expected behaviour:** When the schedule's row is locked by a transaction
  that then deletes it and commits while `ensure_job` waits, `ensure_job`
  returns a schedule that exists
  (`jobs::ensure_job_survives_a_concurrent_delete`). It is a new row, with a
  new id and a cursor computed from the database's `now()`, and, like any
  schedule recreated under the same name, it does not inherit the deleted
  one's pause.
- **Changed by review 03:** exhausting the 3 passes returns
  `SchedulerError::Contention`, not `Database(NotFound)`
  ([entry 38](#38-enqueue-retries-when-cleanup-deletes-the-incumbent)).

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

## Review 03: findings

### 34. A handler never starts on a claim that arrived after its lease

- **Finding:** The claim cycle computes a local deadline from when it sent the
  claim, but the worker spawned the dispatch, and polled the handler first,
  even when the claim's response arrived after that deadline. A handler could
  start external work on a lease that had already expired. A later
  `LostOwnership` protects the database, not an external effect. The review
  reproduced it with a 60 ms lease and a trigger that delayed the claim's
  response by 200 ms.
- **Source:** call.
- **Decision:** `dispatch` checks the claim's monotonic deadline (`sent +
  lease`, with `sent` taken before the claim request) before it builds the
  `TaskContext` or polls the handler. If the deadline has passed, it starts
  nothing: it increments `pg_task_scheduler_leases_lost_total`, logs a warning
  (`claim arrived after its lease; handler not started`), and leaves the task
  to the reaper. The post-lock checks in SQL stay the authority for every
  transition.
- **Why:** An external effect cannot be undone, so the worker must not start one
  on a lease it already knows has passed.
- **Expected behaviour:**
  - With a 90 ms lease and a trigger that delays the claim's response past it,
    the handler never runs
    (`worker::handler_does_not_start_when_the_claim_arrives_after_its_lease`).
  - The task stays `running` until its lease expires. The reaper then closes
    the attempt as `lease_expired`, and requeues the task or marks it `dead`,
    as for any expired lease, so the attempt counts.

### 35. A heartbeat is bounded by the lease and ends with its dispatch

- **Finding:** The spawned heartbeat of entry 21 owned both the pool
  acquisition and the whole update, so cancelling the lease keeper dropped only
  a `JoinHandle`. A heartbeat waiting on a pool or a row lock could renew a
  valid lease after its dispatch and its scheduler had stopped, which postponed
  crash recovery for a task with no handler, and a stuck wait could keep the
  task and its pool reference alive past shutdown. The review reproduced the
  renewal after aborting the scheduler future.
- **Source:** user (approach b: keep the two-statement heartbeat, and bound it).
- **Decision:**
  - **Acquisition:** the lease keeper takes the pool connection itself, in its
    cancellable future, so a wait for one is dropped with the keeper. Only the
    transaction is spawned, and it owns the connection. `SchedulerPool::Conn`
    gains a `'static` bound, which the deadpool, bb8 and mobc adapters already
    meet.
  - **Bounded wait:** the transaction first sets `lock_timeout` and
    `statement_timeout`, local to the transaction, both to the time left until
    the local deadline, in milliseconds rounded up and at least 1 (0 would mean
    no timeout). PostgreSQL applies `lock_timeout` to each lock wait, and a
    statement can wait more than once, so `statement_timeout` bounds each
    statement as a whole. The spawned transaction's lock statement therefore
    ends by the local deadline, even after the keeper has stopped awaiting it.
  - **Cancellation check:** after the lock statement, the transaction checks a
    per-dispatch token, and if it is cancelled rolls back without renewing
    (`LostOwnership`). A drop guard cancels the token however the dispatch ends:
    finished, lease lost, released, or dropped with the scheduler future.
    Graceful shutdown does not cancel it, so handlers keep their leases while
    they drain. A window of one round trip remains between the check and the
    update: a dispatch that ends inside it gets one last renewal, which delays
    the reaper by at most one lease.
  - **API:** `store::heartbeat` is unchanged. The worker calls
    `store::heartbeat_bounded`, which also takes the lock wait and the token; it
    is exported hidden from the docs, like the SQL constants, for the
    integration tests.
  - Ruling: both timeouts are capped at `i32::MAX` milliseconds (about 24.8
    days), the largest value PostgreSQL accepts. `LeaseDuration` allows 5
    years, and without the cap every heartbeat of a lease over 24.8 days would
    fail. With such a lease, an orphaned heartbeat can wait on a lock for up to
    24.8 days, but it still renews nothing once its dispatch has ended.
  - Ruling: the lease keeper checks the deadline after each sleep, and returns
    before starting a beat it could not await. Before, after failing beats it
    slept to the deadline and then started one more, which a pool with a free
    connection could spawn after the keeper had given up. It checks again after
    taking a connection, and returns without spawning if no time is left, so a
    slow pool cannot start a beat at the deadline either.
  - Ruling: when the lease keeper gives up, the dispatch cancels the token at
    once. Otherwise the guard would fire only after the check for a completion
    or cancel made elsewhere, which can take up to `heartbeat_interval`, and an
    in-flight heartbeat could renew meanwhile.
- **Why:** Ending a dispatch must end its hold on the lease, and no spawned
  work should outlive the lease it serves. Keeping the two statements keeps
  entry 21's guarantee that a dropped heartbeat never leaves an idle
  transaction holding the row.
- **Expected behaviour:**
  - With a heartbeat blocked on a row lock, aborting the scheduler future and
    then releasing the lock leaves `locked_until` unchanged
    (`worker::dropped_scheduler_does_not_renew_a_pending_heartbeat`).
  - A handler that ignores `ctx.cancellation` and outlives three leases while
    graceful shutdown drains it still completes its task, `done` with one
    `done` attempt (`worker::draining_handler_keeps_its_lease`).
  - A bounded heartbeat whose lock wait outlasts its bound returns a database
    error (`statement timeout`, or `lock timeout`) soon after the bound
    (`transitions::bounded_heartbeat_gives_up_when_its_lock_wait_exceeds_the_bound`),
    and one whose token is cancelled returns `LostOwnership` without moving
    `locked_until` (`transitions::cancelled_heartbeat_does_not_renew`).
  - The timeout value is milliseconds rounded up, from `1ms` to
    `2147483647ms`
    (`transitions::tests::lock_timeout_is_whole_ms_at_least_one_at_most_i32_max`).
  - A DB-contained handler that commits while a heartbeat waits on its lock
    still leaves no connection idle in a transaction
    (`worker::db_contained_handler_survives_heartbeat_while_holding_row_lock`).
  - The worker's heartbeat runs three statements: the timeout settings, the
    row lock and the update.
  - A custom `SchedulerPool` whose `Conn` borrows from the pool no longer
    compiles.
- **Changed by review 04:** each timeout is the time left, or the session's own
  value when that is non-zero and smaller, so the heartbeat never raises a
  stricter timeout the host set, instead of overriding it
  ([entry 46](#46-the-heartbeat-never-raises-the-hosts-timeouts)). The helper is
  now `timeout_budget`, and its test
  `transitions::tests::timeout_budget_is_whole_ms_at_least_one_at_most_i32_max`.

### 36. Producers reject payloads the worker cannot parse

- **Finding:** `enqueue` and `CreateJob` converted arguments with
  `serde_json::to_value`, which has no nesting limit, while the claim parses
  the payload text with `serde_json::from_str`, which stops at 128 levels. A
  payload 140 arrays deep was enqueued and then dead-lettered by the claim with
  a recursion-limit error. `store::task` decoded the same payload, so it could
  not read that row either, and the operator could not open the record that
  explained the dead letter.
- **Source:** call.
- **Decision:**
  - **Producer boundary:** `enqueue` and `CreateJob::{new, for_task}` serialize
    the arguments to JSON text and parse it back with the claim's parser, in
    one shared crate-private helper. The typed `enqueue::<T>` and
    `CreateJob::for_task::<T>` also decode the parsed value as `T::Args`, as
    the worker does, so an `f64` NaN, which serializes as `null`, is rejected;
    the untyped `CreateJob::new` has no type to decode into. A failure is
    `SchedulerError::Serde`, and nothing is written. The claim still
    dead-letters a row written outside this boundary, by direct SQL (entry 22).
  - **Reading back:** `TaskView.payload` is a `TaskPayload`, exported from the
    crate root: `Json(serde_json::Value)`, or `Unreadable { text, error }`,
    with the stored text as PostgreSQL prints it and `serde_json`'s error.
    `store::task` reads `payload::text`, and returns every other field even
    when the payload does not parse.
  - **Admin:** the task page shows an unreadable payload as its text, then
    `Unreadable payload:` and the error. `GET /api/tasks/:id` gains `payload`,
    a tagged sum like `TaskPayload`: `{"json": <value>}`, or `{"unreadable":
    {"text": "...", "error": "..."}}`. The spec said the JSON kept `payload`,
    but it had no such field, so it is new.
  - Ruling (controller, final review): the admin JSON tags the two cases.
    Before, `payload` held either the value or the raw text as a JSON string,
    and only a separate `payload_error` told them apart, so a client reading
    `payload` alone could take an unreadable payload's text for a string
    argument.
  - Ruling: the test checks the `Unreadable` text by its `{"x": 1000` prefix,
    not by containing `1e500`, because PostgreSQL prints the JSONB number
    `1e500` as a 1 followed by 500 zeros. The plan's expected text was wrong
    about PostgreSQL's output; the intent, that the raw text is preserved,
    holds. If wrong, it is a test string.
- **Why:** A producer's success should mean the worker can parse the payload
  and, for a typed producer, decode it as the handler's arguments, and the
  record that explains a dead letter must stay readable. Nothing checks what
  the handler then does with the arguments.
- **Expected behaviour:**
  - `enqueue` of a value 140 arrays deep is `Serde` and inserts nothing
    (`enqueue::enqueue_rejects_a_payload_the_worker_cannot_parse`);
    `CreateJob::new` rejects the same value
    (`jobs::create_job_rejects_a_payload_the_worker_cannot_parse`).
  - A task stored by SQL with the payload `{"x":1e500}` is dead-lettered by the
    claim, and `store::task` then returns it, `dead`, with an `Unreadable`
    payload (`claim::task_with_an_unparseable_payload_is_still_readable`).
  - `enqueue` of a `T::Args` with an `f64` field set to NaN is `Serde` and
    inserts nothing (`enqueue::enqueue_rejects_args_that_do_not_round_trip`),
    and `CreateJob::for_task` rejects the same value
    (`jobs::for_task_rejects_args_that_do_not_round_trip`).
  - `GET /api/tasks/:id` returns `"payload": {"json": {}}` for a task enqueued
    with `{}`, and `{"unreadable": {...}}` with the text and error for the
    dead-lettered `{"x":1e500}`
    (`api::task_route_payload_is_json_or_unreadable`).
  - Code that read `TaskView.payload` as a `serde_json::Value` matches on
    `TaskPayload` instead.
- **Changed by review 04:** the typed producers decode the payload as PostgreSQL
  stores it, not the value before storage, and reject arguments that decode to
  different values with `SchedulerError::PayloadNotPreserved`. `enqueue::<T>`
  checks before its insert, and `jobs::create` and `jobs::ensure_job` check a
  `for_task` spec before writing. `for_task` still rejects a NaN when built
  ([entry 43](#43-typed-producers-check-their-arguments-as-jsonb-stores-them)).
  The claim returns a row it cannot parse as poisoned, and the worker
  dead-letters it
  ([entry 45](#45-claim-returns-poisoned-rows-for-the-caller-to-dead-letter)).

### 37. Task diagnostics escape NUL

- **Finding:** The worker binds a failure's text as PostgreSQL `TEXT`, which
  cannot hold NUL. A NUL from handler code or a dependency made the retry or
  permanent-failure update fail, the transition and its attempt close rolled
  back, and the task stayed `running`. The reaper could then run it again with
  the attempts it had left, and replace the diagnostic with `worker lease
  expired`.
- **Source:** call.
- **Decision:** A crate-private `Diagnostic` escapes each NUL as the two
  characters `\0`. It is the one place task error text is bound: the
  `last_error` of `retry` and of `fail_permanently` (whose path the claim's
  dead-letter also takes), and the attempt close's `error`. The public
  parameters stay `&str`. Fixed texts, such as the reaper's `worker lease
  expired`, do not go through it, and the admin replay's `last_error` filter is
  an operator's search, not a diagnostic.
- **Why:** The outcome the worker chose, retry or permanent, must land whatever
  the message says; the message only describes it.
- **Expected behaviour:**
  - `fail_permanently` and `retry` with the error `"bad\0news"` (a Rust string
    with a NUL) record `bad\0news`, a backslash and a zero, as `last_error`, and
    close the attempt (`transitions::fail_permanently_with_a_nul_is_recorded`,
    `transitions::retry_with_a_nul_is_recorded`,
    `transitions::tests::diagnostic_escapes_every_nul`).
  - A handler that returns `TaskError::permanent("bad\0news")` leaves its task
    `dead` after one attempt, with that `last_error`
    (`worker::permanent_failure_with_a_nul_in_its_message_is_recorded`).
  - Text that already contains a backslash followed by `0` reads the same as an
    escaped NUL.
- **Changed by review 04:** the claim's dead-letter is now `store::dead_letter`,
  which the worker calls after the claim; it binds its reason the same way
  ([entry 45](#45-claim-returns-poisoned-rows-for-the-caller-to-dead-letter)).

### 38. Enqueue retries when cleanup deletes the incumbent

- **Finding:** `enqueue` inserts with `ON CONFLICT DO NOTHING`, then reads the
  conflicting row in a second statement. Cleanup could delete that finished row
  in between, even at READ COMMITTED, and `enqueue` returned
  `Invariant("conflicting task_key not visible")` with no task in place.
- **Source:** call.
- **Decision:**
  - When the read-back finds the row gone, `enqueue` runs its insert again, for
    at most 3 passes, all on the caller's connection, so the task still
    commits with the caller's transaction. The content check applies to every
    incumbent it does find.
  - Exhausting the passes returns a new `SchedulerError::Contention(&'static
    str)`, which says to retry the call (`concurrent changes kept invalidating
    the operation: task_key vanished during enqueue`).
  - `ensure_job`'s exhausted passes (entry 30) return the same variant
    (`schedule deleted during ensure_job`), replacing the manufactured
    `Database(NotFound)`.
- **Why:** Cleanup and enqueue are both supported concurrent operations, so
  their interleaving is not a broken invariant. An enqueue should end with the
  task in place, or with an error that says to retry.
- **Expected behaviour:**
  - With `enqueue` held between its conflicting insert and its read-back while
    cleanup deletes the finished incumbent, it inserts a fresh `queued` task
    and returns `inserted = true`
    (`enqueue::enqueue_survives_cleanup_deleting_the_incumbent`).
  - `SchedulerError` has a new variant.

### 39. Cleanup checks retention before its first delete

- **Finding:** `cleanup` converted every interval before deleting, but
  `pg_interval` checks only that a duration fits a PostgreSQL interval, not
  that `now()` minus it is a valid timestamp. With `done = 0` and `dead` near
  `i64::MAX` microseconds, the `done` delete committed, the next delete failed
  with `timestamp out of range`, and the error did not say what had been
  deleted. The builder's 5-year cap excluded this input, but direct callers
  bypassed it.
- **Source:** call.
- **Decision:**
  - A crate-private `Retention::check` applies the 5-year cap to `done`, `dead`
    and `outbox_published` (`SchedulerError::Config`, `every retention duration
    must be at most 5 years`). `build()` and `store::cleanup` both run it
    before anything else.
  - `cleanup`'s doc says that its batches commit separately, so a database
    error mid-call leaves the earlier batches committed. The error does not
    report them.
  - Ruling: `build()` checks retention right after the lease, before the
    empty-registry check, so a bad retention is reported even when no handler
    is registered. Its message was `every duration must be at most 5 years`.
- **Why:** The builder's cap is a range the deletes can use, and a public
  function must not rely on the builder having run. Checking first means a bad
  input deletes nothing.
- **Expected behaviour:** `cleanup` with `done = 0` and `dead = i64::MAX` µs
  returns `Config` and leaves the old `done` row in place
  (`maintenance::cleanup_rejects_retention_it_cannot_apply_before_deleting`).
  Retention from zero to 5 years behaves as before.

### 40. Schedule reads do not decode arguments

- **Finding:** `jobs::get` and `jobs::list` selected the whole `SchedulerJob`
  row, including `job_args`, which the public `Job` does not return. A schedule
  whose arguments PostgreSQL accepts but `serde_json` cannot read
  (`{"x":1e500}`) materialized, as entry 23 intends, but broke both reads, so
  `GET /api/jobs` hid every schedule from the operator who needed to find it.
- **Source:** call.
- **Decision:**
  - The crate-internal `SchedulerJob` read projection drops `job_args`, so no
    read of a `Job` decodes them. The write path (`NewJob`) and the
    materializer's copy in SQL keep them.
  - The README says where each schedule defect shows:
    - **Arguments:** an argument the worker cannot parse dead-letters that
      occurrence's task, and the schedule keeps advancing.
    - **Cron or routing:** a bad cron expression, task name, queue or
      `max_attempts` pauses the schedule.
- **Why:** A read should decode only what it returns, as entry 23 settled for
  the materializer.
- **Expected behaviour:**
  - A schedule with unreadable `job_args` is listed and found by id
    (`jobs::schedules_list_even_when_their_args_are_unreadable`), and
    `GET /api/jobs` returns 200 with it
    (`api::list_jobs_survives_unreadable_args`).
  - Its occurrences are still created, and the claim dead-letters each of them
    (entries 22 and 23).
- **Changed by review 04:** the claim returns each such occurrence as poisoned,
  and the worker dead-letters it
  ([entry 45](#45-claim-returns-poisoned-rows-for-the-caller-to-dead-letter)).

### 41. `max_poll_interval` is a wake cadence, not a latency bound

- **Finding:** The README said an occurrence becomes a task within
  `max_poll_interval` of its time. A materializer pass handles at most 100 due
  schedules, counting any it pauses or skips, so with 101 due at once one pass
  cannot reach them all, and query time and lock contention add delay of their
  own. `materialize_due_jobs`'s doc said it materializes all due jobs.
- **Source:** call.
- **Decision:** Documentation only; the cap stays.
  - The README calls `max_poll_interval` the wake cadence for one bounded pass
    of at most 100 due schedules, and says a larger backlog catches up over
    later passes and that query time and lock contention add delay.
  - `materialize_due_jobs`'s doc says it runs one bounded pass, and that each
    due job the pass reaches uses one of the 100, inserted or not.
- **Why:** The cap limits how long materializing can delay the claims that
  follow it in the worker's loop. The docs should state the timing the cap
  gives, not a bound it cannot meet.
- **Expected behaviour:** No code change. A backlog of N due schedules needs at
  least ⌈N / 100⌉ passes, shared by the running schedulers.

### 42. `queue_admin_actions` is one durable row per action

- **Finding:** [Normal Form](SchedulerDesign.md#normal-form) called
  `queue_admin_actions` append-only and said its rows are never updated, but
  bulk cancel and replay add to `affected` after every batch. A role
  configured from the design could lack `UPDATE` and fail on bulk actions; the
  admin README's grants already include it.
- **Source:** call.
- **Decision:** Documentation only. The exception describes one durable row per
  action, with a running `affected` count that each bulk batch updates, and
  says that a role running bulk actions needs `UPDATE`. Its reason stays the
  action's shape CHECK. The table's own section says the same of `affected`.
- **Why:** A downstream role is configured from the design, so the design must
  say which statements the table takes.
- **Expected behaviour:** No code or schema change.

## Review 04: findings

### 43. Typed producers check their arguments as JSONB stores them

- **Finding:** The typed `enqueue::<T>` and `CreateJob::for_task::<T>` decoded
  the arguments as `T::Args` from the `serde_json::Value` before PostgreSQL
  stored it as JSONB, but the worker decodes the stored text, and JSONB
  normalizes numbers. An untagged enum of `Count(u64)` and `Ratio(f64)` passed
  the check as `Ratio(1e18)`, PostgreSQL stored the number as
  `1000000000000000000`, and the worker decoded `Count(1000000000000000000)`, a
  different effect. A type that accepts only a JSON float passed with `1e18`,
  then failed to decode in the worker, a permanent failure after a successful
  enqueue.
- **Source:** user (approach a: keep JSONB, and check the stored form before
  writing).
- **Decision:**
  - **The check:** before writing, a typed producer asks PostgreSQL for the
    stored form of the payload (`SELECT $1::jsonb::text`), parses that text and
    decodes it as `T::Args`, as the worker does. A decode failure is
    `SchedulerError::Serde`. If the stored form differs from the payload, the
    decoded value is serialized again, and if that differs from the payload
    too, the call returns the new `SchedulerError::PayloadNotPreserved`
    (`task arguments change meaning when stored as JSONB`). `Value` equality
    tells a `u64` number from an `f64` one. Nothing is written either way.
  - **Where it runs:** `enqueue::<T>` runs it once, before its insert.
    `CreateJob::for_task::<T>` records it for `T::Args` as a function pointer,
    and `jobs::create` and `jobs::ensure_job` run it before their transaction.
    `CreateJob::new` has no argument type, so it records none. `for_task` still
    decodes the serialized value when it is built, so a NaN fails there, as in
    entry 36.
  - **Idempotency** compares the canonical JSONB text (see the ruling
    below), not JSONB equality.
  - **Cost:** one more query per typed `enqueue`, and per `create` or
    `ensure_job` of a `for_task` spec.
  - Ruling: when the stored form parses to a `Value` equal to the payload, the
    check accepts without serializing the decoded value again; otherwise it
    compares as specified. The spec compared the re-serialized value always,
    which rejected a `HashSet<String>` of 20 entries, because a new `HashSet`
    serializes in another order. When storage changed nothing, the worker
    decodes exactly what the producer serialized, so this accepts only cases
    where meaning cannot have changed. The remaining gap, arguments with both
    an unordered collection and a whole float of at least 1e16 (a `HashSet`
    beside an `f64` of `1e18`), is marked `ponytail:` in `check_stored`:
    accepting them would need `PartialEq` on `Task::Args`. If wrong, those
    arguments are still wrongly rejected, which is safe.
  - Ruling: idempotency must not merge payloads the handler decodes
    differently. JSONB equality says `1 = 1.0`, so `Count(1)` then `Ratio(1.0)`
    on one key returned `inserted: false` and the worker decoded `Count(1)`.
    `enqueue` now compares `payload::text = $3::jsonb::text`: JSONB text sorts
    keys and normalizes whitespace but keeps numeric scale, so identical
    arguments and reordered keys still match, and a scale difference is
    `IdempotencyConflict`
    (`enqueue::idempotency_distinguishes_payloads_that_decode_differently`,
    `enqueue::idempotency_ignores_payload_key_order`). The same rule applies
    to the outbox (entry on `complete_with_outbox`): receivers decode payloads.
    Changed by entry 54: a typed retry now compares decoded arguments, and
    `Count(1)` then `Ratio(1.0)` still conflict.
  - Caveat: `Value` equality treats `-0.0` and `0.0` as equal and JSONB stores
    `0.0`, so a negative zero is accepted and the worker decodes `+0.0`.
- **Why:** A producer's success should mean the handler decodes the arguments
  the producer sent. Checking the stored form covers every codec, not just
  untagged enums, and keeps the JSONB column; idempotency compares the
  canonical JSONB text, so it stays in step with what the handler decodes.
- **Expected behaviour:**
  - `enqueue` of `Amount::Ratio(1e18)` is `PayloadNotPreserved` and inserts
    nothing (`enqueue::enqueue_rejects_args_whose_meaning_changes_in_jsonb`).
    `jobs::create` and `jobs::ensure_job` reject the same `for_task` schedule
    and write no row
    (`jobs::for_task_rejects_args_whose_meaning_changes_in_jsonb`).
  - A struct with an `f64` of `1e18`, and `Amount::Count(5)`, are accepted, and
    their claimed payloads decode to equal values
    (`enqueue::accepted_args_decode_equal_after_storage`). A `HashSet<String>`
    of 20 entries is accepted
    (`enqueue::enqueue_accepts_args_that_reserialize_in_another_order`).
  - A task with `Args = serde_json::Value` holding an `f64` of magnitude 1e16
    or more in the 64-bit integer range, such as `json!({"x": 1e18})`, is now
    `PayloadNotPreserved`. `serde_json` writes such a float in exponent form,
    JSONB stores it as an integer, and the handler would receive an integer
    `Value`. Smaller floats, and floats beyond the 64-bit range, which read
    back as floats, are accepted as before. The `enqueue` rustdoc and the
    README's Enqueue section say so.
  - `enqueue` of an `f64` NaN is still `Serde`, now because the stored `null`
    does not decode.
  - `SchedulerError` has a new variant.
- **Changed by entry 48:** the stored-form check now starts from the caller's
  exact value, because `serde_json` parses floats exactly
  ([entry 48](#48-serde_json-parses-floats-exactly)).
- **Changed by entry 50:** the check compares decoded `T::Args` values, which
  now need `PartialEq`, not `Value`s, and the error reads `task arguments change
  meaning when stored as JSON`
  ([entry 50](#50-typed-producers-require-arguments-to-decode-back-equal)).

### 44. A dead letter with an invalid task key can be opened

- **Finding:** The claim reads `task_key` as text, so it can dead-letter a row
  whose key breaks the identifier rule (entry 4), but `store::task` decoded the
  column as a `TaskKey` and failed on that row. The admin task page and
  `GET /api/tasks/:id` both read through it, so an operator could not open the
  dead letter, the failure that entry 36 removed for an unparseable payload.
  The review stored an empty key by SQL: the claim marked the row `dead` with
  `invalid task_key`, and `store::task` returned an error for its id.
- **Source:** call.
- **Decision:**
  - `TaskView.task_key` is a `StoredTaskKey`, exported from the crate root:
    `Valid(TaskKey)`, or `Invalid { text, error }` with the stored text and the
    `IdentifierError`. `StoredTaskKey::as_str` gives the key or the raw text.
    `TaskKey` keeps its rule.
  - `store::task` reads `task_key::text` and returns every other field even
    when the key is invalid; `store::task_detail` reads through it.
  - **Admin:** `GET /api/tasks/:id` tags `task_key` like `payload`:
    `{"valid": "<key>"}`, or `{"invalid": {"text": "...", "error": "..."}}`.
    It was a string. The task page shows the stored text followed by
    `(invalid: <error>)`.
- **Why:** The record that explains a dead letter must stay readable, whatever
  made the row unusable.
- **Expected behaviour:**
  - After the claim poisons a row with a 300-character key and it is
    dead-lettered, `store::task` returns it with `StoredTaskKey::Invalid`, the
    300-character text and `IdentifierError::TooLong`
    (`claim::claim_poisons_a_row_with_an_invalid_task_key_and_returns_the_rest`).
  - `GET /api/tasks/:id` returns `{"valid": ...}` for an enqueued task, and
    `{"invalid": {...}}` with the text and a non-empty error for one with a
    300-character key (`api::task_route_task_key_is_valid_or_invalid`).
  - The page of a dead task with a 300-character key shows the key and
    `(invalid: identifier must be at most 255 characters)`
    (`pages::task_page_shows_why_a_task_key_is_invalid`).
  - Code that read `TaskView.task_key` as a `TaskKey` matches on
    `StoredTaskKey` or calls `as_str()`. A JSON client reads `task_key.valid`.
- **Changed by entry 47:** `queue_tasks.task_key` has an identifier CHECK, so
  SQL can no longer store the review's empty key or a 300-character one; only a
  database whose CHECK was dropped by hand can hold an invalid key.
  `StoredTaskKey`, the tagged JSON and the page's key error stay as defence in
  depth (user decision), and the three tests above drop
  `queue_tasks_task_key_identifier` first
  ([entry 47](#47-identifier-columns-carry-the-identifier-rule-as-sql-checks)).

### 45. Claim returns poisoned rows for the caller to dead-letter

- **Finding:** Claim leases its whole batch in one statement, then awaited a
  dead-letter transition for each row that failed conversion before returning
  the healthy rows. A slow dead-letter therefore spent the healthy rows' lease.
  The review delayed the dead-letter update by 220 ms under a 90 ms lease:
  claim returned the healthy row with its lease already expired, so entry 34's
  deadline check rightly started no handler, but the row lost an attempt, and
  repeated delays could exhaust its attempts.
- **Source:** call.
- **Decision:**
  - **Return type:** `store::claim` returns a
    `ClaimBatch { ready, poisoned }` and runs no dead-letter statement.
    `ready` holds the `ClaimedTask`s. Each row whose attempt number,
    `task_key`, payload or `max_attempts` fails to convert is a
    `PoisonedClaim { task_id, worker_id, lock_token, reason }`, still
    `running` under its lease, with the reason text entry 22 records (for
    example `invalid payload: …`).
  - **Dead-lettering:** `store::dead_letter(conn, &PoisonedClaim)` fails the
    row permanently by its ownership key, through the same lock-then-check
    path as `fail_permanently`, with `reason` as its error. It returns
    `LostOwnership` once the lease has expired or moved on.
  - **Direct callers:** `claim`'s doc tells them to dead-letter each poisoned
    row after dispatching the ready ones. A row left alone stays `running`
    until the reaper requeues it, and a later claim poisons it again until its
    attempts run out. `Scheduler` does this itself.
  - **Worker:** it dispatches every ready claim first, then, if the batch has
    poisoned rows, spawns one task for them, which the claim cycle never
    awaits. The task logs each row (`claimed task is unusable; marking it
    dead`), takes a connection of its own and dead-letters the rows in turn. A
    failure is logged (`could not dead-letter the task; skipping it`, or
    `could not dead-letter the tasks; skipping them` when it cannot get a
    connection), and the reaper recovers the row. The task runs in the same
    set as the dispatches, so shutdown drains it like one.
  - Ruling: `ClaimBatch` and `PoisonedClaim` stay in `store`, with the other
    store results, reached as `store::ClaimBatch`; the crate root does not
    re-export them. If wrong, it is a one-line re-export.
  - Ruling: a full batch, and whether the cycle claimed anything, count only
    ready rows. Counting poisoned rows too, as first implemented, removed the
    back-pressure: a poisoned row takes no permit, so a full batch of them made
    the loop claim again at once and spawn a connection-holding dead-letter
    task per batch of a poisoned backlog. Counting ready rows restores the
    wait, which a finished task ends, and that paces dead-letter work to about
    one task per queue at a time. If wrong, a one-permit semaphore is the
    alternative. **Changed by entry 49:** that semaphore is now in place too,
    so at most one dead-letter task runs per scheduler, and rows poisoned
    while it runs are left to the reaper
    ([entry 49](#49-dead-lettering-has-a-fixed-one-connection-budget)).
- **Why:** The claim has already committed every row's lease, so quarantine
  work for one row must not spend another's. The reaper is already the
  recovery path for a lease nobody finishes.
- **Expected behaviour:**
  - Right after `claim`, a row with an unparseable payload is in `poisoned`,
    with its id, worker, `LockToken(1)` and an `invalid payload: …` reason, and
    the task is still `running` with its attempt open
    (`claim::claim_returns_poisoned_rows_without_touching_them`).
  - With a 90 ms lease and a trigger that delays every dead-letter update by
    250 ms, a healthy task claimed beside a `{"x":1e500}` row still reaches
    `done` (`worker::slow_dead_letter_does_not_delay_a_healthy_claim`).
  - Six poisoned rows with `claim_batch_size = 1` are all dead-lettered, and no
    more than one of them is ever `running` at once
    (`claim_loop::poisoned_backlog_is_dead_lettered_one_batch_at_a_time`).
  - `store::dead_letter` leaves a poisoned row `dead`, with the same
    `last_error` and closed attempt as before
    (`claim::claim_poisons_a_row_with_an_invalid_task_key_and_returns_the_rest`,
    `claim::claim_poisons_a_row_whose_payload_does_not_parse_and_returns_the_rest`).
  - In a running worker the bad row of a batch now becomes `dead` shortly
    after the claim, not within it
    (`worker::healthy_task_in_a_batch_with_a_bad_payload_runs`).
  - Dead-lettering takes one pool connection per scheduler, so size the pool
    at the sum of queue limits + 5 per scheduler; the README's Connections
    note says so. **Changed by entry 49:** before the one-permit semaphore,
    each claim cycle could start another dead-letter task while earlier ones
    still ran, and this said to budget about one more connection per queue,
    possibly more
    ([entry 49](#49-dead-lettering-has-a-fixed-one-connection-budget)).
    **Changed by entry 51:** this said + 3, counting maintenance with claims
    and leaving out the worker registry
    ([entry 51](#51-the-pool-minimum-counts-every-background-connection)).
  - Code calling `store::claim` reads `.ready`, and dead-letters `.poisoned`
    itself or leaves those rows to the reaper.

### 46. The heartbeat never raises the host's timeouts

- **Finding:** The worker's bounded heartbeat (entry 35) set `lock_timeout`
  and `statement_timeout` for its transaction to the time left on the lease,
  replacing the session's values. A host that set short timeouts on its pool
  could have a heartbeat hold a connection far longer: with both at 30 ms and
  a 2 s budget, the review's heartbeat was still waiting on a row lock after
  120 ms, and succeeded when the blocker committed. Review 03 documented the
  override; this reverses that call.
- **Source:** call.
- **Decision:** In the heartbeat's `set_config` statement, each of
  `lock_timeout` and `statement_timeout` keeps the session's current value
  when that is non-zero and smaller than the lease budget, and takes the
  budget otherwise. A session value of `0` means no limit, so the budget
  applies. Both stay local to the transaction, and the budget is computed as
  before (milliseconds rounded up, 1 to `i32::MAX`). The post-lock
  cancellation check and the ownership checks are unchanged. The README's
  Timeouts note says the heartbeat never raises a stricter `lock_timeout` or
  `statement_timeout` the host set.
- **Why:** The host's timeouts are its caps on a pool it owns. The heartbeat
  only needs a bound no later than its lease, and a stricter host bound
  already meets that.
- **Expected behaviour:**
  - With both session timeouts at 30 ms and a 2 s budget, a bounded heartbeat
    blocked on a row lock returns a database error within 500 ms, and the
    session's values are still 30 ms afterwards
    (`transitions::bounded_heartbeat_respects_a_stricter_session_timeout`).
  - With PostgreSQL's default of `0`, the budget applies as before
    (`transitions::bounded_heartbeat_gives_up_when_its_lock_wait_exceeds_the_bound`).
  - A heartbeat that hits a host timeout counts as a failed heartbeat
    (`pg_task_scheduler_heartbeat_failures_total`), and the lease keeper tries
    again at the next interval until the local deadline, as for any other
    heartbeat error.

## Review 04: cross-cutting decisions

### 47. Identifier columns carry the identifier rule as SQL CHECKs

- **Source:** user (add the CHECKs, on every identifier column, and keep
  `StoredTaskKey`); call (one shared function, the explicit White_Space class,
  the constraint names).
- **Decision:**
  - **The function:** `0001` defines `queue_is_identifier(s TEXT)`, the rule of
    `validate_identifier` in `src/ids.rs`: `char_length(s) BETWEEN 1 AND 255`,
    with no leading or trailing Unicode White_Space. NUL needs no test, because
    PostgreSQL text cannot hold it. Every identifier CHECK calls it, so SQL
    keeps the rule in one place, as Rust does.
  - **The columns** (user: every one): a column has the CHECK exactly when the
    crate reads or writes it as an identifier newtype (`TaskKey`, `TaskName`,
    `QueueName`, `WorkerId`, `EffectName`, `Topic`, `ScheduleName`):
    - `queue_tasks`: `task_key`, `task_name`, `queue`, `locked_by`;
    - `queue_task_attempts`: `worker_id`;
    - `queue_outbox`: `task_key`, `effect_name`, `topic`;
    - `scheduler_jobs` (`cron`): `name`, `task_name`, `queue`;
    - `queue_admin_actions`: `task_name`, `queue`;
    - `queue_pauses`: `queue`;
    - `queue_workers`: `worker_id`;
    - `queue_worker_queues`: `worker_id`, `queue`;
    - `queue_worker_task_names`: `worker_id`, `task_name`.

    Plain-text columns have none: `operator`, `reason`, `label`, `last_error`,
    `last_error_filter`, `error` and `cron_expression`. `locked_by` and the two
    audit columns stay nullable: the function returns NULL for NULL, which a
    CHECK accepts.
  - **The names** (call): each CHECK is `<table>_<column>_identifier` (for
    example `queue_tasks_task_key_identifier`), so tests and operators can name
    it. `queue_outbox.command_key` gets `queue_outbox_command_key_length`,
    `CHECK (octet_length(command_key) BETWEEN 1 AND 2048)`, which is
    `CommandKey`'s rule (entry 28). That makes 20 constraints with `cron`, and
    17 without.
  - **The whitespace class** (call): U+0009–U+000D, U+0020, U+0085, U+00A0,
    U+1680, U+2000–U+200A, U+2028, U+2029, U+202F, U+205F and U+3000, which is
    what Rust's `str::trim` strips, written as `\uXXXX` regex escapes so the
    file stays ASCII. It is not `[[:space:]]`, which follows the locale:
    probing PostgreSQL 13 with the project database's locale showed it rejects
    neither U+00A0 nor U+0085. The old `scheduler_jobs` CHECKs on `name` and
    `task_name` used it, so they let through names that the Rust decode
    rejected. They are replaced, and `scheduler_jobs.queue`, which had no
    CHECK, gets one.
  - Ruling: the function is `LANGUAGE sql IMMUTABLE PARALLEL SAFE`, not
    `STRICT` as planned. PostgreSQL does not inline a STRICT SQL function with
    this body, so each CHECK ran through the function executor. Without
    `STRICT` the verdicts are the same (NULL in gives NULL out, which a CHECK
    accepts), and `EXPLAIN VERBOSE` shows the body inlined. On PostgreSQL 13,
    200,000 rows with four identifier CHECKs each took 1152 ms to insert with
    `STRICT` and 324 ms inlined (94 ms with no CHECK), and 1211 ms against
    373 ms (186 ms) to update: about 4× cheaper per write. PostgreSQL checks
    every CHECK on every update of a row, including claim, heartbeat and
    completion. If wrong, it is one word back.
  - Ruling: the migrations require a UTF8 database. `0001` opens with a `DO`
    block that raises `pg_task_scheduler requires a UTF8 database; this one is
    <encoding>` unless `server_encoding` is `UTF8`, before it creates anything.
    The code-point class and `char_length` need UTF8, and so does JSONB's
    non-ASCII text: in another encoding they see bytes (in SQL_ASCII, `voilà`
    ends in byte 0xA0 and fails the CHECK). Enforcing it states a requirement
    the JSONB columns already had. If wrong, it becomes a line in the README.
  - Ruling: the two regex patterns are escape strings (`E'…\\u0009…'`), so a
    session with `standard_conforming_strings = off` logs no warning about
    nonstandard escapes. The verdicts and the inlining are the same with the
    setting on or off.
  - **Defence in depth** (user): `StoredTaskKey` stays, with the admin JSON's
    tagged `task_key`, the task page's key error, the claim's
    `invalid task_key` quarantine and every identifier newtype's decode check,
    for a database whose CHECK was dropped by hand. The tests of those paths
    drop the named constraint first, through `TestDb::drop_constraint`.
  - The migrations were edited in place, as in entries 17 and 33. There is no
    `0004`.
- **Why:** The Rust types enforced the rule only on what the crate itself wrote
  and read. A raw-SQL producer could store a key, name or queue that a later
  read rejected, and the crate had to quarantine it there (entries 1, 4 and
  44). A CHECK rejects the value at the write, where the producer sees the
  error.
- **Expected behaviour:**
  - For every BMP code point except the surrogates and NUL,
    `queue_is_identifier` gives the same verdict as `TaskName::try_from` on
    `chr(cp) || 'x'` and on `'x' || chr(cp)`; Unicode has no White_Space above
    the BMP. 255 `é` or 255 U+1F600 pass on both sides, and 256 fail
    (`identifier_checks::sql_identifier_rule_matches_rust`).
  - Each covered column has its named CHECK with the expected definition, and
    so does `queue_outbox_command_key_length`
    (`identifier_checks::identifier_constraints_cover_every_identifier_column`).
  - Inserting the task name `' x'` by SQL fails with a check violation
    (SQLSTATE 23514) on `queue_tasks_task_name_identifier`
    (`identifier_checks::queue_tasks_rejects_an_invalid_task_name`).
  - A NULL `locked_by`, and a NULL `queue` or `task_name` on an audit row, are
    accepted (`identifier_checks::nullable_identifier_columns_accept_null`).
  - On a SQL_ASCII database, `0001` fails with `pg_task_scheduler requires a
    UTF8 database; this one is SQL_ASCII` and creates no table (checked by
    hand). The README's requirements line says so.
  - A role that writes the queue tables needs EXECUTE on
    `queue_is_identifier(text)`, because every write's CHECKs call it as that
    role, and inlining does not skip the ACL check. PUBLIC has it by default; a
    database that revokes it must grant it (the admin README's role recipe
    does).
  - A `queue_tasks` write costs about 1 µs more for its four CHECKs (measured
    on PostgreSQL 13).
  - A database built before this change lacks the function and the CHECKs and
    must be dropped and recreated, as must the example's
    (`docker compose down -v`). No Rust API changed.

## Review 05: findings

### 48. serde_json parses floats exactly

- **Finding:** (review 05 #1) `serde_json`'s default parser can read a finite
  `f64` back one ULP off: `0.10000000000000005` (bits `0x3fb999999999999d`)
  parses as `0x3fb999999999999c`. A typed enqueue then stored and claimed a
  different number than the caller passed, and idempotency treated adjacent
  values as equal.
- **Source:** call (the reviewer's fix).
- **Decision:** enable the `float_roundtrip` feature of `serde_json`, and no
  code change.
- **Why not `serde_json::to_value` in `to_payload`:** claim
  (`src/store/claim.rs:161`) and `check_stored` parse with the same parser.
  Changing only `to_payload` would make `check_stored` reject exactly stored
  values with `PayloadNotPreserved`, and claim would still misread them.
- **Cost:** float parsing is about 2x slower. Cargo features unify, so the
  feature applies to every crate in a dependent's build that uses
  `serde_json`.
- **Expected behaviour:**
  - An `f64` enqueued as `0.10000000000000005` is claimed with the same bits,
    and enqueueing the adjacent value under the same key is an
    `IdempotencyConflict`
    (`enqueue::enqueue_preserves_f64_bits_and_rejects_the_adjacent_value`).

### 49. Dead-lettering has a fixed one-connection budget

- **Finding:** (review 05 #2) Every claim batch with poisoned rows spawned a
  dead-letter task holding a pool connection, and nothing bounded how many ran
  at once. Entry 45's pacing kept the loop from spinning, but each poll could
  still start another task while earlier ones ran, so dead-letter writes
  stalled in the database could fill the pool and starve claims, heartbeats
  and finalization.
- **Source:** call (the reviewer's fix, and entry 45's named fallback).
- **Decision:**
  - **The budget:** `QUARANTINE_CONNECTIONS = 1`, a constant in
    `src/runtime/worker.rs`, not a setting. `run_until_shutdown` makes one
    `Semaphore` with that many permits and passes it to every claim cycle.
  - **The permit:** a cycle with poisoned rows takes it with
    `try_acquire_owned` and spawns `dead_letter` with it. The task holds it
    until it returns, so the wake its end causes finds it free.
  - **Skip and log:** when the permit is taken, the cycle spawns nothing and
    carries on with its other queues. It logs each row at warn level, with
    `task_id` and `reason`: `dead-lettering is busy; the reaper recovers the
    task after its lease`. The row stays `running`.
- **Why:**
  - An unbounded number of tasks gives no allowance to size the pool for.
  - Waiting for the permit instead would hold the batch's leases, and its
    rows in memory, unbounded, while the stall lasts.
  - The reaper is already the recovery path for a failed dead-letter write
    (entry 45). After the lease it requeues a skipped row, and a later claim
    poisons it again.
- **Note:** a skipped row with no attempts left ends `dead` with `worker lease
  expired`, not its poison reason, the same as after a failed write.
- **Expected behaviour:**
  - With the scheduler's pool at 6 connections (queue limit 1 + 5), every
    move to `dead` stalled on an advisory lock, and five poisoned rows due
    before a healthy task: the healthy task reaches `done` while the lock is
    held, exactly one session waits on the lock, and all five rows end `dead`
    once it is released
    (`worker::stalled_background_work_leaves_room_for_healthy_work`).
    **Changed by entry 51:** the test ran on 4 connections (1 + 3), with only
    dead-lettering stalled, as
    `worker::stalled_dead_letters_leave_room_for_healthy_work`; it now stalls
    maintenance and the registry refresh too
    ([entry 51](#51-the-pool-minimum-counts-every-background-connection)).
  - Six poisoned rows with `claim_batch_size = 1` are all dead-lettered, and
    no more than one is ever `running` at once
    (`claim_loop::poisoned_backlog_is_dead_lettered_one_batch_at_a_time`).
  - The README and `docs/Concurrency.md` size the pool at the sum of queue
    limits + 5 per scheduler, 1 of them for dead-lettering. The default queue
    needs 16 + 5 = 21. **Changed by entry 51:** this said + 3: 1 for
    `LISTEN`, 1 for claims and maintenance, and 1 for dead-lettering, so the
    default queue needed 16 + 3 = 19
    ([entry 51](#51-the-pool-minimum-counts-every-background-connection)).

## Review 06: findings

### 50. Typed producers require arguments to decode back equal

- **Finding:** (review 06 #1) `serde_json` writes a non-finite `f64` as
  `null`, so `Some(f64::INFINITY)` with `Args = Option<f64>` was stored, and
  claimed as `None`. The check compared `Value`s after the loss, and
  `Serialize + DeserializeOwned` cannot show that encoding preserved the
  caller's value.
- **Source:** user (require `PartialEq`, rather than documenting the gap or
  using a serializer that rejects non-finite floats).
- **Decision:**
  - `Task::Args` is `Serialize + DeserializeOwned + PartialEq + Send + 'static`.
  - `typed_payload` serializes the arguments, decodes the payload as `T::Args`,
    and returns `PayloadNotPreserved` if the result differs from the caller's
    value. `enqueue::<T>` and `CreateJob::for_task::<T>` use it.
  - `check_stored` decodes the payload and the stored JSONB text as `T::Args`
    and returns `PayloadNotPreserved` if they differ. It no longer compares
    `Value`s or re-serializes.
  - The error reads `task arguments change meaning when stored as JSON`.
- **Why:**
  - A `Serialize + DeserializeOwned` bound cannot show that encoding preserved
    the caller's value, and checking `Value`s starts after the loss.
  - So the user chose `PartialEq`, over documenting the gap or a serializer
    that rejects non-finite floats.
  - Comparing decoded values checks what the handler receives, whatever the
    type. It also accepts a `HashSet` beside a large `f64`, which the `Value`
    comparison rejected.
- **Expected behaviour:**
  - `Some(f64::INFINITY)` and `Some(f64::NAN)` with `Args = Option<f64>` are
    `PayloadNotPreserved`, and nothing is written
    (`enqueue::enqueue_rejects_non_finite_floats_inside_an_option`,
    `jobs::for_task_rejects_non_finite_floats_inside_an_option`).
  - `Mixed { tags: HashSet<String>, x: 1e18 }` is accepted and claimed equal
    (`enqueue::enqueue_accepts_a_set_beside_a_large_float`).
  - Entry 43's typed-check tests still pass
    (`enqueue::enqueue_rejects_args_that_do_not_round_trip`,
    `enqueue::enqueue_rejects_args_whose_meaning_changes_in_jsonb`,
    `jobs::for_task_rejects_args_that_do_not_round_trip`,
    `jobs::for_task_rejects_args_whose_meaning_changes_in_jsonb`).

### 51. The pool minimum counts every background connection

- **Finding:** (review 06 #2) The documented minimum, the sum of queue
  limits + 3 per scheduler, counted maintenance together with claims and left
  out the worker registry. Maintenance and the registry run on tasks of their
  own, each taking its own connection, so they can stall alongside the one
  dead-letter task. The review stalled all three on a pool of 4 with a queue
  limit of 1: `LISTEN` held the fourth connection, the next claim waited for
  the pool, and a healthy task stayed queued.
- **Source:** call: document the real minimum. Admission control for the
  background paths, to keep a smaller minimum, was rejected as more machinery
  than one constant.
- **Decision:**
  - **The minimum:** the sum of queue limits + 5 per scheduler, one connection
    each for `LISTEN`, the claim loop (claims and cron materialization),
    maintenance, the worker registry, and dead-lettering.
  - **On top of it:** a handler that finishes while its heartbeat transaction
    is still in flight briefly holds a second connection, and its finishing
    update waits on that transaction's row lock anyway. Handlers and the host
    add whatever they take.
  - **Where:** the README's Sizing workers and Connections notes, and the
    Connection pool note in `docs/Concurrency.md`. The Deadpool example is
    16 + 5 = 21.
  - **Example app:** the worker pool is 14: 5 handler slots + 5 + 4 for
    `place_order` handlers taking their own connection.
- **Why:** maintenance and the registry run on their own spawned tasks, each
  acquiring its own connection, and they can stall alongside dead-lettering.
  All five users can hold a connection at once, so the minimum counts five.
- **Expected behaviour:**
  - With the scheduler's pool at 6 connections (queue limit 1 + 5), five
    poisoned rows due before a healthy task, and three stalls held outside the
    pool (the move to `dead` on an advisory lock, maintenance on
    `queue_outbox`, the registry refresh on `queue_worker_task_names`): all
    three are seen waiting on their locks before anything else is checked,
    the healthy task reaches `done` while they hold, exactly one session waits
    on the advisory lock, and all five rows end `dead` once the locks are
    released (`worker::stalled_background_work_leaves_room_for_healthy_work`).

### 52. Browse pages start from ±infinity, not year 1 or 9999

- **Finding:** (review 06 #3) With no cursor, the browse pages bound year 1
  (queued and running) or year 9999 (done, dead and cancelled) in place of a
  timestamp. PostgreSQL and chrono admit finite timestamps beyond them, and no
  CHECK rules them out, so a `done` row with
  `completed_at = '10000-01-01 00:00:00+00'` was missing from the first page,
  with no continuation. The stand-ins also called `unwrap` outside tests.
- **Source:** call.
- **Decision:**
  - **Binds:** a page with no cursor binds `NULL` for its timestamp. The SQL
    bound is `COALESCE($n, 'infinity'::timestamptz)` in `DONE`, `FAILED` and
    `FAILED_BY_NAME`, and `COALESCE($n, '-infinity'::timestamptz)` in
    `RUNNING`, `QUEUED_DUE` and `QUEUED_FUTURE`.
  - **Removed:** `earliest()` and `latest()`, with their `unwrap`s.
  - **Kept:** the id and priority stand-ins (`0`, `i64::MAX`, `i32::MAX + 1`)
    are in-domain bounds: `i64::MAX` is the top of `bigint`, `0` is below every
    identity id (they start at 1) and `i32::MAX + 1` is above every `integer`.
  - **CHECKs:** migration 0001 adds `CHECK (isfinite(completed_at))` and
    `CHECK (isfinite(locked_until))`, as `due_at` already has (**Changed by
    entry 55:** the `queue_timestamptz` domain replaces all four). A stored
    infinity would sort onto the first page, where chrono cannot decode it, so
    the CHECKs keep the `±infinity` bounds from ever meeting one. NULL passes
    them, and every writer sets finite values.
  - **Ruling:** one query per page is enough. The `COALESCE` bound is part of
    the index condition in both custom and generic plans, so the fallback of
    two query variants per page was not needed.
- **Why:** the year-1 and year-9999 stand-ins were manufactured values with
  `unwrap`s. Finite timestamps beyond them are valid in PostgreSQL and chrono,
  so those rows dropped out of first pages, and `±infinity` are the type's own
  bounds.
- **Expected behaviour:**
  - A `done` row and a `dead` row completed at `10000-01-01 00:00:00+00` each
    appear on their first page
    (`browse::first_pages_include_timestamps_past_year_9999`).
  - Every page, first (`NULL`) and continued, reads its index with no seq scan
    (`performance::queued_page_uses_claim_and_due_indexes`,
    `performance::running_page_uses_lease_index`,
    `performance::done_page_uses_terminal_index`,
    `performance::failed_page_uses_failed_index`,
    `performance::failed_page_by_name_uses_failed_name_index`), and so do the
    custom and generic plans of `FAILED` and `FAILED_BY_NAME`
    (`performance::failed_page_generic_plan_uses_failed_index`,
    `performance::failed_page_by_name_generic_plan_uses_failed_name_index`).
  - For ordinary data, first pages and continuations return the same rows as
    before (`browse::queued_page_matches_claim_order_across_segments`,
    `browse::running_page_orders_by_lease_expiry`,
    `browse::done_and_failed_pages_newest_first_with_filters`,
    `browse::done_and_failed_pages_break_completed_at_ties_by_id`).
  - A row with an infinite `completed_at` or `locked_until` cannot be stored
    (`schema_checks::infinite_completed_at_is_rejected`,
    `schema_checks::infinite_locked_until_is_rejected`).

## Review 07: findings

### 53. A handler suspended past its lease is never resumed

- **Finding:** (review 07 #1) `dispatch` polled the handler first in a biased
  `select!`. When the handler's awaited event and the lease keeper's timer were
  both ready on one poll, the handler ran one more segment after its local lease
  deadline, and could start an effect outside PostgreSQL on an expired lease.
- **Source:** call (the reviewer's fix).
- **Decision:** the arms are ordered `keep_lease`, `shutdown.expired`, then the
  handler; the arm bodies are unchanged. A handler suspended past the local
  deadline is dropped when it would next resume. A segment already running at
  the deadline is not preempted, and a result returned in the same poll as the
  deadline is dropped as a lost lease, which the reaper retries.
- **Why:** the database fences worker transitions but cannot fence an effect
  started outside PostgreSQL. This matches the rule that no handler starts on an
  expired lease.
- **Expected behaviour:**
  - A handler whose timer is due when the local deadline passes is not resumed
    (`worker::a_handler_suspended_past_its_lease_is_not_resumed`).
  - A DB-contained handler holding its row lock still progresses while a
    heartbeat waits, and a draining handler keeps its lease
    (`worker::db_contained_handler_survives_heartbeat_while_holding_row_lock`,
    `worker::draining_handler_keeps_its_lease`).

### 54. Typed retries match on equal arguments

- **Finding:** (review 07 #2) `enqueue` compared canonical JSONB text, so two
  equal `HashSet`s that serialize in different orders were an
  `IdempotencyConflict`, though the handler decodes them to the same value.
- **Source:** user (compare typed values, rather than keeping canonical JSONB
  text and documenting that a retry must reproduce the same array order).
- **Decision:** `EXISTING` selects `task_name = $2 AS same_name` and
  `payload::text AS stored`, and no longer binds the payload. A retry returns
  the existing task when the name is the same and `decode_stored::<T::Args>`
  (text, then `Value`, then `T::Args`, as the worker decodes it) yields a value
  equal to `args` by `PartialEq`. Anything else, including a stored payload that
  does not decode, is `IdempotencyConflict`. Admission already proved the new
  arguments decode back equal, so comparing with `args` compares what the
  handler would receive. The queue and priority are still not compared. The
  outbox `same_content` check keeps canonical JSONB text.
- **Why:** the handler receives the decoded value, so equal values are the same
  request whatever order a set serializes in. `Value` equality still tells `1`
  from `1.0`, and a stored payload the type cannot decode is not the same
  request.
- **Expected behaviour:**
  - Equal sets in another serialization order return the first task
    (`enqueue::idempotency_matches_equal_sets_in_another_order`).
  - A stored payload that does not decode as `T::Args` is a conflict
    (`enqueue::idempotency_conflicts_when_the_stored_payload_does_not_decode`).
  - With `Args = Value`, `1` then `1.0` conflicts
    (`enqueue::idempotency_with_value_args_keeps_numeric_scale`).
  - Kept: `Count(1)` then `Ratio(1.0)` conflicts, reordered object keys match,
    and different content conflicts
    (`enqueue::idempotency_distinguishes_payloads_that_decode_differently`,
    `enqueue::idempotency_ignores_payload_key_order`,
    `enqueue::conflicting_duplicate_is_rejected`).

### 55. Every stored timestamp is one chrono can hold

- **Finding:** (review 07 #3) PostgreSQL's finite `TIMESTAMPTZ` range runs to
  294276 AD, but chrono 0.4's `NaiveDate::MAX` is 262142-12-31. A row past it
  (for example from a SQL import) fails Diesel's decode ("Tried to deserialize
  a timestamp that is too large for Chrono"), and one such row failed a whole
  browse page. The four `isfinite` CHECKs covered only four columns.
- **Source:** call.
- **Decision:** migration 0001 creates
  `queue_timestamptz AS TIMESTAMPTZ CHECK (isfinite(VALUE) AND VALUE < '262143-01-01 00:00:00+00')`.
  Every `TIMESTAMPTZ` column in migrations 0001 to 0003 (19 columns) uses it,
  keeping `NOT NULL` and defaults, and the four `isfinite` CHECKs (`due_at`,
  `locked_until`, `completed_at`, `next_run_at`) are removed. `src/schema.rs`
  and every bind stay `Timestamptz`. PostgreSQL's lower bound (4713 BC) is
  already inside chrono's range.
- **Why:** a SQL import can write any timestamp column, and Rust decodes all of
  them. Following entry 47, every column gets the rule, and a domain states it
  once. The browse pages' `±infinity` bounds still lie outside the domain, so
  they never meet a stored value.
- **Expected behaviour:**
  - A timestamp at or past `262143-01-01` is a check violation
    (`schema_checks::timestamp_past_chronos_range_is_rejected`).
  - No `timestamptz` column lacks the domain
    (`schema_checks::every_timestamp_column_uses_queue_timestamptz`).
  - The last instant chrono can hold appears on a first page
    (`browse::the_last_instant_chrono_can_hold_appears_on_a_first_page`).
  - Kept: `schema_checks::infinite_due_at_is_rejected`,
    `schema_checks::infinite_completed_at_is_rejected`,
    `schema_checks::infinite_locked_until_is_rejected`,
    `schema_checks::infinite_next_run_at_is_rejected` and
    `browse::first_pages_include_timestamps_past_year_9999` still pass.
