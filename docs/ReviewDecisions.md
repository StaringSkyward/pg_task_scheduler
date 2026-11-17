# Review 01: Decisions and Expected Behaviour

This records what was decided about each finding of the first whole-repository
review, made before the first release (run at `ee353a0`, triaged against
`cade0a5`), and what each decision looks like from outside. The review itself
is kept out of the repository, so each entry restates its finding. Entries 1–16
follow the review's findings in order; entries 17–20 cut across them.

It describes the code as shipped. Each entry has a source: **(user)** means the
user decided it; **(call)** means it was a judgement call, open to change.
**Ruling** marks a call made during implementation, recorded in the entry it
affects.

**Existing databases:** the migrations were edited in place (entry 17). A
database built by an earlier version of this repository must be dropped and
recreated.

## Findings

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

### 9. Intervals must be at least one microsecond

- **Finding:** Durations were checked at nanosecond resolution, then truncated
  to PostgreSQL's microseconds. A 3 ns lease passed `build()` and was stored as
  zero, so every completion lost ownership.
- **Source:** call.
- **Decision:** `build()` rejects a lease, heartbeat, poll, reaper or registry
  interval under 1 µs, the resolution PostgreSQL stores.
- **Why:** One rule at the builder covers the case the review demonstrated.
- **Expected behaviour:** A 3 ns lease with a 1 ns heartbeat, or a 999 ns poll
  interval, fails to build with `lease, heartbeat, poll, reaper and registry
  intervals must be at least 1 microsecond`; 1 µs builds
  (`builder::tests::build_rejects_intervals_below_one_microsecond`). Retry
  delays, jitter, the crash-retry delay, the shutdown timeout and retention may
  still be zero. Store functions that take a `Duration` directly, such as
  `store::claim` and `store::retry`, still truncate to microseconds and
  saturate a huge value; the review's bounded duration type was not added.

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
  for the format check. It has no job on Rust 1.88, so a newer language
  feature could raise the real minimum unnoticed
  ([Publishing, entry 10](Publishing.md#10-minimum-rust-version-is-188)).

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

### 16. Outbox identities are typed and NUL is rejected

- **Finding:** `OutboxCommand` took plain strings for its effect, command key
  and topic, so empty or swapped values were accepted. Identifier validation
  accepted NUL, which PostgreSQL text cannot store, so the error came from the
  database instead.
- **Source:** call.
- **Decision:**
  - `EffectName` and `Topic` follow the identifier rule: 1 to 255 characters,
    no leading or trailing whitespace.
  - `CommandKey` is non-empty with no length limit, so it can hold the JSON
    encoding of `(task_key, effect_name)`.
  - Every identifier constructor rejects NUL (`IdentifierError::ContainsNul`).
- **Why:** These values identify and route a command, so they get the same
  boundary as the queue's own identifiers.
- **Expected behaviour:** `ids::identifier_tests::nul_is_rejected`,
  `ids::identifier_tests::effect_name_and_topic_follow_the_identifier_policy`
  and `ids::identifier_tests::command_key_allows_long_values_and_spaces`. The
  three types are exported from the crate root, and the README and the example
  app build commands with them.

## Cross-cutting decisions

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
