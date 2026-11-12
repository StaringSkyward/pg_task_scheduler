# Admin UI: Decisions and Expected Behaviour

This is the record of every decision behind the optional admin UI, and behind
the core features it needs: the worker registry, cancel, bulk actions and pause.
It says what was decided, why, and what an operator or maintainer should see as
a result. Setup instructions are in [admin/README.md](../admin/README.md); the
core API is in the [README](../README.md).

It describes the code as shipped. Where that differs from the design spec
(`docs/superpowers/specs/2026-11-11-optional-admin-ui-design.md`, kept out of
the repository), the entry says so under **Divergence**. Each entry has a
source:

- **(user)**: the user decided it.
- **(call)**: a design call in the spec, open to change.
- **Plan ruling**: decided in the implementation plan.
- **Ledger ruling**: decided during implementation and recorded in the
  implementation ledger (`.superpowers/sdd/2026-11-11-optional-admin-ui/progress.md`).
- **Ledger note**: behaviour the ledger flagged for documentation.

Tests are cited as `file::test`, under `tests/` for the core crate and
`admin/tests/` (or a `src` module) for the admin crate. "Spec §N" is a section
of the design spec; a bare "§N.M" is an entry in this document.

**Contents**

1. [Architecture and crates](#1-architecture-and-crates)
2. [Auth and security](#2-auth-and-security)
3. [Database cost and counts](#3-database-cost-and-counts)
4. [Health rules](#4-health-rules)
5. [Actions: cancel, replay, bulk, pause](#5-actions-cancel-replay-bulk-pause)
6. [Worker registry](#6-worker-registry)
7. [Pages and refresh](#7-pages-and-refresh)
8. [Standalone server](#8-standalone-server)
9. [Upgrade](#9-upgrade)
10. [Known limits](#10-known-limits)

## 1. Architecture and crates

### 1.1 Who it is for, and the first question

- **Source:** spec §1, §2 goal 5, §7.1 (user).
- **Decision:** The users are operators, admins, SREs and engineers who know
  what good and bad look like. The first page answers one question: are workers
  registered, are tasks being dequeued and completed, and if not, why not and
  how big is the problem? Lists, task detail and actions come after. The
  overview shows only the numbers listed below (spec §7.1); the user said those are
  enough.
- **Why:** An operator opening the page during an incident needs the verdict
  before the detail.
- **Expected behaviour:** The overview, top to bottom: an alert for queues that
  are paused or have due tasks and no live worker; a workers summary (live,
  lost, last seen); one row per queue with a status and its reason, and its
  queued, due, done and dead counts; failure
  groups; due task names with no handler; the last 10 admin actions; links to
  the lists. Nothing else: no charts, no history (`pages::overview_*` tests).

### 1.2 The UI is optional, in its own crate

- **Source:** spec §1, §4 (user: separate crate; call: workspace layout).
- **Decision:** The project is a Cargo workspace. The core crate
  `pg_task_scheduler` stays at the repository root. The UI, its templates and
  assets, the auth layers, the overview cache and the `pg-task-admin` binary
  are in `pg_task_scheduler_admin` (`admin/`), which depends on core by path and
  version. Core's `axum` feature and `src/admin.rs` moved into the admin crate.
  The admin crate's features are `deadpool` (default) and `cron`, which forward
  to core.
- **Why:** An application that doesn't use the UI takes on none of its HTTP
  dependencies.
- **Expected behaviour:** The core crate has no axum, askama or argon2
  dependency, and no `axum` feature (a breaking change). Both crates are at
  version 0.1.0 and move together.

### 1.3 Registry, cancel and pause are in core and always on

- **Source:** spec §1 (call).
- **Decision:** The worker registry, the `cancelled` status, cancel, bulk
  actions, pause and the action audit live in the core crate, with no feature
  flag.
- **Why:** Their running cost is one upsert per worker every 15 s and one
  primary-key probe of a tiny table per claim. A standalone UI pointed at
  workers that never opted in would report every queue as having no live
  worker. Cancel and pause are useful from code too.
- **Expected behaviour:** Every user must apply migration `0003`, UI or not
  (§9). The core API exposes `store::cancel`, `store::cancel_matching`,
  `store::replay_matching`, `store::pause_queue`, `store::resume_queue`,
  `store::workers` and the bounded read APIs.

### 1.4 The JSON routes moved under `/api/` and take the operator from auth

- **Source:** spec §4 (call).
- **Decision:** `GET /tasks/:id`, `POST /tasks/:id/replay` and the cron
  `/jobs` routes became `/api/tasks/:id`, `/api/tasks/:id/replay` and
  `/api/jobs…`. Replay's operator is the authenticated `Operator`; the body
  carries only `reason`.
- **Why:** The HTML pages own the short paths, and an audited operator must be
  who authenticated, not what the client claims.
- **Expected behaviour:** A replay body with an `operator` field is accepted and
  the field ignored; the audit row records the authenticated name
  (`api::replay_route_requeues_dead_task_and_409s_otherwise`,
  `auth::trusted_header_sets_operator`). Replay returns 204, or 409 with
  `task is not dead or cancelled, or does not exist`. Like the forms, it trims
  the reason and answers 400 `a reason is required` when it is blank
  (`api::replay_route_requires_reason`).

### 1.5 `queue_admin_actions` replaces `queue_task_replays`

- **Source:** spec §5.1.6 (call); Ledger ruling 1 (pre-flight).
- **Decision:** `0003` creates `queue_admin_actions`, copies every
  `queue_task_replays` row into it as a `replay` action, then drops
  `queue_task_replays`. **Ledger ruling 1:** the same change updated the
  then-existing route test (`tests/admin.rs`) to count
  `queue_admin_actions WHERE action='replay'`, because that file still ran
  under `--all-features` until the admin crate replaced it.
- **Why:** One audit table for every operator action, so the overview and the
  task page can show them together. Updating the test in the same step kept
  every commit's gate green; it cost nothing, since the file was deleted later.
- **Expected behaviour:** After `0003`, old replays appear as `replay` actions
  with their task id, operator, reason and time, and `affected = 1`. Their
  `task_key` and new `lock_token` were not carried over
  (`admin_audit::migration_keeps_replay_audit`). `to_regclass('queue_task_replays')`
  is NULL.

### 1.6 One audit row per action, no foreign keys

- **Source:** spec §5.6 (call).
- **Decision:** Each replay, cancel, pause or resume writes one
  `queue_admin_actions` row: the action, `task_id` (single-task actions only),
  a bulk action's filter (`task_name`, `queue`, `last_error_filter`),
  `max_task_id`, `affected`, `operator`, `reason` and `at`. A bulk action writes
  one row however many tasks it touches. There is no foreign key.
- **Why:** A 100,000-task bulk cancel must not write 100,000 audit rows, and
  the audit must outlive task retention.
- **Expected behaviour:** A bulk cancel of 3 tasks writes one row with
  `affected = 3` (`bulk::cancel_matching_cancels_queued_and_running_of_that_name_only`).
  Pause and resume rows have `affected = 0` and the queue name. Deleting a task
  leaves its audit rows.

### 1.7 `queue_tasks.admin_action_id` points at the last action

- **Source:** spec §5.1.3 (call); Ledger note (Task 2).
- **Decision:** A nullable, unindexed column on `queue_tasks`. Cancel and replay,
  single or bulk, set it to their action row. The latest action wins.
- **Why:** The task page can then show who cancelled or replayed a task and why,
  even when a bulk action did it (a bulk row has no `task_id`). Adding a
  nullable column only changes table metadata, and no hot-path statement writes
  it.
- **Expected behaviour:** The task page's Actions table lists the rows with this
  task's `task_id` plus the row `admin_action_id` points to, newest first, with
  no duplicates (`admin_audit::task_actions_include_linked_bulk_action`). A task
  cancelled in bulk and then replayed singly shows both actions; its
  `admin_action_id` is the replay.

### 1.8 One copy of each topic in the docs

- **Source:** Ledger ruling 3 (pre-flight).
- **Decision:** The feature tasks each added their own README sections (cancel,
  bulk, pause, registry); the documentation task consolidated and checked them
  rather than writing a second copy.
- **Why:** Two copies of the same semantics drift apart.
- **Expected behaviour:** Core semantics (cancel, bulk, pause, registry, upgrade
  order) are in the README; UI setup is in `admin/README.md`; the reasoning and
  expected behaviour are here. Each links to the others instead of repeating
  them.

### 1.9 What the UI deliberately does not do

- **Source:** spec §2 non-goals (user: no charts or history; call: the rest).
- **Decision:** No charts, history, time series or metrics beyond the overview
  numbers (user: they would tax the database). No exact all-time totals of
  completed and failed tasks (§3.4). No payload or free-text search. No built-in
  LDAP, OIDC or SAML (§2.6). No page for cron schedules; the JSON `/api/jobs`
  routes remain. No roles (§2.7). No TLS inside the standalone server (§2.10).
- **Why:** Each would either cost the database on every view, or duplicate what
  a proxy, the host application or the `metrics` facade already does.
- **Expected behaviour:** Dashboards come from the core crate's metrics. The
  overview's "Done" and "Dead" are counts over a window.

## 2. Auth and security

### 2.1 No operator, no access

- **Source:** spec §6.1 (call).
- **Decision:** Every route reads the `Operator` request extension, a newtype
  over `String`. A request without one gets 401, GET included. There is no
  "none" auth mode.
- **Why:** A deployment that forgets authentication must be closed, not open.
- **Expected behaviour:** `router(..)` with no auth layer answers 401 to every
  page, asset, form and API route (`auth::every_route_401_without_operator`,
  `pages::overview_requires_operator`). The operator check runs before the CSRF
  check, so an unauthenticated cross-site POST is 401, not 403 (same test).

### 2.2 Unknown paths

- **Source:** Ledger note (Task 9).
- **Decision:** The operator check is a `route_layer`, so it runs only for
  requests that match a route.
- **Why:** That is how axum attaches middleware to routes; a fallback has
  nothing to protect.
- **Expected behaviour:** The bare router answers a path it does not serve with
  404 and no operator check. A served path with an unsupported method is 401
  without an operator. The shipped auth layers wrap the whole router, so behind
  `with_basic_auth` or `with_trusted_header` every request, unknown paths
  included, is authenticated first (unknown path without credentials: 401).
  These results come from a direct probe of the router; no test pins them.
  **Divergence from the ledger note**, which said unknown paths return 404/405
  without auth: a known path with the wrong method gets 401, not 405.

### 2.3 Authentication is a pluggable tower layer

- **Source:** spec §2 goal 8, §6.2 (user: configurable, basic or LDAP or other;
  call: the shape).
- **Decision:** Authentication is any tower layer that inserts `Operator`. The
  crate ships two: `auth::with_basic_auth` and `auth::with_trusted_header`.
  Embedding applications can use their own (`admin/README.md` shows a
  ten-line `axum::middleware::from_fn` layer).
- **Why:** Deployments differ: some use basic auth, others LDAP or single
  sign-on. One extension point covers all of them.
- **Expected behaviour:** The operator recorded on every action is the name the
  layer inserted (`auth::trusted_header_sets_operator`,
  `actions::cancel_and_replay_via_forms`).

### 2.4 Basic auth with argon2id hashes

- **Source:** spec §6.2 (call).
- **Decision:** Users come from configuration as argon2id PHC strings, made by
  `auth::hash_password` or `pg-task-admin hash-password` (default argon2
  parameters). An unknown user name is verified against a fixed dummy hash. The
  challenge is `Basic realm="pg_task_scheduler admin", charset="UTF-8"`.
  `with_basic_auth` rejects any hash that is not argon2id; the standalone config
  also rejects duplicate user names.
- **Why:** A leaked config does not reveal passwords, and argon2id makes
  guessing them expensive. Verifying unknown names keeps response time from
  revealing which names exist. argon2's verify compares its output in constant
  time.
- **Expected behaviour:** Good credentials pass; a wrong password, unknown user,
  malformed header, other scheme or no header gets 401 with the challenge
  (`auth::basic_auth_accepts_valid_rejects_invalid`). A hash that isn't
  argon2id fails `with_basic_auth` validation, and the error names the user
  but does not echo the hash (same test). Duplicate names fail the standalone
  config's validation, also without echoing a hash
  (`pg-task-admin config::rejects_duplicate_users`).

### 2.5 At most one argon2 verify per CPU

- **Source:** Ledger note (Task 9 review fix).
- **Decision:** Verifies run on the blocking pool, limited by a semaphore with
  one permit per CPU (`available_parallelism`, or 4 if unknown). The permit
  moves into the blocking task.
- **Why:** Each verify takes about 19 MiB and a core for tens of milliseconds,
  and anyone who can reach the server can trigger one. Without a bound, a flood
  of bad credentials exhausts memory and CPU.
- **Expected behaviour:** Under a flood, logins queue rather than run in
  parallel beyond the CPU count. A client that disconnects mid-verify does not
  free its slot early
  (`auth::tests::verifies_are_bounded_even_when_callers_give_up`).

### 2.6 Trusted header behind an authenticating proxy; LDAP through a proxy

- **Source:** spec §2 non-goals, §6.2 (call).
- **Decision:** `with_trusted_header(router, header)` takes the operator from a
  header that an authenticating proxy (oauth2-proxy, Authelia, nginx
  `auth_request`, an LDAP gateway) sets. LDAP, OIDC and SAML are not built in;
  standalone deployments use such a proxy, and embedded ones their own layer.
- **Why:** Those protocols are better served by existing proxies and the host's
  own sign-in than by a second implementation here.
- **Expected behaviour:** A missing, empty or repeated header is 401; a repeated
  header means the proxy appended to a client-supplied value instead of
  overwriting it (`auth::trusted_header_sets_operator`). The setup is safe only
  when nothing but the proxy can reach the server and the proxy overwrites the
  header; `admin/README.md` states both.

### 2.7 No roles

- **Source:** spec §2 non-goals (call).
- **Decision:** Every authenticated operator can see and do everything.
- **Why:** Roles add configuration most deployments don't need; a host that
  wants read-only users can block non-GET requests in its own layer.
- **Expected behaviour:** Any authenticated user can cancel, replay, bulk act,
  and pause. A database role with only SELECT grants can view but not act
  (`admin/README.md`, Database role).

### 2.8 CSRF check on every state-changing request

- **Source:** spec §6.1 (call); **Ledger ruling (Task 9)**: the guard applies
  to every method other than GET, HEAD and OPTIONS, not only POST.
- **Decision:** Browsers attach basic-auth credentials and proxy cookies to
  cross-site requests, so every unsafe request, `/api/*` included, passes:
  1. If `Sec-Fetch-Site` is present, allow `same-origin` and `none`; reject
     anything else.
  2. Else if `Origin` is present, allow it only if it equals `http://` or
     `https://` plus the request's `Host`, or equals `public_origin`.
  3. Else allow (scripts and curl send neither header, and still authenticate).
- **Why:** Without it, a hostile page could post a cancel with the operator's
  credentials. Every state-changing method needs the check, and covering DELETE
  (the cron API) costs nothing. Header-less scripts still pass rule 3.
- **Expected behaviour:** `Sec-Fetch-Site: cross-site` or `same-site` → 403;
  `Origin: https://evil.example` → 403; `Origin: null` → 403; a matching
  `Origin` → passes; no headers → passes; `Sec-Fetch-Site` wins over a matching
  `Origin`; GET is never checked; behind a proxy that rewrites `Host`, an
  `Origin` equal to `public_origin` passes (`auth::csrf_rejects_cross_site_post`,
  `actions::cross_site_form_post_is_rejected`). The rejection body is
  `cross-site request rejected`. **Divergence:** the spec says "every POST";
  the shipped guard covers every unsafe method.

### 2.9 Names only in query strings; everything escaped

- **Source:** global constraints, review focus 1 and 2.
- **Decision:** Queue and task names may contain any character, so they travel
  only in query strings, percent-encoded. Only numeric task ids go in paths.
  askama escapes every interpolated value.
- **Why:** A name such as `eu/bulk & co #1` would break a path segment, and task
  data (payloads, errors, reasons, labels) is untrusted.
- **Expected behaviour:** Links and forms for queue `eu/bulk & co #1` and task
  `a&b=c` with error `x&y` round-trip exactly
  (`pages::names_with_url_characters_round_trip`,
  `actions::pause_form_with_url_characters`). HTML in a payload, `last_error`,
  reason or worker label renders as text (`pages::task_data_is_escaped`); the
  operator name goes through the same escaping but no test uses a hostile one.

### 2.10 TLS is terminated in front

- **Source:** spec §2 non-goals, §6.2 (call).
- **Decision:** Neither the embedded router nor the standalone server terminates
  TLS. The standalone server warns `basic auth over plain HTTP: terminate TLS in
  front of this server` when basic auth is bound to a non-loopback address.
- **Why:** TLS termination belongs to the proxy or load balancer that already
  handles certificates.
- **Expected behaviour:** Bind to loopback or a private network and put a TLS
  proxy in front. With `bind = "0.0.0.0:…"` and basic auth, the warning appears
  at startup.

### 2.11 No page can be framed

- **Source:** **Ledger ruling (final review, minor 6)**.
- **Decision:** One `map_response` layer sets `X-Frame-Options: DENY` and
  `Content-Security-Policy: frame-ancestors 'none'`. `router(..)` applies it
  with `layer`, not `route_layer`, so unmatched paths and every error response
  carry it; `with_basic_auth` and `with_trusted_header` apply it outside their
  own check, so their 401s carry it too.
- **Why:** A hostile page could otherwise load the admin UI in an invisible
  frame and trick a signed-in operator into clicking Cancel or Pause
  (clickjacking), which the CSRF check (§2.8) does not stop: the click comes
  from the admin page itself.
- **Expected behaviour:** A page, the router's own 401, and the 401s of both
  shipped auth layers carry both headers (`auth::responses_forbid_framing`). A
  host's own auth layer answers its rejections itself, without them.

## 3. Database cost and counts

### 3.1 Every UI query is bounded

- **Source:** spec §8 (binding), global constraints.
- **Decision:** Every query the UI runs is bounded by page size, `count_cap`, the
  number of queues or workers, or the number of rows in a rare state. It must
  not grow with retained rows or throughput. Each list and overview statement
  has an EXPLAIN test that asserts its index and no `Seq Scan` on `queue_tasks`
  or `queue_task_attempts`.
- **Why:** Anti-goal: opening the UI, or leaving it open in several tabs, must
  not measurably slow claims or completions.
- **Expected behaviour:** `tests/performance.rs`:
  `queued_page_uses_claim_and_due_indexes`, `running_page_uses_lease_index`,
  `done_page_uses_terminal_index`, `failed_page_uses_failed_index`,
  `failed_page_by_name_uses_failed_name_index`,
  `overview_distinct_queues_skip_scan_uses_due_index`,
  `overview_due_count_uses_due_index`, `overview_done_window_uses_terminal_index`,
  `overview_dead_window_uses_failed_index`,
  `overview_failed_attempts_use_started_index`,
  `overview_running_uses_lease_index`, `overview_dead_groups_use_failed_index`,
  `overview_no_handler_probe_is_a_limited_due_index_scan`,
  `replay_matching_uses_failed_name_index`,
  `bulk_cancel_batch_reads_only_live_rows`,
  `count_cancellable_reads_only_live_rows`,
  `count_replayable_uses_failed_name_index`,
  `failed_page_generic_plan_uses_failed_index`,
  `failed_page_by_name_generic_plan_uses_failed_name_index`. The last five run
  on an `ANALYZE`d table of 20,000 done rows and a small live backlog, and check
  both the custom and the generic plan. Without an EXPLAIN test: primary-key reads
  (task detail, recent actions) and the small registry and pause tables
  (`store::workers`, `store::paused_queues`). The UI never calls
  `store::queue_stats`, which scans every retained row.

### 3.2 Bulk cancel by name reads every live row

- **Source:** Plan ruling (Task 4, `ponytail:` comment); global constraints
  (the one accepted exception).
- **Decision:** `cancel_matching` and `count_cancellable` find queued and
  running rows through their partial indexes (§3.11) and filter them by
  `task_name`, which has no index there. Dead and cancelled rows have
  `queue_tasks_failed_name_idx`, so bulk replay and its count use an index.
- **Why:** An index on `task_name` over queued rows would be maintained by every
  enqueue and claim, which the hot-path rules forbid.
- **Expected behaviour:** A bulk cancel, and its confirm-page count, read every
  queued and running row, of every queue (a custom plan narrows the queued part
  to the given queue; the generic plan a repeated batch may switch to does
  not), and never the retained done, dead and cancelled rows. With a large
  backlog this takes noticeable time; it is an operator action, not a page
  refresh. Add an index only if enqueue can afford it.

### 3.3 Counts are exact up to `count_cap`

- **Source:** spec §8 (binding).
- **Decision:** Every count reads at most `count_cap` (default 10,000) rows and
  is shown with thousands separators, or as `10,000+` when the cap was reached.
- **Why:** An exact count of a large backlog costs a scan that grows with the
  backlog.
- **Expected behaviour:** `999`, `1,234`, `10,000+`
  (`pages::tests::count_is_exact_below_cap_and_marks_capped`). With
  `count_cap = 5` and 8 due tasks, due is `5+` (`overview::overview_caps_counts`).
  The bulk confirm page shows its capped count as `N+` without separators.

### 3.4 Windowed done and dead counts, and the span they cover

- **Source:** spec §7.3, §8 (call: 1 h window; binding: never show a truncated
  count as exact).
- **Decision:** "Done" and "Dead" per queue count the last `window` (default
  1 h), from one scan per status of the newest `count_cap` rows. If that scan
  hits the cap, every queue's count is marked capped, and the column header
  shows the span actually covered. The failure groups use the same window
  (their failed-attempt count has its own scan and covered span, §3.12).
- **Why:** All-time totals would scan every retained row. A count cut short by
  another queue's volume is a lower bound and must be shown as one.
- **Expected behaviour:** Headers read `Done (1h)` and `Dead (1h)`, or for
  example `Done (last 7m)` when the newest 10,000 done tasks span only 7
  minutes (`pages::tests::windowed_header_shows_covered_span_only_when_short`,
  `overview::overview_caps_counts`).

### 3.5 Distinct queues by skip-scan, through the due index

- **Source:** spec §8; **Ledger ruling (Task 8)**: the skip-scan may use
  `queue_tasks_due_idx` instead of `queue_tasks_claim_idx`.
- **Decision:** The queues with queued tasks come from a recursive CTE that
  jumps from one distinct `queue` to the next through a queued-row index leading
  with `queue`. The planner picks `queue_tasks_due_idx`, the smaller of the two.
- **Why:** Both indexes are partial on queued rows and lead with `queue`; the
  spec requires only a skip-scan bounded by the number of queues.
- **Expected behaviour:** One index probe per distinct queue, however many tasks
  are queued (`performance::overview_distinct_queues_skip_scan_uses_due_index`).
  Every queue with queued tasks appears
  (`overview::overview_queue_set_covers_every_queued_queue`). **Divergence:** the
  spec and plan name the claim index.

### 3.6 The overview is cached and single-flight

- **Source:** spec §7.4 (call: 5 s).
- **Decision:** The overview is computed at most once per
  `overview_cache` (default 5 s) and shared by every viewer; concurrent
  requests wait for the one computation in flight. Errors are cached for the
  same time. Lists and task detail are not cached.
- **Why:** Ten open tabs must cost the database one overview every 5 s, not ten.
- **Expected behaviour:** Two concurrent requests trigger one computation
  (`cache::tests::single_flight`); after the TTL it runs again
  (`cache::tests::expires_after_ttl`). After a database outage, the error page
  can persist for up to 5 s after recovery.

### 3.7 A statement timeout on page reads; actions run outside it

- **Source:** spec §6.1 (call: about 5 s).
- **Decision:** Every page read runs in a transaction whose first statement sets
  `statement_timeout` (default 5 s, via `set_config(…, true)`), and the same
  idle-in-transaction timeout (§3.13); both end with the transaction. Actions
  (cancel, replay, pause, bulk) run on a plain connection as their own
  statements.
- **Why:** No page query runs longer than the timeout, and no read cut off
  mid-transaction holds its connection's locks longer than that (§3.13).
  Actions have their own bounds (§5).
- **Expected behaviour:** Both timeouts apply inside the read and are gone after
  it (`api::read_sets_statement_timeout_for_its_transaction_only`). A page whose
  read exceeds it is a 503 error page; the JSON API answers 500 (503 only when
  no connection is available). **Divergence:** the bulk confirm page's
  count also runs outside the read transaction, so a slow `count_cancellable`
  over a large live backlog (§3.2) is not cut off at 5 s; it is still capped at
  `count_cap` matching rows.

### 3.8 Small pages, keyset paging, next links only

- **Source:** spec §5.7, §6.1 (call: page size 50).
- **Decision:** Lists show 50 rows and a "Next" link built from a keyset cursor
  carried in query fields. No page numbers or totals.
- **Why:** Offsets and totals grow with the table.
- **Expected behaviour:** A queue with 60 due tasks shows 50, and Next shows the
  other 10 (`pages::queued_page_lists_in_dequeue_order_with_next_link`). An
  incomplete cursor is 400.

### 3.9 Overview statements see slightly different moments

- **Source:** Ledger note (Task 8).
- **Decision:** The overview runs about ten statements in one read-committed
  transaction, so each sees its own snapshot.
- **Why:** Display only; one repeatable-read snapshot across them would buy
  little.
- **Expected behaviour:** On a busy system, numbers in different sections (for
  example a queue's due count and the running count) can be a few milliseconds
  apart and not add up exactly.

### 3.10 New indexes never tax the hot path

- **Source:** spec §3, §5.1.7 (call: exact set left to the builder).
- **Decision:** `0003` adds `queue_tasks_failed_idx` and
  `queue_tasks_failed_name_idx` (partial on `dead`/`cancelled`) and
  `queue_admin_actions_task_idx`. It widens `queue_tasks_terminal_cleanup_idx` to
  include `cancelled`. Nothing on queued or running rows, and nothing on
  `queue_task_attempts` (§3.12).
- **Why:** Enqueue, claim and running→done must not maintain a new index.
- **Expected behaviour:** Enqueue, claim and completion write exactly the
  indexes they wrote before `0003`, and closing an attempt can stay a HOT update
  (`performance::attempt_close_stays_hot_eligible`). The one extra hot-path work
  is the pause check in claims (§5.15).

### 3.11 Bulk cancel writes its status test as `OR`

- **Source:** **Ledger ruling (final review)**, fixing its Critical 1.
- **Decision:** The bulk-cancel batch (`CANCEL_BATCH`) and `count_cancellable`
  test `(status = 'queued' OR status = 'running')`, never
  `status IN ('queued', 'running')`.
- **Why:** PostgreSQL cannot match an `IN` list (a ScalarArrayOp) to the
  partial indexes on `status = 'queued'` and `status = 'running'`, so the `IN`
  form planned as a seq scan over every retained row: on PostgreSQL 13 with
  300,000 done rows, a (parallel) Seq Scan of `queue_tasks`. The `OR` form plans
  as a BitmapOr over `queue_tasks_due_idx` and `queue_tasks_expired_lease_idx`,
  for custom plans and for the generic plan alike.
- **Expected behaviour:** On an `ANALYZE`d table with 20,000 done rows, both
  statements use both partial indexes and no `Seq Scan on queue_tasks`, under
  `force_custom_plan` and `force_generic_plan`
  (`performance::bulk_cancel_batch_reads_only_live_rows`,
  `performance::count_cancellable_reads_only_live_rows`). With the `IN` form
  both tests fail with a seq scan. Write any new status test on live rows the
  same way.

### 3.12 Failed attempts come from the newest attempts, not their own index

- **Source:** **Ledger ruling (final review)**, replacing the plan's
  `queue_task_attempts_failed_idx`.
- **Decision:** `0003` adds no index on `queue_task_attempts`. The overview's
  failed-attempt groups scan the newest `count_cap` attempts with
  `started_at >= now() - window`, newest first, through the existing
  `queue_task_attempts_started_idx`, and count those with outcome `retry` or
  `lease_expired` by task name and error (joining tasks by primary key). If the
  scan read `count_cap` attempts, each group's count is capped (a lower bound)
  and `Overview::attempts_covered` is `generated_at` minus the oldest scanned
  start; otherwise it is the window, like `done_covered` and `dead_covered`.
  The "Failed attempts" header shows that span (§3.4).
- **Why:** The partial index on failed attempts blocked HOT on every attempt
  close, because both its key (`finished_at`) and its predicate (`outcome`)
  change then: 0 of 5,000 closes were HOT with it, 4,953 without. Every
  completion then wrote all of the attempts indexes, against spec §3. Spec §3's
  intent and the anti-goal "measurably slows completions" outrank an exact
  failed-attempt window. `0003` was unreleased, so it was edited in place.
- **Expected behaviour:** The statement plans as a limited index scan of
  `queue_task_attempts_started_idx` with no seq scan
  (`performance::overview_failed_attempts_use_started_index`). With
  `count_cap = 5` and eight attempts a minute apart (one done, seven retries),
  the group counts `4+` and `attempts_covered` is about 5 minutes; with the
  default cap it counts 7 over the whole window
  (`overview::overview_caps_failed_attempts`); the page header then reads, for
  example, `Failed attempts (last 2m)`
  (`pages::failed_attempts_header_shows_covered_span`). Any index on
  `queue_task_attempts` that names `finished_at`, `outcome` or `error` in its
  key, INCLUDE list or predicate fails
  `performance::attempt_close_stays_hot_eligible`. Cost: on a busy system the
  attempt counts cover a shorter span than the window (stated on the page), an
  attempt that started before the window is not counted even if it failed
  inside it, and in-flight and successful attempts use up the cap. Dead counts
  stay window-wide.

### 3.13 Transactions left idle are ended by the server

- **Source:** **Ledger ruling (final review)**.
- **Decision:** `db::read` also sets `idle_in_transaction_session_timeout` to
  the statement timeout, and the core cron materializer's per-job transaction
  (`store::materialize_due_jobs`) sets it to 30 s, both with
  `set_config(…, true)` inside the transaction.
- **Why:** When a future holding a transaction is dropped (a client disconnects
  mid-read, or shutdown cuts off the materializer), diesel-async leaves the
  transaction open on the pooled connection, and deadpool only discards it at
  the next checkout. Until then it sits idle in transaction, holding its locks:
  a read's AccessShareLocks, which block DDL such as a migration, or the
  materializer's lock on its schedule row, which other workers then skip. The
  materializer case predates the admin UI.
- **Expected behaviour:** Inside a read both settings are in force and both are
  gone afterwards on the same connection
  (`api::read_sets_statement_timeout_for_its_transaction_only`); inside the
  materializer's transaction the idle timeout is `30s`, and `0` after it
  (`materialize::materializer_transaction_ends_itself_when_left_idle`, `cron`
  feature). A transaction left idle that long ends with its session, releasing
  its locks; the pool discards the closed connection. Cost: a legitimately idle
  transaction over the limit would be ended too; neither path has one.

### 3.14 The Failed and Cancelled lists repeat their status set as a literal

- **Source:** PR #5 review (`docs/reviews/v2-review-002.md`, finding 1).
- **Decision:** `FAILED` and `FAILED_BY_NAME` test `status = $1` and also the
  literal `status IN ('dead', 'cancelled')`, the predicate of
  `queue_tasks_failed_idx` and `queue_tasks_failed_name_idx`.
- **Why:** A partial index is usable only when the planner can prove the
  query's `WHERE` implies the index predicate. A custom plan sees `$1 = 'dead'`
  and can; a generic plan sees an unknown `$1` and cannot, so a prepared
  statement that switches to its generic plan read the Failed page as a seq
  scan and sort of every retained row. `FailedStatus` admits only `Dead` and
  `Cancelled`, so the literal never removes a row the caller asked for.
- **Expected behaviour:** On an `ANALYZE`d table with 20,000 done and 1,000
  dead rows, both lists use their failed index and no `Seq Scan on
  queue_tasks`, under `force_custom_plan` and `force_generic_plan`
  (`performance::failed_page_generic_plan_uses_failed_index`,
  `performance::failed_page_by_name_generic_plan_uses_failed_name_index`).
  Without the literal both tests fail with a seq scan. Any new query on a
  partial index whose status is a bind parameter repeats the index predicate
  as a literal the same way.

## 4. Health rules

### 4.1 Queue status, first match wins

- **Source:** spec §7.2 (call: rules, defaults and reasons; overridable per
  queue).
- **Decision:** For each queue, the first rule that matches:
  1. **paused**: the queue has a `queue_pauses` row. Reason `paused`.
  2. **stalled**: tasks are due, and either no live worker serves the queue
     (reason `no live worker`), or no serving worker has finished a task there,
     or if it never has, started, within `stall_after` (reason
     `nothing finished in 5m`).
  3. **backlog**: the oldest due task has waited longer than `backlog_after`.
     The reason is the first that applies: `N task names have no handler`;
     `all N slots busy` (running ≥ the serving workers' summed limits);
     `oldest due task waited 14m 03s`.
  4. **healthy**, with no reason.
- **Why:** "Not being processed" must never read as healthy, and every status
  must say why.
- **Expected behaviour:** Paused beats stalled; no worker → stalled; last finish
  6 min ago → `nothing finished in 5m`; a worker started 1 min ago that has
  finished nothing is not stalled; each backlog reason; nothing due → healthy,
  even with no worker (`health::tests::table`). A queue whose only work is
  running tasks of a dead worker reads healthy until the reaper requeues them;
  the Running list shows their lease expiry. Durations render as `5m`,
  `14m 03s`, `1h 05m`.

### 4.2 Stall detection from the registry

- **Source:** spec §5.2, §7.2 (call).
- **Decision:** "Nothing finished" uses each worker's in-memory
  `last_finished_at` per queue, published in the registry (§6.5), not a query
  over finished tasks.
- **Why:** No index has `queue` among completed rows, and spec §3 rules out adding
  one; the registry costs nothing.
- **Expected behaviour:** The stall check lags reality by up to
  `registry_interval` (15 s) plus the overview cache. A queue whose tasks
  normally run longer than `stall_after`, with every slot busy, reads stalled
  until one finishes: raise that queue's `stall_after`.

### 4.3 Thresholds and overrides

- **Source:** spec §6.3, §7.2 (call).
- **Decision:** `backlog_after` and `stall_after` default to 5 minutes, with
  per-queue overrides (`Thresholds::per_queue`, or
  `[thresholds.queues."name"]` in TOML, where a missing key falls back to the
  top-level value).
- **Why:** A bulk queue tolerates waits a user-facing queue does not.
- **Expected behaviour:** With `[thresholds.queues."bulk"] backlog_after_seconds =
  1800`, `bulk` uses 30 min for backlog and the default for stall
  (`pg-task-admin config::parses_full_example`).

### 4.4 Lost workers never serve a queue

- **Source:** spec §5.2 (call).
- **Decision:** A worker whose row has not been written for 3 ×
  `registry_interval` is lost. Lost workers are listed greyed out and never
  count as serving a queue, in the status rules, the slot totals or the
  no-handler check.
- **Why:** A killed process leaves its row behind; counting it would hide the
  outage.
- **Expected behaviour:** A due task in `email` whose only worker row is lost:
  the alert lists `email` and the row reads `stalled`, `no live worker`
  (`pages::lost_worker_does_not_serve_queue`).

### 4.5 No-handler probe

- **Source:** spec §6.4 item 5 (call: oldest 100 due tasks per queue).
- **Decision:** For each queue with due tasks and a live worker, read the
  task names of the oldest `no_handler_probe` (100) due rows and report those
  that no live serving worker lists in `task_names`. A queue with no live worker
  is left to the status rule.
- **Why:** Claims filter on registered names, so such a task is never dequeued.
  A full scan of due rows would be unbounded.
- **Expected behaviour:** A live worker serving `default` with handler `a`, due
  tasks `a` and `b`: "No handler" lists `default: b`
  (`overview::overview_finds_unhandled_due_names`). Names that first appear
  beyond the oldest 100 due rows are not reported; the queue still turns
  backlog once its oldest due task is old enough.

### 4.6 A database error is never a verdict

- **Source:** review focus 4; spec anti-goal.
- **Decision:** If building the overview fails, the page is a 503 with
  `Overview unavailable: {error}` and no statuses.
- **Why:** The UI must never render "all good" when it doesn't know.
- **Expected behaviour:** With `queue_tasks` dropped, `GET /` is 503, contains
  `Overview unavailable`, and does not contain `healthy`
  (`pages::overview_reports_database_error`).

### 4.7 Failure groups

- **Source:** spec §6.4 item 4 (call).
- **Decision:** Failures in the window grouped by task name and error, with two
  counts: dead tasks with that `last_error`, and failed attempts (outcome
  `retry` or `lease_expired`) with that error, among the newest `count_cap`
  attempts started in the window (§3.12). Groups are sorted by the sum, and
  the top 20 are shown.
- **Why:** An operator needs "what is failing and how often" in one line, and a
  retrying task is not yet dead. Counting tasks waiting to retry would need
  another index on `queue_tasks`.
- **Expected behaviour:** Two `retry` attempts and one dead task for
  `send`/`smtp refused` form one group with Dead 1 and Failed attempts 2
  (`overview::overview_groups_failures`). The second column counts attempts, not
  tasks. A dead task with no `last_error` groups under an empty error.

### 4.8 The alert

- **Source:** spec §2 goal 6 (user: queues with no registered worker; call:
  plus paused queues, dismissal).
- **Decision:** A dismissible alert at the top lists queues that are stalled
  with `no live worker`, and paused queues. Dismissal is stored in the
  browser's `localStorage`, keyed by the sorted list of queue names.
- **Why:** Those two states mean no work will happen on a queue until someone
  acts.
- **Expected behaviour:** A due task in `email` and no worker: the alert lists
  `email` and its row reads `stalled` (`pages::overview_alerts_unserved_queue`).
  A dismissed alert stays hidden on refresh until the set of listed queues
  changes. A queue with no worker but nothing due is not in the alert (rule 2
  needs due tasks).

## 5. Actions: cancel, replay, bulk, pause

### 5.1 What an operator can do

- **Source:** spec §2 goal 7 (user).
- **Decision:** Replay one task, or all tasks with a name; cancel one task, or
  all tasks with a name; pause and resume a queue. Every action needs a reason,
  and records the authenticated operator.
- **Why:** These are the interventions an incident needs.
- **Expected behaviour:** The task page shows Cancel on queued and running
  tasks and Replay on dead and cancelled ones; the overview rows show Pause or
  Resume; failure groups offer "Replay all…". An empty reason is 400
  (`actions::cancel_requires_reason`). Cancel and replay redirect (303) to the
  task; pause and resume redirect to `{base}/overview`
  (`actions::cancel_and_replay_via_forms`,
  `actions::pause_and_resume_queue_via_forms`).

### 5.2 Cancel covers queued and running tasks

- **Source:** spec §5.3 (user: single and by name, including running tasks;
  call: the rest).
- **Decision:** New admin-only transitions `queued → cancelled` and
  `running → cancelled`. A running cancel clears `locked_by`/`locked_until`,
  closes the open attempt with outcome `cancelled` and error `cancelled by
  operator`, and sets `completed_at`.
- **Why:** An operator must be able to stop a task that is already running.
  Fencing already removes the task from its worker.
- **Expected behaviour:** A queued cancel returns `Cancelled`; the row is
  `cancelled` with `completed_at` and `admin_action_id` set, and one `cancel`
  action has `affected = 1` (`cancel::cancel_queued_task`). A running cancel
  closes the attempt as `cancelled`; the old worker's heartbeat then returns
  `LostOwnership` (`cancel::cancel_running_task_closes_attempt_and_fences_worker`).

### 5.3 The worker side: within one heartbeat, nothing undone

- **Source:** spec §5.3 (user accepted both points).
- **Decision:** The worker learns of a cancel through the existing fence: its
  next heartbeat matches no row, the lease keeper drops the handler, and
  `ctx.cancellation` fires.
- **Why:** No new signalling path; ownership stays exactly as it was.
- **Expected behaviour:** A cancelled running task's handler stops within one
  `heartbeat_interval` (default 20 s); anything it already did stays done
  (`cancel::worker_drops_cancelled_handler_and_counts_cancellation`: lease 3 s,
  heartbeat 1 s, token fires within 3 s of the cancel).

### 5.4 A handler never observes its own cancellation

- **Source:** Ledger note (Task 3).
- **Decision:** When the lease keeper gives up (lost lease, operator cancel or
  local deadline), `tokio::select!` drops the handler future before
  `ctx.cancellation` is cancelled.
- **Why:** The lease keeper's arm completes first, and select drops the other
  arms.
- **Expected behaviour:** Code inside the handler's own future never sees the
  token fire on lease loss or cancel; it simply stops being polled. Work the
  handler spawned with a clone of the token does see it, and must stop issuing
  side effects. The test above watches the token from a spawned task for this
  reason. On shutdown, by contrast, the handler keeps running and can observe
  the token.

### 5.5 The worker tells a cancellation from a lost lease

- **Source:** spec §5.3 (call).
- **Decision:** After a heartbeat or transition matches no row, the worker reads
  the task's status under its own `lock_token` (`store::lease_status`).
  `cancelled` → info log `task cancelled by an operator; handler dropped` and
  `pg_task_scheduler_tasks_cancelled_total{task_name}`; `done` → the handler
  completed it itself; anything else → a lost lease, as before.
- **Why:** A bulk cancel of a busy task name must not trip lease-loss alerts.
- **Expected behaviour:** One cancel of a running task counts
  `tasks_cancelled_total = 1` and `leases_lost_total = 0` (same test). A handler
  that finishes before the next heartbeat gets `LostOwnership` on completion,
  and that is also counted as a cancellation
  (`cancel::completion_after_cancel_counts_cancellation`). A cancel followed by
  a replay before the old worker's heartbeat bumps `lock_token`, so that worker
  reports a lost lease instead.

### 5.6 A single cancel never blocks

- **Source:** Plan ruling (Task 3).
- **Decision:** The cancel is one statement that locks the row
  `FOR UPDATE SKIP LOCKED`. If no row was cancelled but a lock-free read in the
  same statement still sees it `queued` or `running`, another transaction holds
  it: retry every 100 ms for up to 2 s, then return `Busy`. A terminal or
  missing task is `NotCancellable`.
- **Why:** A DB-contained handler can hold the row with `lock_owned` for as long
  as its transaction runs, and an admin action must never stall a worker or wait
  behind one. This gives the spec's "short lock timeout" without parsing
  SQLSTATE.
- **Expected behaviour:** With the row held `FOR UPDATE` elsewhere, `cancel`
  returns `Busy` in under 3 s and the row is unchanged
  (`cancel::cancel_locked_row_is_busy`); the form shows 409 `Task is busy; try
  again.` Done, dead, cancelled and missing tasks are `NotCancellable`, shown as
  409 `Task is already finished or does not exist.`
  (`cancel::cancel_terminal_or_missing_is_not_cancellable`). **Divergence:** the
  spec describes a `lock_timeout` of about 2 s; the shipped cancel never waits on
  a lock at all.

### 5.7 The attempt is closed by upsert

- **Source:** **Ledger ruling (Task 3)**: attempt close on cancel is an
  `INSERT … ON CONFLICT (task_id, lock_token) DO UPDATE`, not a plain `UPDATE`.
- **Decision:** Single and bulk cancel close a running task's attempt by
  inserting it, or on conflict updating the existing row where `finished_at` is
  NULL.
- **Why:** Under read committed, a claim that commits after the cancel's
  snapshot but before its row lock leaves an attempt row the snapshot cannot
  see. The row lock follows the claim's update, but an `UPDATE … FROM` would
  miss the attempt; `ON CONFLICT` finds it anyway.
- **Expected behaviour:** A cancelled running task always has exactly one
  attempt row for its last `lock_token`, closed as `cancelled`. The race cannot
  be forced deterministically, so it is argued (in
  [Concurrency](Concurrency.md#admin-actions)) rather than tested.

### 5.8 `cancelled` is its own terminal status

- **Source:** spec §5.3 (call).
- **Decision:** `cancelled` is terminal and separate from `dead`. It doesn't
  count as a failure in overview health, has its own list, is kept for
  `Retention.dead`, and cleanup deletes it with a third bounded statement
  sharing the batch budget.
- **Why:** An operator's decision is not a task failure; mixing them would
  pollute failure counts and groups.
- **Expected behaviour:** Cancelled tasks appear under Cancelled, not Failed, and
  not in failure groups or the Dead column. They are deleted after
  `Retention.dead` (`maintenance::cleanup_deletes_expired_cancelled_tasks`), and
  the terminal CHECK requires their `completed_at`
  (`maintenance::cancelled_rows_obey_terminal_check`).

### 5.9 A cancelled task keeps its `task_key`

- **Source:** spec §5.3 (call; "document this").
- **Decision:** A cancelled row keeps its key until retention deletes it.
- **Why:** The key's idempotency horizon must hold for every terminal state.
- **Expected behaviour:** Enqueueing the same key again returns the existing
  cancelled task (`inserted = false`) and does not run it. Replay runs it. No
  test pins this; it follows from `task_key`'s unique constraint, which covers
  every status.

### 5.10 Replay accepts dead and cancelled tasks

- **Source:** spec §5.4 (call).
- **Decision:** `queue::replay` moves a `dead` or `cancelled` task to `queued`:
  `attempts = 0`, `due_at = now()`, `lock_token + 1`, `last_error` cleared,
  `admin_action_id` set, one `replay` audit row, then
  `NOTIFY queue_available`. It locks the row `FOR UPDATE` (dead and cancelled
  rows are not normally locked by anyone).
- **Why:** Replay is the way back from a mistaken cancel as well as from a
  failure.
- **Expected behaviour:** A cancelled task replays to queued with attempts 0 and
  no last error, linked to the new action
  (`admin_audit::replay_of_cancelled_task_requeues_it`). A queued, running or
  done task cannot be replayed: the form shows 409 `Task is not dead or
  cancelled, or does not exist.`

### 5.11 Bulk actions by task name

- **Source:** spec §5.4 (user: by name; call: mechanics); Plan ruling
  (Task 2: `max_task_id` stored).
- **Decision:** `cancel_matching` (queued and running) and `replay_matching`
  (dead and cancelled) take a task name, an optional queue, and for replay an
  optional exact `last_error`. Each call:
  1. inserts one audit row with `max_task_id = max(id)` from `queue_tasks`, in
     the same statement;
  2. runs batches of 500 rows, each one statement and transaction:
     `id ≤ max_task_id`, `FOR UPDATE SKIP LOCKED`, the same transition as the
     single action, and `affected = affected + n` on the audit row;
  3. sends one `NOTIFY` after each non-empty replay batch;
  4. stops when a batch is short (`more_remain = false`), or after a full batch
     once the budget (10 s) has passed (`more_remain = true`).
  The bulk cancel includes running tasks.
- **Why:** The id bound makes a call finish even while producers keep enqueuing
  the name; storing it shows in the audit which tasks the action could cover.
  Short batches never hold many locks or stall claims. Running tasks are
  included because a running task that then fails would retry and slip back
  into the queue (user accepted).
- **Expected behaviour:** Cancel `a` with 3 queued (one claimed) and 2 `b`:
  `affected = 3`, every `a` cancelled and linked, `b` untouched
  (`bulk::cancel_matching_cancels_queued_and_running_of_that_name_only`). Tasks
  enqueued during the call are not touched; every row with this action has
  `id ≤ max_task_id`
  (`bulk::cancel_matching_ignores_tasks_enqueued_after_it_started`). With 1,200
  tasks and a zero budget: 500 per call with `more_remain`, then a final short
  batch (`bulk::bulk_stops_at_budget_and_continues`). Replay with error `x`
  leaves the `y` rows dead (`bulk::replay_matching_filters_by_last_error`).
  To stop new arrivals, pause the queue or fix the producer.

### 5.12 Concurrent and double-submitted bulk actions split the work

- **Source:** review focus 5; spec §5.4 (call).
- **Decision:** `SKIP LOCKED` in every batch; each statement counts only the rows
  it changed.
- **Why:** A double-clicked Continue, or two operators acting at once, must not
  count a task twice or fail.
- **Expected behaviour:** Two concurrent cancels of 1,200 tasks: `affected`
  values sum to 1,200, 1,200 rows are cancelled, two action rows exist, and
  neither call errors (`bulk::concurrent_bulk_cancels_split_the_work`).

### 5.13 `more_remain = false` does not mean nothing is left

- **Source:** Ledger note (Task 4).
- **Decision:** A batch shorter than 500 ends the call. A batch can be short
  because rows were locked elsewhere (`SKIP LOCKED`), or because a row changed
  between the batch's snapshot and its lock and no longer matched.
- **Why:** Waiting for locked rows would make an admin action block workers.
- **Expected behaviour:** After a bulk run reports no more, tasks held by a
  `lock_owned` handler or a concurrent claim can still match. Open the confirm
  page again to recount, and run it again if needed.

### 5.14 The bulk confirm step

- **Source:** spec §5.4, §6.4 (call).
- **Decision:** `GET {base}/bulk` shows the capped match count and asks for the
  task name typed again and a reason. `POST {base}/bulk` runs one budgeted call
  and shows the count; if more remain, a Continue button re-posts the same
  fields, which is a new action with its own audit row and id bound.
- **Why:** Bulk actions are hard to undo; typing the name prevents a slip.
- **Expected behaviour:** A mismatched name is 400 `Type the task name to
  confirm.` and nothing changes (`actions::bulk_requires_typed_name`). Three
  queued `a`: the confirm page shows `3`, the result `3 cancelled` with no
  Continue (`actions::bulk_cancel_runs_and_offers_continue`). The name must match
  exactly, including case and spaces.

### 5.15 Pause: running tasks finish, nothing new or retried is claimed

- **Source:** spec §5.5 (user: running tasks finish, no new or retry claims;
  call: mechanics).
- **Decision:** A pause is a `queue_pauses` row, for every worker at once,
  surviving restarts and deploys. The claim statement skips a paused queue with
  `NOT EXISTS` on that table's primary key, and `next_due_in` ignores paused
  queues. Resume deletes the row and sends `NOTIFY queue_available`. Pause and
  resume are idempotent, and write an audit row only when the state changes. A
  queue needs no tasks to be paused, only a valid name.
- **Why:** An operator needs to stop a queue's work now, without a deploy, while
  letting work in progress finish.
- **Expected behaviour:** A paused queue's due task is not claimed; after resume
  it is (`pause::paused_queue_is_not_claimed`). A running task completes while
  paused (`pause::running_task_completes_while_paused`). A task retried while
  paused is queued and not claimed (`pause::retried_task_waits_while_paused`).
  Idle workers don't wake for a paused queue's due tasks
  (`pause::next_due_in_ignores_paused_queues`). Pause twice gives `true` then
  `false` with one audit row; the same for resume
  (`pause::pause_is_idempotent_and_audited`). After resume, a worker with a 5 s
  poll interval runs the task within 2 s, woken by the notification
  (`pause::worker_resumes_promptly`). Producers and cron keep enqueuing; tasks
  released or reaped while paused wait in `queued`.

### 5.16 Pause against a claim already in flight

- **Source:** spec §5.5 (call).
- **Decision:** The pause check is part of the claim statement's own snapshot.
- **Why:** Locking out in-flight claims would need a lock every claim takes.
- **Expected behaviour:** A claim that started before the pause committed may
  still take tasks from the queue. Every claim after that skips it.

## 6. Worker registry

### 6.1 Display only

- **Source:** spec §3, §5.2 (call).
- **Decision:** `queue_workers` is written by each worker and read only by the
  UI. The ownership predicate and `LostOwnership` are unchanged.
- **Why:** A display feature must not become a correctness dependency.
- **Expected behaviour:** Deleting or corrupting the registry changes what the
  UI shows, never what workers do.

### 6.2 What a row holds; the label

- **Source:** spec §5.2 (call).
- **Decision:** One row per worker: `worker_id`, `label`, `started_at`,
  `last_seen_at`, `lost_after`, `status` (`starting`, `healthy`, `degraded` or
  `stopping`, from the worker's health), `last_error`, `queues` (JSONB: name,
  limit, in use, paused, last finished, in configuration order) and
  `task_names`. `.label(..)` sets the label; it defaults to `HOSTNAME` when set
  and non-empty, else NULL, and the UI shows the worker id instead.
- **Why:** These are what an operator needs to see which workers serve which
  queues and whether they keep up.
- **Expected behaviour:** A worker with `.label("w1")`, queues `default`/2 and
  `bulk`/1 and two handlers has, within 1 s, label `w1`, status `starting` or
  `healthy`, both queues with limit, 0 in use, not paused, no last finish, both
  task names, and `lost = false`
  (`registry::worker_registers_queues_and_task_names`).

### 6.3 `registry_interval`

- **Source:** spec §5.2 (call: 15 s, validated like the other intervals).
- **Decision:** A builder setting, default 15 s. `build()` rejects zero and
  values over 5 years.
- **Why:** Frequent enough for a live view, cheap enough to ignore: one upsert
  per worker per interval.
- **Expected behaviour:** `registry_interval(Duration::ZERO)` fails to build
  (`builder::tests::build_rejects_zero_registry_interval`).

### 6.4 `lost_after` is stored, so readers need no worker config

- **Source:** Plan ruling (Task 6).
- **Decision:** An extra column, `lost_after = now() + 3 × registry_interval`,
  written at every upsert. `lost = lost_after < now()`.
- **Why:** Workers may run different intervals, and the standalone UI knows none
  of them.
- **Expected behaviour:** A row whose `lost_after` has passed reads as lost
  (`registry::lost_and_pruned_workers`). **Divergence:** the spec's column list
  does not include `lost_after`.

### 6.5 `last_finished_at` from the worker's own clock, stamped by PostgreSQL

- **Source:** spec §5.2 (call: tracked in memory); Plan ruling (Task 6:
  computed by PostgreSQL).
- **Decision:** Each queue keeps, in memory, the `Instant` its last dispatch
  ended with any outcome. The upsert sends the elapsed time, and PostgreSQL
  stores `now() - elapsed`.
- **Why:** In-memory tracking costs the database nothing, and using the
  database's clock means host clock skew doesn't matter.
- **Expected behaviour:** After a task finishes, `last_finished_at` is within 5 s
  of the database's `now()` (`registry::registry_reports_last_finished_and_paused`).

### 6.6 `paused` is read inside the upsert

- **Source:** Plan ruling (Task 6).
- **Decision:** Each queue entry's `paused` flag comes from `queue_pauses` in the
  same statement that writes the row.
- **Why:** The worker doesn't track pauses; the database already knows.
- **Expected behaviour:** Within one `registry_interval` of a pause, the
  worker's entry for that queue shows `paused = true` (same test).

### 6.7 Lifecycle: write, refresh, `stopping`, delete, prune

- **Source:** spec §5.2 (call).
- **Decision:** The worker upserts its row at start and every
  `registry_interval`. When shutdown begins it writes `stopping` once and stops
  refreshing; after the drain it deletes the row. Every worker's maintenance
  deletes rows not written for 24 h.
- **Why:** A clean shutdown leaves no row; a killed worker's row turns lost and
  is eventually removed by anyone.
- **Expected behaviour:** During a drain the row reads `stopping` within 1 s of
  the shutdown signal; after `run_until_shutdown` returns it is gone
  (`registry::worker_row_stopping_during_drain_then_removed`). A row last seen
  25 h ago is pruned (`registry::lost_and_pruned_workers`).

### 6.8 Shutdown writes are bounded by 500 ms each

- **Source:** **Ledger ruling 2 (pre-flight)**.
- **Decision:** The `stopping` write and the delete are each bounded by 500 ms
  (`REGISTRY_SHUTDOWN_TIMEOUT`), and every other await in the registry task
  races the shutdown signal.
- **Why:** Two 2 s bounds would break the guarantee that shutdown on a stalled
  pool returns within 2 s; 500 ms each adds at most 1 s.
- **Expected behaviour:** Shutdown on a stalled pool still returns within 2 s
  (`shutdown::shutdown_returns_when_the_pool_stalls`). The delete runs after the
  drain, so set the orchestrator's grace period above `shutdown_timeout + 6 s`
  (5 s of releases, 0.5 s for the delete). On a slow but live database, a write
  that times out leaves the row to read as lost after 3 × `registry_interval`
  and be pruned after 24 h; display only, so harmless. A timed-out write counts
  in `registry_failures_total`.

### 6.9 A new worker reads `starting` until its second write

- **Source:** Ledger note (Task 6).
- **Decision:** The first upsert happens at once, before the first claim cycle
  sets the worker healthy; the next is a `registry_interval` later.
- **Why:** Writing at once makes a worker visible immediately.
- **Expected behaviour:** For up to 15 s after start, a worker shows `starting`
  even though it is already claiming.

### 6.10 A draining worker's row is frozen

- **Source:** Ledger note (Task 6).
- **Decision:** After the `stopping` write the registry task exits, so the row
  is not refreshed during the drain.
- **Why:** The registry must not delay or outlive shutdown (§6.8).
- **Expected behaviour:** If the drain lasts longer than 3 × `registry_interval`
  (45 s by default), the stopping worker reads as lost. With the defaults
  (`shutdown_timeout` 25 s plus at most 5 s of releases) it doesn't.

### 6.11 A late write can briefly re-create a deleted row

- **Source:** Ledger note (Task 6).
- **Decision:** A periodic upsert cut off by shutdown, or a `stopping` write that
  timed out, may still execute on the server after the delete.
- **Why:** Dropping a future does not cancel a statement already sent.
- **Expected behaviour:** Rarely, a cleanly stopped worker reappears, reads
  live for up to 3 × `registry_interval`, then lost, and is pruned after 24 h.

### 6.12 Registry failures never affect work

- **Source:** spec §5.2 (call).
- **Decision:** A failed upsert, shutdown write or prune is logged at warn and
  counted in `pg_task_scheduler_registry_failures_total`. It never changes
  health or claiming.
- **Why:** Display only (§6.1).
- **Expected behaviour:** With `queue_workers` dropped while a worker runs,
  enqueued tasks still complete (`registry::registry_failure_does_not_stop_claiming`).

## 7. Pages and refresh

### 7.1 Server-rendered, no build step

- **Source:** spec §6.4 (call).
- **Decision:** askama templates, one embedded stylesheet and one small script
  for refresh and alert dismissal. No JavaScript framework, build step or CDN.
- **Why:** It works air-gapped and has nothing to keep up to date.
- **Expected behaviour:** `{base}/assets/admin.css` and `{base}/assets/admin.js`
  are served from the binary (`pages::overview_links_respect_base_path`). Pages
  work with JavaScript off, minus refresh and dismissal.

### 7.2 Which queues get a row

- **Source:** spec §6.4 item 3 (call).
- **Decision:** One row per queue that has queued or running tasks, a live
  worker serving it, or a pause, sorted by name. Columns: queue, status and
  reason, live workers serving it, slots in use / total, queued (every queued
  task, due or not), due, oldest due wait, Done (window), Dead (window), and a
  Pause or Resume form.
- **Why:** A queue with only finished tasks and no worker needs no attention.
- **Expected behaviour:** A paused or registered queue appears with no tasks
  (`overview::overview_lists_paused_and_registered_queues_without_tasks`). A
  queue with 2 due and 3 future tasks reads Queued 5 and Due 2
  (`pages::overview_shows_queued_total_beside_due`); both are capped counts
  (§3.3).
  "Slots in use" counts the queue's running tasks across all workers; "total"
  sums the live serving workers' limits.

### 7.3 The lists

- **Source:** spec §2 goals 1–4 (user), §6.4 item 7 (call: columns).
- **Decision:** Workers; Queued (one queue, in dequeue order); Running;
  Completed (newest first); Failed (dead only, newest first, with last error);
  Cancelled (same columns as Failed). Failed and Cancelled take `task_name` and
  exact `error` filters.
- **Why:** The user's goals: workers and their queues, and a total plus a
  paginated list for queued, completed and failed tasks.
- **Expected behaviour:** Every list route resolves
  (`pages::all_list_routes_resolve`); a failure group's link filters Failed to
  exactly that name and error (`pages::failed_page_filters_by_group`). The
  totals are the overview's capped counts: Queued (and Due) for queued tasks
  (§7.2), and Done and Dead over the window for completed and failed ones
  (§3.4). Queued without a queue lists the known queues to pick from
  (`pages::queued_without_queue_offers_known_queues`).

### 7.4 Queued: dequeue order, split at `now()`

- **Source:** spec §5.7 (user: the order they will be dequeued); Ledger note
  (Task 7).
- **Decision:** Due tasks first in the claim order `(priority DESC, due_at, id)`,
  then not-yet-due tasks by `(due_at, id)`, with one keyset cursor spanning both
  and a running position number.
- **Why:** That is the order a worker would take them.
- **Expected behaviour:** Priorities 10, 10, 0 and two future tasks list in that
  order across pages, with positions continuing
  (`browse::queued_page_matches_claim_order_across_segments`). The split is
  relative to the database's `now()` at each load, so a task can move from the
  second segment to the first between loads; if it then ranks ahead of the
  cursor, Next skips it. Tasks claimed or enqueued between loads shift the
  positions too. The list shows every name in the queue; a worker claims only
  the names it handles. The Waiting column is computed with the admin server's
  clock.

### 7.5 Running: by lease expiry

- **Source:** Plan ruling (Task 7).
- **Decision:** Running tasks ordered by `(locked_until, id)`, soonest expiry
  first.
- **Why:** It is the only bounded order on `queue_tasks_expired_lease_idx`, and
  it puts tasks about to lose their lease at the top.
- **Expected behaviour:** `browse::running_page_orders_by_lease_expiry`. Each
  heartbeat moves a task's expiry, so rows reorder between loads. The Worker
  column shows the worker id, not its label.

### 7.6 Task detail

- **Source:** spec §6.4 (call: contents and actions by status).
- **Decision:** Every task field, the payload pretty-printed, the last error,
  every attempt (worker, started, finished, run time, outcome, error), and the
  admin actions on the task (§1.7). Cancel on queued and running; Replay on dead
  and cancelled.
- **Why:** Everything needed to decide whether to act on one task.
- **Expected behaviour:** After a claim, retry and cancel, the detail has one
  attempt with outcome `retry` and a `cancel` action
  (`browse::task_detail_has_attempts_and_actions`). An unknown id is a 404 page
  (`pages::task_page_shows_attempts_and_404s`).

### 7.7 Mounted under a prefix

- **Source:** spec §6.1; review focus 3; **Ledger ruling (Task 10)**: under a
  nested mount the overview is served at `{base}` and `{base}/overview`.
- **Decision:** Every link, asset, form action and redirect is
  `{base_path}/…`. The overview is routed at `/` and `/overview`; the home link
  and redirects use `{base}/overview`.
- **Why:** axum 0.7's `nest` maps the inner `/` to the bare prefix, so
  `{base}/` would 404 and could not be a link target.
- **Expected behaviour:** Under `.nest("/admin", ..)`, `/admin` and
  `/admin/overview` serve the overview, and every `href`, `src` and `action`
  starts with `/admin/` (`pages::overview_links_respect_base_path`); forms post
  and redirect under `/admin/` (`actions::forms_work_under_base_path`). A
  bookmark of `/admin/` is a 404 until the host adds a redirect.
  **Divergence:** the plan expected `GET /admin/` to work.

### 7.8 Failure-group links and "Replay all…"

- **Source:** spec §6.4 item 4; **Ledger ruling (Task 10)**: the links to the
  Failed list came with the list pages, and "Replay all…" with the actions,
  because their target routes did not exist yet.
- **Decision:** Each failure group links to `{base}/failed?task_name=…&error=…`
  and to `{base}/bulk?action=replay&task_name=…&error=…`.
- **Why:** An operator goes from "what is failing" to the tasks, or to replaying
  them, in one click.
- **Expected behaviour:** Both links are on the overview, encoded
  (`actions::overview_offers_resume_and_replay_all`,
  `pages::names_with_url_characters_round_trip`). The error filter is the full
  error text, so the link matches exactly.

### 7.9 Auto-refresh

- **Source:** spec §6.5 (call: the toggle).
- **Decision:** Every page reloads every `refresh_seconds` (default 10; 0 turns
  it off). The reload is skipped while a form field or button has focus, and on
  the bulk confirm and result pages. A failed action's error page renders with
  refresh 0 (**Ledger ruling (final review, minor 5)**). An Auto-refresh
  checkbox, stored in `localStorage`, turns it off for that browser.
- **Why:** A wall display stays current, and an operator typing a reason doesn't
  lose it. An action's error page is the result of a POST; reloading it would
  ask to resubmit, and resubmitting could run the action again.
- **Expected behaviour:** With the cursor in a reason field the page does not
  reload. A half-typed reason in a field that has lost focus is lost on the next
  reload. Unchecking Auto-refresh persists across pages. Storage errors (private
  mode) are ignored. A failed action's page carries `data-refresh="0"`
  (`actions::failed_action_page_does_not_auto_refresh`).

### 7.10 Always dark, in the style of Grafana

- **Source:** user.
- **Decision:** One dark theme whatever the computer's setting
  (`color-scheme: dark`), styled after Grafana:
  - a top bar linking to every list, with the current page underlined;
  - one panel per section, with a small uppercase title;
  - compact tables with tabular figures, and the overview's number columns
    right-aligned;
  - status pills tinted green, amber, red or grey;
  - themed inputs and buttons, with Pause, Cancel and bulk cancel in red.

  It is still one stylesheet and no framework or web font (§7.1). Number
  columns are aligned by position in CSS (`nth-child` on the table's class), so
  cell markup stays plain `<th>`/`<td>`.
- **Why:** The pages are information-dense and were hard to scan. The old dark
  mode switched the background but left links in the browser's default blue
  and form controls light-themed.
- **Expected behaviour:** Links are a soft blue (`#6e9fff`) that is readable on
  the dark background, and inputs, buttons and checkboxes render dark. Every
  page shows the nav bar. The overview no longer ends with a "Lists" section,
  because the nav bar replaced it.

### 7.11 Basic viewport support, no responsive layouts

- **Source:** user.
- **Decision:** Below 700 px wide, the nav bar scrolls sideways under the brand,
  and table rows stay one line high. Each table scrolls inside its panel. There
  is no menu and no stacked "card" form of a table.
- **Why:** The pages should be usable on a phone or a narrow window without
  complex layouts.
- **Expected behaviour:** The page itself never scrolls sideways; only a panel's
  table does.

## 8. Standalone server

### 8.1 Two ways to run

- **Source:** spec §2 goal 9 (user: open either way), §6.3 (call: the shape).
- **Decision:** The same router is mounted by the host application, or served by
  the `pg-task-admin` binary with its own pool. The binary needs only database
  credentials.
- **Why:** Some teams want the UI in their app, others as a separate deployable.
- **Expected behaviour:** Both serve identical pages and routes.

### 8.2 Two commands, parsed by hand

- **Source:** Plan ruling (Task 13).
- **Decision:** `pg-task-admin --config <path>` runs the server;
  `pg-task-admin hash-password` reads one line from standard input and prints
  its argon2id hash. Anything else prints the usage and exits 1. No CLI crate.
- **Why:** Two fixed forms don't justify a dependency.
- **Expected behaviour:** `pg-task-admin hash-password <<< secret` prints a
  `$argon2id$` string. A config error prints `pg-task-admin: …` and exits 1.

### 8.3 TOML config, strict

- **Source:** spec §6.3 (call).
- **Decision:** A TOML file. Unknown keys are errors. `database_url` falls back
  to `DATABASE_URL`. `[auth] mode` is required and is `basic` (with at least one
  user) or `trusted_header`; there is no "none".
- **Why:** A typo in a security setting must fail loudly, not fall back to a
  default.
- **Expected behaviour:** The documented example parses and maps onto
  `AdminConfig` (`config::parses_full_example`); a missing `[auth]`, an unknown
  mode, basic without users, and an unknown key are rejected
  (`config::rejects_missing_auth`, `config::rejects_unknown_mode`,
  `config::rejects_basic_without_users`, `config::rejects_unknown_key`); the
  environment supplies the URL when the file doesn't
  (`config::database_url_falls_back_to_env`). A parse error gives the line and
  the parser's message, never the surrounding config text. The message itself
  can quote a value of the wrong type (a string where a number belongs, say),
  so a secret typed into the wrong key can appear in it (§10). Validation
  errors after parsing (no users, duplicate user names) never include a hash
  (`config::rejects_duplicate_users`). **Divergence:** the spec says "secrets can come
  from the environment"; only the database URL can. Password hashes live in the
  file.

### 8.4 Defaults: a small pool on loopback

- **Source:** spec §6.3 (call).
- **Decision:** `bind = 127.0.0.1:8080`, `pool_size = 4`, and the
  `AdminConfig` defaults for everything else.
- **Why:** The pool size caps the UI's concurrent database load no matter how
  many people have it open. Loopback by default keeps an unconfigured server
  off the network.
- **Expected behaviour:** `config::defaults_apply`. Settings not in the TOML
  (`page_size`, `no_handler_probe`, `failure_groups`, `recent_actions`) take
  their defaults: 50, 100, 20, 10.

### 8.5 `base_path` is the public prefix

- **Source:** spec §6.1, §6.3.
- **Decision:** The standalone server always serves at `/`; `base_path` only
  changes the links it emits.
- **Why:** A reverse proxy that publishes the server under a prefix strips it
  before forwarding.
- **Expected behaviour:** With `base_path = "/admin"` behind a proxy mapping
  `/admin/*` to the server's `/*`, every link works. Without such a proxy, set
  `base_path = ""`.

### 8.6 Logging and shutdown

- **Source:** plan (Task 13).
- **Decision:** `tracing_subscriber` with `RUST_LOG` (default `info`); graceful
  shutdown on Ctrl-C or SIGTERM.
- **Why:** The usual behaviour for a container.
- **Expected behaviour:** In-flight requests finish on SIGTERM. The binary is
  Unix-only (it installs a SIGTERM handler).

## 9. Upgrade

### 9.1 `0003` is required for everyone, and goes first

- **Source:** spec §1, §3, §9 (call: a new migration the host adds to its own
  migrations).
- **Decision:** Every schema change is in `migrations/0003_admin/up.sql`,
  forward-only. Apply it before rolling out workers built from this version.
- **Why:** Cancel, pause and the registry are always on in core (§1.3). New
  workers' claims check `queue_pauses`, which only `0003` creates.
- **Expected behaviour:** New workers against a schema without `0003` fail every
  claim and report degraded. Old workers keep running against `0003` (§9.2).

### 9.2 What pre-upgrade workers do

- **Source:** spec §9 (call).
- **Decision:** Workers from before `0003` keep claiming and completing tasks
  against the new schema, with these limits.
- **Why:** A rolling upgrade must not stop the queue.
- **Expected behaviour:** Pre-upgrade workers:
  - ignore pauses, so a paused queue is still claimed until they are replaced;
  - don't register, so the UI shows their queues as `no live worker`;
  - drop a cancelled running task through the existing fencing, but report a
    lost lease (`leases_lost_total`, a warning) instead of a cancellation;
  - fail to replay, because their replay writes `queue_task_replays`, which
    `0003` drops; old code that loads a cancelled task (`store::task`) reports an
    unknown status.
  Cancelled rows are only deleted by new workers' cleanup.

### 9.3 The migration takes locks

- **Source:** the SQL of `0003`.
- **Decision:** `0003` replaces CHECK constraints on `queue_tasks` and
  `queue_task_attempts` and builds its indexes without `CONCURRENTLY`.
- **Why:** Like `0001` and `0002`, it is plain SQL that the host runs with its
  own migration runner. Runners such as Diesel's wrap each migration in a
  transaction, where `CONCURRENTLY` is not allowed.
- **Expected behaviour:** Replacing a CHECK constraint locks the table
  exclusively and scans it, and an index build blocks writes. In one
  transaction those locks are held until it commits, so on a large
  `queue_tasks` enqueues and claims wait until it finishes. Apply it at a quiet
  time.

### 9.4 Breaking changes

- **Source:** spec §9.
- **Expected behaviour:**
  - core's `axum` feature and `admin` module moved to `pg_task_scheduler_admin`;
  - `queue_task_replays` is replaced by `queue_admin_actions`;
  - `TaskStatus` and `AttemptOutcome` gain `Cancelled`;
  - `queue::replay` accepts cancelled tasks;
  - the JSON routes moved under `/api/` and take the operator from
    authentication.

## 10. Known limits

Found in review and deliberately left; none affects correctness of the queue.

- A trusted header made only of whitespace is accepted as the operator name.
- `public_origin` must match the browser's `Origin` exactly (no trailing slash,
  same case). The CSRF check reads only the `Host` header, not HTTP/2's
  `:authority`, so over HTTP/2 a POST with `Origin` and no `Sec-Fetch-Site`
  passes only through `public_origin` (it fails closed).
- A `statement_timeout` under 1 ms becomes `0`, which means no limit.
- Unknown basic-auth users are verified against a dummy hash with default
  argon2 parameters; a real user hashed with other parameters takes a different
  time to verify.
- The standalone config: `bind` takes an IP address, not a host name;
  `pool_size = 0` and a negative `count_cap` are not rejected; `auth.header` is
  checked only at startup; keys that don't apply to the chosen mode are ignored;
  a TOML parse error can quote a value of the wrong type, which may be a secret
  typed into the wrong key (§8.3).
- The binary's dependencies (`toml`, `tracing-subscriber`, tokio's signal and
  net features) are compiled by embedders too.
- The overview's dismiss button keeps focus after a click, which pauses
  auto-refresh until something else is focused; hidden tabs still reload.
- `/completed` echoes `task_name` and `error` query parameters in its heading
  and Next link without filtering by them. Failed and Cancelled have filter
  links but no filter form.
- The queued list does not hide tasks that have used all their attempts (they
  practically never exist in `queued`).
- With the `cron` feature, a materializer transaction cut off at shutdown is
  ended by the server after 30 s idle (§3.13). Until then, if the host keeps its
  pool alive after `run_until_shutdown` returns, it holds its schedule row lock
  and other workers skip that schedule.
