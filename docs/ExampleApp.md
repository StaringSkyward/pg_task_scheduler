# Example App: Decisions and Expected Behaviour

This is the record of every decision behind `example-app/`, the
`docker compose up` demo of the crate and its admin UI. It says what was
decided, why, and what a newcomer should see as a result. Setup and the guided
tour are in [example-app/README.md](../example-app/README.md).

It describes the code as shipped. Where that differs from the design spec
(`docs/superpowers/specs/2026-11-12-example-app-design.md`, kept out of the
repository), the entry says so. Each entry has a source:

- **(user)**: the user decided it.
- **(call)**: a design call in the spec, open to change.
- **Plan ruling**: decided in the implementation plan.
- **Ledger ruling**: decided during implementation and recorded in the
  implementation ledger (`.superpowers/sdd/2026-11-12-example-app/progress.md`).
- **Ledger note**: behaviour the ledger flagged for documentation.

**Contents**

1. [Purpose and shape](#1-purpose-and-shape)
2. [Build and run](#2-build-and-run)
3. [Tasks and workers](#3-tasks-and-workers)
4. [The page](#4-the-page)
5. [The guarantees](#5-the-guarantees)
6. [Tests and verification](#6-tests-and-verification)

## 1. Purpose and shape

### 1.1 One command to a working demo

- **Source:** spec goals (user).
- **Decision:** `example-app/` holds `compose.yml`, `.env.example` and a
  README. The reader copies `.env.example` to `.env` and runs
  `docker compose up`. One command starts Postgres, the web app with the admin
  UI embedded, and two workers. There is no manual migration step, no
  `cargo install` and no password hashing step.
- **Why:** An evaluator should see tasks flowing within minutes, without reading
  the crate docs.
- **Expected behaviour:** After the first build, `web` logs
  `demo at http://localhost:8080`, each worker logs `worker started: …`, and the
  page and `/admin` work.

### 1.2 A page of buttons, simulated work only

- **Source:** spec goals (user).
- **Decision:** A server-rendered page at `/` enqueues work through plain form
  posts. The admin UI is at `/admin` behind basic auth, with the login in
  `.env`. Every task fakes its work with sleeps and log lines.
- **Why:** No script or extra binary is needed, and nothing can leave the
  machine.
- **Expected behaviour:** No handler makes an external call. Each button answers
  `303 See Other` back to `/`.

### 1.3 The task mix and the marking of deliberate failures

- **Source:** spec goals and anti-goals (user).
- **Decision:** Short tasks (`send-email`), a long task (`bulk-import`), a task
  every 5 minutes (`digest`), and an intermittently failing task
  (`flaky-demo`). The flaky task and the first attempt of `place-order` carry a
  **FAILS ON PURPOSE** badge on the page and a comment in the code.
- **Why:** A newcomer must not mistake a deliberate failure for a bug in the
  crate.
- **Expected behaviour:** The flaky card has a dashed amber border. The
  `flaky-demo` error text says `(demo: this task fails on purpose)`.

### 1.4 Two worker containers

- **Source:** spec goals (user agreed).
- **Decision:** `worker-1` and `worker-2` are two named services, not
  `replicas: 2`.
- **Why:** The admin UI's Workers page then shows more than one worker with
  readable labels (the scheduler's label defaults to `HOSTNAME`), and the README
  can say `docker compose stop worker-2`.
- **Expected behaviour:** The Workers page lists `worker-1` and `worker-2`.
  A cleanly stopped worker deletes its registry row, so it
  disappears from the page (a killed one would read as lost), and its running
  tasks move to the other.

### 1.5 Layout A, plus a Guarantees section and a database panel

- **Source:** spec "The page" (user).
- **Decision:** Section 1 is four cards in a 2x2 grid, each with a "Try:" line.
  Section 2 is three guarantee cards. Below is an "In the database" panel with a
  "reload" link. The page has no JavaScript and no auto-refresh.
- **Why:** The user approved this layout from mockups. Guarantees are what make
  a Postgres queue worth having, so each gets text and a way to prove it.
- **Expected behaviour:** The README's guided tour covers every "Try:" line.

### 1.6 The example is not a workspace member

- **Source:** spec "One crate, one image, two roles" (call).
- **Decision:** `example-app/` is its own crate with its own committed
  `Cargo.lock`. The root `Cargo.toml` has `exclude = ["example-app"]`. CI runs
  fmt, clippy and the tests on it separately, with `--locked` (see §6.4).
- **Why:** As a member, its `cron` features would be merged into CI's
  `--workspace` default-feature builds, and the no-`cron` paths of core and
  admin would silently stop being checked.
- **Expected behaviour:** `cargo test --workspace` does not build the example.
- **Ledger note (implementation fact):** `example-app/Cargo.toml` needs its own
  empty `[workspace]` table besides the root `exclude`. Without it Cargo walks
  up to the root manifest and refuses to build.

### 1.7 One binary, two roles

- **Source:** spec (call).
- **Decision:** One binary and one image. The first argument chooses the role:
  `example-app web` or `example-app worker`. Compose sets `command:` per service.
- **Why:** One build serves both, and the source stays small.
- **Expected behaviour:** Any other argument prints a usage line and exits 2. A
  missing required environment variable names the variable and points to the
  README, then exits 1.

## 2. Build and run

### 2.1 Migrations run through Postgres's init hook

- **Source:** spec (call).
- **Decision:** Compose mounts the three migration `up.sql` files and
  `example-app/demo.sql` into `/docker-entrypoint-initdb.d/` as `0001.sql` to
  `0004.sql`. `demo.sql` creates `demo_orders`.
- **Why:** The reader runs no migration step, and the app needs no migration
  code. It only works on an empty volume, which suits a demo. Real applications
  use their own tool, per the root README.
- **Expected behaviour:** A fresh volume has every table. After a reset
  (`docker compose down -v`) the next `up` starts again from nothing. The README
  lists this as a demo shortcut.

### 2.2 The database is private; the web port is loopback only

- **Source:** spec "Containers" (call).
- **Decision:** `db` publishes no port, so it cannot clash with the root
  compose's `55432`. `web` publishes `127.0.0.1:8080:8080`.
- **Why:** The page has no auth, and the admin UI uses basic auth over plain
  HTTP.
- **Expected behaviour:** The app is reachable from the host only. The README
  uses `docker compose exec db psql` to look at tables.

### 2.3 Worker grace period of 35 s

- **Source:** spec (call), from the core README's Shutdown section.
- **Decision:** `stop_grace_period: 35s` on both workers.
- **Why:** The crate's default `shutdown_timeout` is 25 s, and the grace period
  should be at least that plus 6 s.
- **Expected behaviour:** `docker compose stop worker-2` lets running handlers
  see cancellation and release their tasks. The worker logs
  `worker stopped cleanly`.

### 2.4 Handle SIGTERM in the binary

- **Source:** spec "Worker role" (call).
- **Decision:** `run_until_shutdown` with a `CancellationToken` fired on SIGTERM
  or Ctrl-C. The `web` role's server and the relay stop on the same signal.
- **Why:** The binary is PID 1 in the container and gets no default signal
  handling.
- **Expected behaviour:** Stops are prompt and clean, not a 10 s SIGKILL.

### 2.5 The Dockerfile builds from the repository root

- **Source:** spec (call).
- **Decision:** The build context is the repository root, because the crate
  depends on its parents by path. A root `.dockerignore` keeps `target/`,
  `example-app/target/`, `.git/` and `.superpowers/` out of it.
- **Why:** The root `target/` alone can be gigabytes.
- **Expected behaviour:** The first `docker compose up` compiles for several
  minutes. Later runs reuse the cache.

### 2.6 The admin password is hashed at startup

- **Source:** spec "Web role" (call).
- **Decision:** `ADMIN_PASSWORD` is read in plain text and hashed once with
  `auth::hash_password`.
- **Why:** The reader never runs `hash-password`. A comment says a real
  deployment stores only the hash.
- **Expected behaviour:** Basic auth with the `.env` login works.
  The README lists this as a demo shortcut.

### 2.7 Fast admin refresh

- **Source:** spec "Web role" (call).
- **Decision:** `refresh_seconds: 2` and `overview_cache: 1 s`, with a comment
  saying the defaults (10 s and 5 s) suit production.
- **Why:** The reader can watch a batch drain.
- **Expected behaviour:** The overview reloads every 2 s.

### 2.8 `/admin/` redirects to `/admin`

- **Source:** spec (call), from `admin/README.md`.
- **Decision:** A route redirects `/admin/` to `/admin`.
- **Why:** Under `.nest`, `/admin/` is otherwise a 404.
- **Expected behaviour:** Both addresses reach the overview.

## 3. Tasks and workers

### 3.1 Queue sizes: `default` 4, `bulk` 1

- **Source:** spec "Worker role" (call).
- **Decision:** Each worker runs 4 `default` tasks and 1 `bulk` task at once.
- **Why:** At 8 `default` slots across two workers, 100 emails (1 to 3 s each)
  drain in about 30 s, long enough to watch. With `bulk` at 1, two workers run
  two imports at once and a third waits in Queued.
- **Expected behaviour:** A third import stays Queued until a slot frees. An
  import (about 160 s) finishes before the default 5-minute `backlog_after`, so
  no backlog status shows.

### 3.2 Short heartbeat so a cancel shows quickly

- **Source:** spec "Worker role" (call).
- **Decision:** `heartbeat_interval` 5 s and `lease_duration` 30 s.
- **Why:** A handler sees an operator's cancel at its next heartbeat. With the
  defaults that takes about 20 s.
- **Expected behaviour:** Cancelling a running import stops its `batch i/40`
  logs within about 5 s.

### 3.3 Bulk import stops promptly and restarts from batch 1

- **Source:** spec "Tasks" (call).
- **Decision:** Each of 40 steps (about 4 s each) waits on `ctx.cancellation`
  alongside its sleep and returns `TaskError::interrupted()` when it fires. A
  released or replayed import restarts at batch 1.
- **Why:** An operator cancel never fires the token: the worker sees it at its
  next heartbeat (every 5 s here) and drops the handler. The `select!` is for
  shutdown, so the import releases promptly instead of being killed. Making it
  resumable is a non-goal, and a comment says real long tasks keep durable
  progress keyed by `task_key`.
- **Expected behaviour:** After `docker compose stop worker-2` mid-import, the
  other worker logs the same task key from batch 1.

### 3.4 The flaky task fails about 70% of attempts

- **Source:** spec "Tasks" (call).
- **Decision:** `flaky-demo` sleeps 0.5 s, then fails with a retryable error on
  about 70% of attempts. It keeps the default `max_attempts` (3).
- **Why:** About 0.34 of tasks need all three attempts to fail, so a batch of 10
  almost always leaves at least one dead. Failure groups and "Replay all" then
  have something to show.
- **Expected behaviour:** Some of 10 tasks end `dead`, and the overview shows a
  failure group with the `simulated failure` message.

### 3.5 The digest uses cron, created idempotently

- **Source:** spec "Tasks" (call).
- **Decision:** The `web` role calls `jobs::ensure_job` for `digest` with
  `*/5 * * * *` at startup.
- **Why:** It shows the `cron` feature. `ensure_job` never duplicates the job on
  a restart and never un-pauses a job an operator paused.
- **Expected behaviour:** A `digest` task reaches `done` within 5 minutes of
  startup, then every 5 minutes.

### 3.6 Workers log their start and clean stop

- **Source:** Ledger ruling (Task 2 dispatch).
- **Decision:** The worker logs `worker started: queues default(4), bulk(1)`
  and, after shutdown, `worker stopped cleanly`.
- **Why:** The README's "stop `worker-2`" exercise needs visible evidence in
  `docker compose logs`. Cost: two log lines to delete.
- **Expected behaviour:** Both lines appear in each worker's log.

## 4. The page

### 4.1 Buttons are POSTs that redirect with the banner in the query string

- **Source:** spec "The page" (call).
- **Decision:** Each button enqueues and answers `303` to `/?kind=…&text=…&task=…`.
  The banner's link is derived from `task` alone: the task page for a single
  task, `/admin` for a batch. Only the green and blue banners carry it; the
  amber and red ones have nothing to watch (§6.4). Each batch button (emails,
  flaky, orders) enqueues its whole batch in one transaction.
- **Why:** Reloading never re-submits. A crafted URL cannot make the banner link
  anywhere else. Without the transaction, a database failure partway through a
  batch would show the red banner while the tasks already written still ran,
  and pressing again would break the walkthrough's counts (PR review 001).
- **Expected behaviour:** Green banner after an enqueue; blue for an existing
  key (`inserted = false`); amber for an idempotency conflict; red for bad input
  or an error; after a red banner from a batch button, none of that batch was
  enqueued. A malformed query shows no banner, not a 400. Banner text is
  escaped (`web::tests::index_page_escapes_flash_text`), and only an enqueued or
  existing banner links to the admin UI
  (`web::tests::enqueued_banner_links_to_the_admin_ui`).

### 4.2 Order input is validated on the server

- **Source:** spec "The page" (call).
- **Decision:** The Order is a whole number from 1 to 999,999, digits only. Any
  other value, or a missing field, gets the red banner.
- **Why:** It is the page's only free-form input, and it ends up in a task key.
- **Expected behaviour:** `+5`, `0`, `1000000` and `abc` are rejected with
  "Order must be a whole number from 1 to 999999."
  (`web::tests::parse_order_no_accepts_only_1_to_999999`). A form post with the
  field missing also gets the red banner (a fix after review).

### 4.3 The Order key hint is static

- **Source:** Ledger note (Task 3).
- **Decision:** The page shows `receipt:order-<n>` as fixed text.
- **Why:** There is no JavaScript to update it as you type.
- **Expected behaviour:** The hint never changes with the input. The banner
  shows the real key.

### 4.4 The panel reads the demo's tables directly

- **Source:** spec "Web role" (call).
- **Decision:** The panel runs SQL on `demo_orders` (joined to `queue_tasks` on
  `task_key`) and `queue_outbox`. If the read fails the panel says
  "database unavailable" and the rest of the page still renders.
- **Why:** The admin UI never shows these tables, and this is only a demo page.
- **Expected behaviour:** The summary reads
  `demo_orders: N rows from M completed place-order tasks (A attempts)`.

### 4.5 A generic look that follows the computer's theme

- **Source:** user.
- **Decision:** A neutral style:
  - white cards on a light grey page, with the same layout in darker neutrals
    when the computer is in dark mode (`color-scheme: light dark`);
  - section 1 is a 2×2 grid, and section 2 is three cards in a row;
  - one column below 760 px;
  - blue primary buttons;
  - banners with a coloured left edge.

  The deliberate-failure markings keep their dashed amber border and badge.
  Everything is in the page's one `<style>` block, with no framework.
- **Why:** The admin UI carries the product look (Grafana-style dark). The
  example page only needs to be clear.
- **Expected behaviour:** The page is readable in both themes and on a phone.
  The FAILS ON PURPOSE cards stand out in both.

## 5. The guarantees

### 5.1 Duplicate-safe enqueue

- **Source:** spec "The page" (user).
- **Decision:** "Send receipt" enqueues `send-email` under the key
  `receipt:order-<n>`. "Same key, different subject" reuses the key with a
  changed payload.
- **Why:** Retried requests and double-clicks must not create a second task.
- **Expected behaviour:** The first press is green. A second press is blue with
  the same task id. The other button is amber, and the original task is
  unchanged.

### 5.2 Exactly-once work, kept by the transaction alone

- **Source:** spec anti-goals (user); spec "Tasks" (call).
- **Decision:** `place-order` runs `store::lock_owned`, an `INSERT` into
  `demo_orders` and `store::complete_with_outbox` in one transaction. On
  attempt 1 it fails right after the `INSERT`, which rolls the transaction back.
  `demo_orders` has no unique key on `task_key`.
- **Why:** If a constraint kept it to one row, the demo would only look
  exactly-once. Without one, the transaction is the only thing that does.
- **Expected behaviour:** After "Place 5 orders" the panel shows 5 rows from 5
  tasks with 10 attempts. A replayed order crashes once again, because a replay
  resets `attempt_no`. When the handler has already completed the task, the
  runtime's own completion finds it done and logs at debug, which is the
  documented path.
- **Ledger note (implementation fact):** `demo_orders` ids have gaps, because
  the rolled-back first-attempt inserts use up identity values. That is visible
  proof of the rollback.

### 5.3 The outbox relay lives in the `web` role

- **Source:** spec "Web role" (call); delivery order by Ledger ruling (final
  review).
- **Decision:** A loop every 2 s runs one transaction: it selects unpublished
  `queue_outbox` rows created at least 3 s ago (oldest first, up to 100) with
  `FOR UPDATE SKIP LOCKED`, "delivers" each by logging
  `delivered order-placed for order <id> (command_key …)`, then sets
  `published_at` on those ids. The outbox command's `command_key` is the JSON of
  `(task_key, "order-placed")`.
- **Why:** `web` runs as one instance, and the 3 s delay lets the page show
  *pending*. Delivering before marking, in the same transaction, is the
  at-least-once order a real relay needs: a crash before the commit leaves the
  rows unpublished, so they are delivered again, and consumers deduplicate by
  `command_key`. A comment in `outbox.rs` says so.
- **Divergence:** The plan prescribed a single
  `UPDATE … SET published_at … RETURNING`, logging after it committed. That
  marks first and delivers second, which loses messages on a crash once the log
  line becomes a broker call. The final review caught it, and the shipped relay
  uses the order above.
- **Expected behaviour:** A message shows *pending* for about 3 to 5 s, then
  *published HH:MM:SS UTC*. The crashed first attempts leave no messages.

## 6. Tests and verification

### 6.1 Two unit tests stay

- **Source:** Ledger ruling 1.
- **Decision:** The spec's non-goal says no automated tests. The plan's two pure
  tests stay: the banner-escaping test and the order-input test. The final
  review's banner-link fix added a third, as small and as pure (§6.4).
- **Why:** They guard the page's only trust boundaries (reflected banner text,
  the order input) at almost no cost. If wrong, delete two small tests.
- **Expected behaviour:** `cargo test --manifest-path example-app/Cargo.toml`
  runs 3 tests.

### 6.2 Checks that count rows start from a fresh volume

- **Source:** Ledger ruling 2.
- **Decision:** The end-to-end counts (5 rows, 5 tasks, 10 attempts, 5 outbox
  rows) are absolute, so verification starts with `docker compose down -v`.
- **Why:** An older volume carries earlier orders.
- **Expected behaviour:** Run the counts after "Place 5 orders" on a fresh
  volume, or expect larger totals.

### 6.3 Behavioural checks were driven from the command line

- **Source:** Ledger ruling (Task 2 review).
- **Decision:** The "Try:" steps (cancel and replay, mid-import handoff, flaky
  dead tasks, digest within 5 minutes) were checked in the README walkthrough
  with `curl -u` form posts, `psql` and `docker compose logs`.
- **Why:** The page has no scripted browser test, and the README needs these
  steps to work as written.
- **Expected behaviour:** Each README step behaves as written.

### 6.4 What the final review changed

- **Source:** Ledger ruling (final review).
- **Decision:** The whole-branch review found one Important issue and some
  polish. The fix wave took the Important one and the polish a template reader
  would copy, and left out cosmetic items. It also left out one nit: only the
  email button enqueued its batch in a transaction. The PR review showed that
  nit's real effect, and it is fixed (§4.1).
  - CI runs the example's clippy and tests with `--locked`, as the Dockerfile
    builds.
  - The comments say how an operator cancel really works: the worker sees it at
    its next heartbeat and drops the handler, and `bulk_import`'s `select!` on
    `ctx.cancellation` is for shutdown (§3.3).
  - The relay delivers before it marks (§5.3).
  - The panel labels its times `UTC`.
  - `.env.example` and the README warn that `POSTGRES_PASSWORD` goes into a URL
    (letters and digits only), and that changing it after the first run needs
    `docker compose down -v`.
  - `compose.yml` says where to mount a new crate migration (`demo.sql` stays
    last).
  - The README says to run `docker compose up --build` after pulling new code.
  - Only the green and blue banners link to the admin UI (§4.1).
- **Why:** Without `--locked`, a routine change to the core or admin crates can
  leave `example-app/Cargo.lock` stale. CI would re-resolve it and stay green,
  while a newcomer's `docker compose up` fails at the locked build. The other
  items are what a newcomer copies or trips on first.
- **Expected behaviour:** A stale example lockfile fails CI, not the demo.
