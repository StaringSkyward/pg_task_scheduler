# pg_task_scheduler example app

A demo you run with `docker compose up`. It shows `pg_task_scheduler` and its
admin UI working together: a web page with buttons that enqueue simulated work,
two worker containers that run it, and the admin UI embedded at `/admin`. You
can cancel, replay, pause and break things, and see what the crate does.

Everything is simulated with sleeps and log lines. Nothing leaves your machine.

The page at `http://localhost:8080/` has two sections of cards. "Watch work
flow" has Send emails, Bulk import, a flaky task and a scheduled digest.
"Guarantees you can check" has duplicate-safe enqueue, exactly-once database
work and the transactional outbox. A panel at the bottom reads the demo's own
tables, and a green, blue, amber or red banner at the top says what your last
button press did.

Two things on the page fail on purpose: the flaky task, and the first attempt of
`place-order`. Both are marked **FAILS ON PURPOSE**. They are not bugs.

The reasoning behind the app's choices is in
[docs/ExampleApp.md](../docs/ExampleApp.md).

## Prerequisites

Docker with Compose v2 (`docker compose version` should work). Nothing else.

## Start

From this directory (`example-app/`):

```sh
cp .env.example .env
docker compose up
```

Edit `.env` first if you like. It holds the database password, the admin login
(`ADMIN_USER` and `ADMIN_PASSWORD`, `admin` and `change-me` by default) and the
log filter.

The first run compiles the app inside Docker, which takes several minutes.
Later runs reuse the build. Wait for `web` to log `demo at http://localhost:8080`
and for both workers to log `worker started`.

Open <http://localhost:8080>. The admin UI is at <http://localhost:8080/admin>.
Your browser asks for the login from `.env`.

The page is on `127.0.0.1` only. Run the commands below in a second terminal,
also from `example-app/`.

## A guided tour

Each step names the button to press and what to look for in the admin UI. Keep
`/admin` open in another tab. It refreshes every 2 seconds here.

### 1. Watch work flow

**Send emails.** Press "Send 100 emails", then open the overview. Queued drops
and Done climbs over about 30 seconds. Eight tasks run at once (4 per worker),
each sleeping 1 to 3 seconds. Short tasks spread over several workers, and the
overview is where you see it.

**Bulk import.** Press "Start bulk import" and follow the banner link to the
task's page. It runs for 2 to 3 minutes in 40 steps and logs each one:

```sh
docker compose logs -f worker-1 worker-2
```

- *Cancel and replay.* On the task page (or the overview's Running list),
  cancel it with a reason. The cancel is seen at the worker's next heartbeat,
  every 5 seconds here, so the batch logs stop within about 5 seconds (often sooner). Then
  replay it from the cancelled task's page. It runs again from batch 1. A
  stopped long task should stop promptly and not run to the end.
- *Stop a worker mid-run.* Find which worker has it in the logs, say
  `worker-2`, and stop that container (use the other name if `worker-1` has it):

  ```sh
  docker compose stop worker-2
  ```

  The worker releases the import on shutdown. The other worker picks up the
  same task key and logs from batch 1 again (the import is not resumable; the
  code says so). The Workers page stops listing `worker-2`. Start it again with `docker compose start worker-2`. This is what a
  deploy or a crash looks like: the work is not lost.
- Each worker runs one `bulk` task at a time, so a third import waits in the
  Queued list until a slot frees.

**Flaky task.** Press "Enqueue 10 flaky tasks". Each fails about 70 percent of
the time, retries with backoff, and some end up dead. On the overview, look at
Failure groups, then press "Replay all..." to requeue the dead ones. Failures
are normal, and the admin UI shows what failed and why, and lets you retry in
bulk.

**Scheduled digest.** The `digest` task runs every 5 minutes (`*/5 * * * *`),
created when `web` starts. Wait for the next five-minute mark and find it under
Completed. Recurring work needs no external cron, and a restart does not
duplicate the schedule.

**Pause a queue.** On the overview, pause `default` with a reason. Press "Send
100 emails". The tasks stay queued and nothing runs. Resume the queue and they
drain. Pausing stops new claims and lets running tasks finish, so you can stop
a bad queue without a deploy.

**No live worker.** Stop both workers:

```sh
docker compose stop worker-1 worker-2
```

Press "Send 100 emails". Once the tasks are due, the overview shows an alert
that queue `default` is stalled, with "no live worker" as the reason. Start them again with
`docker compose start worker-1 worker-2`, and the backlog drains.

### 2. Guarantees you can check

**Duplicate-safe enqueue.** Every task has a key you choose. Press "Send
receipt" twice: the banner says the same task id both times, in blue
("Already enqueued"). Press "Same key, different subject": the key exists with
a different payload, so it is rejected, in amber. This is what protects you
from retries and double-clicks. Change Order to try another key.

**Exactly-once database work.** Press "Place 5 orders". The order row, the
outbox message and the task's completion share one transaction. The first
attempt of each order crashes right after writing its row, which rolls the row
back, and the task retries. In the admin UI each order shows 2 attempts. In
the "In the database" panel (press "reload"), the summary reads
`demo_orders: 5 rows from 5 completed place-order tasks (10 attempts)`: one row
per order. The table has no unique key on the task key, so only the transaction
keeps it to one row. The order ids have gaps, because the rolled-back inserts
use up identity values. That is visible proof of the rollback.

**Transactional outbox.** Completing an order also writes an `order-placed`
message to `queue_outbox` in the same transaction, so a message exists only if
the order does. A relay in the `web` container delivers each one a few seconds
later (it logs it) and marks it published. After placing orders, reload the
page: messages show *pending* for 3 to 5 seconds and then *published*. The
crashed first attempts left no messages. You can also look at the tables:

```sh
docker compose exec db psql -U postgres demo -c \
  "select status, count(*) from queue_tasks group by 1"
docker compose exec db psql -U postgres demo -c \
  "select * from queue_outbox order by id desc limit 5"
```

## Where to look in the source

All in `src/`. It is meant to be read as a template.

| File | What it shows |
| --- | --- |
| `tasks.rs` | The task definitions, the handlers, and `scheduler()`: queues, intervals and registration. Also `place_order`, the exactly-once and outbox pattern. |
| `web.rs` | The page, the enqueue routes, cron registration (`ensure_job` for the digest) and embedding the admin UI with basic auth. |
| `outbox.rs` | A toy outbox relay. |
| `panel.rs` | The page's database reads. |
| `main.rs` | Role selection (`worker` or `web`), pools and shutdown signals. |

## Demo shortcuts

This app is set up for a demo, not for production. Each shortcut has a real
practice:

- **Plain-text admin password in `.env`.** The app hashes it at startup. A real
  deployment stores only the hash. See [Authentication](../admin/README.md#authentication).
- **Migrations through Postgres's init hook.** `compose.yml` mounts the SQL files
  into `/docker-entrypoint-initdb.d/`. That runs once, on an empty volume. Real
  applications apply them with their own migration tool. See
  [Database Schema](../README.md#database-schema).
- **No TLS.** The admin UI uses basic auth over plain HTTP, so the port is bound
  to `127.0.0.1`. See [TLS](../admin/README.md#tls).
- **A superuser database role.** Real deployments give the admin UI its own
  role. See [Database role](../admin/README.md#database-role).
- **Fast admin refresh** (2 s, 1 s cache) so you can watch things move. The
  defaults suit production.

## Reset

```sh
docker compose down -v
```

This stops everything and deletes the database volume. Plain `docker compose
down` keeps the data.
