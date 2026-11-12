# pg_task_scheduler_admin

An optional admin UI and JSON API for
[`pg_task_scheduler`](../README.md). Its first page answers one question: are
workers registered, are tasks being dequeued and completed, and if not, why not
and how big is the problem? Below that it lists workers and tasks, shows a task's
attempts and audit trail, and lets an operator cancel, replay, bulk cancel or
replay by task name, and pause or resume a queue.

It is a separate crate so that an application that doesn't use it takes on none
of its HTTP dependencies. Cancel, pause and the worker registry themselves live
in the core crate; this crate only calls them. Why each part works the way it
does, and what to expect from it, is recorded in
[Admin UI decisions](../docs/AdminUI.md).

The UI reads the queue tables directly, so it needs migration `0003` (see the
core README's [Database Schema](../README.md#database-schema)) and workers built
from the same version: older workers don't register, so the UI shows their
queues as having no live worker.

## Two ways to run it

### Embedded in your application

`router` returns an `axum::Router` (axum 0.7) for any `SchedulerPool`. Wrap it
in an authentication layer and mount it under a prefix:

```rust,no_run
use axum::Router;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::deadpool::Pool;
use pg_task_scheduler_admin::auth::{BasicUser, with_basic_auth};
use pg_task_scheduler_admin::{AdminConfig, router};

# async fn example(admin_pool: Pool<AsyncPgConnection>) -> Result<(), Box<dyn std::error::Error>> {
let admin = router(
    admin_pool,
    AdminConfig {
        base_path: "/admin".into(), // the prefix it is nested under, below
        ..AdminConfig::default()
    },
);
let admin = with_basic_auth(
    admin,
    vec![BasicUser {
        name: "alice".into(),
        // From `pg-task-admin hash-password`; keep it out of source control.
        password_hash: std::env::var("ADMIN_ALICE_HASH")?,
    }],
)?;
let app = Router::new().nest("/admin", admin);
let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
axum::serve(listener, app).await?;
# Ok(())
# }
```

- `base_path` must equal the prefix the router is nested under, or `""` when it
  is not nested. Every link, asset, form action and redirect is built from it.
- Under `.nest("/admin", ..)` the overview is served at `/admin` and
  `/admin/overview`, but `/admin/` is a 404 (axum 0.7 maps the inner `/` to the
  bare prefix). Add a redirect in the host if you want `/admin/` to work.
- Give the router its own small pool (a handful of connections) rather than the
  workers' pool. The pool size caps the UI's concurrent load on the database
  however many people have it open, and the UI can then never take a connection
  a worker's heartbeat needs.
- Features: `deadpool` (default) forwards to the core crate's deadpool pool;
  `cron` adds the `/api/jobs` routes.

### Standalone: `pg-task-admin`

The `pg-task-admin` binary serves the same router with its own pool. It needs
only database credentials, not your application's code.

```sh
cargo install --path admin              # from the repository root
pg-task-admin hash-password <<< 'secret' # prints an argon2id PHC string
pg-task-admin --config admin.toml        # runs the server
```

`hash-password` reads one line from standard input. Those two forms are the
whole command line. The server logs to standard error (`RUST_LOG`, default
`info`), stops gracefully on Ctrl-C or SIGTERM, and runs on Unix only. A minimal
config:

```toml
bind = "127.0.0.1:8080"
# database_url = "postgres://..."   # or set DATABASE_URL in the environment

[auth]
mode = "basic"

[[auth.users]]
name = "alice"
password_hash = "$argon2id$v=19$m=19456,t=2,p=1$..."
```

The server itself always serves at `/`. Set `base_path` only when a reverse proxy
publishes it under a prefix and strips that prefix before forwarding; links then
point at the public path. All settings are in the
[configuration reference](#configuration-reference).

## Authentication

Every route answers **401** unless the request carries an `Operator`
extension, GET included. A deployment that forgets authentication is closed, not
open, and there is no "none" mode. The operator's name is what the audit records
for every action; an `operator` field in a request body is ignored.
Authentication is any tower layer that inserts `Operator`. The crate ships two,
and the standalone server uses one of them.

### Basic auth

`auth::with_basic_auth` (standalone: `mode = "basic"`) checks HTTP basic
credentials against a list of users with argon2id PHC hashes, made by
`auth::hash_password` or `pg-task-admin hash-password`.

- A failure is 401 with `WWW-Authenticate: Basic realm="pg_task_scheduler admin"`.
- An unknown user name is verified against a dummy hash, so the response time
  does not reveal which names exist.
- At most one verify runs per CPU; further requests queue for a slot. Each
  verify costs about 19 MiB and tens of milliseconds of CPU.
- A hash that is not argon2id is a configuration error. The standalone config
  also rejects duplicate user names.
- Browsers send the password with every request, so use TLS (below). The
  standalone server logs a warning when basic auth is bound to a non-loopback
  address.

### Trusted header behind an authenticating proxy

`auth::with_trusted_header` (standalone: `mode = "trusted_header"`, header
`X-Forwarded-User` by default) takes the operator from a header set by a proxy
that has already authenticated the user: oauth2-proxy, Authelia, or nginx with
`auth_request`, for example. A missing, empty or repeated header is 401.

> **Warning.** This is safe only when:
>
> - nothing but the proxy can reach the admin server (bind it to loopback or a
>   private network, or firewall it); and
> - the proxy **overwrites** the header on every request, so that a value sent
>   by the client never gets through.
>
> Otherwise anyone can send `X-Forwarded-User: admin` and act as that user.

With nginx in front of oauth2-proxy, for example:

```nginx
location / {
    auth_request /oauth2/auth;
    auth_request_set $user $upstream_http_x_auth_request_user;
    proxy_set_header X-Forwarded-User $user;   # replaces any client value
    proxy_pass http://127.0.0.1:8080;
}
```

### Your own layer

An embedding application can use its existing sign-in instead. The layer only
has to insert `Operator`, or answer the request itself:

```rust,no_run
use axum::Router;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::Response;
use pg_task_scheduler_admin::Operator;

async fn sso(mut req: Request, next: Next) -> Result<Response, StatusCode> {
    let user = signed_in_user(&req).ok_or(StatusCode::UNAUTHORIZED)?;
    req.extensions_mut().insert(Operator(user));
    Ok(next.run(req).await)
}
# fn signed_in_user(_: &Request) -> Option<String> { None }

fn with_sso(admin: Router) -> Router {
    admin.layer(middleware::from_fn(sso))
}
```

There are no roles: every authenticated operator can see and do everything. To
give someone read-only access, reject their non-GET requests in your layer.

### LDAP, OIDC and SAML

None is built in. Standalone deployments put an authenticating proxy that speaks
the protocol (Authelia, oauth2-proxy with your identity provider, or an nginx
`auth_request` LDAP service) in front and use the trusted header. Embedded
deployments use their own layer, as above.

### Cross-site requests

Browsers attach basic-auth credentials and proxy cookies to cross-site requests,
so every request other than GET, HEAD and OPTIONS also passes a CSRF check, after
the operator check. The `/api/*` routes are included:

1. If `Sec-Fetch-Site` is present, only `same-origin` and `none` pass. Anything
   else, `same-site` included, is 403.
2. Otherwise, if `Origin` is present, it must be `http://` or `https://`
   followed by the request's `Host`, or exactly `public_origin`.
3. Otherwise the request passes. Only non-browser clients, such as scripts and
   curl, send neither header, and they still have to authenticate.

Behind a proxy that rewrites `Host`, set `public_origin` to the address browsers
use (for example `https://admin.example.com`, with no trailing slash), so that
browsers that send `Origin` but not `Sec-Fetch-Site` can still post.

Every response, errors and 401s included, carries `X-Frame-Options: DENY` and
`Content-Security-Policy: frame-ancestors 'none'`, so no other site can show the
pages in a frame and trick an operator into clicking a button (clickjacking).
The router sets them, and the shipped auth layers add them to their own 401s;
a custom auth layer's rejections are its own.

## TLS

Neither mode terminates TLS. Put a TLS-terminating proxy or load balancer in
front, and bind the admin server to loopback or a private address. Basic auth
over plain HTTP sends the password in the clear on every request.

## Database role

Give the UI its own role, so its grants and connection count are visible and
bounded. The pages read five tables; the actions update tasks, close attempts,
write audit rows, and add or remove pauses:

```sql
CREATE ROLE pg_task_admin LOGIN PASSWORD '...';
GRANT USAGE ON SCHEMA public TO pg_task_admin;   -- the schema holding the queue tables
-- pages
GRANT SELECT ON queue_tasks, queue_task_attempts, queue_admin_actions,
                queue_workers, queue_pauses TO pg_task_admin;
-- actions: cancel and replay; cancel closes the attempt; audit rows (bulk updates
-- their count); pause and resume
GRANT UPDATE ON queue_tasks TO pg_task_admin;
GRANT INSERT, UPDATE ON queue_task_attempts, queue_admin_actions TO pg_task_admin;
GRANT INSERT, DELETE ON queue_pauses TO pg_task_admin;
-- only with the `cron` feature (/api/jobs)
GRANT SELECT, UPDATE, DELETE ON scheduler_jobs TO pg_task_admin;
```

These grants were checked by running every page query and action as such a role.
A role with only the `SELECT` grants can view the pages but not act.

Page reads run in a transaction that sets `statement_timeout` (5 s by default)
and `idle_in_transaction_session_timeout` to the same value, so no page statement
runs longer than that. If a client disconnects mid-read, the transaction stays
open on its pooled connection until the next checkout; the idle timeout makes
the server end that session after the same 5 s, releasing its locks. Actions,
and the bulk confirm page's count, run outside that transaction, as their own
statements. A single cancel never waits on a row lock (it retries for up to 2 s,
then reports the task busy); a single replay locks a dead or cancelled row,
which nothing else normally holds; a bulk run lasts up to its 10 s budget plus
one batch.

## Configuration reference

Embedded applications set `AdminConfig`; the standalone server reads the TOML
keys, which map onto it. Unknown TOML keys are an error.

| `AdminConfig` field | TOML key | Default | Meaning |
| --- | --- | --- | --- |
| `base_path` | `base_path` | `""` | Prefix for every link, asset, form and redirect (see above). |
| `public_origin` | `public_origin` | none | Browser-facing origin accepted by the CSRF check when a proxy rewrites `Host`. Must match exactly. |
| `refresh_seconds` | `refresh_seconds` | 10 | Page auto-refresh; 0 turns it off. |
| `overview_cache` | `overview_cache_seconds` | 5 s | The overview is computed at most once per this, shared by every viewer. |
| `page_size` | (none) | 50 | Rows per list page. |
| `statement_timeout` | `statement_timeout_seconds` | 5 s | Limit on each page-read statement, and on idle time inside a page read's transaction. |
| `overview.window` | `window_seconds` | 1 h | How far back the Done and Dead columns and the failure groups look. |
| `overview.count_cap` | `count_cap` | 10,000 | Most rows any count reads; larger counts show as `10,000+`. Also caps the bulk confirm count. |
| `overview.no_handler_probe` | (none) | 100 | Oldest due tasks per queue checked for task names no live worker handles. |
| `overview.failure_groups` | (none) | 20 | Failure groups shown. |
| `overview.recent_actions` | (none) | 10 | Recent admin actions shown. |
| `thresholds.default.backlog_after` | `[thresholds] backlog_after_seconds` | 5 min | When a queue's oldest due task makes it `backlog`. |
| `thresholds.default.stall_after` | `[thresholds] stall_after_seconds` | 5 min | How recently a serving worker must have finished a task (or started) for a queue with due tasks not to be `stalled`. |
| `thresholds.per_queue` | `[thresholds.queues."name"]` | none | Per-queue overrides of either threshold; a key left out falls back to `[thresholds]`. |

Standalone-only keys:

| TOML key | Default | Meaning |
| --- | --- | --- |
| `database_url` | `DATABASE_URL` from the environment | Required one way or the other; the file wins. |
| `bind` | `127.0.0.1:8080` | Socket address, IP and port (no host names). |
| `pool_size` | 4 | Connections in the server's pool. |
| `[auth] mode` | required | `"basic"` or `"trusted_header"`. |
| `[auth] header` | `X-Forwarded-User` | The trusted header (`trusted_header` only). |
| `[[auth.users]] name`, `password_hash` | at least one | Basic-auth users (`basic` only); names must be unique. |

A full file:

```toml
database_url = "postgres://pg_task_admin:...@db/app"
bind = "127.0.0.1:8080"
pool_size = 4
base_path = ""
public_origin = "https://admin.example.com"
refresh_seconds = 10
overview_cache_seconds = 5
statement_timeout_seconds = 5
count_cap = 10000
window_seconds = 3600

[thresholds]
backlog_after_seconds = 300
stall_after_seconds = 300

[thresholds.queues."bulk"]
backlog_after_seconds = 1800

[auth]
mode = "trusted_header"
header = "X-Forwarded-User"
```

## Pages and routes

All paths are relative to `base_path`. Queue and task names travel only in query
strings, so any name works, `/`, `&` and `#` included.

| Route | What it shows or does |
| --- | --- |
| `GET /`, `GET /overview` | The overview: alert, workers summary, queue table with status and counts, failure groups, tasks with no handler, recent actions. |
| `GET /workers` | Every registered worker, live first; lost workers greyed. |
| `GET /queued?queue=…` | One queue in dequeue order. Without `queue`, a list of known queues. |
| `GET /running` | Running tasks, soonest lease expiry first. |
| `GET /completed`, `/failed`, `/cancelled` | Done, dead and cancelled tasks, newest first. `/failed` and `/cancelled` take `task_name` and `error` filters. |
| `GET /tasks/:id` | A task, its payload, every attempt, and its admin actions. |
| `POST /tasks/:id/cancel`, `/tasks/:id/replay` | Form posts with a `reason`; redirect back to the task. |
| `POST /queues/pause`, `/queues/resume` | Form posts with `queue` and `reason`; redirect to the overview. |
| `GET /bulk?action=cancel\|replay&task_name=…[&queue=…][&error=…]`, `POST /bulk` | Confirm page with the capped match count, then one budgeted run with a Continue button if more remain. |
| `GET /api/tasks/:id` | The task as JSON. |
| `POST /api/tasks/:id/replay` | JSON body `{"reason": "…"}`; 204, 400 if the reason is blank, or 409 if the task is not dead or cancelled. |
| `GET /api/jobs`, `POST /api/jobs/:id/pause`, `POST /api/jobs/:id/resume`, `DELETE /api/jobs/:id` | Cron schedules (`cron` feature only). |

Every action needs a non-empty reason (400 otherwise). A bulk action also needs
the task name typed again. Lists show "Next" links only, with no page numbers or
totals. Pages reload every `refresh_seconds`, except while a form field has
focus, and never on the bulk confirm and result pages or on a failed action's
error page (reloading a POST result would offer to resubmit it); the
Auto-refresh checkbox turns it off for that browser.

## Queue status: what it means and what to do

Each queue row on the overview gets the first status below that applies, with a
reason. The alert at the top lists queues that are paused or stalled with no
live worker; dismissing it lasts until that set of queues changes.

| Status (reason) | Meaning | What to do |
| --- | --- | --- |
| **paused** | An operator paused the queue. No worker claims from it; running tasks finish. | Resume it when ready. Recent actions show who paused it and why. |
| **stalled** (no live worker) | Tasks are due and no live, registered worker serves the queue. | Start or fix a worker that serves this queue. On the Workers page, check for lost workers, and for workers from before `0003`, which never register. |
| **stalled** (nothing finished in 5m) | Tasks are due, and every live worker serving the queue last finished a task there (or, if it never has, started) more than `stall_after` ago. | Look at the Running list: handlers that run for minutes or hang hold every slot. Check workers' status and last error on the Workers page (a `degraded` worker is failing to claim). For a queue whose normal tasks run longer than `stall_after`, raise its threshold. |
| **backlog** (N task names have no handler) | The oldest due task has waited longer than `backlog_after`, and N due task names have no handler on any live worker serving the queue. Those tasks never run. | Register the handlers, or route those tasks to a queue whose workers have them. The "No handler" section names them. |
| **backlog** (all N slots busy) | The oldest due task waited longer than `backlog_after`, and running tasks fill every slot the serving workers offer. | Add instances or raise the queue's limit, or find handlers that run too long. |
| **backlog** (oldest due task waited X) | The oldest due task waited longer than `backlog_after`, though slots are free and every checked name has a handler. | Look for a degraded worker (claim errors), an undersized pool, rows held by long DB-contained transactions, or due task names beyond the oldest 100 that no worker handles. |
| **healthy** | None of the above. A queue with nothing due is healthy, even with no worker. | Nothing. |

Other things the overview shows:

- **Queued and Due** count the queue's queued tasks: all of them, and those
  already due. Each reads at most `count_cap` rows and shows `10,000+` beyond
  that.
- **Done and Dead columns** count the last `window` (1 h). When at least
  `count_cap` tasks finished in that time, the header shows the span actually
  covered, for example `Done (last 7m)`, and every count in the column is a
  lower bound, marked `+`.
- **Failure groups** group the window's dead tasks and failed attempts by task
  name and error. "Failed attempts" counts attempts that ended in a retry or an
  expired lease, not tasks, so it measures the failure rate. It looks at the
  newest `count_cap` attempts that started in the window, failed or not; when
  more than that started, the header shows the span covered, for example
  `Failed attempts (last 7m)`, and the counts are lower bounds, marked `+`. Each
  group links to the matching Failed list and to "Replay all…" for its dead
  tasks.
- **Recent admin actions** are the last 10, from every operator.
- If the database is unreachable or a query fails, the overview is a 503 error
  page. It never shows a status it couldn't compute.
