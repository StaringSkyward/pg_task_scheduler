mod common;

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use chrono::Utc;
use common::TestDb;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::deadpool::Pool;
use pg_task_scheduler::AttemptOutcome;
use pg_task_scheduler::AttemptState;
use pg_task_scheduler::EnqueueOptions;
use pg_task_scheduler::HealthStatus;
use pg_task_scheduler::MaxAttempts;
use pg_task_scheduler::Priority;
use pg_task_scheduler::QueueName;
use pg_task_scheduler::Retention;
use pg_task_scheduler::Scheduler;
use pg_task_scheduler::SchedulerError;
use pg_task_scheduler::Task;
use pg_task_scheduler::TaskError;
use pg_task_scheduler::TaskId;
use pg_task_scheduler::TaskKey;
use pg_task_scheduler::TaskState;
use pg_task_scheduler::TaskStatus;
use pg_task_scheduler::TaskView;
use pg_task_scheduler::WorkerId;
use pg_task_scheduler::enqueue;
use pg_task_scheduler::store;
use serde_json::json;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

struct Job;
impl Task for Job {
    const NAME: &'static str = "job";
    type Args = serde_json::Value;
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn n(v: usize) -> NonZeroUsize {
    NonZeroUsize::new(v).unwrap()
}

fn bulk() -> QueueName {
    QueueName::try_from("bulk").unwrap()
}

async fn enqueue_job(db: &TestDb, key: &str, options: EnqueueOptions) -> TaskId {
    let mut conn = db.pool.get().await.unwrap();
    enqueue::<Job>(
        &mut conn,
        TaskKey::try_from(key).unwrap(),
        json!({}),
        options,
    )
    .await
    .unwrap()
    .id
}

struct Worker {
    cancel: CancellationToken,
    handle: JoinHandle<Result<(), SchedulerError>>,
}

fn start(scheduler: Scheduler<Pool<AsyncPgConnection>>) -> Worker {
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(scheduler.run_until_shutdown(cancel.clone()));
    Worker { cancel, handle }
}

impl Worker {
    async fn stop(self) {
        self.cancel.cancel();
        self.handle.await.unwrap().unwrap();
    }
}

/// Poll `store::task` until the task reaches `status`; panics after `timeout`.
async fn wait_for(db: &TestDb, id: TaskId, status: TaskStatus, timeout: Duration) -> TaskView {
    let start = Instant::now();
    loop {
        let t = {
            let mut conn = db.pool.get().await.unwrap();
            store::task(&mut conn, id).await.unwrap().unwrap()
        };
        if t.state.status() == status {
            return t;
        }
        assert!(
            start.elapsed() < timeout,
            "task still {:?} after {timeout:?}, wanted {status:?}",
            t.state
        );
        tokio::time::sleep(ms(20)).await;
    }
}

async fn outcomes(db: &TestDb, id: TaskId) -> Vec<Option<AttemptOutcome>> {
    let mut conn = db.pool.get().await.unwrap();
    store::attempts(&mut conn, id)
        .await
        .unwrap()
        .into_iter()
        .map(|a| common::outcome(&a))
        .collect()
}

#[tokio::test]
async fn worker_completes_task_and_reports_healthy() {
    let db = TestDb::new().await;
    let id = enqueue_job(&db, "k", EnqueueOptions::immediate()).await;
    let scheduler = Scheduler::builder(db.pool.clone())
        .lease_duration(ms(300))
        .heartbeat_interval(ms(100))
        .register_task::<Job, _, _>(|_ctx, _args| async {
            // Outlives the lease: only heartbeats keep it.
            tokio::time::sleep(ms(700)).await;
            Ok(())
        })
        .unwrap()
        .build()
        .unwrap();
    let health = scheduler.health();
    let run = start(scheduler);

    wait_for(&db, id, TaskStatus::Done, Duration::from_secs(5)).await;
    assert_eq!(outcomes(&db, id).await, [Some(AttemptOutcome::Done)]);
    assert_eq!(health.borrow().status, HealthStatus::Healthy);

    run.stop().await;
    assert_eq!(health.borrow().status, HealthStatus::Stopped);
    db.cleanup().await;
}

#[tokio::test]
async fn notification_wakes_idle_worker() {
    let db = TestDb::new().await;
    let scheduler = Scheduler::builder(db.pool.clone())
        .max_poll_interval(Duration::from_secs(10))
        .register_task::<Job, _, _>(|_ctx, _args| async { Ok(()) })
        .unwrap()
        .build()
        .unwrap();
    let run = start(scheduler);
    tokio::time::sleep(ms(300)).await; // idle and listening

    let id = enqueue_job(&db, "k", EnqueueOptions::immediate()).await;
    wait_for(&db, id, TaskStatus::Done, Duration::from_secs(2)).await;

    run.stop().await;
    db.cleanup().await;
}

#[tokio::test]
async fn scheduled_task_runs_at_due_time_not_poll_interval() {
    let db = TestDb::new().await;
    let scheduler = Scheduler::builder(db.pool.clone())
        .max_poll_interval(Duration::from_secs(10))
        .register_task::<Job, _, _>(|_ctx, _args| async { Ok(()) })
        .unwrap()
        .build()
        .unwrap();
    let run = start(scheduler);
    tokio::time::sleep(ms(300)).await; // idle and listening

    let due = Utc::now() + chrono::Duration::seconds(1);
    let id = enqueue_job(&db, "k", EnqueueOptions::at(due)).await;
    let task = wait_for(&db, id, TaskStatus::Done, Duration::from_secs(3)).await;

    let mut conn = db.pool.get().await.unwrap();
    let attempts = store::attempts(&mut conn, id).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert!(
        attempts[0].started_at >= task.due_at,
        "claimed at {} before due {}",
        attempts[0].started_at,
        task.due_at
    );
    drop(conn);

    run.stop().await;
    db.cleanup().await;
}

#[tokio::test]
async fn retryable_error_requeues_with_backoff_then_dies() {
    let db = TestDb::new().await;
    let options = EnqueueOptions::immediate().max_attempts(MaxAttempts::try_from(2).unwrap());
    let id = enqueue_job(&db, "k", options).await;
    let scheduler = Scheduler::builder(db.pool.clone())
        .base_retry_delay(ms(200))
        .retry_jitter(Duration::ZERO)
        .register_task::<Job, _, _>(|_ctx, _args| async { Err(TaskError::retry("flaky")) })
        .unwrap()
        .build()
        .unwrap();
    let run = start(scheduler);

    let task = wait_for(&db, id, TaskStatus::Dead, Duration::from_secs(5)).await;
    assert_eq!(task.last_error.as_deref(), Some("flaky"));
    let mut conn = db.pool.get().await.unwrap();
    let attempts = store::attempts(&mut conn, id).await.unwrap();
    let got: Vec<_> = attempts.iter().map(common::outcome).collect();
    assert_eq!(
        got,
        [Some(AttemptOutcome::Retry), Some(AttemptOutcome::Dead)]
    );
    let AttemptState::Closed { finished_at, .. } = attempts[0].state else {
        panic!("first attempt still open");
    };
    let gap = attempts[1].started_at - finished_at;
    assert!(
        gap >= chrono::Duration::milliseconds(200),
        "backoff gap {gap}"
    );
    drop(conn);

    run.stop().await;
    db.cleanup().await;
}

#[tokio::test]
async fn permanent_error_marks_dead_after_one_attempt() {
    let db = TestDb::new().await;
    let id = enqueue_job(&db, "k", EnqueueOptions::immediate()).await;
    let scheduler = Scheduler::builder(db.pool.clone())
        .register_task::<Job, _, _>(|_ctx, _args| async { Err(TaskError::permanent("bad input")) })
        .unwrap()
        .build()
        .unwrap();
    let run = start(scheduler);

    let task = wait_for(&db, id, TaskStatus::Dead, Duration::from_secs(5)).await;
    assert_eq!(task.attempts, 1);
    assert_eq!(task.last_error.as_deref(), Some("bad input"));
    assert_eq!(outcomes(&db, id).await, [Some(AttemptOutcome::Dead)]);

    run.stop().await;
    db.cleanup().await;
}

#[tokio::test]
async fn permanent_failure_with_a_nul_in_its_message_is_recorded() {
    let db = TestDb::new().await;
    let id = enqueue_job(&db, "k", EnqueueOptions::immediate()).await;
    let scheduler = Scheduler::builder(db.pool.clone())
        .register_task::<Job, _, _>(|_ctx, _args| async { Err(TaskError::permanent("bad\0news")) })
        .unwrap()
        .build()
        .unwrap();
    let run = start(scheduler);

    let task = wait_for(&db, id, TaskStatus::Dead, Duration::from_secs(5)).await;
    assert_eq!(task.attempts, 1);
    assert_eq!(task.last_error.as_deref(), Some("bad\\0news"));

    run.stop().await;
    db.cleanup().await;
}

#[tokio::test]
async fn interrupted_without_shutdown_is_a_counted_retry() {
    let db = TestDb::new().await;
    let id = enqueue_job(&db, "k", EnqueueOptions::immediate()).await;
    let scheduler = Scheduler::builder(db.pool.clone())
        .base_retry_delay(Duration::from_secs(3600))
        .max_retry_delay(Duration::from_secs(3600))
        .register_task::<Job, _, _>(|_ctx, _args| async { Err(TaskError::interrupted()) })
        .unwrap()
        .build()
        .unwrap();
    let run = start(scheduler);

    let start = Instant::now();
    while outcomes(&db, id).await != [Some(AttemptOutcome::Retry)] {
        assert!(start.elapsed() < Duration::from_secs(3), "never retried");
        tokio::time::sleep(ms(20)).await;
    }
    let mut conn = db.pool.get().await.unwrap();
    let task = store::task(&mut conn, id).await.unwrap().unwrap();
    drop(conn);
    assert_eq!(task.state.status(), TaskStatus::Queued);
    assert_eq!(task.attempts, 1, "outside shutdown the attempt counts");
    assert!(task.due_at > Utc::now() + chrono::Duration::minutes(30));

    run.stop().await;
    db.cleanup().await;
}

#[tokio::test]
async fn db_contained_handler_survives_heartbeat_while_holding_row_lock() {
    let db = TestDb::new().await;
    db.pool
        .get()
        .await
        .unwrap()
        .batch_execute("CREATE TABLE effects (task_key TEXT NOT NULL)")
        .await
        .unwrap();
    let id = enqueue_job(&db, "k", EnqueueOptions::immediate()).await;
    let pool = db.pool.clone();
    let scheduler = Scheduler::builder(db.pool.clone())
        .lease_duration(ms(600))
        .heartbeat_interval(ms(200))
        .register_task::<Job, _, _>(move |ctx, _args| {
            let pool = pool.clone();
            async move {
                let mut conn = pool
                    .get()
                    .await
                    .map_err(|e| TaskError::retry(e.to_string()))?;
                conn.transaction::<_, SchedulerError, _>(async |c| {
                    store::lock_owned(c, &ctx.lease).await?;
                    // Hold the row lock across the heartbeat at 200 ms (and the one due at 400 ms).
                    tokio::time::sleep(ms(450)).await;
                    diesel::sql_query("INSERT INTO effects (task_key) VALUES ($1)")
                        .bind::<diesel::sql_types::Text, _>(ctx.task_key.as_str())
                        .execute(c)
                        .await?;
                    store::complete(c, &ctx.lease).await
                })
                .await
                .map_err(|e| TaskError::retry(e.to_string()))
            }
        })
        .unwrap()
        .build()
        .unwrap();
    let run = start(scheduler);

    wait_for(&db, id, TaskStatus::Done, Duration::from_secs(3)).await;
    assert_eq!(db.scalar("SELECT count(*) AS n FROM effects").await, 1);
    assert_eq!(outcomes(&db, id).await, [Some(AttemptOutcome::Done)]);
    // A cancelled heartbeat must not leave a transaction open on the row.
    let mut idle = -1;
    for _ in 0..50 {
        idle = db
            .scalar(
                "SELECT count(*) AS n FROM pg_locks l \
                 JOIN pg_stat_activity a ON a.pid = l.pid \
                 WHERE a.state = 'idle in transaction' AND a.pid <> pg_backend_pid() \
                 AND l.relation = 'queue_tasks'::regclass",
            )
            .await;
        if idle == 0 {
            break;
        }
        tokio::time::sleep(ms(100)).await;
    }
    assert_eq!(idle, 0, "idle-in-transaction backend left behind");

    run.stop().await;
    db.cleanup().await;
}

async fn locked_until(db: &TestDb, id: TaskId) -> chrono::DateTime<Utc> {
    let mut conn = db.pool.get().await.unwrap();
    match store::task(&mut conn, id).await.unwrap().unwrap().state {
        TaskState::Running { locked_until, .. } => locked_until,
        other => panic!("task not running: {other:?}"),
    }
}

/// Dropping the scheduler future ends its dispatches, so a heartbeat already waiting on the
/// row lock renews nothing once the lock is free.
#[tokio::test]
async fn dropped_scheduler_does_not_renew_a_pending_heartbeat() {
    let db = TestDb::new().await;
    let id = enqueue_job(&db, "k", EnqueueOptions::immediate()).await;
    let scheduler = Scheduler::builder(db.pool.clone())
        .lease_duration(ms(600))
        .heartbeat_interval(ms(150))
        .register_task::<Job, _, _>(|_ctx, _args| async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(())
        })
        .unwrap()
        .build()
        .unwrap();
    let run = start(scheduler);
    wait_for(&db, id, TaskStatus::Running, Duration::from_secs(3)).await;

    let mut holder = db.pool.get().await.unwrap();
    holder
        .batch_execute(&format!(
            "BEGIN; SELECT 1 FROM queue_tasks WHERE id = {} FOR UPDATE",
            id.0
        ))
        .await
        .unwrap();
    // This test's schema resolves the regclass, so only its own heartbeat counts.
    let start = Instant::now();
    while db
        .scalar(
            "SELECT count(*) AS n FROM pg_locks l \
             JOIN pg_stat_activity a ON a.pid = l.pid \
             WHERE a.wait_event_type = 'Lock' AND l.relation = 'queue_tasks'::regclass",
        )
        .await
        == 0
    {
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "no heartbeat blocked on the row lock"
        );
        tokio::time::sleep(ms(10)).await;
    }

    run.handle.abort();
    assert!(run.handle.await.unwrap_err().is_cancelled());
    let before = locked_until(&db, id).await;
    holder.batch_execute("ROLLBACK").await.unwrap();
    tokio::time::sleep(ms(300)).await;
    assert_eq!(
        locked_until(&db, id).await,
        before,
        "a heartbeat renewed the lease after its scheduler was dropped"
    );
    db.cleanup().await;
}

/// Graceful shutdown does not stop heartbeats: a handler that ignores `ctx.cancellation` and
/// outlives three leases while draining still completes its task.
#[tokio::test]
async fn draining_handler_keeps_its_lease() {
    let db = TestDb::new().await;
    let id = enqueue_job(&db, "k", EnqueueOptions::immediate()).await;
    let scheduler = Scheduler::builder(db.pool.clone())
        .lease_duration(ms(300))
        .heartbeat_interval(ms(100))
        .reaper_interval(ms(50))
        .shutdown_timeout(Duration::from_secs(5))
        .register_task::<Job, _, _>(|_ctx, _args| async {
            tokio::time::sleep(ms(1000)).await;
            Ok(())
        })
        .unwrap()
        .build()
        .unwrap();
    let run = start(scheduler);
    wait_for(&db, id, TaskStatus::Running, Duration::from_secs(3)).await;
    run.stop().await;
    let mut conn = db.pool.get().await.unwrap();
    let task = store::task(&mut conn, id).await.unwrap().unwrap();
    assert_eq!(task.state.status(), TaskStatus::Done);
    assert_eq!(outcomes(&db, id).await, [Some(AttemptOutcome::Done)]);
    db.cleanup().await;
}

#[tokio::test]
async fn bulk_workers_process_each_task_once() {
    let db = TestDb::new().await;
    for i in 0..200 {
        enqueue_job(&db, &format!("k{i}"), EnqueueOptions::immediate()).await;
    }
    let runs: Vec<_> = (0..3)
        .map(|_| {
            start(
                Scheduler::builder(db.pool.clone())
                    .queue("default", n(4))
                    .unwrap()
                    .register_task::<Job, _, _>(|_ctx, _args| async { Ok(()) })
                    .unwrap()
                    .build()
                    .unwrap(),
            )
        })
        .collect();

    let start = Instant::now();
    while db
        .scalar("SELECT count(*) AS n FROM queue_tasks WHERE status = 'done'")
        .await
        < 200
    {
        assert!(start.elapsed() < Duration::from_secs(5), "not all done");
        tokio::time::sleep(ms(20)).await;
    }
    for run in runs {
        run.stop().await;
    }

    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_task_attempts")
            .await,
        200
    );
    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_tasks WHERE attempts <> 1")
            .await,
        0
    );
    db.cleanup().await;
}

/// One pool of 2 would give both slots to the priority-10 tasks forever; queues isolate capacity.
#[tokio::test]
async fn busy_queue_does_not_starve_another_queue() {
    let db = TestDb::new().await;
    for i in 0..3 {
        let options = EnqueueOptions::immediate().priority(Priority::new(10));
        enqueue_job(&db, &format!("hi{i}"), options).await;
    }
    let lo = enqueue_job(&db, "lo", EnqueueOptions::immediate().queue(bulk())).await;
    let scheduler = Scheduler::builder(db.pool.clone())
        .queue("default", n(1))
        .unwrap()
        .queue("bulk", n(1))
        .unwrap()
        .shutdown_timeout(ms(100))
        .register_task::<Job, _, _>(|ctx, _args| async move {
            if ctx.task_key.as_str().starts_with("hi") {
                std::future::pending::<()>().await;
            }
            Ok(())
        })
        .unwrap()
        .build()
        .unwrap();
    let run = start(scheduler);

    wait_for(&db, lo, TaskStatus::Done, Duration::from_secs(3)).await;
    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_tasks WHERE status = 'running'")
            .await,
        1
    );
    assert_eq!(
        db.scalar(
            "SELECT count(*) AS n FROM queue_tasks WHERE status = 'queued' AND task_key LIKE 'hi%'"
        )
        .await,
        2
    );

    run.stop().await;
    db.cleanup().await;
}

#[tokio::test]
async fn worker_claims_only_its_configured_queues() {
    let db = TestDb::new().await;
    let d = enqueue_job(&db, "d", EnqueueOptions::immediate()).await;
    let b = enqueue_job(&db, "b", EnqueueOptions::immediate().queue(bulk())).await;
    let scheduler = Scheduler::builder(db.pool.clone())
        .queue("bulk", n(1))
        .unwrap()
        .register_task::<Job, _, _>(|_ctx, _args| async { Ok(()) })
        .unwrap()
        .build()
        .unwrap();
    let run = start(scheduler);

    wait_for(&db, b, TaskStatus::Done, Duration::from_secs(3)).await;
    tokio::time::sleep(ms(300)).await;
    let task = {
        let mut conn = db.pool.get().await.unwrap();
        store::task(&mut conn, d).await.unwrap().unwrap()
    };
    assert_eq!(task.state.status(), TaskStatus::Queued);
    assert_eq!(task.attempts, 0);

    run.stop().await;
    db.cleanup().await;
}

/// Fake remote API that deduplicates by idempotency key: calls per key.
type Remote = Arc<Mutex<HashMap<String, u32>>>;

fn call_remote(remote: &Remote, key: &str) {
    *remote.lock().unwrap().entry(key.to_owned()).or_default() += 1;
}

#[tokio::test]
async fn crash_after_remote_accept_does_not_duplicate_external_effect() {
    let db = TestDb::new().await;
    let id = enqueue_job(&db, "k", EnqueueOptions::immediate()).await;
    let remote = Remote::default();
    let accepted = Arc::new(Notify::new());

    let (remote_a, accepted_a) = (remote.clone(), accepted.clone());
    let a = Scheduler::builder(db.pool.clone())
        .lease_duration(ms(500))
        .heartbeat_interval(ms(150))
        .register_task::<Job, _, _>(move |ctx, _args| {
            let (remote, accepted) = (remote_a.clone(), accepted_a.clone());
            async move {
                call_remote(&remote, ctx.task_key.as_str());
                accepted.notify_one();
                // "Crash" after the remote accepted, before the ACK.
                std::future::pending().await
            }
        })
        .unwrap()
        .build()
        .unwrap();
    let run_a = start(a);
    tokio::time::timeout(Duration::from_secs(5), accepted.notified())
        .await
        .expect("worker A should call the remote");
    // Crash worker A: dropping its run aborts the handler with no release, so the lease is left
    // for the reaper (a graceful stop would release the task).
    run_a.handle.abort();
    let _ = run_a.handle.await;

    let remote_b = remote.clone();
    let b = Scheduler::builder(db.pool.clone())
        .crash_retry_delay(Duration::ZERO)
        .reaper_interval(ms(100))
        .register_task::<Job, _, _>(move |ctx, _args| {
            let remote = remote_b.clone();
            async move {
                call_remote(&remote, ctx.task_key.as_str());
                Ok(())
            }
        })
        .unwrap()
        .build()
        .unwrap();
    let run_b = start(b);
    wait_for(&db, id, TaskStatus::Done, Duration::from_secs(5)).await;
    run_b.stop().await;

    let calls = remote.lock().unwrap().clone();
    assert_eq!(calls.values().sum::<u32>(), 2, "remote calls");
    assert_eq!(calls.len(), 1, "distinct effects");
    assert_eq!(
        outcomes(&db, id).await,
        [
            Some(AttemptOutcome::LeaseExpired),
            Some(AttemptOutcome::Done)
        ]
    );
    db.cleanup().await;
}

/// Claim one queued `name` task outside any scheduler, with a 60 s lease.
async fn claim_elsewhere(db: &TestDb, name: &str) -> TaskId {
    let mut conn = db.pool.get().await.unwrap();
    store::claim(
        &mut conn,
        &WorkerId::generate(),
        &pg_task_scheduler::QueueName::default(),
        &[pg_task_scheduler::TaskName::try_from(name).unwrap()],
        NonZeroU32::MIN,
        common::lease(Duration::from_secs(60)),
    )
    .await
    .unwrap()
    .ready
    .pop()
    .unwrap()
    .lease
    .task_id
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

/// Dropping the run future without cancelling it (an aborted JoinHandle, a failed
/// `try_join!` sibling) must stop maintenance, and must not hand the LISTEN connection
/// back to the pool still subscribed, where it would buffer every notification.
#[tokio::test]
async fn aborted_run_stops_maintenance_and_unlistens() {
    let db = TestDb::new().await;
    struct Other;
    impl Task for Other {
        const NAME: &'static str = "other";
        type Args = serde_json::Value;
    }
    let mut conn = db.pool.get().await.unwrap();
    enqueue::<Other>(
        &mut conn,
        TaskKey::try_from("other").unwrap(),
        json!({}),
        EnqueueOptions::immediate(),
    )
    .await
    .unwrap();
    drop(conn);
    let other = claim_elsewhere(&db, "other").await;
    let scheduler = Scheduler::builder(db.pool.clone())
        .reaper_interval(ms(50))
        .register_task::<Job, _, _>(|_ctx, _args| async { Ok(()) })
        .unwrap()
        .build()
        .unwrap();
    let health = scheduler.health();
    let Worker { cancel, handle } = start(scheduler);
    tokio::time::sleep(ms(300)).await; // idle and listening
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());
    assert_eq!(health.borrow().status, HealthStatus::Stopped);

    // A detached maintenance task would reap this within one 50 ms interval.
    db.expire_lease(other).await;
    tokio::time::sleep(ms(300)).await;
    let task = {
        let mut conn = db.pool.get().await.unwrap();
        store::task(&mut conn, other).await.unwrap().unwrap()
    };
    assert_eq!(
        task.state.status(),
        TaskStatus::Running,
        "maintenance outlived the run"
    );
    assert_pool_not_listening(&db).await;
    drop(cancel);
    db.cleanup().await;
}

/// Dropping the run future from a thread outside the runtime (e.g. a future polled by
/// `block_on` and dropped afterwards) must still UNLISTEN the pooled connection.
#[test]
fn run_dropped_outside_runtime_still_unlistens() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = rt.block_on(TestDb::new());
    let scheduler = Scheduler::builder(db.pool.clone())
        .register_task::<Job, _, _>(|_ctx, _args| async { Ok(()) })
        .unwrap()
        .build()
        .unwrap();
    let health = scheduler.health();
    let mut run = Box::pin(scheduler.run_until_shutdown(CancellationToken::new()));
    rt.block_on(async {
        tokio::select! {
            _ = &mut run => panic!("run ended without cancellation"),
            () = tokio::time::sleep(ms(300)) => {} // idle and listening
        }
    });
    drop(run); // outside any runtime context
    assert_eq!(health.borrow().status, HealthStatus::Stopped);
    rt.block_on(async {
        assert_pool_not_listening(&db).await;
        db.cleanup().await;
    });
}

/// Wait for every pooled connection to be idle in the pool (a spawned UNLISTEN holds the
/// listener until it is done), then check that none of them is still listening.
async fn assert_pool_not_listening(db: &TestDb) {
    let start = Instant::now();
    while db.pool.status().available < db.pool.status().size {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "a connection never returned"
        );
        tokio::time::sleep(ms(20)).await;
    }
    let mut conns = Vec::new();
    for _ in 0..db.pool.status().size {
        conns.push(db.pool.get().await.unwrap());
    }
    for conn in &mut conns {
        let listening = diesel::sql_query("SELECT count(*) AS n FROM pg_listening_channels()")
            .get_result::<Count>(conn)
            .await
            .unwrap()
            .n;
        assert_eq!(listening, 0, "a pooled connection is still LISTENing");
    }
}

/// A retention backlog must not hold up lease recovery: the reaper runs between cleanup
/// batches, not only after the backlog drains.
#[tokio::test]
async fn cleanup_backlog_does_not_starve_reaper() {
    let db = TestDb::new().await;
    db.pool
        .get()
        .await
        .unwrap()
        .batch_execute(
            "INSERT INTO queue_tasks (task_key, task_name, status, completed_at) \
             SELECT 'old-' || i, 'job', 'done', now() - interval '31 days' \
             FROM generate_series(1, 20000) AS i",
        )
        .await
        .unwrap();
    let id = enqueue_job(&db, "k", EnqueueOptions::immediate()).await;
    claim_elsewhere(&db, "job").await;
    let scheduler = Scheduler::builder(db.pool.clone())
        .reaper_interval(Duration::from_secs(60)) // one pass, starting now
        .crash_retry_delay(Duration::from_secs(3600)) // keep the reaped task queued
        .retention(Retention {
            batch_size: NonZeroU32::MIN, // one row per cleanup batch: a long drain
            ..Retention::default()
        })
        .register_task::<Job, _, _>(|_ctx, _args| async { Ok(()) })
        .unwrap()
        .build()
        .unwrap();
    let run = start(scheduler);
    tokio::time::sleep(ms(200)).await; // the pass is now draining the backlog

    db.expire_lease(id).await;
    wait_for(&db, id, TaskStatus::Queued, Duration::from_secs(1)).await;
    let left = db
        .scalar("SELECT count(*) AS n FROM queue_tasks WHERE status = 'done'")
        .await;
    assert!(
        left > 0,
        "the backlog drained first, so this proves nothing"
    );

    run.stop().await;
    db.cleanup().await;
}

/// Counter totals by metric name. A local recorder sees every task here: `#[tokio::test]`
/// runs them all on the test thread.
#[derive(Clone, Default)]
struct Counters(Arc<Mutex<HashMap<String, u64>>>);

impl Counters {
    fn get(&self, name: &str) -> u64 {
        self.0.lock().unwrap().get(name).copied().unwrap_or(0)
    }
}

struct NamedCounter(Counters, String);

impl metrics::CounterFn for NamedCounter {
    fn increment(&self, value: u64) {
        *self.0.0.lock().unwrap().entry(self.1.clone()).or_default() += value;
    }
    fn absolute(&self, _value: u64) {}
}

impl metrics::Recorder for Counters {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::from_arc(Arc::new(NamedCounter(self.clone(), key.name().to_owned())))
    }
    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

/// A DB-contained handler completes the task and is still running when a heartbeat
/// matches no row. The task is done by this lease, so it counts as done, not as a lost lease.
#[tokio::test]
async fn db_contained_completion_before_handler_returns_counts_as_done() {
    let counters = Counters::default();
    let _recorder = metrics::set_default_local_recorder(&counters);
    let db = TestDb::new().await;
    let id = enqueue_job(&db, "k", EnqueueOptions::immediate()).await;
    let pool = db.pool.clone();
    let scheduler = Scheduler::builder(db.pool.clone())
        .lease_duration(ms(600))
        .heartbeat_interval(ms(200))
        .register_task::<Job, _, _>(move |ctx, _args| {
            let pool = pool.clone();
            async move {
                let mut conn = pool
                    .get()
                    .await
                    .map_err(|e| TaskError::retry(e.to_string()))?;
                conn.transaction::<_, SchedulerError, _>(async |c| {
                    store::lock_owned(c, &ctx.lease).await?;
                    // The heartbeat at 200 ms waits on this row lock, then matches no row.
                    tokio::time::sleep(ms(300)).await;
                    store::complete(c, &ctx.lease).await
                })
                .await
                .map_err(|e| TaskError::retry(e.to_string()))?;
                // Never return: the lease keeper ends this dispatch.
                std::future::pending().await
            }
        })
        .unwrap()
        .build()
        .unwrap();
    let run = start(scheduler);

    wait_for(&db, id, TaskStatus::Done, Duration::from_secs(3)).await;
    let (done, lost) = (
        "pg_task_scheduler_tasks_done_total",
        "pg_task_scheduler_leases_lost_total",
    );
    let start = Instant::now();
    while counters.get(done) + counters.get(lost) == 0 {
        assert!(
            start.elapsed() < Duration::from_secs(3),
            "dispatch never ended"
        );
        tokio::time::sleep(ms(20)).await;
    }
    assert_eq!((counters.get(done), counters.get(lost)), (1, 0));

    run.stop().await;
    db.cleanup().await;
}

#[tokio::test]
async fn healthy_task_in_a_batch_with_a_bad_payload_runs() {
    let db = TestDb::new().await;
    let bad = TaskId(
        db.scalar(
            "WITH t AS (INSERT INTO queue_tasks (task_key, task_name, payload) \
             VALUES ('bad', 'job', '{\"x\":1e500}'::jsonb) RETURNING id) SELECT id AS n FROM t",
        )
        .await,
    );
    let good = TaskId(
        db.scalar(
            "WITH t AS (INSERT INTO queue_tasks (task_key, task_name, payload) \
             VALUES ('good', 'job', '{}') RETURNING id) SELECT id AS n FROM t",
        )
        .await,
    );
    let scheduler = Scheduler::builder(db.pool.clone())
        .register_task::<Job, _, _>(|_ctx, _args| async { Ok(()) })
        .unwrap()
        .build()
        .unwrap();
    let run = start(scheduler);

    wait_for(&db, good, TaskStatus::Done, Duration::from_secs(5)).await;
    // Dead-lettered on its own task, after the dispatch.
    wait_for(&db, bad, TaskStatus::Dead, Duration::from_secs(5)).await;

    run.stop().await;
    db.cleanup().await;
}

#[tokio::test]
async fn handler_does_not_start_when_the_claim_arrives_after_its_lease() {
    let db = TestDb::new().await;
    // The claim's response is delayed past the lease, after the row is leased.
    db.pool
        .get()
        .await
        .unwrap()
        .batch_execute(
            "CREATE FUNCTION slow_claim() RETURNS trigger AS $$ BEGIN PERFORM pg_sleep(0.3); RETURN NEW; END $$ LANGUAGE plpgsql;
             CREATE TRIGGER slow_claim AFTER UPDATE ON queue_tasks FOR EACH ROW WHEN (NEW.status = 'running') EXECUTE FUNCTION slow_claim();",
        )
        .await
        .unwrap();
    enqueue_job(&db, "k", EnqueueOptions::immediate()).await;
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = started.clone();
    let scheduler = Scheduler::builder(db.pool.clone())
        .lease_duration(ms(90))
        .heartbeat_interval(ms(30))
        .register_task::<Job, _, _>(move |_ctx, _args| {
            let flag = flag.clone();
            async move {
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap()
        .build()
        .unwrap();
    let run = start(scheduler);
    tokio::time::sleep(Duration::from_secs(1)).await;
    run.stop().await;

    assert!(!std::sync::atomic::AtomicBool::load(
        &started,
        std::sync::atomic::Ordering::SeqCst
    ));
    db.cleanup().await;
}

#[tokio::test]
async fn slow_dead_letter_does_not_delay_a_healthy_claim() {
    let db = TestDb::new().await;
    // Every dead-letter takes longer than the whole lease.
    db.pool
        .get()
        .await
        .unwrap()
        .batch_execute(
            "CREATE FUNCTION slow_dead() RETURNS trigger AS $$ BEGIN PERFORM pg_sleep(0.25); RETURN NEW; END $$ LANGUAGE plpgsql;
             CREATE TRIGGER slow_dead BEFORE UPDATE ON queue_tasks FOR EACH ROW WHEN (NEW.status = 'dead') EXECUTE FUNCTION slow_dead();",
        )
        .await
        .unwrap();
    // Both queued before the worker starts, so one claim takes them together.
    db.scalar(
        "WITH t AS (INSERT INTO queue_tasks (task_key, task_name, payload) \
         VALUES ('bad', 'job', '{\"x\":1e500}'::jsonb) RETURNING id) SELECT id AS n FROM t",
    )
    .await;
    let good = enqueue_job(&db, "good", EnqueueOptions::immediate()).await;
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = started.clone();
    let scheduler = Scheduler::builder(db.pool.clone())
        .lease_duration(ms(90))
        .heartbeat_interval(ms(30))
        .register_task::<Job, _, _>(move |_ctx, _args| {
            let flag = flag.clone();
            async move {
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap()
        .build()
        .unwrap();
    let run = start(scheduler);

    wait_for(&db, good, TaskStatus::Done, Duration::from_secs(5)).await;
    assert!(std::sync::atomic::AtomicBool::load(
        &started,
        std::sync::atomic::Ordering::SeqCst
    ));
    run.stop().await;
    db.cleanup().await;
}

/// Maintenance, the registry and dead-lettering each hold a connection of their own: all three
/// stalled in the database must not fill a pool sized at queue limits + 5 and starve the
/// healthy task's claim.
#[tokio::test]
async fn stalled_background_work_leaves_room_for_healthy_work() {
    let db = TestDb::new().await;
    // The scheduler's own pool, at the stated budget (queue limit 1 + 5); the test reads
    // through `db.pool`, so it never waits behind the scheduler.
    let pool = db.sized_pool(6);
    // Held outside the pool: every move to `dead` waits on the advisory lock, maintenance's
    // outbox cleanup on `queue_outbox`, and the registry refresh on `queue_worker_task_names`.
    let mut lock = AsyncPgConnection::establish(&common::database_url())
        .await
        .unwrap();
    lock.batch_execute(&format!(
        "SELECT pg_advisory_lock(4949); BEGIN; \
         LOCK TABLE \"{s}\".queue_outbox, \"{s}\".queue_worker_task_names IN ACCESS EXCLUSIVE MODE",
        s = db.schema
    ))
    .await
    .unwrap();
    // Poisoned rows due first, so the claims reach them before the healthy task.
    db.install(
        "CREATE FUNCTION stall_dead() RETURNS trigger AS $$ BEGIN PERFORM pg_advisory_lock(4949); PERFORM pg_advisory_unlock(4949); RETURN NEW; END $$ LANGUAGE plpgsql;
         CREATE TRIGGER stall_dead BEFORE UPDATE ON queue_tasks FOR EACH ROW WHEN (NEW.status = 'dead') EXECUTE FUNCTION stall_dead();
         INSERT INTO queue_tasks (task_key, task_name, payload, max_attempts, due_at)
         SELECT 'bad' || i, 'job', '{\"x\":1e500}'::jsonb, 1, now() - interval '1 minute' FROM generate_series(1, 5) AS i;",
    )
    .await;
    let good = enqueue_job(&db, "good", EnqueueOptions::immediate()).await;
    let scheduler = Scheduler::builder(pool.clone())
        .queue("default", n(1))
        .unwrap()
        .max_poll_interval(ms(35))
        .lease_duration(Duration::from_secs(2))
        .heartbeat_interval(ms(150))
        .reaper_interval(ms(200))
        .registry_interval(ms(200))
        .register_task::<Job, _, _>(|_ctx, _args| async { Ok(()) })
        .unwrap()
        .build()
        .unwrap();
    let run = start(scheduler);

    // Each stall is real before anything is checked: its statement waits on its lock. The
    // relations resolve in this test's schema through `db.pool`'s search_path.
    let advisory_waiters = "SELECT count(*) AS n FROM pg_locks WHERE locktype = 'advisory' AND objid = 4949 AND NOT granted";
    for (what, waiters) in [
        ("dead-lettering", advisory_waiters),
        (
            "maintenance",
            "SELECT count(*) AS n FROM pg_locks WHERE relation = 'queue_outbox'::regclass AND NOT granted",
        ),
        (
            "the registry refresh",
            "SELECT count(*) AS n FROM pg_locks WHERE relation = 'queue_worker_task_names'::regclass AND NOT granted",
        ),
    ] {
        let started = Instant::now();
        while db.scalar(waiters).await == 0 {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "{what} never stalled"
            );
            tokio::time::sleep(ms(20)).await;
        }
    }
    wait_for(&db, good, TaskStatus::Done, Duration::from_secs(5)).await;
    // The reaper's move to `dead` would be a second waiter, but it is stalled with maintenance.
    assert_eq!(
        db.scalar(advisory_waiters).await,
        1,
        "sessions waiting to dead-letter"
    );

    lock.batch_execute("COMMIT; SELECT pg_advisory_unlock(4949)")
        .await
        .unwrap();
    let started = Instant::now();
    while db
        .scalar("SELECT count(*) AS n FROM queue_tasks WHERE status = 'dead'")
        .await
        < 5
    {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "poisoned rows not dead after the locks were released"
        );
        tokio::time::sleep(ms(20)).await;
    }

    run.stop().await;
    pool.close();
    db.cleanup().await;
}
