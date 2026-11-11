mod common;

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::Utc;
use common::TestDb;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use pg_task_scheduler::store;
use pg_task_scheduler::{
    AttemptOutcome, EnqueueOptions, HealthStatus, MaxAttempts, Retention, Scheduler,
    SchedulerError, Task, TaskError, TaskId, TaskKey, TaskStatus, TaskView, WorkerId, enqueue,
};
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
        if t.status == status {
            return t;
        }
        assert!(
            start.elapsed() < timeout,
            "task still {:?} after {timeout:?}, wanted {status:?}",
            t.status
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
        .map(|a| a.outcome)
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
    let got: Vec<_> = attempts.iter().map(|a| a.outcome).collect();
    assert_eq!(
        got,
        [Some(AttemptOutcome::Retry), Some(AttemptOutcome::Dead)]
    );
    let gap = attempts[1].started_at - attempts[0].finished_at.unwrap();
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

    run.stop().await;
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
                    .max_concurrency(NonZeroUsize::new(4).unwrap())
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
        .shutdown_timeout(ms(50))
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
    run_a.stop().await; // aborts the handler; the lease is left in place

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
        &[name.to_owned()],
        NonZeroUsize::MIN,
        Duration::from_secs(60),
    )
    .await
    .unwrap()
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
        task.status,
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
            batch_size: NonZeroUsize::MIN, // one row per cleanup batch: a long drain
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
