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
    AttemptOutcome, EnqueueOptions, HealthStatus, MaxAttempts, Scheduler, SchedulerError, Task,
    TaskError, TaskId, TaskKey, TaskStatus, TaskView, enqueue,
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
