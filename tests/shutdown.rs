mod common;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use common::TestDb;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::deadpool::Pool;
use pg_task_scheduler::AttemptOutcome;
use pg_task_scheduler::EnqueueOptions;
use pg_task_scheduler::Scheduler;
use pg_task_scheduler::SchedulerError;
use pg_task_scheduler::Task;
use pg_task_scheduler::TaskError;
use pg_task_scheduler::TaskId;
use pg_task_scheduler::TaskKey;
use pg_task_scheduler::TaskStatus;
use pg_task_scheduler::TaskView;
use pg_task_scheduler::enqueue;
use pg_task_scheduler::store;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

struct Blocker;
impl Task for Blocker {
    const NAME: &'static str = "blocker";
    type Args = serde_json::Value;
}

struct Boom;
impl Task for Boom {
    const NAME: &'static str = "boom";
    type Args = serde_json::Value;
}

async fn enqueue_now<T: Task<Args = serde_json::Value>>(db: &TestDb) -> TaskId {
    let mut conn = db.pool.get().await.unwrap();
    enqueue::<T>(
        &mut conn,
        TaskKey::try_from("k").unwrap(),
        serde_json::json!({}),
        EnqueueOptions::immediate(),
    )
    .await
    .unwrap()
    .id
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

async fn task_of(db: &TestDb, id: TaskId) -> TaskView {
    let mut conn = db.pool.get().await.unwrap();
    store::task(&mut conn, id).await.unwrap().unwrap()
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

/// Run `scheduler` until its handler announces `started`.
async fn run_until_started(
    scheduler: Scheduler<Pool<AsyncPgConnection>>,
    started: &Notify,
) -> (CancellationToken, JoinHandle<Result<(), SchedulerError>>) {
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(scheduler.run_until_shutdown(cancel.clone()));
    tokio::time::timeout(Duration::from_secs(10), started.notified())
        .await
        .expect("handler should start within 10s");
    (cancel, handle)
}

/// Regression test for "Shutdown can detach handler work from finalization".
///
/// The handler announces STARTED, then awaits a sleep that outlasts the shutdown
/// deadline, then sets AFTER. If shutdown *cancels* the handler (correct), the
/// future is dropped at the sleep and AFTER never runs. If shutdown *detaches* it
/// (the bug), the orphaned task finishes its sleep and sets AFTER after the
/// scheduler has already returned.
#[tokio::test]
async fn shutdown_cancels_in_flight_handler_no_detach() {
    let db = TestDb::new().await;
    enqueue_now::<Blocker>(&db).await;

    let started = Arc::new(Notify::new());
    let after = Arc::new(AtomicBool::new(false));
    let started_h = started.clone();
    let after_h = after.clone();

    let scheduler = Scheduler::builder(db.pool.clone())
        .max_poll_interval(Duration::from_millis(50))
        .reaper_interval(Duration::from_secs(60))
        .shutdown_timeout(Duration::from_millis(200))
        .register_task::<Blocker, _, _>(move |_ctx, _a| {
            let started = started_h.clone();
            let after = after_h.clone();
            async move {
                started.notify_one();
                // Outlasts shutdown_timeout (200ms) so the deadline forces an
                // abort while the handler is still here; the 3s post-shutdown wait
                // then leaves a wide margin for a detached orphan to set AFTER.
                tokio::time::sleep(Duration::from_secs(1)).await;
                after.store(true, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap()
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(scheduler.run_until_shutdown(cancel.clone()));

    // Shut down only once the handler is actually in flight.
    tokio::time::timeout(Duration::from_secs(10), started.notified())
        .await
        .expect("handler should start within 10s");

    cancel.cancel();
    handle.await.unwrap().unwrap();

    // Wait past the handler's full sleep. A detached handler would have set AFTER
    // by now; a cancelled one never will.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        !after.load(Ordering::SeqCst),
        "handler kept running after shutdown: work was detached from finalization, not cancelled"
    );

    db.cleanup().await;
}

/// A panicking handler is NOT finalized: the task stays `running` with its attempt
/// open (no terminal outcome carrying a panic string), and the reaper recovers it
/// once the lease expires.
#[tokio::test]
async fn panicking_handler_is_not_finalized_and_is_recoverable() {
    let db = TestDb::new().await;
    let id = enqueue_now::<Boom>(&db).await;

    let started = Arc::new(Notify::new());
    let started_h = started.clone();

    let scheduler = Scheduler::builder(db.pool.clone())
        .lease_duration(Duration::from_millis(1500))
        .heartbeat_interval(Duration::from_millis(500))
        .reaper_interval(Duration::from_millis(100))
        // Keep the recovered task queued so it does not panic again.
        .crash_retry_delay(Duration::from_secs(3600))
        .shutdown_timeout(Duration::from_millis(200))
        .register_task::<Boom, _, _>(move |_ctx, _a| {
            let started = started_h.clone();
            async move {
                started.notify_one();
                panic!("handler boom");
            }
        })
        .unwrap()
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(scheduler.run_until_shutdown(cancel.clone()));

    tokio::time::timeout(Duration::from_secs(10), started.notified())
        .await
        .expect("handler should start within 10s");
    // Margin for the panic to unwind the dispatch task.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut conn = db.pool.get().await.unwrap();
    let task = store::task(&mut conn, id).await.unwrap().unwrap();
    assert_eq!(
        task.status,
        TaskStatus::Running,
        "panicked task must stay running"
    );
    let attempts = store::attempts(&mut conn, id).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].outcome, None, "attempt must stay open");
    drop(conn);

    // The reaper recovers it after the lease.
    let start = std::time::Instant::now();
    let task = loop {
        let mut conn = db.pool.get().await.unwrap();
        let task = store::task(&mut conn, id).await.unwrap().unwrap();
        if task.status == TaskStatus::Queued {
            break task;
        }
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "reaper never recovered the task"
        );
        drop(conn);
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(task.last_error.as_deref(), Some("worker lease expired"));
    let mut conn = db.pool.get().await.unwrap();
    let attempts = store::attempts(&mut conn, id).await.unwrap();
    assert_eq!(attempts[0].outcome, Some(AttemptOutcome::LeaseExpired));
    drop(conn);

    cancel.cancel();
    handle.await.unwrap().unwrap();
    db.cleanup().await;
}

/// ctx.cancellation fires when shutdown begins, not at the abort.
#[tokio::test]
async fn shutdown_cancels_handlers_and_interrupted_task_is_released() {
    let db = TestDb::new().await;
    let id = enqueue_now::<Blocker>(&db).await;
    let started = Arc::new(Notify::new());
    let started_h = started.clone();
    let scheduler = Scheduler::builder(db.pool.clone())
        .max_poll_interval(ms(50))
        .reaper_interval(Duration::from_secs(60))
        .shutdown_timeout(Duration::from_secs(30))
        .register_task::<Blocker, _, _>(move |ctx, _a| {
            let started = started_h.clone();
            async move {
                started.notify_one();
                ctx.cancellation.cancelled().await;
                Err(TaskError::interrupted())
            }
        })
        .unwrap()
        .build()
        .unwrap();
    let (cancel, handle) = run_until_started(scheduler, &started).await;

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect(
            "shutdown waited for shutdown_timeout: ctx.cancellation did not fire at drain start",
        )
        .unwrap()
        .unwrap();

    let task = task_of(&db, id).await;
    assert_eq!(task.status, TaskStatus::Queued);
    assert_eq!(task.attempts, 0, "the interrupted attempt must not count");
    assert_eq!(task.locked_by, None);
    assert_eq!(outcomes(&db, id).await, [Some(AttemptOutcome::Released)]);
    db.cleanup().await;
}

#[tokio::test]
async fn handler_still_running_at_shutdown_timeout_is_released() {
    let db = TestDb::new().await;
    let id = enqueue_now::<Blocker>(&db).await;
    let started = Arc::new(Notify::new());
    let started_h = started.clone();
    let scheduler = Scheduler::builder(db.pool.clone())
        .max_poll_interval(ms(50))
        .reaper_interval(Duration::from_secs(60))
        .shutdown_timeout(ms(200))
        .register_task::<Blocker, _, _>(move |_ctx, _a| {
            let started = started_h.clone();
            async move {
                started.notify_one();
                std::future::pending::<()>().await;
                Ok(())
            }
        })
        .unwrap()
        .build()
        .unwrap();
    let (cancel, handle) = run_until_started(scheduler, &started).await;

    let stopping = Instant::now();
    cancel.cancel();
    handle.await.unwrap().unwrap();
    assert!(stopping.elapsed() < ms(200) + Duration::from_secs(5));

    let task = task_of(&db, id).await;
    assert_eq!(task.status, TaskStatus::Queued);
    assert_eq!(task.attempts, 0, "the dropped attempt must not count");
    assert_eq!(outcomes(&db, id).await, [Some(AttemptOutcome::Released)]);
    db.cleanup().await;
}

#[tokio::test]
async fn handler_finishing_during_drain_is_completed() {
    let db = TestDb::new().await;
    let id = enqueue_now::<Blocker>(&db).await;
    let started = Arc::new(Notify::new());
    let started_h = started.clone();
    let scheduler = Scheduler::builder(db.pool.clone())
        .max_poll_interval(ms(50))
        .reaper_interval(Duration::from_secs(60))
        .shutdown_timeout(Duration::from_secs(5))
        .register_task::<Blocker, _, _>(move |_ctx, _a| {
            let started = started_h.clone();
            async move {
                // Ignores ctx.cancellation and finishes inside shutdown_timeout.
                started.notify_one();
                tokio::time::sleep(ms(300)).await;
                Ok(())
            }
        })
        .unwrap()
        .build()
        .unwrap();
    let (cancel, handle) = run_until_started(scheduler, &started).await;

    cancel.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(task_of(&db, id).await.status, TaskStatus::Done);
    assert_eq!(outcomes(&db, id).await, [Some(AttemptOutcome::Done)]);
    db.cleanup().await;
}

#[tokio::test]
async fn shutdown_with_row_lock_held_returns_in_bounded_time() {
    let db = TestDb::new().await;
    let id = enqueue_now::<Blocker>(&db).await;
    let started = Arc::new(Notify::new());
    let started_h = started.clone();
    let pool = db.pool.clone();
    let scheduler = Scheduler::builder(db.pool.clone())
        .max_poll_interval(ms(50))
        .reaper_interval(Duration::from_secs(60))
        .shutdown_timeout(ms(200))
        .register_task::<Blocker, _, _>(move |ctx, _a| {
            let started = started_h.clone();
            let pool = pool.clone();
            async move {
                let mut conn = pool
                    .get()
                    .await
                    .map_err(|e| TaskError::retry(e.to_string()))?;
                // Holds the row lock until dropped, so a release can block behind it.
                conn.transaction(async |c| {
                    store::lock_owned(c, &ctx.lease).await?;
                    started.notify_one();
                    std::future::pending::<()>().await;
                    Ok::<_, SchedulerError>(())
                })
                .await
                .map_err(|e| TaskError::retry(e.to_string()))
            }
        })
        .unwrap()
        .build()
        .unwrap();
    let (cancel, handle) = run_until_started(scheduler, &started).await;

    cancel.cancel();
    tokio::time::timeout(
        ms(200) + Duration::from_secs(5) + Duration::from_secs(2),
        handle,
    )
    .await
    .expect("shutdown hung behind the row lock")
    .unwrap()
    .unwrap();

    let status = task_of(&db, id).await.status;
    assert!(
        matches!(status, TaskStatus::Queued | TaskStatus::Running),
        "{status:?}"
    );
    db.cleanup().await;
}

/// A pool whose `acquire` never completes, standing in for a stalled database.
#[derive(Clone)]
struct StalledPool;

impl pg_task_scheduler::SchedulerPool for StalledPool {
    type Conn = Box<AsyncPgConnection>;

    async fn acquire(&self) -> Result<Self::Conn, pg_task_scheduler::PoolError> {
        std::future::pending().await
    }
}

/// The loop's pool and query awaits race the shutdown token, so a stalled database cannot
/// hold shutdown past its bound.
#[tokio::test]
async fn shutdown_returns_when_the_pool_stalls() {
    let scheduler = Scheduler::builder(StalledPool)
        .max_poll_interval(ms(50))
        .shutdown_timeout(ms(200))
        .register_task::<Blocker, _, _>(|_ctx, _a| async { Ok(()) })
        .unwrap()
        .build()
        .unwrap();
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(scheduler.run_until_shutdown(cancel.clone()));
    tokio::time::sleep(ms(100)).await; // the loop is now waiting on the pool
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("shutdown hung behind a stalled pool")
        .unwrap()
        .unwrap();
}
