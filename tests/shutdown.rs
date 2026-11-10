mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::TestDb;
use pg_task_scheduler::store;
use pg_task_scheduler::{
    AttemptOutcome, EnqueueOptions, Scheduler, Task, TaskId, TaskKey, TaskStatus, enqueue,
};
use tokio::sync::Notify;
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
