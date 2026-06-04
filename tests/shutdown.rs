mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::Utc;
use common::TestDb;
use pg_task_scheduler::{RunId, RunState, Scheduler, WorkerId};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Regression test for "Shutdown can detach handler work from finalization".
///
/// The handler announces STARTED, then awaits a sleep that outlasts the shutdown
/// deadline, then sets AFTER. If shutdown *cancels* the handler (correct), the
/// future is dropped at the sleep and AFTER never runs. If shutdown *detaches* it
/// (the bug), the orphaned task finishes its sleep and sets AFTER after the
/// scheduler has already returned. The AFTER flag is the precise discriminator:
/// in the buggy build the handler's side effect happens while finalization does
/// not — exactly the "detach work from finalization" failure.
#[tokio::test]
async fn shutdown_cancels_in_flight_handler_no_detach() {
    let db = TestDb::new().await;

    // A job that is already due, so the scheduler materializes + claims it.
    let _ = db
        .insert_job_full(
            "blocker",
            "*/1 * * * *",
            Utc::now() - chrono::Duration::minutes(1),
            "5 minutes",
            3,
            false,
        )
        .await;

    let started = Arc::new(Notify::new());
    let after = Arc::new(AtomicBool::new(false));
    let started_h = started.clone();
    let after_h = after.clone();

    let scheduler = Scheduler::builder(db.pool.clone(), WorkerId::new("shutdown-test"))
        .poll_interval(Duration::from_millis(50))
        .reaper_interval(Duration::from_secs(60))
        .shutdown_timeout(Duration::from_millis(200))
        .register::<serde_json::Value, _, _>("blocker", move |_ctx, _a| {
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

/// Locks the panic invariant: a panicking handler is NOT finalized. It must leave
/// the run with its lease held and no terminal outcome (still `Running`), so it is
/// recoverable via lease expiry — not written as a terminal `Failed` carrying an
/// internal panic string.
#[tokio::test]
async fn panicking_handler_is_not_finalized_and_is_recoverable() {
    let db = TestDb::new().await;

    let job_id = db
        .insert_job_full(
            "boom",
            "*/1 * * * *",
            Utc::now() - chrono::Duration::minutes(1),
            "5 minutes", // long lease: must NOT expire during the test
            3,
            false,
        )
        .await;

    let started = Arc::new(Notify::new());
    let started_h = started.clone();

    let scheduler = Scheduler::builder(db.pool.clone(), WorkerId::new("panic-test"))
        .poll_interval(Duration::from_millis(50))
        .reaper_interval(Duration::from_secs(60))
        .shutdown_timeout(Duration::from_millis(200))
        .register::<serde_json::Value, _, _>("boom", move |_ctx, _a| {
            let started = started_h.clone();
            async move {
                started.notify_one();
                panic!("handler boom");
            }
        })
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(scheduler.run_until_shutdown(cancel.clone()));

    // The handler signals STARTED immediately before panicking. By the time we see
    // it, claim has already created the lease row (claim precedes dispatch), so the
    // panic will produce no outcome.
    tokio::time::timeout(Duration::from_secs(10), started.notified())
        .await
        .expect("handler should start within 10s");
    // Small margin for the panic to unwind the dispatch task.
    tokio::time::sleep(Duration::from_millis(300)).await;

    cancel.cancel();
    handle.await.unwrap().unwrap();

    let run_id = db
        .run_ids(job_id)
        .await
        .into_iter()
        .next()
        .expect("a run should have materialized");
    let mut conn = db.pool.get().await.unwrap();
    let state = pg_task_scheduler::store::run_state(&mut conn, RunId(run_id))
        .await
        .unwrap();
    assert!(
        matches!(state, Some(RunState::Running(_))),
        "panicked run must remain Running (lease held, no outcome), got {state:?}"
    );

    db.cleanup().await;
}
