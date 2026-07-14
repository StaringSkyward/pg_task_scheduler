mod common;

use std::num::NonZeroUsize;
use std::time::Duration;

use chrono::Utc;
use common::TestDb;
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use pg_task_scheduler::store;
use pg_task_scheduler::{
    CancelOutcome, DeduplicationKey, EnqueueOptions, FailureOutcome, FinalizeOutcome, HealthStatus,
    LeaseDuration, MaxAttempts, Outcome, Priority, RenewalOutcome, RetryBackoff, RunState,
    Scheduler, Task, WorkerId, cancel, enqueue, prune_terminal,
};
use tokio_util::sync::CancellationToken;

struct ExampleTask;

impl Task for ExampleTask {
    const NAME: &'static str = "example-task";
    type Args = Args;
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Args {
    value: i32,
}

fn names() -> Vec<String> {
    vec![ExampleTask::NAME.to_owned()]
}

#[tokio::test]
async fn immediate_enqueue_claims_typed_payload() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let enqueued =
        enqueue::<ExampleTask>(&mut conn, Args { value: 42 }, EnqueueOptions::immediate())
            .await
            .unwrap();

    let claimed = store::claim_one(&mut conn, &WorkerId::try_from("worker").unwrap(), &names())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.run_id, enqueued.task_id);
    assert!(claimed.job_id.is_none());
    assert_eq!(claimed.job_args["value"], 42);
    db.cleanup().await;
}

#[tokio::test]
async fn enqueue_rolls_back_with_callers_transaction() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute("BEGIN").await.unwrap();
    let task = enqueue::<ExampleTask>(&mut conn, Args { value: 1 }, EnqueueOptions::immediate())
        .await
        .unwrap();
    conn.batch_execute("ROLLBACK").await.unwrap();

    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        count: i64,
    }
    let count = diesel::sql_query("SELECT count(*) AS count FROM scheduler_runs WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task.task_id.0)
        .get_result::<Count>(&mut conn)
        .await
        .unwrap()
        .count;
    assert_eq!(count, 0);
    db.cleanup().await;
}

#[tokio::test]
async fn delayed_and_priority_control_claim_order() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let delayed = enqueue::<ExampleTask>(
        &mut conn,
        Args { value: 1 },
        EnqueueOptions::at(Utc::now() + chrono::Duration::hours(1)),
    )
    .await
    .unwrap();
    let low = enqueue::<ExampleTask>(
        &mut conn,
        Args { value: 2 },
        EnqueueOptions::immediate().priority(Priority::new(-1)),
    )
    .await
    .unwrap();
    let high = enqueue::<ExampleTask>(
        &mut conn,
        Args { value: 3 },
        EnqueueOptions::immediate().priority(Priority::new(10)),
    )
    .await
    .unwrap();

    let claimed = store::claim_batch(
        &mut conn,
        &WorkerId::try_from("worker").unwrap(),
        &names(),
        NonZeroUsize::new(1).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(claimed[0].run_id, high.task_id);
    assert!(matches!(
        store::run_state(&mut conn, low.task_id).await.unwrap(),
        Some(RunState::Pending)
    ));
    assert!(matches!(
        store::run_state(&mut conn, delayed.task_id).await.unwrap(),
        Some(RunState::Pending)
    ));
    db.cleanup().await;
}

#[tokio::test]
async fn deduplication_returns_stable_task_id() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let key = DeduplicationKey::try_from("invoice-42").unwrap();
    let first = enqueue::<ExampleTask>(
        &mut conn,
        Args { value: 1 },
        EnqueueOptions::immediate().deduplicate(key.clone()),
    )
    .await
    .unwrap();
    let second = enqueue::<ExampleTask>(
        &mut conn,
        Args { value: 2 },
        EnqueueOptions::immediate().deduplicate(key),
    )
    .await
    .unwrap();
    assert_eq!(first, second);
    db.cleanup().await;
}

#[tokio::test]
async fn retryable_failure_requeues_then_exhausts() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let task = enqueue::<ExampleTask>(
        &mut conn,
        Args { value: 1 },
        EnqueueOptions::immediate()
            .max_attempts(MaxAttempts::try_from(2).unwrap())
            .retry_backoff(RetryBackoff::try_from(Duration::ZERO).unwrap()),
    )
    .await
    .unwrap();
    let worker = WorkerId::try_from("worker").unwrap();
    let first = store::claim_one(&mut conn, &worker, &names())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        store::fail_run(
            &mut conn,
            first.run_id,
            first.lease_token,
            "transient".into(),
            true,
        )
        .await
        .unwrap(),
        FailureOutcome::Retried { .. }
    ));
    let second = store::claim_one(&mut conn, &worker, &names())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.attempt.get(), 2);
    assert_eq!(
        store::fail_run(
            &mut conn,
            second.run_id,
            second.lease_token,
            "still failing".into(),
            true,
        )
        .await
        .unwrap(),
        FailureOutcome::Failed
    );
    assert!(matches!(
        store::run_state(&mut conn, task.task_id).await.unwrap(),
        Some(RunState::Failed { .. })
    ));
    let attempts = store::task_attempts(&mut conn, task.task_id).await.unwrap();
    assert_eq!(attempts.len(), 2);
    assert!(attempts.iter().all(|attempt| matches!(
        attempt.state,
        pg_task_scheduler::AttemptState::Failed { .. }
    )));
    db.cleanup().await;
}

#[tokio::test]
async fn renewal_is_token_guarded_and_extends_attempt() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    enqueue::<ExampleTask>(
        &mut conn,
        Args { value: 1 },
        EnqueueOptions::immediate()
            .lease_duration(LeaseDuration::try_from(Duration::from_secs(2)).unwrap()),
    )
    .await
    .unwrap();
    let claimed = store::claim_one(&mut conn, &WorkerId::try_from("worker").unwrap(), &names())
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    match store::renew_lease(&mut conn, claimed.run_id, claimed.lease_token)
        .await
        .unwrap()
    {
        RenewalOutcome::Renewed { lease_expires_at } => {
            assert!(lease_expires_at > claimed.lease_expires_at)
        }
        other => panic!("expected renewal, got {other:?}"),
    }
    assert_eq!(
        store::renew_lease(
            &mut conn,
            claimed.run_id,
            pg_task_scheduler::LeaseToken::generate(),
        )
        .await
        .unwrap(),
        RenewalOutcome::Fenced
    );
    db.cleanup().await;
}

#[tokio::test]
async fn cancellation_fences_running_worker_and_retention_is_terminal_only() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let task = enqueue::<ExampleTask>(&mut conn, Args { value: 1 }, EnqueueOptions::immediate())
        .await
        .unwrap();
    let claimed = store::claim_one(&mut conn, &WorkerId::try_from("worker").unwrap(), &names())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        cancel(&mut conn, task.task_id).await.unwrap(),
        CancelOutcome::Cancelled
    );
    assert_eq!(
        store::finalize_run(
            &mut conn,
            claimed.run_id,
            claimed.lease_token,
            Outcome::Completed,
        )
        .await
        .unwrap(),
        FinalizeOutcome::AlreadyTerminal
    );
    assert!(matches!(
        store::run_state(&mut conn, task.task_id).await.unwrap(),
        Some(RunState::Cancelled { .. })
    ));
    assert_eq!(
        prune_terminal(
            &mut conn,
            Utc::now() + chrono::Duration::seconds(1),
            NonZeroUsize::new(10).unwrap(),
        )
        .await
        .unwrap(),
        1
    );
    assert!(
        store::run_state(&mut conn, task.task_id)
            .await
            .unwrap()
            .is_none()
    );
    db.cleanup().await;
}

#[tokio::test]
async fn worker_executes_immediate_task_and_renews_short_lease() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let task = enqueue::<ExampleTask>(
        &mut conn,
        Args { value: 7 },
        EnqueueOptions::immediate()
            .lease_duration(LeaseDuration::try_from(Duration::from_millis(150)).unwrap()),
    )
    .await
    .unwrap();
    drop(conn);

    let completed = std::sync::Arc::new(tokio::sync::Notify::new());
    let signal = completed.clone();
    let scheduler = Scheduler::builder(db.pool.clone(), WorkerId::try_from("runtime").unwrap())
        .poll_interval(Duration::from_millis(10))
        .reaper_interval(Duration::from_millis(10))
        .register_task::<ExampleTask, _, _>(move |_ctx, args| {
            let signal = signal.clone();
            async move {
                assert_eq!(args.value, 7);
                tokio::time::sleep(Duration::from_millis(350)).await;
                signal.notify_one();
                Ok(())
            }
        })
        .unwrap()
        .build()
        .unwrap();
    let health = scheduler.health();
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let handle = tokio::spawn(async move { scheduler.run_until_shutdown(worker_shutdown).await });

    tokio::time::timeout(Duration::from_secs(3), completed.notified())
        .await
        .expect("handler did not finish");
    let start = std::time::Instant::now();
    loop {
        let mut conn = db.pool.get().await.unwrap();
        if matches!(
            store::run_state(&mut conn, task.task_id).await.unwrap(),
            Some(RunState::Completed { .. })
        ) {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(3));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(health.borrow().status, HealthStatus::Healthy);
    shutdown.cancel();
    handle.await.unwrap().unwrap();
    db.cleanup().await;
}
