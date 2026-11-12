#![cfg(feature = "cron")]
mod common;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::TestDb;
use pg_task_scheduler::CronExpression;
use pg_task_scheduler::EnqueueOptions;
use pg_task_scheduler::HealthStatus;
use pg_task_scheduler::JobLifecycle;
use pg_task_scheduler::MaxAttempts;
use pg_task_scheduler::Scheduler;
use pg_task_scheduler::TaskId;
use pg_task_scheduler::TaskKey;
use pg_task_scheduler::TaskName;
use pg_task_scheduler::TaskStatus;
use pg_task_scheduler::enqueue;
use pg_task_scheduler::jobs::CreateJob;
use pg_task_scheduler::jobs::{self};
use pg_task_scheduler::store;
use tokio_util::sync::CancellationToken;

#[derive(serde::Deserialize)]
struct Args {
    n: i64,
}

#[tokio::test]
async fn runs_due_job_end_to_end() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let job = jobs::create(
        &mut conn,
        CreateJob::new(
            TaskName::try_from("counter").unwrap(),
            CronExpression::parse("*/1 * * * *").unwrap(),
            MaxAttempts::try_from(3u32).unwrap(),
            JobLifecycle::Active,
            serde_json::json!({"n": 1}),
        )
        .unwrap(),
    )
    .await
    .unwrap();

    {
        use diesel_async::RunQueryDsl;
        diesel::sql_query(
            "UPDATE scheduler_jobs SET next_run_at = now() - interval '1 minute' WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(job.id)
        .execute(&mut conn)
        .await
        .unwrap();
    }
    drop(conn);

    static COUNT: AtomicU32 = AtomicU32::new(0);
    let scheduler = Scheduler::builder(db.pool.clone())
        .max_poll_interval(Duration::from_millis(100))
        .reaper_interval(Duration::from_secs(5))
        .shutdown_timeout(Duration::from_secs(5))
        .register::<Args, _, _>("counter", |_ctx, a: Args| async move {
            assert_eq!(a.n, 1);
            COUNT.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap()
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(scheduler.run_until_shutdown(cancel.clone()));

    let id = TaskId(
        db.task_id_eventually(job.id.0, Duration::from_secs(10))
            .await,
    );
    let mut conn = db.pool.get().await.unwrap();
    let completed = wait_done(&mut conn, id, Duration::from_secs(10)).await;
    assert!(completed);
    assert_eq!(COUNT.load(Ordering::SeqCst), 1);

    cancel.cancel();
    handle.await.unwrap().unwrap();
    db.cleanup().await;
}

async fn wait_done(
    conn: &mut diesel_async::AsyncPgConnection,
    id: TaskId,
    timeout: Duration,
) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if let Some(t) = store::task(conn, id).await.unwrap()
            && t.status == TaskStatus::Done
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

struct Hold;
impl pg_task_scheduler::Task for Hold {
    const NAME: &'static str = "hold";
    type Args = serde_json::Value;
}

/// A failing materialization degrades health, but claiming goes on: a full batch still
/// loops straight into the next claim instead of sleeping for `max_poll_interval`.
#[tokio::test]
async fn materialize_failure_degrades_health_without_slowing_claims() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    {
        use diesel_async::RunQueryDsl;
        diesel::sql_query("DROP TABLE scheduler_jobs")
            .execute(&mut conn)
            .await
            .unwrap();
    }
    for i in 0..3 {
        enqueue::<Hold>(
            &mut conn,
            TaskKey::try_from(format!("k{i}")).unwrap(),
            serde_json::json!({}),
            EnqueueOptions::immediate(),
        )
        .await
        .unwrap();
    }
    drop(conn);

    let started = Arc::new(AtomicU32::new(0));
    let counter = started.clone();
    let scheduler = Scheduler::builder(db.pool.clone())
        .claim_batch_size(NonZeroUsize::MIN) // every claim of one task is a full batch
        .max_poll_interval(Duration::from_secs(10))
        .shutdown_timeout(Duration::from_millis(100))
        .register_task::<Hold, _, _>(move |_ctx, _args| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                std::future::pending().await // no handler finishing wakes the loop
            }
        })
        .unwrap()
        .build()
        .unwrap();
    let health = scheduler.health();
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(scheduler.run_until_shutdown(cancel.clone()));

    let start = std::time::Instant::now();
    while started.load(Ordering::SeqCst) < 3 {
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "full batches waited for max_poll_interval"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let h = health.borrow().clone();
    assert_eq!(h.status, HealthStatus::Degraded);
    // One failed materialization is one failure, however many claim cycles follow it.
    assert_eq!(h.consecutive_failures, 1, "{h:?}");
    assert!(
        h.last_error
            .as_deref()
            .unwrap_or("")
            .contains("scheduler_jobs"),
        "{h:?}"
    );

    cancel.cancel();
    handle.await.unwrap().unwrap();
    db.cleanup().await;
}
