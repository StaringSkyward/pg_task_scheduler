mod common;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use common::TestDb;
use pg_task_scheduler::jobs::{self, CreateJob};
use pg_task_scheduler::{
    CronExpression, JobName, LeaseDuration, MaxAttempts, RunId, RunState, Scheduler, WorkerId,
};
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
        CreateJob {
            name: JobName::try_from("counter").unwrap(),
            cron: CronExpression::parse("*/1 * * * *").unwrap(),
            job_args: serde_json::json!({"n": 1}),
            lease_duration: LeaseDuration::try_from(Duration::from_secs(60)).unwrap(),
            max_attempts: MaxAttempts(NonZeroU32::new(3).unwrap()),
            is_paused: false,
        },
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
    let scheduler = Scheduler::builder(db.pool.clone(), WorkerId::try_from("test").unwrap())
        .poll_interval(Duration::from_millis(100))
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

    let run = RunId(
        db.run_ids_eventually(job.id.0, Duration::from_secs(10))
            .await,
    );
    let mut conn = db.pool.get().await.unwrap();
    let completed = wait_completed(&db, &mut conn, run, Duration::from_secs(10)).await;
    assert!(completed);
    assert_eq!(COUNT.load(Ordering::SeqCst), 1);

    cancel.cancel();
    handle.await.unwrap().unwrap();
    db.cleanup().await;
}

async fn wait_completed(
    _db: &TestDb,
    conn: &mut diesel_async::AsyncPgConnection,
    run: RunId,
    timeout: Duration,
) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if let Some(RunState::Completed { .. }) = pg_task_scheduler::store::run_state(conn, run)
            .await
            .unwrap()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}
