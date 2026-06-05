mod common;
use std::time::Duration;

use chrono::Utc;
use common::TestDb;
use pg_task_scheduler::jobs::{self, CreateJob, ScheduleUpdate};
use pg_task_scheduler::{
    CorruptJobRow, CronExpression, JobId, JobLifecycle, JobName, LeaseDuration, MaxAttempts,
    SchedulerError, store,
};

fn spec(name: &str, cron: &str) -> CreateJob {
    CreateJob {
        name: JobName::try_from(name).unwrap(),
        cron: CronExpression::parse(cron).unwrap(),
        job_args: serde_json::json!({"k": "v"}),
        lease_duration: LeaseDuration::try_from(Duration::from_secs(300)).unwrap(),
        max_attempts: MaxAttempts::try_from(3u32).unwrap(),
        is_paused: false,
    }
}

#[tokio::test]
async fn create_sets_future_next_run() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let job = jobs::create(&mut conn, spec("digest", "*/5 * * * *"))
        .await
        .unwrap();
    assert!(job.next_run_at > Utc::now());
    db.cleanup().await;
}

#[tokio::test]
async fn ensure_job_inserts_then_reconciles_config() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let a = jobs::ensure_job(&mut conn, spec("sync", "*/5 * * * *"))
        .await
        .unwrap();
    assert!(a.next_run_at > Utc::now()); // insert path: cursor in the future
    let b = jobs::ensure_job(&mut conn, spec("sync", "0 * * * *"))
        .await
        .unwrap();
    assert_eq!(a.id, b.id); // idempotent by name — same row
    assert_eq!(b.cron.as_str(), "0 * * * *"); // config reconciled on conflict
    db.cleanup().await;
}

#[tokio::test]
async fn ensure_job_preserves_due_cursor_and_no_slot_skipped() {
    let db = TestDb::new().await;
    // An existing job whose next_run_at is already due (in the past).
    let past = Utc::now() - chrono::Duration::minutes(5);
    let id = db.insert_job("sync", "*/5 * * * *", past).await;
    // Read the cursor back from the DB so the comparison is at DB (microsecond)
    // precision and cannot flake on sub-microsecond truncation.
    let before = db.job_next_run_at(id).await;

    // ensure_job with a CHANGED cron must NOT advance the cursor.
    let mut conn = db.pool.get().await.unwrap();
    let job = jobs::ensure_job(&mut conn, spec("sync", "0 * * * *"))
        .await
        .unwrap();
    assert_eq!(job.id, JobId(id)); // updated the existing row
    assert_eq!(job.cron.as_str(), "0 * * * *"); // config reconciled
    let after = db.job_next_run_at(id).await;
    assert_eq!(after, before, "cursor must be preserved, not advanced");
    assert!(
        after < Utc::now(),
        "cursor still due — the slot was not skipped"
    );

    // The materializer catches up the missed slot: the occurrence IS created.
    assert_eq!(store::materialize_due_jobs(&mut conn).await.unwrap(), 1);
    assert_eq!(db.run_ids(id).await.len(), 1);
    db.cleanup().await;
}

#[tokio::test]
async fn ensure_job_preserves_is_paused_on_conflict() {
    let db = TestDb::new().await;
    // An existing job that an operator has paused.
    let id = db
        .insert_job_full(
            "p",
            "*/5 * * * *",
            Utc::now() + chrono::Duration::hours(1),
            "5 minutes",
            3,
            true,
        )
        .await;
    // ensure_job carries is_paused=false (the spec() default); it must NOT resume.
    let mut conn = db.pool.get().await.unwrap();
    jobs::ensure_job(&mut conn, spec("p", "*/5 * * * *"))
        .await
        .unwrap();
    assert!(
        db.is_paused(id).await,
        "ensure_job must not resurrect a paused job"
    );
    db.cleanup().await;
}

#[tokio::test]
async fn pause_resume_list_get_delete() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let job = jobs::create(&mut conn, spec("p", "*/5 * * * *"))
        .await
        .unwrap();
    jobs::pause(&mut conn, job.id).await.unwrap();
    assert_eq!(
        jobs::get(&mut conn, job.id).await.unwrap().unwrap().lifecycle,
        JobLifecycle::Paused
    );
    jobs::resume(&mut conn, job.id).await.unwrap();
    assert_eq!(
        jobs::get(&mut conn, job.id).await.unwrap().unwrap().lifecycle,
        JobLifecycle::Active
    );
    assert_eq!(jobs::list(&mut conn).await.unwrap().len(), 1);
    jobs::delete(&mut conn, job.id).await.unwrap();
    assert!(jobs::get(&mut conn, job.id).await.unwrap().is_none());
    db.cleanup().await;
}

#[tokio::test]
async fn reschedule_reset_from_now_advances_to_future() {
    let db = TestDb::new().await;
    let past = Utc::now() - chrono::Duration::minutes(5);
    let id = db.insert_job("r", "*/5 * * * *", past).await;
    let mut conn = db.pool.get().await.unwrap();
    let job = jobs::reschedule(&mut conn, JobId(id), ScheduleUpdate::ResetFromNow)
        .await
        .unwrap()
        .expect("job exists");
    assert!(job.next_run_at > Utc::now(), "cursor reset into the future");
    db.cleanup().await;
}

#[tokio::test]
async fn reschedule_set_next_run_at_pins_instant() {
    let db = TestDb::new().await;
    let id = db
        .insert_job("r", "*/5 * * * *", Utc::now() + chrono::Duration::hours(1))
        .await;
    let target = Utc::now() + chrono::Duration::hours(6);
    let mut conn = db.pool.get().await.unwrap();
    jobs::reschedule(&mut conn, JobId(id), ScheduleUpdate::SetNextRunAt(target))
        .await
        .unwrap()
        .expect("job exists");
    // Read back from the DB; assert it landed ~6h out — clearly the pin, not the
    // original 1h. A range check avoids sub-microsecond truncation flakiness.
    let stored = db.job_next_run_at(id).await;
    assert!(stored > Utc::now() + chrono::Duration::hours(5));
    assert!(stored < Utc::now() + chrono::Duration::hours(7));
    db.cleanup().await;
}

#[tokio::test]
async fn reschedule_unknown_id_returns_none() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let missing = JobId(uuid::Uuid::new_v4());
    assert!(
        jobs::reschedule(&mut conn, missing, ScheduleUpdate::ResetFromNow)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        jobs::reschedule(&mut conn, missing, ScheduleUpdate::SetNextRunAt(Utc::now()))
            .await
            .unwrap()
            .is_none()
    );
    db.cleanup().await;
}

#[tokio::test]
async fn reschedule_reset_from_now_corrupt_cron_errors() {
    let db = TestDb::new().await;
    let id = db
        .insert_job(
            "c",
            "*/5 * * * *",
            Utc::now() - chrono::Duration::minutes(1),
        )
        .await;
    {
        use diesel_async::RunQueryDsl;
        let mut conn = db.pool.get().await.unwrap();
        diesel::sql_query("UPDATE scheduler_jobs SET cron_expression = 'garbage' WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(id)
            .execute(&mut conn)
            .await
            .unwrap();
    }
    let mut conn = db.pool.get().await.unwrap();
    let res = jobs::reschedule(&mut conn, JobId(id), ScheduleUpdate::ResetFromNow).await;
    assert!(matches!(res, Err(SchedulerError::CorruptJob { .. })));
    db.cleanup().await;
}

#[tokio::test]
async fn get_unparseable_cron_is_corrupt_job() {
    let db = TestDb::new().await;
    let id = db
        .insert_job("c", "*/5 * * * *", Utc::now() + chrono::Duration::hours(1))
        .await;
    {
        use diesel_async::RunQueryDsl;
        let mut conn = db.pool.get().await.unwrap();
        diesel::sql_query("UPDATE scheduler_jobs SET cron_expression = 'garbage' WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(id)
            .execute(&mut conn)
            .await
            .unwrap();
    }
    let mut conn = db.pool.get().await.unwrap();
    let res = jobs::get(&mut conn, JobId(id)).await;
    assert!(matches!(
        res,
        Err(SchedulerError::CorruptJob { source: CorruptJobRow::Cron(_), .. })
    ));
    db.cleanup().await;
}

#[tokio::test]
async fn get_calendar_lease_interval_is_corrupt_job() {
    let db = TestDb::new().await;
    // '1 day' passes the SQL CHECK (> 0) but is not a pure-microsecond lease,
    // so it must surface as CorruptJob, not as a successful projection.
    let id = db
        .insert_job_full(
            "d",
            "*/5 * * * *",
            Utc::now() + chrono::Duration::hours(1),
            "1 day",
            3,
            false,
        )
        .await;
    let mut conn = db.pool.get().await.unwrap();
    let res = jobs::get(&mut conn, JobId(id)).await;
    assert!(matches!(
        res,
        Err(SchedulerError::CorruptJob { source: CorruptJobRow::LeaseDuration(_), .. })
    ));
    db.cleanup().await;
}
