mod common;
use chrono::{Duration, Utc};
use common::TestDb;
use pg_task_scheduler::jobs::{self, CreateJob, ScheduleUpdate};
use pg_task_scheduler::store;
use pg_task_scheduler::{
    CronExpression, JobId, JobLifecycle, JobName, LeaseDuration, MaxAttempts, WorkerId,
};
use std::time::Duration as StdDuration;

#[tokio::test]
async fn materializes_missed_slot_and_advances() {
    let db = TestDb::new().await;
    let past = Utc::now() - Duration::minutes(5);
    let job = db.insert_job("j", "*/1 * * * *", past).await;
    let mut conn = db.pool.get().await.unwrap();

    assert_eq!(store::materialize_due_jobs(&mut conn).await.unwrap(), 1);
    let runs = db.run_ids(job).await;
    assert_eq!(runs.len(), 1);
    assert!(db.job_next_run_at(job).await > Utc::now());
    // second pass: next_run_at now future → nothing
    assert_eq!(store::materialize_due_jobs(&mut conn).await.unwrap(), 0);
    assert_eq!(db.run_ids(job).await.len(), 1);
    db.cleanup().await;
}

#[tokio::test]
async fn skips_not_due_and_paused() {
    let db = TestDb::new().await;
    let future = Utc::now() + Duration::hours(1);
    db.insert_job("future", "*/1 * * * *", future).await;
    let paused = db
        .insert_job_full(
            "paused",
            "*/1 * * * *",
            Utc::now() - Duration::minutes(1),
            "5 minutes",
            3,
            true,
        )
        .await;
    let mut conn = db.pool.get().await.unwrap();
    assert_eq!(store::materialize_due_jobs(&mut conn).await.unwrap(), 0);
    assert!(db.run_ids(paused).await.is_empty());
    db.cleanup().await;
}

#[tokio::test]
async fn pauses_job_with_uncorrupted_cron_left_intact() {
    // Corrupt the stored cron directly, then materialize: the job is paused, not advanced.
    let db = TestDb::new().await;
    let job = db
        .insert_job("bad", "*/1 * * * *", Utc::now() - Duration::minutes(1))
        .await;
    {
        use diesel_async::RunQueryDsl;
        let mut conn = db.pool.get().await.unwrap();
        diesel::sql_query("UPDATE scheduler_jobs SET cron_expression = 'garbage' WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(job)
            .execute(&mut conn)
            .await
            .unwrap();
    }
    let mut conn = db.pool.get().await.unwrap();
    let _ = store::materialize_due_jobs(&mut conn).await.unwrap();
    assert!(db.is_paused(job).await, "bad cron pauses the job");
    db.cleanup().await;
}

#[tokio::test]
async fn materialized_task_keeps_payload_and_survives_schedule_deletion() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let make_spec = |version| {
        CreateJob::new(
            JobName::try_from("snapshot").unwrap(),
            CronExpression::parse("*/5 * * * *").unwrap(),
            LeaseDuration::try_from(StdDuration::from_secs(60)).unwrap(),
            MaxAttempts::try_from(3).unwrap(),
            JobLifecycle::Active,
            serde_json::json!({ "version": version }),
        )
        .unwrap()
    };
    let job = jobs::create(&mut conn, make_spec(1)).await.unwrap();
    jobs::reschedule(
        &mut conn,
        job.id,
        ScheduleUpdate::SetNextRunAt(Utc::now() - Duration::seconds(1)),
    )
    .await
    .unwrap();
    assert_eq!(store::materialize_due_jobs(&mut conn).await.unwrap(), 1);
    jobs::ensure_job(&mut conn, make_spec(2)).await.unwrap();
    assert_eq!(
        jobs::delete(&mut conn, job.id).await.unwrap(),
        jobs::Applied::Changed
    );

    let claimed = store::claim_one(
        &mut conn,
        &WorkerId::try_from("worker").unwrap(),
        &["snapshot".into()],
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(claimed.job_id, Some(JobId(job.id.0)));
    assert_eq!(claimed.job_args["version"], 1);
    db.cleanup().await;
}
