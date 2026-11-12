#![cfg(feature = "cron")]
mod common;
use chrono::DateTime;
use chrono::Duration;
use chrono::SecondsFormat;
use chrono::Utc;
use common::TestDb;
use pg_task_scheduler::CronExpression;
use pg_task_scheduler::JobLifecycle;
use pg_task_scheduler::MaxAttempts;
use pg_task_scheduler::QueueName;
use pg_task_scheduler::TaskId;
use pg_task_scheduler::TaskName;
use pg_task_scheduler::jobs::CreateJob;
use pg_task_scheduler::jobs::ScheduleUpdate;
use pg_task_scheduler::jobs::{self};
use pg_task_scheduler::store;

fn occurrence_key(job: impl std::fmt::Display, at: DateTime<Utc>) -> String {
    format!(
        "schedule:{job}:{}",
        at.to_rfc3339_opts(SecondsFormat::Micros, true)
    )
}

#[tokio::test]
async fn materializes_missed_slot_and_advances() {
    let db = TestDb::new().await;
    let past = Utc::now() - Duration::minutes(5);
    let job = db.insert_job("j", "*/1 * * * *", past).await;
    let mut conn = db.pool.get().await.unwrap();

    let old_next_run_at = db.job_next_run_at(job).await;
    assert_eq!(store::materialize_due_jobs(&mut conn).await.unwrap(), 1);
    let runs = db.task_ids(job).await;
    assert_eq!(runs.len(), 1);
    let task = store::task(&mut conn, TaskId(runs[0]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.task_key.as_str(), occurrence_key(job, old_next_run_at));
    assert_eq!(task.task_name.as_str(), "j");
    assert_eq!(task.due_at, old_next_run_at);
    assert_eq!(task.payload, serde_json::json!({}));
    assert_eq!(task.max_attempts, 3);
    assert!(db.job_next_run_at(job).await > Utc::now());
    // second pass: next_run_at now future → nothing
    assert_eq!(store::materialize_due_jobs(&mut conn).await.unwrap(), 0);
    assert_eq!(db.task_ids(job).await.len(), 1);
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
            3,
            true,
        )
        .await;
    let mut conn = db.pool.get().await.unwrap();
    assert_eq!(store::materialize_due_jobs(&mut conn).await.unwrap(), 0);
    assert!(db.task_ids(paused).await.is_empty());
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
    // A paused job inserts no task.
    assert_eq!(store::materialize_due_jobs(&mut conn).await.unwrap(), 0);
    assert!(db.is_paused(job).await, "bad cron pauses the job");
    db.cleanup().await;
}

#[tokio::test]
async fn materialized_task_keeps_payload_and_survives_schedule_deletion() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let make_spec = |version| {
        CreateJob::new(
            TaskName::try_from("snapshot").unwrap(),
            CronExpression::parse("*/5 * * * *").unwrap(),
            MaxAttempts::try_from(3).unwrap(),
            JobLifecycle::Active,
            serde_json::json!({ "version": version }),
        )
        .unwrap()
    };
    let job = jobs::create(&mut conn, make_spec(1)).await.unwrap();
    let due = Utc::now() - Duration::seconds(1);
    jobs::reschedule(&mut conn, job.id, ScheduleUpdate::SetNextRunAt(due))
        .await
        .unwrap();
    assert_eq!(store::materialize_due_jobs(&mut conn).await.unwrap(), 1);
    jobs::ensure_job(&mut conn, make_spec(2)).await.unwrap();
    assert_eq!(
        jobs::delete(&mut conn, job.id).await.unwrap(),
        jobs::Applied::Changed
    );

    let ids = db.task_ids(job.id.0).await;
    assert_eq!(ids.len(), 1);
    let task = store::task(&mut conn, TaskId(ids[0]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        task.task_key.as_str(),
        occurrence_key(job.id.0, db_time(&due))
    );
    assert_eq!(task.payload["version"], 1);
    db.cleanup().await;
}

/// Postgres stores microseconds; truncate the host timestamp the same way.
fn db_time(t: &DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(t.timestamp_micros()).unwrap()
}

#[tokio::test]
async fn rematerializing_same_occurrence_creates_one_task() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let job = db
        .insert_job("again", "*/5 * * * *", Utc::now() + Duration::hours(1))
        .await;
    let t = Utc::now() - Duration::minutes(2);
    // The second pass finds the occurrence already present and inserts nothing.
    for expected in [1, 0] {
        jobs::reschedule(
            &mut conn,
            pg_task_scheduler::JobId(job),
            ScheduleUpdate::SetNextRunAt(t),
        )
        .await
        .unwrap();
        assert_eq!(
            store::materialize_due_jobs(&mut conn).await.unwrap(),
            expected
        );
    }
    let ids = db.task_ids(job).await;
    assert_eq!(ids.len(), 1);
    let task = store::task(&mut conn, TaskId(ids[0]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.task_key.as_str(), occurrence_key(job, db_time(&t)));
    db.cleanup().await;
}

#[tokio::test]
async fn materialized_task_uses_the_job_queue() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let job = jobs::create(
        &mut conn,
        CreateJob::new(
            TaskName::try_from("bulk-job").unwrap(),
            CronExpression::parse("*/1 * * * *").unwrap(),
            MaxAttempts::try_from(3u32).unwrap(),
            JobLifecycle::Active,
            serde_json::json!({}),
        )
        .unwrap()
        .queue(QueueName::try_from("bulk").unwrap()),
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
    assert_eq!(store::materialize_due_jobs(&mut conn).await.unwrap(), 1);
    let runs = db.task_ids(job.id.0).await;
    let task = store::task(&mut conn, TaskId(runs[0]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.queue.as_str(), "bulk");
    db.cleanup().await;
}

#[derive(diesel::QueryableByName)]
struct Setting {
    #[diesel(sql_type = diesel::sql_types::Text)]
    v: String,
}

/// Each materializer transaction sets `idle_in_transaction_session_timeout` to 30 s, so one
/// cut off by shutdown is ended by the server instead of holding its schedule row lock on an
/// idle pooled connection. A trigger records the setting from inside the transaction.
#[tokio::test]
async fn materializer_transaction_ends_itself_when_left_idle() {
    use diesel_async::RunQueryDsl;
    use diesel_async::SimpleAsyncConnection;
    let db = TestDb::new().await;
    db.insert_job("j", "*/1 * * * *", Utc::now() - Duration::minutes(5))
        .await;
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute(
        "CREATE TABLE seen (v text); \
         CREATE FUNCTION record_idle() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN \
           INSERT INTO seen VALUES (current_setting('idle_in_transaction_session_timeout')); \
           RETURN NEW; END $$; \
         CREATE TRIGGER record_idle BEFORE INSERT ON queue_tasks \
           FOR EACH ROW EXECUTE FUNCTION record_idle()",
    )
    .await
    .unwrap();
    assert_eq!(store::materialize_due_jobs(&mut conn).await.unwrap(), 1);
    let inside = diesel::sql_query("SELECT v FROM seen")
        .get_result::<Setting>(&mut conn)
        .await
        .unwrap()
        .v;
    assert_eq!(inside, "30s");
    let after =
        diesel::sql_query("SELECT current_setting('idle_in_transaction_session_timeout') AS v")
            .get_result::<Setting>(&mut conn)
            .await
            .unwrap()
            .v;
    assert_eq!(after, "0");
    db.cleanup().await;
}
