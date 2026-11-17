mod common;

use common::TestDb;
#[cfg(feature = "cron")]
use diesel::result::DatabaseErrorKind;
#[cfg(feature = "cron")]
use diesel::result::Error as DieselError;
#[cfg(feature = "cron")]
use diesel_async::RunQueryDsl;
use pg_task_scheduler::SchedulerError;
use pg_task_scheduler::TaskId;
use pg_task_scheduler::store;

#[cfg(feature = "cron")]
/// Raw insert of a job with the given names, bypassing the Rust `ScheduleName` and
/// `TaskName` constructors so the SQL CHECK is what is under test.
async fn insert_job_with_names(
    db: &TestDb,
    name: &str,
    task_name: &str,
) -> Result<usize, DieselError> {
    let mut conn = db.pool.get().await.unwrap();
    diesel::sql_query(
        "INSERT INTO scheduler_jobs (name, task_name, cron_expression, next_run_at) \
         VALUES ($1, $2, '*/5 * * * *', now())",
    )
    .bind::<diesel::sql_types::Text, _>(name)
    .bind::<diesel::sql_types::Text, _>(task_name)
    .execute(&mut conn)
    .await
}

#[cfg(feature = "cron")]
fn is_check_violation(err: &DieselError) -> bool {
    matches!(
        err,
        DieselError::DatabaseError(DatabaseErrorKind::CheckViolation, _)
    )
}

#[cfg(feature = "cron")]
#[tokio::test]
async fn empty_job_name_is_rejected_by_check() {
    let db = TestDb::new().await;
    let err = insert_job_with_names(&db, "", "t")
        .await
        .expect_err("empty name must violate the CHECK");
    assert!(is_check_violation(&err), "got: {err:?}");
    db.cleanup().await;
}

#[cfg(feature = "cron")]
#[tokio::test]
async fn empty_task_name_is_rejected_by_check() {
    let db = TestDb::new().await;
    let err = insert_job_with_names(&db, "ok", "")
        .await
        .expect_err("empty task name must violate the CHECK");
    assert!(is_check_violation(&err), "got: {err:?}");
    db.cleanup().await;
}

#[cfg(feature = "cron")]
#[tokio::test]
async fn leading_whitespace_job_name_is_rejected_by_check() {
    let db = TestDb::new().await;
    let err = insert_job_with_names(&db, " x", "t")
        .await
        .expect_err("leading-whitespace name must violate the CHECK");
    assert!(is_check_violation(&err), "got: {err:?}");
    db.cleanup().await;
}

#[cfg(feature = "cron")]
#[tokio::test]
async fn valid_job_name_is_accepted() {
    let db = TestDb::new().await;
    let n = insert_job_with_names(&db, "ok", "t")
        .await
        .expect("valid name should insert");
    assert_eq!(n, 1);
    db.cleanup().await;
}

#[tokio::test]
async fn decode_error_names_the_identifier_rule() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    // Raw SQL bypasses QueueName::try_from; queue_tasks has no CHECK on queue.
    let id: i64 = db
        .scalar(
            "WITH t AS (INSERT INTO queue_tasks (task_key, task_name, queue) \
             VALUES ('k', 'n', '') RETURNING id) SELECT id AS n FROM t",
        )
        .await;
    let err = store::task(&mut conn, TaskId(id)).await.unwrap_err();
    assert!(matches!(err, SchedulerError::Database(_)), "{err:?}");
    assert!(
        err.to_string().contains("identifier must not be empty"),
        "{err}"
    );
    db.cleanup().await;
}
