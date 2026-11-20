mod common;

use common::TestDb;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use diesel_async::RunQueryDsl;

fn is_check_violation(err: &DieselError) -> bool {
    matches!(
        err,
        DieselError::DatabaseError(DatabaseErrorKind::CheckViolation, _)
    )
}

async fn exec(db: &TestDb, sql: &str) -> Result<usize, DieselError> {
    let mut conn = db.pool.get().await.unwrap();
    diesel::sql_query(sql).execute(&mut conn).await
}

#[tokio::test]
async fn attempt_cannot_close_without_an_outcome() {
    let db = TestDb::new().await;
    exec(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name) VALUES ('k', 'n')",
    )
    .await
    .unwrap();
    let e = exec(
        &db,
        "INSERT INTO queue_task_attempts (task_id, attempt_no, lock_token, worker_id, finished_at) \
         SELECT id, 1, 1, 'w', now() FROM queue_tasks",
    )
    .await
    .unwrap_err();
    assert!(is_check_violation(&e), "{e:?}");
    db.cleanup().await;
}

#[tokio::test]
async fn attempt_number_starts_at_one() {
    let db = TestDb::new().await;
    exec(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name) VALUES ('k', 'n')",
    )
    .await
    .unwrap();
    let e = exec(
        &db,
        "INSERT INTO queue_task_attempts (task_id, attempt_no, lock_token, worker_id) \
         SELECT id, 0, 1, 'w' FROM queue_tasks",
    )
    .await
    .unwrap_err();
    assert!(is_check_violation(&e), "{e:?}");
    db.cleanup().await;
}

#[tokio::test]
async fn admin_action_shape_is_checked() {
    let db = TestDb::new().await;
    for bad in [
        "('pause', 1, NULL, 'q', NULL, NULL)",
        "('cancel', NULL, NULL, 'q', NULL, NULL)",
        "('cancel', NULL, 'n', NULL, 'boom', 10)",
        "('replay', 1, NULL, 'q', NULL, NULL)",
    ] {
        let e = exec(
            &db,
            &format!(
                "INSERT INTO queue_admin_actions (action, task_id, task_name, queue, last_error_filter, max_task_id, operator, reason) \
                 VALUES {}",
                bad.replace(')', ", 'op', 'r')")
            ),
        )
        .await
        .unwrap_err();
        assert!(is_check_violation(&e), "{bad}: {e:?}");
    }
    db.cleanup().await;
}

#[tokio::test]
async fn infinite_due_at_is_rejected() {
    let db = TestDb::new().await;
    let e = exec(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, due_at) VALUES ('k', 'n', '-infinity')",
    )
    .await
    .unwrap_err();
    assert!(is_check_violation(&e), "{e:?}");
    db.cleanup().await;
}

#[tokio::test]
async fn timestamp_past_chronos_range_is_rejected() {
    let db = TestDb::new().await;
    let e = exec(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, status, completed_at) \
         VALUES ('k', 'n', 'done', '262143-01-01 00:00:00+00')",
    )
    .await
    .unwrap_err();
    assert!(is_check_violation(&e), "{e:?}");
    db.cleanup().await;
}

#[derive(diesel::QueryableByName)]
struct ColumnName {
    #[diesel(sql_type = diesel::sql_types::Text)]
    name: String,
}

#[tokio::test]
async fn every_timestamp_column_uses_queue_timestamptz() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let rows: Vec<ColumnName> = diesel::sql_query(
        "SELECT table_name || '.' || column_name AS name FROM information_schema.columns \
         WHERE table_schema = current_schema() AND data_type = 'timestamp with time zone' \
         AND domain_name IS DISTINCT FROM 'queue_timestamptz'",
    )
    .load(&mut conn)
    .await
    .unwrap();
    assert!(
        rows.is_empty(),
        "columns outside the domain: {:?}",
        rows.iter().map(|r| &r.name).collect::<Vec<_>>()
    );
    let proof: Vec<ColumnName> = diesel::sql_query(
        "SELECT column_name AS name FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = 'queue_tasks' \
         AND column_name = 'completed_at' AND domain_name = 'queue_timestamptz'",
    )
    .load(&mut conn)
    .await
    .unwrap();
    assert_eq!(proof.len(), 1);
    drop(conn);
    db.cleanup().await;
}

#[tokio::test]
async fn infinite_completed_at_is_rejected() {
    let db = TestDb::new().await;
    let e = exec(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, status, completed_at) \
         VALUES ('k', 'n', 'done', 'infinity')",
    )
    .await
    .unwrap_err();
    assert!(is_check_violation(&e), "{e:?}");
    db.cleanup().await;
}

#[tokio::test]
async fn infinite_locked_until_is_rejected() {
    let db = TestDb::new().await;
    let e = exec(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, status, locked_by, locked_until) \
         VALUES ('k', 'n', 'running', 'w', '-infinity')",
    )
    .await
    .unwrap_err();
    assert!(is_check_violation(&e), "{e:?}");
    db.cleanup().await;
}

#[cfg(feature = "cron")]
#[tokio::test]
async fn infinite_next_run_at_is_rejected() {
    let db = TestDb::new().await;
    let e = exec(
        &db,
        "INSERT INTO scheduler_jobs (name, task_name, cron_expression, next_run_at) \
         VALUES ('j', 'j', '*/1 * * * *', '-infinity')",
    )
    .await
    .unwrap_err();
    assert!(is_check_violation(&e), "{e:?}");
    db.cleanup().await;
}
