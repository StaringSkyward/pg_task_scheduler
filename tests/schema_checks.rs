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
