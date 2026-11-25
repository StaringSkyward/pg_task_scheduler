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

/// A CHECK violation of the named constraint.
fn violates(err: &DieselError, name: &str) -> bool {
    matches!(
        err,
        DieselError::DatabaseError(DatabaseErrorKind::CheckViolation, info)
            if info.constraint_name() == Some(name)
    )
}

fn is_foreign_key_violation(err: &DieselError) -> bool {
    matches!(
        err,
        DieselError::DatabaseError(DatabaseErrorKind::ForeignKeyViolation, _)
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
        "('pause', 'n', 'q', NULL, NULL)",
        "('cancel', NULL, 'q', NULL, NULL)",
        "('cancel', 'n', NULL, 'boom', 10)",
        "('replay', 'n', NULL, NULL, NULL)",
        // A bulk replay must say which errors it means (error_filter is NULL here).
        "('replay', 'n', NULL, NULL, 10)",
        "('replay', 'n', NULL, 'boom', 10)",
    ] {
        let e = exec(
            &db,
            &format!(
                "INSERT INTO queue_admin_actions (action, task_name, queue, last_error_filter, max_task_id, operator, reason) \
                 VALUES {}",
                bad.replace(')', ", 'op', 'r')")
            ),
        )
        .await
        .unwrap_err();
        assert!(violates(&e, "queue_admin_actions_shape"), "{bad}: {e:?}");
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
        "INSERT INTO queue_tasks (task_key, task_name, status, attempts, locked_by, locked_until) \
         VALUES ('k', 'n', 'running', 1, 'w', '-infinity')",
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
        "WITH j AS (INSERT INTO scheduler_jobs (name, task_name, cron_expression) \
                    VALUES ('j', 'j', '*/1 * * * *') RETURNING id) \
         INSERT INTO scheduler_job_cursors (job_id, next_run_at) SELECT id, '-infinity' FROM j",
    )
    .await
    .unwrap_err();
    assert!(is_check_violation(&e), "{e:?}");
    db.cleanup().await;
}

#[cfg(feature = "cron")]
#[tokio::test]
async fn a_cursor_needs_its_job() {
    let db = TestDb::new().await;
    let e = exec(
        &db,
        "INSERT INTO scheduler_job_cursors (job_id, next_run_at) VALUES (gen_random_uuid(), now())",
    )
    .await
    .unwrap_err();
    assert!(is_foreign_key_violation(&e), "{e:?}");
    db.cleanup().await;
}

#[tokio::test]
async fn a_queued_task_with_no_attempts_left_is_rejected() {
    let db = TestDb::new().await;
    let e = exec(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, status, attempts, max_attempts) \
         VALUES ('k', 'n', 'queued', 3, 3)",
    )
    .await
    .unwrap_err();
    assert!(is_check_violation(&e), "{e:?}");
    db.cleanup().await;
}

#[tokio::test]
async fn a_running_task_with_no_attempt_is_rejected() {
    let db = TestDb::new().await;
    let e = exec(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, status, locked_by, locked_until) \
         VALUES ('k', 'n', 'running', 'w', now())",
    )
    .await
    .unwrap_err();
    assert!(is_check_violation(&e), "{e:?}");
    db.cleanup().await;
}

#[tokio::test]
async fn attempts_above_max_attempts_are_rejected() {
    let db = TestDb::new().await;
    let e = exec(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, status, attempts, max_attempts, \
         locked_by, locked_until) VALUES ('k', 'n', 'running', 4, 3, 'w', now())",
    )
    .await
    .unwrap_err();
    assert!(is_check_violation(&e), "{e:?}");
    db.cleanup().await;
}

/// Membership rows record replays and cancels only: the action column's CHECK rejects
/// `pause`, and the composite foreign key rejects a row claiming a pause action is a replay.
#[tokio::test]
async fn a_membership_row_for_a_pause_action_is_rejected() {
    let db = TestDb::new().await;
    exec(
        &db,
        "INSERT INTO queue_admin_actions (action, queue, operator, reason) \
         VALUES ('pause', 'q', 'op', 'r')",
    )
    .await
    .unwrap();
    let member = |action: &str| {
        format!(
            "INSERT INTO queue_admin_action_tasks (action_id, action, task_id) \
             SELECT id, '{action}', 1 FROM queue_admin_actions"
        )
    };
    let e = exec(&db, &member("pause")).await.unwrap_err();
    assert!(is_check_violation(&e), "{e:?}");
    let e = exec(&db, &member("replay")).await.unwrap_err();
    assert!(is_foreign_key_violation(&e), "{e:?}");
    db.cleanup().await;
}

/// An audit row names an operator and a reason, not empty or only whitespace.
#[tokio::test]
async fn a_blank_operator_or_reason_is_rejected() {
    let db = TestDb::new().await;
    for (op, reason) in [
        ("''", "'r'"),
        ("'  '", "'r'"),
        ("'op'", "''"),
        ("'op'", "'  '"),
    ] {
        let e = exec(
            &db,
            &format!(
                "INSERT INTO queue_admin_actions (action, queue, operator, reason) \
                 VALUES ('pause', 'q', {op}, {reason})"
            ),
        )
        .await
        .unwrap_err();
        assert!(
            violates(&e, "queue_admin_actions_text_present"),
            "{op} {reason}: {e:?}"
        );
    }
    db.cleanup().await;
}

/// A pause row's composite foreign key to its action needs a pause action for the same queue.
async fn pause_row_for(db: &TestDb, action: &str, action_queue: &str) -> Result<(), DieselError> {
    exec(
        db,
        &format!(
            "INSERT INTO queue_admin_actions (action, queue, operator, reason) \
             VALUES ('{action}', '{action_queue}', 'op', 'r')"
        ),
    )
    .await
    .unwrap();
    exec(
        db,
        "INSERT INTO queue_pauses (queue, action_id) SELECT 'q', id FROM queue_admin_actions",
    )
    .await
    .map(|_| ())
}

#[tokio::test]
async fn a_pause_naming_a_resume_action_is_rejected() {
    let db = TestDb::new().await;
    let e = pause_row_for(&db, "resume", "q").await.unwrap_err();
    assert!(is_foreign_key_violation(&e), "{e:?}");
    db.cleanup().await;
}

#[tokio::test]
async fn a_pause_naming_another_queues_action_is_rejected() {
    let db = TestDb::new().await;
    let e = pause_row_for(&db, "pause", "other").await.unwrap_err();
    assert!(is_foreign_key_violation(&e), "{e:?}");
    db.cleanup().await;
}

#[tokio::test]
async fn a_pause_naming_its_own_pause_action_is_accepted() {
    let db = TestDb::new().await;
    pause_row_for(&db, "pause", "q").await.unwrap();
    db.cleanup().await;
}

const OPEN_WITH_ERROR: &str = "INSERT INTO queue_task_attempts \
    (task_id, attempt_no, lock_token, worker_id, error) SELECT id, 1, 1, 'w', 'x' FROM queue_tasks";

async fn insert_task(db: &TestDb) {
    exec(
        db,
        "INSERT INTO queue_tasks (task_key, task_name) VALUES ('k', 'n')",
    )
    .await
    .unwrap();
}

fn closed_attempt(outcome: &str, error: Option<&str>) -> String {
    let error = error.map_or("NULL".to_owned(), |e| format!("'{e}'"));
    format!(
        "INSERT INTO queue_task_attempts (task_id, attempt_no, lock_token, worker_id, \
         finished_at, outcome, error) SELECT id, 1, 1, 'w', now(), '{outcome}', {error} \
         FROM queue_tasks"
    )
}

#[tokio::test]
async fn an_open_attempt_with_an_error_is_rejected() {
    let db = TestDb::new().await;
    insert_task(&db).await;
    let e = exec(&db, OPEN_WITH_ERROR).await.unwrap_err();
    assert!(is_check_violation(&e), "{e:?}");
    db.cleanup().await;
}

#[tokio::test]
async fn a_done_attempt_with_an_error_is_rejected() {
    let db = TestDb::new().await;
    insert_task(&db).await;
    let e = exec(&db, &closed_attempt("done", Some("x")))
        .await
        .unwrap_err();
    assert!(is_check_violation(&e), "{e:?}");
    db.cleanup().await;
}

#[tokio::test]
async fn the_outcome_check_accepts_exactly_attempt_outcomes() {
    use pg_task_scheduler::AttemptOutcome::*;
    // The match fails to compile when a variant is added without being listed here.
    fn listed(o: pg_task_scheduler::AttemptOutcome) -> bool {
        match o {
            Done | Retry | Dead | LeaseExpired | Released | Cancelled => true,
        }
    }
    for outcome in [Done, Retry, Dead, LeaseExpired, Released, Cancelled] {
        assert!(listed(outcome));
        // The error CHECK allows a diagnostic only on failures; give one where it does.
        let error = matches!(outcome, Retry | Dead | LeaseExpired | Cancelled).then_some("x");
        let db = TestDb::new().await;
        insert_task(&db).await;
        exec(&db, &closed_attempt(outcome.as_str(), error))
            .await
            .unwrap_or_else(|e| panic!("{outcome:?}: {e:?}"));
        db.cleanup().await;
    }
    let db = TestDb::new().await;
    insert_task(&db).await;
    let e = exec(&db, &closed_attempt("bogus", None)).await.unwrap_err();
    assert!(is_check_violation(&e), "{e:?}");
    db.cleanup().await;
}
