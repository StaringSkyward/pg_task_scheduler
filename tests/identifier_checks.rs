mod common;

use common::TestDb;
use diesel::result::DatabaseErrorKind;
use diesel::result::Error as DieselError;
use diesel_async::RunQueryDsl;
use pg_task_scheduler::SchedulerError;
use pg_task_scheduler::TaskId;
use pg_task_scheduler::TaskName;
use pg_task_scheduler::store;

#[derive(diesel::QueryableByName)]
struct Verdict {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    cp: i32,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    leading: bool,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    trailing: bool,
}

#[derive(diesel::QueryableByName)]
struct Valid {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    ok: bool,
}

#[derive(diesel::QueryableByName)]
struct Def {
    #[diesel(sql_type = diesel::sql_types::Text)]
    def: String,
}

/// `queue_is_identifier` must give the verdict `validate_identifier` (`src/ids.rs`) gives:
/// every BMP code point but NUL (text cannot hold it) and the surrogates, leading and trailing.
#[tokio::test]
async fn sql_identifier_rule_matches_rust() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let rows = diesel::sql_query(
        "SELECT cp, queue_is_identifier(chr(cp) || 'x') AS leading, \
                queue_is_identifier('x' || chr(cp)) AS trailing \
         FROM generate_series(1, 65535) AS cp WHERE cp NOT BETWEEN 55296 AND 57343",
    )
    .get_results::<Verdict>(&mut conn)
    .await
    .unwrap();
    assert_eq!(rows.len(), 65535 - 2048);
    let mismatched: Vec<String> = rows
        .iter()
        .filter_map(|r| {
            let c = char::from_u32(u32::try_from(r.cp).unwrap()).unwrap();
            let rust = (
                TaskName::try_from(format!("{c}x")).is_ok(),
                TaskName::try_from(format!("x{c}")).is_ok(),
            );
            (rust != (r.leading, r.trailing)).then(|| format!("U+{:04X}", r.cp))
        })
        .collect();
    assert!(
        mismatched.is_empty(),
        "SQL and Rust disagree on {mismatched:?}"
    );

    for (s, valid) in [
        (String::new(), false),
        ("é".repeat(255), true),
        ("é".repeat(256), false),
    ] {
        let sql = diesel::sql_query("SELECT queue_is_identifier($1) AS ok")
            .bind::<diesel::sql_types::Text, _>(&s)
            .get_result::<Valid>(&mut conn)
            .await
            .unwrap()
            .ok;
        assert_eq!(TaskName::try_from(s.as_str()).is_ok(), valid, "{s}");
        assert_eq!(sql, valid, "{s}");
    }
    db.cleanup().await;
}

/// Every column the crate reads or writes as an identifier newtype has its named CHECK.
#[tokio::test]
async fn identifier_constraints_cover_every_identifier_column() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let mut actual: Vec<String> = diesel::sql_query(
        "SELECT conrelid::regclass::text || ' ' || conname || ' ' || pg_get_constraintdef(oid) AS def \
         FROM pg_constraint \
         WHERE connamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema()) \
           AND (conname ~ '_identifier$' OR conname = 'queue_outbox_command_key_length')",
    )
    .get_results::<Def>(&mut conn)
    .await
    .unwrap()
    .into_iter()
    .map(|r| r.def)
    .collect();
    actual.sort();

    #[cfg_attr(not(feature = "cron"), allow(unused_mut))]
    let mut columns = vec![
        ("queue_tasks", "task_key"),
        ("queue_tasks", "task_name"),
        ("queue_tasks", "queue"),
        ("queue_tasks", "locked_by"),
        ("queue_task_attempts", "worker_id"),
        ("queue_outbox", "task_key"),
        ("queue_outbox", "effect_name"),
        ("queue_outbox", "topic"),
        ("queue_admin_actions", "task_name"),
        ("queue_admin_actions", "queue"),
        ("queue_pauses", "queue"),
        ("queue_workers", "worker_id"),
        ("queue_worker_queues", "worker_id"),
        ("queue_worker_queues", "queue"),
        ("queue_worker_task_names", "worker_id"),
        ("queue_worker_task_names", "task_name"),
    ];
    #[cfg(feature = "cron")]
    columns.extend([
        ("scheduler_jobs", "name"),
        ("scheduler_jobs", "task_name"),
        ("scheduler_jobs", "queue"),
    ]);
    let mut expected: Vec<String> = columns
        .iter()
        .map(|(t, c)| format!("{t} {t}_{c}_identifier CHECK (queue_is_identifier({c}))"))
        .collect();
    expected.push(
        "queue_outbox queue_outbox_command_key_length \
         CHECK (((octet_length(command_key) >= 1) AND (octet_length(command_key) <= 2048)))"
            .to_owned(),
    );
    expected.sort();
    assert_eq!(actual, expected);
    db.cleanup().await;
}

#[tokio::test]
async fn queue_tasks_rejects_an_invalid_task_name() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let err = diesel::sql_query("INSERT INTO queue_tasks (task_key, task_name) VALUES ('k', ' x')")
        .execute(&mut conn)
        .await
        .unwrap_err();
    match &err {
        DieselError::DatabaseError(DatabaseErrorKind::CheckViolation, info) => {
            assert_eq!(
                info.constraint_name(),
                Some("queue_tasks_task_name_identifier")
            );
        }
        other => panic!("expected a CHECK violation (23514), got {other:?}"),
    }
    db.cleanup().await;
}

/// NULL passes the identifier CHECK, so the nullable identifier columns stay nullable.
#[tokio::test]
async fn nullable_identifier_columns_accept_null() {
    use diesel_async::SimpleAsyncConnection;
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute(
        "INSERT INTO queue_tasks (task_key, task_name, locked_by) VALUES ('k', 'n', NULL); \
         INSERT INTO queue_admin_actions (action, task_name, queue, max_task_id, operator, reason) \
         VALUES ('replay', 'n', NULL, 1, 'op', 'r'), ('pause', NULL, 'q', NULL, 'op', 'r')",
    )
    .await
    .unwrap();
    db.cleanup().await;
}

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
    // Simulates a database whose CHECK was dropped by hand, which the decode re-validation
    // still covers.
    db.drop_constraint("queue_tasks", "queue_tasks_queue_identifier")
        .await;
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
