mod common;

use common::TestDb;
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use pg_task_scheduler::store;

#[derive(diesel::QueryableByName)]
struct PlanLine {
    #[diesel(sql_type = diesel::sql_types::Text)]
    #[diesel(column_name = "QUERY PLAN")]
    line: String,
}

#[tokio::test]
async fn claim_uses_claim_index() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute("SET enable_seqscan = off")
        .await
        .unwrap();
    let plan = diesel::sql_query(
        r#"EXPLAIN SELECT id FROM queue_tasks
           WHERE status = 'queued' AND due_at <= now() AND attempts < max_attempts
             AND queue = 'default' AND task_name = ANY('{email}')
             AND NOT EXISTS (SELECT 1 FROM queue_pauses WHERE queue = 'default')
           ORDER BY priority DESC, due_at, id LIMIT 16"#,
    )
    .get_results::<PlanLine>(&mut conn)
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.line)
    .collect::<Vec<_>>()
    .join("\n");
    assert!(
        plan.contains("queue_tasks_claim_idx"),
        "unexpected query plan:\n{plan}"
    );
    db.cleanup().await;
}

#[tokio::test]
async fn next_due_in_uses_due_index() {
    // The query below mirrors `next_due_in` in src/store/claim.rs; keep them in step.
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute("SET enable_seqscan = off")
        .await
        .unwrap();
    let plan = diesel::sql_query(
        "EXPLAIN SELECT min(next.due_at) - now() AS wait \
         FROM (SELECT queue FROM unnest('{default,bulk}'::text[]) AS u(queue) \
               WHERE NOT EXISTS (SELECT 1 FROM queue_pauses p WHERE p.queue = u.queue)) AS q \
         CROSS JOIN LATERAL ( \
             SELECT min(due_at) AS due_at FROM queue_tasks \
             WHERE status = 'queued' AND queue = q.queue \
               AND attempts < max_attempts AND task_name = ANY('{email}') \
         ) AS next",
    )
    .get_results::<PlanLine>(&mut conn)
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.line)
    .collect::<Vec<_>>()
    .join("\n");
    // Each queue's probe is a Limit over an index scan whose Index Cond fixes `queue`;
    // `queue` as a mere Filter (or `= ANY`) would scan every queued row of the queues.
    let lines: Vec<&str> = plan.lines().collect();
    let scan = lines
        .iter()
        .position(|l| l.contains("Index Scan using queue_tasks_due_idx"));
    assert!(
        !plan.contains("Seq Scan")
            && scan.is_some_and(|i| {
                i > 0
                    && lines[i - 1].contains("Limit")
                    && lines[i + 1].contains("Index Cond:")
                    && lines[i + 1].contains("(queue = ")
            }),
        "unexpected query plan:\n{plan}"
    );
    db.cleanup().await;
}

/// Published-outbox cleanup reaches eligible rows through an index condition on
/// `published_at`, so a backlog of old unpublished rows is never walked.
#[tokio::test]
async fn outbox_cleanup_uses_published_index() {
    // The query below mirrors CLEANUP_OUTBOX's selection in src/store/maintenance.rs.
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute("SET enable_seqscan = off")
        .await
        .unwrap();
    let plan = diesel::sql_query(
        "EXPLAIN SELECT id FROM queue_outbox \
         WHERE published_at < now() - interval '7 days' \
         ORDER BY published_at, id LIMIT 1000 FOR UPDATE SKIP LOCKED",
    )
    .get_results::<PlanLine>(&mut conn)
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.line)
    .collect::<Vec<_>>()
    .join("\n");
    // An ordered Index Scan or a Bitmap Index Scan (the planner's pick on a small table)
    // both qualify: either way `published_at` bounds the scan.
    assert!(
        plan.contains("queue_outbox_published_idx")
            && plan
                .lines()
                .any(|l| l.contains("Index Cond:") && l.contains("published_at")),
        "unexpected query plan:\n{plan}"
    );
    db.cleanup().await;
}

/// Cancelled-task cleanup reaches eligible rows through an index condition on `completed_at`.
#[tokio::test]
async fn cleanup_cancelled_uses_terminal_index() {
    use diesel::pg::data_types::PgInterval;
    use diesel::sql_types::{BigInt, Interval};
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute("SET enable_seqscan = off")
        .await
        .unwrap();
    let plan = diesel::sql_query(format!(
        "EXPLAIN {}",
        pg_task_scheduler::store::CLEANUP_CANCELLED
    ))
    .bind::<Interval, _>(PgInterval::new(0, 30, 0))
    .bind::<BigInt, _>(1000_i64)
    .get_results::<PlanLine>(&mut conn)
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.line)
    .collect::<Vec<_>>()
    .join("\n");
    assert!(
        // `queue_tasks_failed_idx` (added with the admin UI) serves it equally well.
        (plan.contains("queue_tasks_terminal_cleanup_idx")
            || plan.contains("queue_tasks_failed_idx"))
            && plan
                .lines()
                .any(|l| l.contains("Index Cond:") && l.contains("completed_at")),
        "unexpected query plan:\n{plan}"
    );
    db.cleanup().await;
}

/// A bulk replay batch finds dead and cancelled rows by name through the failed-name index.
#[tokio::test]
async fn replay_matching_uses_failed_name_index() {
    use diesel::sql_types::{BigInt, Nullable, Text};
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute("SET enable_seqscan = off")
        .await
        .unwrap();
    let plan = diesel::sql_query(format!(
        "EXPLAIN {}",
        pg_task_scheduler::store::REPLAY_BATCH
    ))
    .bind::<Text, _>("a")
    .bind::<Nullable<Text>, _>(None::<String>)
    .bind::<Nullable<Text>, _>(None::<String>)
    .bind::<BigInt, _>(1000_i64)
    .bind::<BigInt, _>(1_i64)
    .bind::<BigInt, _>(500_i64)
    .get_results::<PlanLine>(&mut conn)
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.line)
    .collect::<Vec<_>>()
    .join("\n");
    assert!(
        plan.contains("queue_tasks_failed_name_idx") && !plan.contains("Seq Scan on queue_tasks"),
        "unexpected query plan:\n{plan}"
    );
    db.cleanup().await;
}

fn browse_plan(rows: Vec<PlanLine>, index: &str) -> String {
    let plan = rows
        .into_iter()
        .map(|row| row.line)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains(index)
            && !plan.contains("Seq Scan on queue_tasks")
            && !plan.contains("Seq Scan on queue_task_attempts"),
        "unexpected query plan:\n{plan}"
    );
    plan
}

async fn seqscan_off_conn(
    db: &TestDb,
) -> diesel_async::pooled_connection::deadpool::Object<diesel_async::AsyncPgConnection> {
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute("SET enable_seqscan = off")
        .await
        .unwrap();
    conn
}

#[tokio::test]
async fn queued_page_uses_claim_and_due_indexes() {
    use diesel::sql_types::{BigInt, Text, Timestamptz};
    let db = TestDb::new().await;
    let mut conn = seqscan_off_conn(&db).await;
    let now = chrono::Utc::now();
    let due = diesel::sql_query(format!("EXPLAIN {}", store::QUEUED_DUE))
        .bind::<Text, _>("default")
        .bind::<BigInt, _>(5_i64)
        .bind::<Timestamptz, _>(now)
        .bind::<BigInt, _>(7_i64)
        .bind::<BigInt, _>(51_i64)
        .get_results::<PlanLine>(&mut conn)
        .await
        .unwrap();
    browse_plan(due, "queue_tasks_claim_idx");
    let future = diesel::sql_query(format!("EXPLAIN {}", store::QUEUED_FUTURE))
        .bind::<Text, _>("default")
        .bind::<Timestamptz, _>(now)
        .bind::<BigInt, _>(7_i64)
        .bind::<BigInt, _>(51_i64)
        .get_results::<PlanLine>(&mut conn)
        .await
        .unwrap();
    browse_plan(future, "queue_tasks_due_idx");
    db.cleanup().await;
}

#[tokio::test]
async fn running_page_uses_lease_index() {
    use diesel::sql_types::{BigInt, Timestamptz};
    let db = TestDb::new().await;
    let mut conn = seqscan_off_conn(&db).await;
    let plan = diesel::sql_query(format!("EXPLAIN {}", store::RUNNING))
        .bind::<Timestamptz, _>(chrono::Utc::now())
        .bind::<BigInt, _>(7_i64)
        .bind::<BigInt, _>(51_i64)
        .get_results::<PlanLine>(&mut conn)
        .await
        .unwrap();
    browse_plan(plan, "queue_tasks_expired_lease_idx");
    db.cleanup().await;
}

#[tokio::test]
async fn done_page_uses_terminal_index() {
    use diesel::sql_types::{BigInt, Timestamptz};
    let db = TestDb::new().await;
    let mut conn = seqscan_off_conn(&db).await;
    let plan = diesel::sql_query(format!("EXPLAIN {}", store::DONE))
        .bind::<Timestamptz, _>(chrono::Utc::now())
        .bind::<BigInt, _>(7_i64)
        .bind::<BigInt, _>(51_i64)
        .get_results::<PlanLine>(&mut conn)
        .await
        .unwrap();
    browse_plan(plan, "queue_tasks_terminal_cleanup_idx");
    db.cleanup().await;
}

#[tokio::test]
async fn failed_page_uses_failed_index() {
    use diesel::sql_types::{BigInt, Nullable, Text, Timestamptz};
    let db = TestDb::new().await;
    let mut conn = seqscan_off_conn(&db).await;
    let plan = diesel::sql_query(format!("EXPLAIN {}", store::FAILED))
        .bind::<Text, _>("dead")
        .bind::<Nullable<Text>, _>(Some("boom"))
        .bind::<Timestamptz, _>(chrono::Utc::now())
        .bind::<BigInt, _>(7_i64)
        .bind::<BigInt, _>(51_i64)
        .get_results::<PlanLine>(&mut conn)
        .await
        .unwrap();
    browse_plan(plan, "queue_tasks_failed_idx");
    db.cleanup().await;
}

#[tokio::test]
async fn failed_page_by_name_uses_failed_name_index() {
    use diesel::sql_types::{BigInt, Nullable, Text, Timestamptz};
    let db = TestDb::new().await;
    // With real statistics, the selective task_name wins over the status-only index.
    let mut setup = db.pool.get().await.unwrap();
    setup
        .batch_execute(
            "INSERT INTO queue_tasks (task_key, task_name, status, completed_at) \
             SELECT 'k' || g, 'name' || (g % 50), 'dead', now() - g * interval '1 second' \
             FROM generate_series(1, 5000) g; ANALYZE queue_tasks",
        )
        .await
        .unwrap();
    drop(setup);
    let mut conn = seqscan_off_conn(&db).await;
    let plan = diesel::sql_query(format!("EXPLAIN {}", store::FAILED_BY_NAME))
        .bind::<Text, _>("dead")
        .bind::<Nullable<Text>, _>(None::<&str>)
        .bind::<Timestamptz, _>(chrono::Utc::now())
        .bind::<BigInt, _>(7_i64)
        .bind::<BigInt, _>(51_i64)
        .bind::<Text, _>("email")
        .get_results::<PlanLine>(&mut conn)
        .await
        .unwrap();
    browse_plan(plan, "queue_tasks_failed_name_idx");
    db.cleanup().await;
}

async fn overview_plan(
    sql: &str,
    index: &str,
    binds: impl FnOnce(
        diesel::query_builder::BoxedSqlQuery<'_, diesel::pg::Pg, diesel::query_builder::SqlQuery>,
    ) -> diesel::query_builder::BoxedSqlQuery<
        '_,
        diesel::pg::Pg,
        diesel::query_builder::SqlQuery,
    >,
) {
    let db = TestDb::new().await;
    let mut conn = seqscan_off_conn(&db).await;
    let rows = binds(diesel::sql_query(format!("EXPLAIN {sql}")).into_boxed())
        .get_results::<PlanLine>(&mut conn)
        .await
        .unwrap();
    browse_plan(rows, index);
    db.cleanup().await;
}

#[tokio::test]
async fn overview_distinct_queues_skip_scan_uses_due_index() {
    // The planner prefers the smaller queued-by-(queue, due_at) index to the claim index; both
    // lead with `queue` among queued rows. EXPLAIN omits the recursive step's subplan, so
    // only the anchor scan is asserted here.
    overview_plan(store::OVERVIEW_QUEUES, "queue_tasks_due_idx", |q| q).await;
}

#[tokio::test]
async fn overview_due_count_uses_due_index() {
    use diesel::sql_types::{Array, BigInt, Text};
    overview_plan(store::OVERVIEW_COUNTS, "queue_tasks_due_idx", |q| {
        q.bind::<Array<Text>, _>(vec!["default"])
            .bind::<BigInt, _>(10_000_i64)
    })
    .await;
}

#[tokio::test]
async fn overview_done_window_uses_terminal_index() {
    use diesel::pg::data_types::PgInterval;
    use diesel::sql_types::{BigInt, Interval};
    overview_plan(
        store::OVERVIEW_DONE,
        "queue_tasks_terminal_cleanup_idx",
        |q| {
            q.bind::<Interval, _>(PgInterval::from_microseconds(3_600_000_000))
                .bind::<BigInt, _>(10_000_i64)
        },
    )
    .await;
}

#[tokio::test]
async fn overview_dead_window_uses_failed_index() {
    use diesel::pg::data_types::PgInterval;
    use diesel::sql_types::{BigInt, Interval};
    overview_plan(store::OVERVIEW_DEAD, "queue_tasks_failed_idx", |q| {
        q.bind::<Interval, _>(PgInterval::from_microseconds(3_600_000_000))
            .bind::<BigInt, _>(10_000_i64)
    })
    .await;
}

#[tokio::test]
async fn overview_failed_attempts_uses_failed_attempts_index() {
    use diesel::pg::data_types::PgInterval;
    use diesel::sql_types::{BigInt, Interval};
    overview_plan(
        store::OVERVIEW_FAILED_ATTEMPTS,
        "queue_task_attempts_failed_idx",
        |q| {
            q.bind::<Interval, _>(PgInterval::from_microseconds(3_600_000_000))
                .bind::<BigInt, _>(10_000_i64)
        },
    )
    .await;
}

#[tokio::test]
async fn overview_running_uses_lease_index() {
    overview_plan(
        store::OVERVIEW_RUNNING,
        "queue_tasks_expired_lease_idx",
        |q| q,
    )
    .await;
}

#[tokio::test]
async fn overview_dead_groups_use_failed_index() {
    use diesel::pg::data_types::PgInterval;
    use diesel::sql_types::{BigInt, Interval};
    overview_plan(store::DEAD_GROUPS, "queue_tasks_failed_idx", |q| {
        q.bind::<Interval, _>(PgInterval::from_microseconds(3_600_000_000))
            .bind::<BigInt, _>(10_000_i64)
    })
    .await;
}

#[tokio::test]
async fn overview_no_handler_probe_is_a_limited_due_index_scan() {
    use diesel::sql_types::{Array, BigInt, Text};
    let db = TestDb::new().await;
    let mut conn = seqscan_off_conn(&db).await;
    let rows = diesel::sql_query(format!("EXPLAIN {}", store::NO_HANDLER))
        .bind::<Array<Text>, _>(vec!["default"])
        .bind::<BigInt, _>(100_i64)
        .get_results::<PlanLine>(&mut conn)
        .await
        .unwrap();
    let plan = browse_plan(rows, "queue_tasks_due_idx");
    // The Limit sits directly on the index scan: no Sort over the queue's due rows. (Sorting
    // the at most `probe` limited rows to de-duplicate names is fine, so look at the child.)
    let lines: Vec<&str> = plan.lines().collect();
    let limit = lines
        .iter()
        .position(|l| l.contains("Limit"))
        .expect("Limit");
    assert!(
        lines[limit + 1].contains("Index"),
        "Limit not directly over the index scan:\n{plan}"
    );
    db.cleanup().await;
}
