mod common;

use common::TestDb;
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};

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
         FROM unnest('{default,bulk}'::text[]) AS q(queue) \
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
        plan.contains("queue_tasks_terminal_cleanup_idx")
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
