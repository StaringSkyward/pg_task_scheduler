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
