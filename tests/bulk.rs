mod common;

use std::num::NonZeroU32;
use std::time::Duration;

use common::TestDb;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::SimpleAsyncConnection;
use pg_task_scheduler::EnqueueOptions;
use pg_task_scheduler::QueueName;
use pg_task_scheduler::SchedulerError;
use pg_task_scheduler::Task;
use pg_task_scheduler::TaskId;
use pg_task_scheduler::TaskKey;
use pg_task_scheduler::TaskName;
use pg_task_scheduler::WorkerId;
use pg_task_scheduler::enqueue;
use pg_task_scheduler::store::BULK_BUDGET;
use pg_task_scheduler::store::BulkFilter;
use pg_task_scheduler::store::BulkOutcome;
use pg_task_scheduler::store::BulkStop;
use pg_task_scheduler::store::CancelOutcome;
use pg_task_scheduler::store::Count;
use pg_task_scheduler::store::ErrorFilter;
use pg_task_scheduler::store::{self};
use serde_json::json;

struct A;
impl Task for A {
    const NAME: &'static str = "a";
    type Args = serde_json::Value;
}

fn filter(name: &str) -> BulkFilter {
    BulkFilter {
        task_name: TaskName::try_from(name).unwrap(),
        queue: None,
    }
}

/// Insert `n` rows named `name` in `status` (terminal statuses need `completed_at`).
async fn insert_many(db: &TestDb, name: &str, status: &str, err: &str, n: i64) {
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute(&format!(
        "INSERT INTO queue_tasks (task_key, task_name, status, last_error, completed_at) \
         SELECT '{name}-{status}-{err}-' || g, '{name}', '{status}', '{err}', \
                CASE WHEN '{status}' IN ('queued','running') THEN NULL ELSE now() END \
         FROM generate_series(1, {n}) g"
    ))
    .await
    .unwrap();
}

async fn count(db: &TestDb, name: &str, status: &str) -> i64 {
    db.scalar(&format!(
        "SELECT count(*) AS n FROM queue_tasks WHERE task_name = '{name}' AND status = '{status}'"
    ))
    .await
}

#[tokio::test]
async fn cancel_matching_cancels_queued_and_running_of_that_name_only() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    insert_many(&db, "a", "queued", "", 3).await;
    insert_many(&db, "b", "queued", "", 2).await;
    let claimed = store::claim(
        &mut conn,
        &WorkerId::generate(),
        &QueueName::default(),
        &[pg_task_scheduler::TaskName::try_from("a").unwrap()],
        NonZeroU32::new(1).unwrap(),
        common::lease(Duration::from_secs(60)),
    )
    .await
    .unwrap()
    .ready;
    assert_eq!(claimed.len(), 1);

    let out = store::cancel_matching(&mut conn, &filter("a"), "ops", "why", store::BULK_BUDGET)
        .await
        .unwrap();
    assert_eq!((out.affected, out.stopped), (3, BulkStop::ShortBatch));
    assert_eq!(count(&db, "a", "cancelled").await, 3);
    assert_eq!(count(&db, "b", "queued").await, 2);
    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_admin_actions")
            .await,
        1
    );
    assert_eq!(
        db.scalar(&format!(
            "SELECT count(*) AS n FROM queue_admin_action_tasks m \
             JOIN queue_tasks t ON t.id = m.task_id \
             WHERE t.task_name = 'a' AND m.action_id = {}",
            out.action_id
        ))
        .await,
        3
    );
    // The running one's attempt was closed.
    assert_eq!(
        db.scalar(
            "SELECT count(*) AS n FROM queue_task_attempts WHERE outcome = 'cancelled' \
             AND finished_at IS NOT NULL"
        )
        .await,
        1
    );
    drop(conn);
    db.cleanup().await;
}

#[tokio::test]
async fn cancel_matching_ignores_tasks_enqueued_after_it_started() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    insert_many(&db, "a", "queued", "", 5).await;
    let pool = db.pool.clone();
    let producer = tokio::spawn(async move {
        let mut c = pool.get().await.unwrap();
        for i in 0..40 {
            enqueue::<A>(
                &mut c,
                TaskKey::try_from(format!("late-{i}")).unwrap(),
                json!({}),
                EnqueueOptions::immediate(),
            )
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let out = store::cancel_matching(&mut conn, &filter("a"), "ops", "why", store::BULK_BUDGET)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    producer.await.unwrap();

    let bound = db
        .scalar(&format!(
            "SELECT max_task_id AS n FROM queue_admin_actions WHERE id = {}",
            out.action_id
        ))
        .await;
    assert_eq!(
        db.scalar(&format!(
            "SELECT count(*) AS n FROM queue_admin_action_tasks \
             WHERE action_id = {} AND task_id > {bound}",
            out.action_id
        ))
        .await,
        0
    );
    let after = format!("task_name = 'a' AND id > {bound}");
    assert!(
        db.scalar(&format!(
            "SELECT count(*) AS n FROM queue_tasks WHERE {after}"
        ))
        .await
            > 0
    );
    assert_eq!(
        db.scalar(&format!(
            "SELECT count(*) AS n FROM queue_tasks WHERE {after} AND status <> 'queued'"
        ))
        .await,
        0
    );
    drop(conn);
    db.cleanup().await;
}

#[tokio::test]
async fn bulk_stops_at_budget_and_continues() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    insert_many(&db, "a", "queued", "", 1200).await;
    let first = store::cancel_matching(&mut conn, &filter("a"), "ops", "why", Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(
        (first.affected, first.stopped),
        (500, BulkStop::BudgetSpent)
    );
    let mut total = first.affected;
    loop {
        let o = store::cancel_matching(&mut conn, &filter("a"), "ops", "why", Duration::ZERO)
            .await
            .unwrap();
        total += o.affected;
        if o.stopped == BulkStop::ShortBatch {
            break;
        }
    }
    assert_eq!(total, 1200);
    assert_eq!(count(&db, "a", "cancelled").await, 1200);
    drop(conn);
    db.cleanup().await;
}

#[tokio::test]
async fn replay_matching_filters_by_last_error() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    insert_many(&db, "a", "dead", "x", 2).await;
    insert_many(&db, "a", "dead", "y", 1).await;
    insert_many(&db, "a", "cancelled", "x", 1).await;
    let out = store::replay_matching(
        &mut conn,
        &filter("a"),
        &ErrorFilter::Exact("x".into()),
        "ops",
        "why",
        store::BULK_BUDGET,
    )
    .await
    .unwrap();
    assert_eq!((out.affected, out.stopped), (3, BulkStop::ShortBatch));
    assert_eq!(count(&db, "a", "queued").await, 3);
    assert_eq!(count(&db, "a", "dead").await, 1);
    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_tasks WHERE last_error IS NOT NULL")
            .await,
        1
    );
    drop(conn);
    db.cleanup().await;
}

/// `Exact("")`, `Missing` and `Exact("x")` each select only their own task, in the count, the
/// replay and the audit row. Each filter has its own task name with all three dead rows, so
/// every count and replay sees `''`, NULL and `'x'` side by side.
#[tokio::test]
async fn error_filters_select_only_their_own_group() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let filters = [
        ("e", ErrorFilter::Exact(String::new()), "empty"),
        ("m", ErrorFilter::Missing, "null"),
        ("x", ErrorFilter::Exact("x".into()), "x"),
    ];
    for (name, ..) in &filters {
        conn.batch_execute(&format!(
            "INSERT INTO queue_tasks (task_key, task_name, status, last_error, completed_at) \
             VALUES ('{name}-empty', '{name}', 'dead', '', now()), \
                    ('{name}-null', '{name}', 'dead', NULL, now()), \
                    ('{name}-x', '{name}', 'dead', 'x', now())"
        ))
        .await
        .unwrap();
    }
    // Every count first, while all nine rows are dead.
    for (name, error, _) in &filters {
        let n = store::count_replayable(&mut conn, &filter(name), error, NonZeroU32::MAX)
            .await
            .unwrap();
        assert_eq!(n, 1, "{error:?}");
    }
    for (name, error, key) in &filters {
        let out = store::replay_matching(&mut conn, &filter(name), error, "op", "why", BULK_BUDGET)
            .await
            .unwrap();
        assert_eq!(out.affected, 1, "{error:?}");
        let queued: Vec<String> = diesel_async::RunQueryDsl::load::<Key>(
            diesel::sql_query(format!(
                "SELECT task_key FROM queue_tasks WHERE task_name = '{name}' AND status = 'queued'"
            )),
            &mut conn,
        )
        .await
        .unwrap()
        .into_iter()
        .map(|k| k.task_key)
        .collect();
        assert_eq!(queued, [format!("{name}-{key}")], "{error:?}");
    }
    // Each audit row reads back the filter it ran with, newest first.
    let nz = |n| NonZeroU32::new(n).unwrap();
    let recorded: Vec<_> = store::admin_actions(&mut conn, None, nz(10), nz(10))
        .await
        .unwrap()
        .into_iter()
        .map(|a| match a.target {
            store::AdminActionTarget::Bulk {
                action: store::BulkAction::Replay { error },
                ..
            } => error,
            t => panic!("not a bulk replay: {t:?}"),
        })
        .collect();
    let ran: Vec<_> = filters.into_iter().rev().map(|(_, e, _)| e).collect();
    assert_eq!(recorded, ran);
    drop(conn);
    db.cleanup().await;
}

#[derive(diesel::QueryableByName)]
struct Key {
    #[diesel(sql_type = diesel::sql_types::Text)]
    task_key: String,
}

#[tokio::test]
async fn counts_are_capped() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    insert_many(&db, "a", "queued", "", 30).await;
    insert_many(&db, "a", "dead", "x", 30).await;
    assert_eq!(
        store::count_cancellable(&mut conn, &filter("a"), NonZeroU32::new(10).unwrap())
            .await
            .unwrap(),
        10
    );
    assert_eq!(
        store::count_cancellable(&mut conn, &filter("a"), NonZeroU32::new(100).unwrap())
            .await
            .unwrap(),
        30
    );
    assert_eq!(
        store::count_replayable(
            &mut conn,
            &filter("a"),
            &ErrorFilter::Exact("x".into()),
            NonZeroU32::new(10).unwrap()
        )
        .await
        .unwrap(),
        10
    );
    drop(conn);
    db.cleanup().await;
}

#[tokio::test]
async fn concurrent_bulk_cancels_split_the_work() {
    let db = TestDb::new().await;
    insert_many(&db, "a", "queued", "", 1200).await;
    let mut c1 = db.pool.get().await.unwrap();
    let mut c2 = db.pool.get().await.unwrap();
    let f = filter("a");
    let (r1, r2) = tokio::join!(
        store::cancel_matching(&mut c1, &f, "ops", "one", store::BULK_BUDGET),
        store::cancel_matching(&mut c2, &f, "ops", "two", store::BULK_BUDGET),
    );
    let (o1, o2) = (r1.unwrap(), r2.unwrap());
    assert_eq!(o1.affected + o2.affected, 1200);
    assert_eq!(count(&db, "a", "cancelled").await, 1200);
    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_admin_actions")
            .await,
        2
    );
    drop((c1, c2));
    db.cleanup().await;
}

#[tokio::test]
async fn bulk_cancel_that_skips_a_locked_row_reports_short_batch() {
    let db = TestDb::new().await;
    insert_many(&db, "x", "queued", "", 1).await;
    let mut holder = db.pool.get().await.unwrap();
    holder
        .batch_execute("BEGIN; SELECT 1 FROM queue_tasks WHERE task_name = 'x' FOR UPDATE")
        .await
        .unwrap();
    let mut conn = db.pool.get().await.unwrap();
    let out = store::cancel_matching(&mut conn, &filter("x"), "op", "r", BULK_BUDGET)
        .await
        .unwrap();
    assert_eq!((out.affected, out.stopped), (0, BulkStop::ShortBatch));
    assert_eq!(count(&db, "x", "queued").await, 1);
    holder.batch_execute("ROLLBACK").await.unwrap();
    db.cleanup().await;
}

#[derive(diesel::QueryableByName)]
struct Act {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    id: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    max_task_id: i64,
}

#[derive(diesel::QueryableByName)]
struct Cnt {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

/// A bulk replay action over every task named `n`, as `replay_matching` starts one.
async fn replay_action(conn: &mut AsyncPgConnection) -> Act {
    use diesel_async::RunQueryDsl;
    diesel::sql_query(
        "INSERT INTO queue_admin_actions \
             (action, task_name, error_filter, max_task_id, operator, reason) \
         SELECT 'replay', 'n', 'any', max(id), 'op', 'why' FROM queue_tasks \
         RETURNING id, max_task_id",
    )
    .get_result::<Act>(conn)
    .await
    .unwrap()
}

/// One `REPLAY_BATCH` of `act`, 500 rows at most; returns how many it replayed.
async fn replay_batch(conn: &mut AsyncPgConnection, act: &Act) -> i64 {
    use diesel::sql_types::BigInt;
    use diesel::sql_types::Nullable;
    use diesel::sql_types::Text;
    use diesel_async::RunQueryDsl;
    diesel::sql_query(store::REPLAY_BATCH)
        .bind::<Text, _>("n")
        .bind::<Nullable<Text>, _>(None::<String>)
        .bind::<Text, _>("any")
        .bind::<Nullable<Text>, _>(None::<String>)
        .bind::<BigInt, _>(act.max_task_id)
        .bind::<BigInt, _>(act.id)
        .bind::<BigInt, _>(500_i64)
        .get_result::<Cnt>(conn)
        .await
        .unwrap()
        .n
}

/// A task replayed by an action that fails again before a later batch is not replayed twice.
#[tokio::test]
async fn a_replayed_task_that_fails_again_is_not_replayed_by_the_same_action() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute(
        "INSERT INTO queue_tasks (task_key, task_name, status, completed_at) \
         SELECT 'k' || g, 'n', 'dead', now() - interval '1 hour' \
         FROM generate_series(1, 501) g",
    )
    .await
    .unwrap();
    let act = replay_action(&mut conn).await;
    assert_eq!(replay_batch(&mut conn, &act).await, 500);
    conn.batch_execute(
        "UPDATE queue_tasks SET status = 'dead', completed_at = clock_timestamp(), \
                last_error = 'again' \
         WHERE id = (SELECT min(id) FROM queue_tasks WHERE status = 'queued')",
    )
    .await
    .unwrap();
    assert_eq!(replay_batch(&mut conn, &act).await, 1);
    assert_eq!(replay_batch(&mut conn, &act).await, 0);
    db.cleanup().await;
}

/// Another action cancelling a task this action replayed does not make it match again.
#[tokio::test]
async fn a_task_cancelled_by_another_action_is_not_replayed_again() {
    let db = TestDb::new().await;
    insert_many(&db, "n", "dead", "e", 1).await;
    let id = TaskId(db.scalar("SELECT id AS n FROM queue_tasks").await);
    let mut conn = db.pool.get().await.unwrap();
    let act = replay_action(&mut conn).await;
    assert_eq!(replay_batch(&mut conn, &act).await, 1);
    assert_eq!(
        store::cancel(&mut conn, id, "op", "other").await.unwrap(),
        CancelOutcome::Cancelled
    );
    assert_eq!(replay_batch(&mut conn, &act).await, 0);
    assert_eq!(count(&db, "n", "cancelled").await, 1);
    drop(conn);
    db.cleanup().await;
}

/// One `CANCEL_BATCH` of a bulk cancel action over every task named `n`, 500 rows at most;
/// returns how many it cancelled.
async fn cancel_batch(conn: &mut AsyncPgConnection, act: &Act) -> i64 {
    use diesel::sql_types::BigInt;
    use diesel::sql_types::Nullable;
    use diesel::sql_types::Text;
    use diesel_async::RunQueryDsl;
    diesel::sql_query(store::CANCEL_BATCH)
        .bind::<Text, _>("n")
        .bind::<Nullable<Text>, _>(None::<String>)
        .bind::<BigInt, _>(act.max_task_id)
        .bind::<BigInt, _>(act.id)
        .bind::<BigInt, _>(500_i64)
        .get_result::<Cnt>(conn)
        .await
        .unwrap()
        .n
}

/// Another action replaying a task this action cancelled does not make it match again.
#[tokio::test]
async fn a_task_replayed_by_another_action_is_not_cancelled_again() {
    use diesel_async::RunQueryDsl;
    let db = TestDb::new().await;
    insert_many(&db, "n", "queued", "", 1).await;
    let id = TaskId(db.scalar("SELECT id AS n FROM queue_tasks").await);
    let mut conn = db.pool.get().await.unwrap();
    let act = diesel::sql_query(
        "INSERT INTO queue_admin_actions (action, task_name, max_task_id, operator, reason) \
         SELECT 'cancel', 'n', max(id), 'op', 'why' FROM queue_tasks \
         RETURNING id, max_task_id",
    )
    .get_result::<Act>(&mut conn)
    .await
    .unwrap();
    assert_eq!(cancel_batch(&mut conn, &act).await, 1);
    assert!(
        pg_task_scheduler::replay(&mut conn, id, "op", "other")
            .await
            .unwrap()
    );
    assert_eq!(cancel_batch(&mut conn, &act).await, 0);
    assert_eq!(count(&db, "n", "queued").await, 1);
    drop(conn);
    db.cleanup().await;
}

/// A cancel and a bulk replay in one caller transaction: the replay is a different action, so
/// it replays the task the cancel just changed.
#[tokio::test]
async fn a_cancel_then_replay_in_one_transaction_replays_the_task() {
    let db = TestDb::new().await;
    insert_many(&db, "a", "queued", "", 1).await;
    let id = TaskId(db.scalar("SELECT id AS n FROM queue_tasks").await);
    let mut conn = db.pool.get().await.unwrap();
    let out = conn
        .transaction::<_, SchedulerError, _>(async |c| {
            assert_eq!(
                store::cancel(c, id, "op", "stop").await?,
                CancelOutcome::Cancelled
            );
            store::replay_matching(
                c,
                &filter("a"),
                &ErrorFilter::Any,
                "op",
                "again",
                BULK_BUDGET,
            )
            .await
        })
        .await
        .unwrap();
    assert_eq!(out.affected, 1);
    assert_eq!(count(&db, "a", "queued").await, 1);
    drop(conn);
    db.cleanup().await;
}

/// Continue runs more batches of the same action: one audit row, whose count covers both calls.
#[tokio::test]
async fn resume_bulk_continues_the_same_action() {
    let db = TestDb::new().await;
    insert_many(&db, "a", "dead", "e", 1100).await;
    let mut conn = db.pool.get().await.unwrap();
    let first = store::replay_matching(
        &mut conn,
        &filter("a"),
        &ErrorFilter::Any,
        "op",
        "why",
        Duration::ZERO,
    )
    .await
    .unwrap();
    assert_eq!(
        (first.affected, first.stopped),
        (500, BulkStop::BudgetSpent)
    );
    let rest = store::resume_bulk(&mut conn, first.action_id, BULK_BUDGET)
        .await
        .unwrap();
    assert_eq!(
        rest,
        Some(BulkOutcome {
            action_id: first.action_id,
            affected: 600,
            stopped: BulkStop::ShortBatch,
        })
    );
    assert_eq!(count(&db, "a", "queued").await, 1100);
    let nz = |n| NonZeroU32::new(n).unwrap();
    let actions = store::admin_actions(&mut conn, None, nz(10), nz(10_000))
        .await
        .unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(
        actions[0].affected,
        Count {
            value: 1100,
            capped: false
        }
    );
    assert_eq!(
        store::resume_bulk(&mut conn, first.action_id + 1, BULK_BUDGET)
            .await
            .unwrap(),
        None
    );
    drop(conn);
    db.cleanup().await;
}

/// An action that changed nothing has no membership rows, and reads as 0, not as missing.
#[tokio::test]
async fn an_empty_replay_reads_zero_affected() {
    let db = TestDb::new().await;
    let mut conn = db.pool.get().await.unwrap();
    let out = store::replay_matching(
        &mut conn,
        &filter("a"),
        &ErrorFilter::Any,
        "op",
        "why",
        BULK_BUDGET,
    )
    .await
    .unwrap();
    assert_eq!((out.affected, out.stopped), (0, BulkStop::ShortBatch));
    let nz = |n| NonZeroU32::new(n).unwrap();
    let actions = store::admin_actions(&mut conn, None, nz(10), nz(10_000))
        .await
        .unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(
        actions[0].affected,
        Count {
            value: 0,
            capped: false
        }
    );
    drop(conn);
    db.cleanup().await;
}

#[tokio::test]
async fn a_failed_wakeup_replays_nothing() {
    let db = TestDb::new().await;
    insert_many(&db, "a", "dead", "e", 1).await;
    let id = db
        .scalar("SELECT id AS n FROM queue_tasks WHERE task_name = 'a'")
        .await;
    let mut conn = db.conn_with_failing_notify().await;
    let r = pg_task_scheduler::replay(&mut conn, pg_task_scheduler::TaskId(id), "op", "why").await;
    assert!(r.is_err(), "{r:?}");
    let r = store::replay_matching(
        &mut conn,
        &filter("a"),
        &ErrorFilter::Any,
        "op",
        "why",
        BULK_BUDGET,
    )
    .await;
    assert!(r.is_err(), "{r:?}");
    assert_eq!(count(&db, "a", "dead").await, 1);
    db.cleanup().await;
}
