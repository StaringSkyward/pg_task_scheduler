mod common;

use std::num::NonZeroUsize;
use std::time::Duration;

use common::TestDb;
use diesel_async::SimpleAsyncConnection;
use pg_task_scheduler::store::{self, AdminActionKind, FailedFilter, FailedStatus};
use pg_task_scheduler::{AttemptOutcome, QueueName, TaskId, TaskName, TaskStatus, WorkerId};

async fn sql(db: &TestDb, stmt: &str) {
    let mut conn = db.pool.get().await.unwrap();
    conn.batch_execute(stmt).await.unwrap();
}

async fn id_of(db: &TestDb, key: &str) -> TaskId {
    TaskId(
        db.scalar(&format!(
            "SELECT id AS n FROM queue_tasks WHERE task_key = '{key}'"
        ))
        .await,
    )
}

/// Collect ids across pages, failing (not hanging) if the cursor never ends.
macro_rules! walk {
    ($cursor:ident => $call:expr) => {{
        let mut seen = Vec::new();
        let mut next = None;
        let mut finished = false;
        for _ in 0..20 {
            let page = {
                let $cursor = next.as_ref();
                $call.await.unwrap()
            };
            seen.extend(ids(&page.items));
            next = page.next;
            if next.is_none() {
                finished = true;
                break;
            }
        }
        assert!(finished, "cursor still Some after 20 pages; seen {seen:?}");
        seen
    }};
}

fn ids(rows: &[store::TaskRow]) -> Vec<TaskId> {
    rows.iter().map(|r| r.id).collect()
}

#[tokio::test]
async fn queued_page_matches_claim_order_across_segments() {
    let db = TestDb::new().await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, priority, due_at) VALUES \
         ('p0', 'job', 0, now() - interval '5 min'), \
         ('p10new', 'job', 10, now() - interval '1 min'), \
         ('p10old', 'job', 10, now() - interval '3 min'), \
         ('late', 'job', 0, now() + interval '2 hour'), \
         ('soon', 'job', 5, now() + interval '1 hour')",
    )
    .await;
    let want = [
        id_of(&db, "p10old").await,
        id_of(&db, "p10new").await,
        id_of(&db, "p0").await,
        id_of(&db, "soon").await,
        id_of(&db, "late").await,
    ];
    let mut conn = db.pool.get().await.unwrap();
    let q = QueueName::default();
    let p1 = store::queued_page(&mut conn, &q, None, 2).await.unwrap();
    assert_eq!(ids(&p1.items), want[0..2]);
    let c1 = p1.next.unwrap();
    assert!(c1.due);
    assert_eq!(c1.position, 2);
    // The second page straddles both segments.
    let p2 = store::queued_page(&mut conn, &q, Some(&c1), 2)
        .await
        .unwrap();
    assert_eq!(ids(&p2.items), want[2..4]);
    let c2 = p2.next.unwrap();
    assert!(!c2.due);
    assert_eq!(c2.position, 4);
    let p3 = store::queued_page(&mut conn, &q, Some(&c2), 2)
        .await
        .unwrap();
    assert_eq!(ids(&p3.items), want[4..]);
    assert!(p3.next.is_none());
    // An exactly-full last page has no next either.
    let all = store::queued_page(&mut conn, &q, None, 5).await.unwrap();
    assert_eq!(ids(&all.items), want);
    assert!(all.next.is_none());
    let other = store::queued_page(&mut conn, &QueueName::try_from("other").unwrap(), None, 5)
        .await
        .unwrap();
    assert!(other.items.is_empty() && other.next.is_none());
    db.cleanup().await;
}

#[tokio::test]
async fn running_page_orders_by_lease_expiry() {
    let db = TestDb::new().await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, status, locked_by, locked_until) VALUES \
         ('late', 'job', 'running', 'w', now() + interval '10 min'), \
         ('soon', 'job', 'running', 'w', now() + interval '1 min'), \
         ('mid', 'job', 'running', 'w', now() + interval '5 min'), \
         ('queued', 'job', 'queued', NULL, NULL)",
    )
    .await;
    let mut conn = db.pool.get().await.unwrap();
    let p1 = store::running_page(&mut conn, None, 2).await.unwrap();
    assert_eq!(
        ids(&p1.items),
        [id_of(&db, "soon").await, id_of(&db, "mid").await]
    );
    let p2 = store::running_page(&mut conn, p1.next.as_ref(), 2)
        .await
        .unwrap();
    assert_eq!(ids(&p2.items), [id_of(&db, "late").await]);
    assert!(p2.next.is_none());
    db.cleanup().await;
}

#[tokio::test]
async fn done_and_failed_pages_newest_first_with_filters() {
    let db = TestDb::new().await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, status, last_error, completed_at) VALUES \
         ('d1', 'a', 'done', NULL, now() - interval '3 min'), \
         ('d2', 'a', 'done', NULL, now() - interval '1 min'), \
         ('dead1', 'a', 'dead', 'x', now() - interval '4 min'), \
         ('dead2', 'b', 'dead', 'x', now() - interval '3 min'), \
         ('dead3', 'a', 'dead', 'y', now() - interval '2 min'), \
         ('dead4', 'a', 'dead', 'x', now() - interval '1 min'), \
         ('canc', 'a', 'cancelled', NULL, now())",
    )
    .await;
    let mut conn = db.pool.get().await.unwrap();
    let p1 = store::done_page(&mut conn, None, 1).await.unwrap();
    assert_eq!(ids(&p1.items), [id_of(&db, "d2").await]);
    let p2 = store::done_page(&mut conn, p1.next.as_ref(), 1)
        .await
        .unwrap();
    assert_eq!(ids(&p2.items), [id_of(&db, "d1").await]);
    assert!(p2.next.is_none());
    assert_eq!(p2.items[0].status, TaskStatus::Done);

    let f = FailedFilter {
        task_name: Some(TaskName::try_from("a").unwrap()),
        last_error: Some("x".into()),
    };
    let p = store::failed_page(&mut conn, FailedStatus::Dead, &f, None, 10)
        .await
        .unwrap();
    assert_eq!(
        ids(&p.items),
        [id_of(&db, "dead4").await, id_of(&db, "dead1").await]
    );
    assert!(p.next.is_none());
    // Unfiltered, paged: newest first, then the cursor continues.
    let none = FailedFilter::default();
    let p1 = store::failed_page(&mut conn, FailedStatus::Dead, &none, None, 3)
        .await
        .unwrap();
    assert_eq!(p1.items.len(), 3);
    let p2 = store::failed_page(&mut conn, FailedStatus::Dead, &none, p1.next.as_ref(), 3)
        .await
        .unwrap();
    assert_eq!(ids(&p2.items), [id_of(&db, "dead1").await]);
    assert!(p2.next.is_none());
    let c = store::failed_page(&mut conn, FailedStatus::Cancelled, &none, None, 10)
        .await
        .unwrap();
    assert_eq!(ids(&c.items), [id_of(&db, "canc").await]);
    db.cleanup().await;
}

#[tokio::test]
async fn task_detail_has_attempts_and_actions() {
    let db = TestDb::new().await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name) VALUES ('t', 'job')",
    )
    .await;
    let id = id_of(&db, "t").await;
    let mut conn = db.pool.get().await.unwrap();
    let claimed = store::claim(
        &mut conn,
        &WorkerId::generate(),
        &QueueName::default(),
        &["job".to_owned()],
        NonZeroUsize::new(1).unwrap(),
        Duration::from_secs(60),
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    store::retry(&mut conn, &claimed.lease, "boom", Duration::from_secs(3600))
        .await
        .unwrap();
    store::cancel(&mut conn, id, "ops", "why").await.unwrap();

    let d = store::task_detail(&mut conn, id).await.unwrap().unwrap();
    assert_eq!(d.task.status, TaskStatus::Cancelled);
    assert_eq!(d.attempts.len(), 1);
    assert_eq!(d.attempts[0].outcome, Some(AttemptOutcome::Retry));
    assert_eq!(d.actions[0].action, AdminActionKind::Cancel);
    assert!(
        store::task_detail(&mut conn, TaskId(id.0 + 1000))
            .await
            .unwrap()
            .is_none()
    );
    db.cleanup().await;
}

/// Page boundaries fall inside a same-priority group: the `priority = p` keyset branch.
#[tokio::test]
async fn queued_page_resumes_inside_equal_priority_group() {
    let db = TestDb::new().await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, priority, due_at) VALUES \
         ('a', 'job', 10, now() - interval '5 min'), \
         ('b', 'job', 10, now() - interval '4 min'), \
         ('c', 'job', 10, now() - interval '3 min'), \
         ('d', 'job', 10, now() - interval '2 min'), \
         ('low', 'job', 0, now() - interval '9 min')",
    )
    .await;
    let want = [
        id_of(&db, "a").await,
        id_of(&db, "b").await,
        id_of(&db, "c").await,
        id_of(&db, "d").await,
        id_of(&db, "low").await,
    ];
    let mut conn = db.pool.get().await.unwrap();
    let q = QueueName::default();
    let seen = walk!(cursor => store::queued_page(&mut conn, &q, cursor, 1));
    assert_eq!(seen, want);
    db.cleanup().await;
}

/// Equal priority and due_at: the id tiebreak decides, and no row is skipped or repeated.
#[tokio::test]
async fn queued_page_breaks_ties_by_id() {
    let db = TestDb::new().await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, priority, due_at) \
         SELECT 't' || g, 'job', 10, '2020-01-01' FROM generate_series(1, 5) g; \
         INSERT INTO queue_tasks (task_key, task_name, due_at) \
         SELECT 'f' || g, 'job', '2999-01-01' FROM generate_series(1, 3) g",
    )
    .await;
    let mut conn = db.pool.get().await.unwrap();
    let q = QueueName::default();
    let seen = walk!(cursor => store::queued_page(&mut conn, &q, cursor, 2));
    let mut sorted = seen.clone();
    sorted.sort_by_key(|i| i.0);
    sorted.dedup();
    assert_eq!(seen.len(), 8);
    // Due group (ids ascending), then the future group (ids ascending).
    assert_eq!(seen, sorted);
    db.cleanup().await;
}

#[tokio::test]
async fn done_and_failed_pages_break_completed_at_ties_by_id() {
    let db = TestDb::new().await;
    sql(
        &db,
        "INSERT INTO queue_tasks (task_key, task_name, status, completed_at) \
         SELECT 'd' || g, 'a', 'done', '2020-01-01' FROM generate_series(1, 5) g; \
         INSERT INTO queue_tasks (task_key, task_name, status, completed_at) \
         SELECT 'x' || g, 'a', 'dead', '2020-01-01' FROM generate_series(1, 5) g",
    )
    .await;
    let mut conn = db.pool.get().await.unwrap();
    let done = walk!(cursor => store::done_page(&mut conn, cursor, 2));
    let f = FailedFilter {
        task_name: Some(TaskName::try_from("a").unwrap()),
        last_error: None,
    };
    let dead = walk!(cursor => store::failed_page(&mut conn, FailedStatus::Dead, &f, cursor, 2));
    for seen in [done, dead] {
        let mut want = seen.clone();
        want.sort_by_key(|i| std::cmp::Reverse(i.0));
        want.dedup();
        assert_eq!(seen.len(), 5);
        assert_eq!(seen, want);
    }
    db.cleanup().await;
}
