mod common;

use std::num::NonZeroUsize;
use std::time::Duration;
use std::time::Instant;

use common::TestDb;
use pg_task_scheduler::EnqueueOptions;
use pg_task_scheduler::QueueName;
use pg_task_scheduler::Scheduler;
use pg_task_scheduler::Task;
use pg_task_scheduler::TaskKey;
use pg_task_scheduler::WorkerId;
use pg_task_scheduler::enqueue;
use pg_task_scheduler::store::AdminActionKind;
use pg_task_scheduler::store::{self};
use serde_json::json;
use tokio_util::sync::CancellationToken;

struct Job;
impl Task for Job {
    const NAME: &'static str = "job";
    type Args = serde_json::Value;
}

fn q(name: &str) -> QueueName {
    QueueName::try_from(name).unwrap()
}

async fn add(db: &TestDb, key: &str, queue: &str) {
    let mut conn = db.pool.get().await.unwrap();
    let opts = EnqueueOptions::immediate().queue(q(queue));
    enqueue::<Job>(&mut conn, TaskKey::try_from(key).unwrap(), json!({}), opts)
        .await
        .unwrap();
}

async fn claim(db: &TestDb, queue: &str) -> Vec<pg_task_scheduler::ClaimedTask> {
    let mut conn = db.pool.get().await.unwrap();
    store::claim(
        &mut conn,
        &WorkerId::generate(),
        &q(queue),
        &["job".to_owned()],
        NonZeroUsize::new(5).unwrap(),
        Duration::from_secs(60),
    )
    .await
    .unwrap()
}

async fn pause(db: &TestDb, queue: &str) -> bool {
    let mut conn = db.pool.get().await.unwrap();
    store::pause_queue(&mut conn, &q(queue), "op", "why")
        .await
        .unwrap()
}

async fn resume(db: &TestDb, queue: &str) -> bool {
    let mut conn = db.pool.get().await.unwrap();
    store::resume_queue(&mut conn, &q(queue), "op", "done")
        .await
        .unwrap()
}

#[tokio::test]
async fn paused_queue_is_not_claimed() {
    let db = TestDb::new().await;
    add(&db, "a", "default").await;
    assert!(pause(&db, "default").await);
    assert!(claim(&db, "default").await.is_empty());
    assert!(resume(&db, "default").await);
    assert_eq!(claim(&db, "default").await.len(), 1);
    db.cleanup().await;
}

#[tokio::test]
async fn running_task_completes_while_paused() {
    let db = TestDb::new().await;
    add(&db, "a", "default").await;
    let claimed = claim(&db, "default").await;
    pause(&db, "default").await;
    let mut conn = db.pool.get().await.unwrap();
    store::complete(&mut conn, &claimed[0].lease).await.unwrap();
    db.cleanup().await;
}

#[tokio::test]
async fn retried_task_waits_while_paused() {
    let db = TestDb::new().await;
    add(&db, "a", "default").await;
    let claimed = claim(&db, "default").await;
    pause(&db, "default").await;
    let mut conn = db.pool.get().await.unwrap();
    store::retry(&mut conn, &claimed[0].lease, "boom", Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(
        db.scalar("SELECT count(*) AS n FROM queue_tasks WHERE status = 'queued'")
            .await,
        1
    );
    assert!(claim(&db, "default").await.is_empty());
    db.cleanup().await;
}

#[tokio::test]
async fn next_due_in_ignores_paused_queues() {
    let db = TestDb::new().await;
    add(&db, "a", "bulk").await;
    let mut conn = db.pool.get().await.unwrap();
    let names = ["job".to_owned()];
    assert!(
        store::next_due_in(&mut conn, &[q("bulk")], &names)
            .await
            .unwrap()
            .is_some()
    );
    pause(&db, "bulk").await;
    assert!(
        store::next_due_in(&mut conn, &[q("bulk")], &names)
            .await
            .unwrap()
            .is_none()
    );
    db.cleanup().await;
}

#[tokio::test]
async fn pause_is_idempotent_and_audited() {
    let db = TestDb::new().await;
    assert!(pause(&db, "bulk").await);
    assert!(!pause(&db, "bulk").await);
    let mut conn = db.pool.get().await.unwrap();
    let paused = store::paused_queues(&mut conn).await.unwrap();
    assert_eq!(paused.len(), 1);
    assert_eq!(paused[0].queue, q("bulk"));
    assert_eq!(
        (paused[0].operator.as_str(), paused[0].reason.as_str()),
        ("op", "why")
    );
    drop(conn);
    assert!(resume(&db, "bulk").await);
    assert!(!resume(&db, "bulk").await);
    let mut conn = db.pool.get().await.unwrap();
    assert!(store::paused_queues(&mut conn).await.unwrap().is_empty());
    let actions = store::admin_actions(&mut conn, None, 10).await.unwrap();
    let kinds: Vec<_> = actions.iter().map(|a| a.action).collect();
    assert_eq!(kinds, [AdminActionKind::Resume, AdminActionKind::Pause]);
    assert!(
        actions
            .iter()
            .all(|a| a.affected == 0 && a.queue == Some(q("bulk")))
    );
    db.cleanup().await;
}

#[tokio::test]
async fn worker_resumes_promptly() {
    let db = TestDb::new().await;
    let scheduler = Scheduler::builder(db.pool.clone())
        .max_poll_interval(Duration::from_secs(5))
        .shutdown_timeout(Duration::from_secs(1))
        .register_task::<Job, _, _>(|_ctx, _args| async { Ok(()) })
        .unwrap()
        .build()
        .unwrap();
    let cancel = CancellationToken::new();
    let run = tokio::spawn(scheduler.run_until_shutdown(cancel.clone()));
    pause(&db, "default").await;
    add(&db, "a", "default").await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let done = "SELECT count(*) AS n FROM queue_tasks WHERE status = 'done'";
    assert_eq!(db.scalar(done).await, 0);

    resume(&db, "default").await;
    let start = Instant::now();
    while db.scalar(done).await == 0 {
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "resume did not wake the worker within 2 s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cancel.cancel();
    run.await.unwrap().unwrap();
    db.cleanup().await;
}
