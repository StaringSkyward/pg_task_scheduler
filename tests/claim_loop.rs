//! Its own test binary: LISTEN/NOTIFY is per database, so other tests' enqueues in a
//! shared binary would wake this worker and inflate the claim count.
mod common;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use common::TestDb;
use diesel_async::SimpleAsyncConnection;
use pg_task_scheduler::EnqueueOptions;
use pg_task_scheduler::Scheduler;
use pg_task_scheduler::Task;
use pg_task_scheduler::TaskKey;
use pg_task_scheduler::enqueue;
use serde_json::json;
use tokio_util::sync::CancellationToken;

struct Job;
impl Task for Job {
    const NAME: &'static str = "job";
    type Args = serde_json::Value;
}

/// The tests here run one at a time: each counts its worker's claims, and the other's
/// enqueue NOTIFY would wake it.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Counts records of `pg_task_scheduler_db_query_seconds{operation="claim"}`. A local
/// recorder sees the worker: `#[tokio::test]` runs every task on the test thread.
#[derive(Clone, Default)]
struct ClaimCounter(Arc<Mutex<u64>>);

impl metrics::HistogramFn for ClaimCounter {
    fn record(&self, _value: f64) {
        *self.0.lock().unwrap() += 1;
    }
}

impl metrics::Recorder for ClaimCounter {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn register_counter(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::noop()
    }
    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(
        &self,
        key: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        let claim = key.name() == "pg_task_scheduler_db_query_seconds"
            && key
                .labels()
                .any(|l| l.key() == "operation" && l.value() == "claim");
        if claim {
            metrics::Histogram::from_arc(Arc::new(self.clone()))
        } else {
            metrics::Histogram::noop()
        }
    }
}

/// A queue with no free permit is left out of the next-due wait, so a saturated queue with
/// overdue tasks does not make the loop poll every `MIN_WAIT`.
#[tokio::test]
async fn saturated_queue_does_not_spin_the_claim_loop() {
    let _serial = SERIAL.lock().await;
    let counter = ClaimCounter::default();
    let _recorder = metrics::set_default_local_recorder(&counter);
    let db = TestDb::new().await;
    {
        let mut conn = db.pool.get().await.unwrap();
        for i in 0..5 {
            let key = TaskKey::try_from(format!("k{i}")).unwrap();
            enqueue::<Job>(&mut conn, key, json!({}), EnqueueOptions::immediate())
                .await
                .unwrap();
        }
    }
    let n1 = std::num::NonZeroUsize::new(1).unwrap();
    let scheduler = Scheduler::builder(db.pool.clone())
        .queue("default", n1)
        .unwrap()
        .queue("bulk", n1)
        .unwrap()
        .max_poll_interval(Duration::from_millis(200))
        .shutdown_timeout(Duration::from_millis(100))
        .register_task::<Job, _, _>(|_ctx, _args| async { std::future::pending().await })
        .unwrap()
        .build()
        .unwrap();
    let cancel = CancellationToken::new();
    let run = tokio::spawn(scheduler.run_until_shutdown(cancel.clone()));

    tokio::time::sleep(Duration::from_secs(1)).await;
    let claims = *counter.0.lock().unwrap();
    assert!(
        claims < 20,
        "claim loop spun: {claims} claim statements in 1 s"
    );

    cancel.cancel();
    run.await.unwrap();
    db.cleanup().await;
}

/// A due task locked by another transaction is skipped by the claim (SKIP LOCKED) but still
/// seen by the next-due query, so the loop must not poll every `MIN_WAIT` for the lock.
#[tokio::test]
async fn locked_due_task_does_not_spin_the_claim_loop() {
    let _serial = SERIAL.lock().await;
    let counter = ClaimCounter::default();
    let _recorder = metrics::set_default_local_recorder(&counter);
    let db = TestDb::new().await;
    let mut locker = db.pool.get().await.unwrap();
    let key = TaskKey::try_from("locked").unwrap();
    enqueue::<Job>(&mut locker, key, json!({}), EnqueueOptions::immediate())
        .await
        .unwrap();
    locker
        .batch_execute("BEGIN; SELECT id FROM queue_tasks WHERE task_key = 'locked' FOR UPDATE")
        .await
        .unwrap();
    let scheduler = Scheduler::builder(db.pool.clone())
        .max_poll_interval(Duration::from_millis(200))
        .shutdown_timeout(Duration::from_millis(100))
        .register_task::<Job, _, _>(|_ctx, _args| async { Ok(()) })
        .unwrap()
        .build()
        .unwrap();
    let cancel = CancellationToken::new();
    let run = tokio::spawn(scheduler.run_until_shutdown(cancel.clone()));

    tokio::time::sleep(Duration::from_secs(1)).await;
    let claims = *counter.0.lock().unwrap();
    assert!(
        claims < 20,
        "claim loop spun on a locked row: {claims} claim statements in 1 s"
    );

    cancel.cancel();
    run.await.unwrap();
    locker.batch_execute("ROLLBACK").await.unwrap();
    drop(locker);
    db.cleanup().await;
}

/// Poisoned rows take no permit, so they must not count toward a full batch: a backlog of
/// them would loop at once and start a dead-letter task (and connection) per batch.
#[tokio::test]
async fn poisoned_backlog_is_dead_lettered_one_batch_at_a_time() {
    let _serial = SERIAL.lock().await;
    let db = TestDb::new().await;
    // Slow dead-letters, so overlapping ones show as several rows running at once.
    db.pool
        .get()
        .await
        .unwrap()
        .batch_execute(
            "CREATE FUNCTION slow_dead() RETURNS trigger AS $$ BEGIN PERFORM pg_sleep(0.1); RETURN NEW; END $$ LANGUAGE plpgsql;
             CREATE TRIGGER slow_dead BEFORE UPDATE ON queue_tasks FOR EACH ROW WHEN (NEW.status = 'dead') EXECUTE FUNCTION slow_dead();
             INSERT INTO queue_tasks (task_key, task_name, payload)
             SELECT 'bad' || i, 'job', '{\"x\":1e500}'::jsonb FROM generate_series(1, 6) AS i;",
        )
        .await
        .unwrap();
    // Raw-SQL rows send no NOTIFY, so only a finished dead-letter (or the 5 s poll) wakes the
    // loop between claims.
    let scheduler = Scheduler::builder(db.pool.clone())
        .claim_batch_size(std::num::NonZeroU32::MIN)
        .max_poll_interval(Duration::from_secs(5))
        .shutdown_timeout(Duration::from_millis(100))
        .register_task::<Job, _, _>(|_ctx, _args| async { Ok(()) })
        .unwrap()
        .build()
        .unwrap();
    let cancel = CancellationToken::new();
    let run = tokio::spawn(scheduler.run_until_shutdown(cancel.clone()));

    let count =
        |status: &str| format!("SELECT count(*) AS n FROM queue_tasks WHERE status = '{status}'");
    let (started, mut max_running) = (std::time::Instant::now(), 0);
    while db.scalar(&count("dead")).await < 6 {
        max_running = max_running.max(db.scalar(&count("running")).await);
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "backlog not dead-lettered"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // A spinning loop leases the whole backlog at once, with a dead-letter task per row.
    assert!(
        max_running <= 1,
        "{max_running} poisoned rows running at once"
    );

    cancel.cancel();
    run.await.unwrap();
    db.cleanup().await;
}
