//! Its own test binary: LISTEN/NOTIFY is per database, so other tests' enqueues in a
//! shared binary would wake this worker and inflate the claim count.
mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::TestDb;
use diesel_async::SimpleAsyncConnection;
use pg_task_scheduler::{EnqueueOptions, Scheduler, Task, TaskKey, enqueue};
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
    run.await.unwrap().unwrap();
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
    run.await.unwrap().unwrap();
    locker.batch_execute("ROLLBACK").await.unwrap();
    drop(locker);
    db.cleanup().await;
}
