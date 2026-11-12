//! Task types shared by the web role (which enqueues) and the worker role
//! (which handles), plus the worker's scheduler setup.

use std::num::NonZeroUsize;
use std::time::Duration;

use pg_task_scheduler::{Scheduler, Task, TaskError};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Pool;

pub struct SendEmail;

#[derive(Serialize, Deserialize)]
pub struct EmailArgs {
    pub to: String,
    pub subject: String,
}

impl Task for SendEmail {
    const NAME: &'static str = "send-email";
    type Args = EmailArgs;
}

pub struct BulkImport;

#[derive(Serialize, Deserialize)]
pub struct BulkImportArgs {
    pub rows: u32,
}

impl Task for BulkImport {
    const NAME: &'static str = "bulk-import";
    const QUEUE: &'static str = "bulk";
    type Args = BulkImportArgs;
}

/// Deliberately fails about 70% of attempts, so a batch of 10 almost always
/// leaves one dead.
pub struct FlakyDemo;

#[derive(Serialize, Deserialize)]
pub struct FlakyArgs {}

impl Task for FlakyDemo {
    const NAME: &'static str = "flaky-demo";
    type Args = FlakyArgs;
}

pub struct Digest;

#[derive(Serialize, Deserialize)]
pub struct DigestArgs {}

impl Task for Digest {
    const NAME: &'static str = "digest";
    type Args = DigestArgs;
}

// Unused until the place-order handler and the web role land.
#[allow(dead_code)]
pub struct PlaceOrder;

#[derive(Serialize, Deserialize)]
#[allow(dead_code)]
pub struct OrderArgs {
    pub amount_cents: i32,
}

impl Task for PlaceOrder {
    const NAME: &'static str = "place-order";
    type Args = OrderArgs;
}

/// A number in `min..=max`, from a v4 UUID's random bits.
pub fn random_between(min: u64, max: u64) -> u64 {
    min + (Uuid::new_v4().as_u128() % u128::from(max - min + 1)) as u64
}

/// The worker role's scheduler: queues, intervals and handlers.
pub fn scheduler(pool: Pool) -> Result<Scheduler<Pool>, Box<dyn std::error::Error>> {
    Ok(Scheduler::builder(pool)
        .queue("default", NonZeroUsize::new(4).unwrap())?
        .queue("bulk", NonZeroUsize::new(1).unwrap())?
        // A handler sees an operator's cancel at its next heartbeat, so a short
        // heartbeat makes cancelling a running import visibly stop it.
        .lease_duration(Duration::from_secs(30))
        .heartbeat_interval(Duration::from_secs(5))
        .register_task::<SendEmail, _, _>(send_email)?
        .register_task::<BulkImport, _, _>(bulk_import)?
        .register_task::<FlakyDemo, _, _>(flaky_demo)?
        .register_task::<Digest, _, _>(digest)?
        .build()?)
}

async fn send_email(
    _ctx: pg_task_scheduler::TaskContext,
    args: EmailArgs,
) -> Result<(), TaskError> {
    tokio::time::sleep(Duration::from_millis(random_between(1000, 3000))).await;
    tracing::info!("pretend-sent \"{}\" to {}", args.subject, args.to);
    Ok(())
}

const IMPORT_STEPS: u32 = 40;

async fn bulk_import(
    ctx: pg_task_scheduler::TaskContext,
    args: BulkImportArgs,
) -> Result<(), TaskError> {
    // A released or replayed import restarts at batch 1. Real long tasks keep
    // their progress somewhere durable, keyed by `task_key`.
    for i in 1..=IMPORT_STEPS {
        tokio::select! {
            _ = ctx.cancellation.cancelled() => return Err(TaskError::interrupted()),
            _ = tokio::time::sleep(Duration::from_secs(4)) => {}
        }
        let rows_so_far = u64::from(args.rows) * u64::from(i) / u64::from(IMPORT_STEPS);
        tracing::info!(
            "bulk-import {}: batch {i}/{IMPORT_STEPS} ({rows_so_far}/{} rows)",
            ctx.task_key.as_str(),
            args.rows
        );
    }
    Ok(())
}

async fn flaky_demo(
    _ctx: pg_task_scheduler::TaskContext,
    _args: FlakyArgs,
) -> Result<(), TaskError> {
    tokio::time::sleep(Duration::from_millis(500)).await;
    if random_between(1, 100) <= 70 {
        return Err(TaskError::retry(
            "simulated failure (demo: this task fails on purpose)",
        ));
    }
    Ok(())
}

async fn digest(_ctx: pg_task_scheduler::TaskContext, _args: DigestArgs) -> Result<(), TaskError> {
    tokio::time::sleep(Duration::from_secs(1)).await;
    tracing::info!("pretend-built digest");
    Ok(())
}
