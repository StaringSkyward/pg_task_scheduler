//! Task types shared by the web role (which enqueues) and the worker role
//! (which handles), plus the worker's scheduler setup.

use std::num::NonZeroUsize;
use std::time::Duration;

use diesel::sql_types::Integer;
use diesel::sql_types::Text;
use diesel_async::AsyncConnection;
use diesel_async::RunQueryDsl;
use pg_task_scheduler::OutboxCommand;
use pg_task_scheduler::Scheduler;
use pg_task_scheduler::SchedulerError;
use pg_task_scheduler::Task;
use pg_task_scheduler::TaskContext;
use pg_task_scheduler::TaskError;
use pg_task_scheduler::store;
use serde::Deserialize;
use serde::Serialize;
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

pub struct PlaceOrder;

#[derive(Serialize, Deserialize)]
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
    Ok(Scheduler::builder(pool.clone())
        .queue("default", NonZeroUsize::new(4).unwrap())?
        .queue("bulk", NonZeroUsize::new(1).unwrap())?
        // The worker sees an operator's cancel at its next heartbeat and drops
        // the handler, so a short heartbeat makes a cancel visibly stop an import.
        .lease_duration(Duration::from_secs(30))
        .heartbeat_interval(Duration::from_secs(5))
        .register_task::<SendEmail, _, _>(send_email)?
        .register_task::<BulkImport, _, _>(bulk_import)?
        .register_task::<FlakyDemo, _, _>(flaky_demo)?
        .register_task::<Digest, _, _>(digest)?
        .register_task::<PlaceOrder, _, _>(move |ctx, args| place_order(pool.clone(), ctx, args))?
        .build()?)
}

async fn send_email(_ctx: TaskContext, args: EmailArgs) -> Result<(), TaskError> {
    tokio::time::sleep(Duration::from_millis(random_between(1000, 3000))).await;
    tracing::info!("pretend-sent \"{}\" to {}", args.subject, args.to);
    Ok(())
}

const IMPORT_STEPS: u32 = 40;

async fn bulk_import(ctx: TaskContext, args: BulkImportArgs) -> Result<(), TaskError> {
    // A released or replayed import restarts at batch 1. Real long tasks keep
    // their progress somewhere durable, keyed by `task_key`.
    //
    // An operator cancel never fires `ctx.cancellation`: the worker drops this
    // future at its next heartbeat. The `select!` is for shutdown, so the import
    // releases promptly instead of being killed.
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

/// Fails on purpose about 70% of the time, so the demo has retries to show.
async fn flaky_demo(_ctx: TaskContext, _args: FlakyArgs) -> Result<(), TaskError> {
    tokio::time::sleep(Duration::from_millis(500)).await;
    if random_between(1, 100) <= 70 {
        return Err(TaskError::retry(
            "simulated failure (demo: this task fails on purpose)",
        ));
    }
    Ok(())
}

async fn digest(_ctx: TaskContext, _args: DigestArgs) -> Result<(), TaskError> {
    tokio::time::sleep(Duration::from_secs(1)).await;
    tracing::info!("pretend-built digest");
    Ok(())
}

/// What can end `place_order`'s transaction early. Anything but `Crash` is a
/// real error.
#[derive(Debug, thiserror::Error)]
enum PlaceOrderError {
    #[error("simulated crash")]
    Crash,
    #[error(transparent)]
    Db(#[from] diesel::result::Error),
    #[error(transparent)]
    Scheduler(#[from] SchedulerError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(diesel::QueryableByName)]
struct NewOrder {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    id: i64,
}

/// Exactly-once database work: the order row, the outbox message and the task's
/// completion commit in one transaction, so all happen or none do.
///
/// The first attempt crashes on purpose, right after the INSERT. That rolls the
/// row back, the task retries, and the second attempt leaves exactly one row.
/// `demo_orders` deliberately has no unique key on `task_key`: only this
/// transaction keeps it to one row per order.
async fn place_order(pool: Pool, ctx: TaskContext, args: OrderArgs) -> Result<(), TaskError> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| TaskError::retry(e.to_string()))?;
    conn.transaction::<_, PlaceOrderError, _>(async |c| {
        // Lock and validate ownership first; the reaper skips a locked row.
        store::lock_owned(c, &ctx.lease).await?;
        let order = diesel::sql_query(
            "INSERT INTO demo_orders (task_key, amount_cents) VALUES ($1, $2) RETURNING id",
        )
        .bind::<Text, _>(ctx.task_key.as_str())
        .bind::<Integer, _>(args.amount_cents)
        .get_result::<NewOrder>(c)
        .await?;
        if ctx.lease.attempt_no == 1 {
            return Err(PlaceOrderError::Crash);
        }
        let command = OutboxCommand {
            effect_name: "order-placed".into(),
            command_key: serde_json::to_string(&(ctx.task_key.as_str(), "order-placed"))?,
            topic: "orders".into(),
            payload: serde_json::json!({"order_id": order.id, "amount_cents": args.amount_cents}),
        };
        store::complete_with_outbox(c, &ctx.lease, &command).await?;
        Ok(())
    })
    .await
    // Map explicitly: `?` into `TaskError` would make the failure permanent.
    .map_err(|e| match e {
        PlaceOrderError::Crash => {
            TaskError::retry("simulated crash after writing the order (demo: fails on purpose)")
        }
        other => TaskError::retry(other.to_string()),
    })
}
