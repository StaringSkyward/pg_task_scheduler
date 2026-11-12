//! A toy outbox relay. It lives in the web role and runs on the app pool.

use std::time::Duration;

use diesel::sql_types::{Array, BigInt, Jsonb, Text};
use diesel::{QueryableByName, sql_query};
use diesel_async::{AsyncConnection, RunQueryDsl};
use tokio_util::sync::CancellationToken;

use crate::Pool;

#[derive(QueryableByName)]
struct Pending {
    #[diesel(sql_type = BigInt)]
    id: i64,
    #[diesel(sql_type = Text)]
    command_key: String,
    #[diesel(sql_type = Jsonb)]
    payload: serde_json::Value,
}

/// Every 2 s, in one transaction: lock the pending messages (older than 3 s,
/// so the page shows *pending* for a moment), deliver them, mark them published.
///
/// A real relay publishes to its broker inside this step, BEFORE marking the
/// rows, so a crash re-delivers: delivery is at-least-once and consumers
/// deduplicate by `command_key`. Here "delivery" is a log line.
pub async fn relay(pool: Pool, cancel: CancellationToken) {
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tick.tick() => {}
        }
        if let Err(e) = deliver_pending(&pool).await {
            tracing::warn!("outbox relay tick failed: {e}");
        }
    }
}

async fn deliver_pending(pool: &Pool) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = pool.get().await?;
    conn.transaction::<(), diesel::result::Error, _>(async |conn| {
        let rows = sql_query(
            "SELECT id, command_key, topic, payload FROM queue_outbox
                 WHERE published_at IS NULL AND created_at < now() - interval '3 seconds'
                 ORDER BY id LIMIT 100
                 FOR UPDATE SKIP LOCKED",
        )
        .load::<Pending>(conn)
        .await?;
        for row in &rows {
            tracing::info!(
                "delivered order-placed for order {} (command_key {})",
                row.payload["order_id"],
                row.command_key
            );
        }
        let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
        sql_query("UPDATE queue_outbox SET published_at = now() WHERE id = ANY($1)")
            .bind::<Array<BigInt>, _>(ids)
            .execute(conn)
            .await?;
        Ok(())
    })
    .await?;
    Ok(())
}
