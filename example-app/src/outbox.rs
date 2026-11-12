//! A toy outbox relay. It lives in the web role and runs on the app pool.

use std::time::Duration;

use diesel::sql_types::{Jsonb, Text};
use diesel::{QueryableByName, sql_query};
use diesel_async::RunQueryDsl;
use tokio_util::sync::CancellationToken;

use crate::Pool;

#[derive(QueryableByName)]
struct Published {
    #[diesel(sql_type = Text)]
    command_key: String,
    #[diesel(sql_type = Text)]
    topic: String,
    #[diesel(sql_type = Jsonb)]
    payload: serde_json::Value,
}

/// Every 2 s, marks the pending messages (older than 3 s, so the page shows
/// *pending* for a moment) as published and logs them.
///
/// A real relay publishes to its broker BEFORE marking a row published, and
/// consumers deduplicate by `command_key`: delivery is at-least-once. Here
/// "delivery" is a log line.
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
    let rows = sql_query(
        "UPDATE queue_outbox SET published_at = now()
         WHERE id IN (SELECT id FROM queue_outbox
                      WHERE published_at IS NULL AND created_at < now() - interval '3 seconds'
                      ORDER BY id LIMIT 100
                      FOR UPDATE SKIP LOCKED)
         RETURNING command_key, topic, payload",
    )
    .load::<Published>(&mut conn)
    .await?;
    for row in rows {
        tracing::info!(
            "delivered {} order-placed for order {} (command_key {})",
            row.topic,
            row.payload["order_id"],
            row.command_key
        );
    }
    Ok(())
}
