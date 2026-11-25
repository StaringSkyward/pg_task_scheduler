//! A toy outbox relay. It lives in the web role and runs on the app pool.

use std::time::Duration;

use diesel::QueryableByName;
use diesel::sql_query;
use diesel::sql_types::Array;
use diesel::sql_types::BigInt;
use diesel::sql_types::Text;
use diesel_async::AsyncConnection;
use diesel_async::RunQueryDsl;
use tokio_util::sync::CancellationToken;

use crate::Pool;
use crate::tasks::OrderPlaced;

#[derive(QueryableByName)]
struct Pending {
    #[diesel(sql_type = BigInt)]
    id: i64,
    #[diesel(sql_type = Text)]
    command_key: String,
    #[diesel(sql_type = Text)]
    payload: String,
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
            "SELECT id, command_key, payload::text AS payload FROM queue_outbox
                 WHERE topic = 'orders' AND published_at IS NULL AND created_at < now() - interval '3 seconds'
                   AND NOT EXISTS (SELECT 1 FROM outbox_rejected r WHERE r.outbox_id = queue_outbox.id)
                 ORDER BY id LIMIT 100
                 FOR UPDATE SKIP LOCKED",
        )
        .load::<Pending>(conn)
        .await?;
        let keys: std::collections::HashMap<i64, String> =
            rows.iter().map(|r| (r.id, r.command_key.clone())).collect();
        let (decoded, rejected) = split(rows.into_iter().map(|r| (r.id, r.payload)).collect());
        let mut ids = Vec::new();
        for (id, order) in decoded {
            tracing::info!(
                "delivered order-placed for order {} (command_key {})",
                order.order_id,
                keys.get(&id).map_or("", String::as_str)
            );
            ids.push(id);
        }
        for (id, error) in rejected {
            tracing::warn!("outbox row {id} rejected: {error}");
            sql_query("INSERT INTO outbox_rejected (outbox_id, error) VALUES ($1, $2)")
                .bind::<BigInt, _>(id)
                .bind::<Text, _>(error)
                .execute(conn)
                .await?;
        }
        sql_query("UPDATE queue_outbox SET published_at = now() WHERE id = ANY($1)")
            .bind::<Array<BigInt>, _>(ids)
            .execute(conn)
            .await?;
        Ok(())
    })
    .await?;
    Ok(())
}

/// Separates the rows that decode as `OrderPlaced` from those that do not,
/// with the error text. One bad row never hides a good one.
type Split = (Vec<(i64, OrderPlaced)>, Vec<(i64, String)>);

fn split(rows: Vec<(i64, String)>) -> Split {
    let mut decoded = Vec::new();
    let mut rejected = Vec::new();
    for (id, text) in rows {
        match serde_json::from_str::<OrderPlaced>(&text) {
            Ok(order) => decoded.push((id, order)),
            Err(e) => rejected.push((id, e.to_string())),
        }
    }
    (decoded, rejected)
}

#[cfg(test)]
mod tests {
    use super::split;

    const GOOD: &str = r#"{"order_id": 7, "amount_cents": 5}"#;

    #[test]
    fn a_bad_row_does_not_hide_a_good_one() {
        let (ok, bad) = split(vec![(1, r#"{"x":1}"#.into()), (2, GOOD.into())]);
        assert_eq!(ok.len(), 1);
        assert_eq!((ok[0].0, ok[0].1.order_id), (2, 7));
        assert_eq!(bad.len(), 1);
        assert_eq!(bad[0].0, 1);
        assert!(!bad[0].1.is_empty());
    }

    #[test]
    fn an_out_of_range_number_is_rejected_per_row() {
        let (ok, bad) = split(vec![
            (1, r#"{"order_id": 1e400, "amount_cents": 5}"#.into()),
            (2, GOOD.into()),
        ]);
        assert_eq!(ok.len(), 1);
        assert_eq!(bad.len(), 1);
        assert_eq!(bad[0].0, 1);
    }
}
