//! The page's "In the database" panel: plain SQL reads of the demo tables. The
//! admin UI never shows these.

use diesel::QueryableByName;
use diesel::sql_query;
use diesel::sql_types::BigInt;
use diesel::sql_types::Int4;
use diesel::sql_types::Nullable;
use diesel::sql_types::Text;
use diesel::sql_types::Timestamptz;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;

pub struct Panel {
    pub order_rows: i64,
    pub orders_done: i64,
    pub order_attempts: i64,
    pub orders: Vec<OrderRow>,
    pub outbox: Vec<OutboxRow>,
}

#[derive(QueryableByName)]
pub struct OrderRow {
    #[diesel(sql_type = BigInt)]
    pub order_id: i64,
    /// `None` when the task has been pruned.
    #[diesel(sql_type = Nullable<BigInt>)]
    pub task_id: Option<i64>,
    #[diesel(sql_type = Nullable<Int4>)]
    pub attempts: Option<i32>,
}

#[derive(QueryableByName)]
pub struct OutboxRow {
    #[diesel(sql_type = Text)]
    pub topic: String,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub order_id: Option<i64>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    pub published_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(QueryableByName)]
struct Counts {
    #[diesel(sql_type = BigInt)]
    order_rows: i64,
    #[diesel(sql_type = BigInt)]
    orders_done: i64,
    #[diesel(sql_type = BigInt)]
    order_attempts: i64,
}

pub async fn load(conn: &mut AsyncPgConnection) -> Result<Panel, diesel::result::Error> {
    let counts = sql_query(
        "SELECT (SELECT count(*) FROM demo_orders) AS order_rows,
                count(*) AS orders_done,
                COALESCE(sum(attempts), 0)::bigint AS order_attempts
         FROM queue_tasks WHERE task_name = 'place-order' AND status = 'done'",
    )
    .get_result::<Counts>(conn)
    .await?;
    let orders = sql_query(
        "SELECT o.id AS order_id, t.id AS task_id, t.attempts
         FROM demo_orders o LEFT JOIN queue_tasks t ON t.task_key = o.task_key
         ORDER BY o.id DESC LIMIT 10",
    )
    .load::<OrderRow>(conn)
    .await?;
    let outbox = sql_query(
        "SELECT topic, (payload->>'order_id')::bigint AS order_id, published_at
         FROM queue_outbox ORDER BY id DESC LIMIT 10",
    )
    .load::<OutboxRow>(conn)
    .await?;
    Ok(Panel {
        order_rows: counts.order_rows,
        orders_done: counts.orders_done,
        order_attempts: counts.order_attempts,
        orders,
        outbox,
    })
}
