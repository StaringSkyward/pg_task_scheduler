//! Bounded reads for pages and the JSON API.
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use diesel::sql_types::Text;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use pg_task_scheduler::{SchedulerError, SchedulerPool};

/// `SET LOCAL statement_timeout`, with the value bound.
const SET_LOCAL_TIMEOUT: &str = "SELECT set_config('statement_timeout', $1, true)";

/// A future borrowing the connection for `'c`.
pub type ConnFuture<'c, T> = Pin<Box<dyn Future<Output = Result<T, SchedulerError>> + Send + 'c>>;

/// Run `f` in a transaction whose first statement sets `statement_timeout`, so no
/// read can hold a connection longer than `timeout`. The setting ends with the
/// transaction. Actions do not use this: they run in their own statements.
pub async fn read<P, T, F>(pool: &P, timeout: Duration, f: F) -> Result<T, SchedulerError>
where
    P: SchedulerPool,
    T: Send,
    F: for<'c> FnOnce(&'c mut AsyncPgConnection) -> ConnFuture<'c, T> + Send,
{
    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| SchedulerError::Pool(e.to_string()))?;
    conn.transaction(async move |c| {
        diesel::sql_query(SET_LOCAL_TIMEOUT)
            .bind::<Text, _>(timeout.as_millis().to_string())
            .execute(c)
            .await?;
        f(c).await
    })
    .await
}
