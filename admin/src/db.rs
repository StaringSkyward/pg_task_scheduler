//! Bounded reads for pages and the JSON API.
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use diesel::sql_types::Text;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use pg_task_scheduler::SchedulerError;
use pg_task_scheduler::SchedulerPool;

/// `SET LOCAL` of `statement_timeout` and `idle_in_transaction_session_timeout`, with the
/// value bound.
const SET_LOCAL_TIMEOUT: &str = "SELECT set_config('statement_timeout', $1, true), \
     set_config('idle_in_transaction_session_timeout', $1, true)";

/// `timeout` as whole milliseconds for PostgreSQL, rounded up and capped at its
/// `i32::MAX` limit. `"0"` (no timeout) only for `Duration::ZERO`.
fn timeout_ms(timeout: Duration) -> String {
    const MAX_MS: u128 = 2_147_483_647;
    timeout
        .as_nanos()
        .div_ceil(1_000_000)
        .min(MAX_MS)
        .to_string()
}

/// A future borrowing the connection for `'c`.
pub type ConnFuture<'c, T> = Pin<Box<dyn Future<Output = Result<T, SchedulerError>> + Send + 'c>>;

/// Run `f` in a transaction whose first statement sets `statement_timeout`, so no
/// statement runs longer than `timeout`, and `idle_in_transaction_session_timeout` to
/// the same value: if the future is dropped mid-read (client gone), diesel-async
/// leaves the transaction open on the pooled connection, and the server then ends
/// that session after `timeout` idle instead of letting it hold its locks until the
/// next checkout. Zero means no timeout; a positive `timeout` rounds up to whole
/// milliseconds. Both settings end with the transaction. Actions do not use this:
/// they run in their own statements.
pub async fn read<P, T, F>(pool: &P, timeout: Duration, f: F) -> Result<T, SchedulerError>
where
    P: SchedulerPool,
    T: Send,
    F: for<'c> FnOnce(&'c mut AsyncPgConnection) -> ConnFuture<'c, T> + Send,
{
    let mut conn = pool.acquire().await.map_err(SchedulerError::Pool)?;
    conn.transaction(async move |c| {
        diesel::sql_query(SET_LOCAL_TIMEOUT)
            .bind::<Text, _>(timeout_ms(timeout))
            .execute(c)
            .await?;
        f(c).await
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_ms_rounds_up_and_keeps_zero() {
        assert_eq!(timeout_ms(Duration::ZERO), "0");
        assert_eq!(timeout_ms(Duration::from_micros(500)), "1");
        assert_eq!(timeout_ms(Duration::from_micros(1500)), "2");
        assert_eq!(timeout_ms(Duration::MAX), "2147483647");
    }
}
