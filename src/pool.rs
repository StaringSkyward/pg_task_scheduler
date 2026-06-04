use diesel_async::AsyncPgConnection;
use std::ops::DerefMut;

/// Opaque pool error type — wraps any pool error without losing the source.
pub type PoolError = Box<dyn std::error::Error + Send + Sync>;

/// A source of pooled `AsyncPgConnection`s.
///
/// The method is named `acquire` (not `get`) to avoid colliding with the
/// inherent `get` methods on concrete pool types (e.g. deadpool's
/// `Pool::get`).
pub trait SchedulerPool: Clone + Send + Sync + 'static {
    /// The guard / smart pointer returned by the pool. Must deref to
    /// `AsyncPgConnection` so callers can pass `&mut *conn` to store fns.
    type Conn: DerefMut<Target = AsyncPgConnection> + Send;

    fn acquire(&self) -> impl std::future::Future<Output = Result<Self::Conn, PoolError>> + Send;
}

#[cfg(feature = "deadpool")]
mod deadpool_impl {
    use super::*;
    use diesel_async::pooled_connection::deadpool::{Object, Pool};

    impl SchedulerPool for Pool<AsyncPgConnection> {
        type Conn = Object<AsyncPgConnection>;

        fn acquire(
            &self,
        ) -> impl std::future::Future<Output = Result<Self::Conn, PoolError>> + Send {
            let pool = self.clone();
            async move { pool.get().await.map_err(|e| Box::new(e) as PoolError) }
        }
    }
}

#[cfg(feature = "bb8")]
mod bb8_impl {
    use super::*;
    use diesel_async::pooled_connection::bb8::{Pool, PooledConnection};

    impl SchedulerPool for Pool<AsyncPgConnection> {
        type Conn = PooledConnection<'static, AsyncPgConnection>;

        fn acquire(
            &self,
        ) -> impl std::future::Future<Output = Result<Self::Conn, PoolError>> + Send {
            let pool = self.clone();
            async move { pool.get_owned().await.map_err(|e| Box::new(e) as PoolError) }
        }
    }
}

#[cfg(feature = "mobc")]
mod mobc_impl {
    use super::*;
    use diesel_async::pooled_connection::mobc::{Pool, PooledConnection};

    impl SchedulerPool for Pool<AsyncPgConnection> {
        type Conn = PooledConnection<AsyncPgConnection>;

        fn acquire(
            &self,
        ) -> impl std::future::Future<Output = Result<Self::Conn, PoolError>> + Send {
            let pool = self.clone();
            async move { pool.get().await.map_err(|e| Box::new(e) as PoolError) }
        }
    }
}
