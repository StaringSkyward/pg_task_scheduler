#![allow(dead_code)]
//! Integration-test harness.
//!
//! Each [`TestDb`] gets its own randomly-named Postgres schema on a shared
//! database. Every connection handed out by the pool sets `search_path` to that
//! schema via diesel-async's `ManagerConfig::custom_setup`, so all queries
//! resolve into the per-test schema. This isolates concurrent tests on one
//! database.
//!
//! This is test code: `unwrap`/`expect` are intentional (fail loudly). The one
//! place we must be careful is the `custom_setup` error mapping, which maps a
//! failed `SET search_path` into the connection error type rather than
//! swallowing it.
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use uuid::Uuid;

static COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn database_url() -> String {
    // Load `.env` exactly once. `std::sync::Once` gives a happens-before edge so
    // concurrent tests don't race on env mutation (relevant under edition 2024),
    // and `dotenvy` never overrides a var already set in the real environment, so
    // CI that exports DATABASE_URL still wins.
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let _ = dotenvy::dotenv();
    });
    std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set (via the environment or a .env file) to run integration tests (PostgreSQL 13+)")
}

fn unique_schema() -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("pgts_test_{}_{n}", std::process::id())
}

pub struct TestDb {
    pub pool: Pool<AsyncPgConnection>,
    pub schema: String,
}

impl TestDb {
    pub async fn new() -> Self {
        let url = database_url();
        let schema = unique_schema();

        // One-off admin connection: create the per-test schema.
        let mut admin = AsyncPgConnection::establish(&url).await.expect("connect");
        diesel::sql_query(format!("CREATE SCHEMA \"{schema}\""))
            .execute(&mut admin)
            .await
            .expect("create schema");

        // Build a pool whose every connection pins `search_path` to this schema.
        //
        // diesel-async 0.5.2 `SetupCallback<C>`:
        //   Box<dyn Fn(&str) -> future::BoxFuture<diesel::ConnectionResult<C>> + Send + Sync>
        // i.e. an `Fn` (callable repeatedly, once per established connection),
        // `Send + Sync`, taking the URL by `&str` and returning a `Send` boxed
        // future of `Result<AsyncPgConnection, diesel::result::ConnectionError>`.
        let setup_schema = schema.clone();
        let mut config = ManagerConfig::default();
        config.custom_setup = Box::new(move |url| {
            let schema = setup_schema.clone();
            Box::pin(async move {
                use diesel::ConnectionError;
                let mut conn = AsyncPgConnection::establish(url).await?;
                diesel::sql_query(format!("SET search_path TO \"{schema}\""))
                    .execute(&mut conn)
                    .await
                    // Map the search_path failure into the connection error type
                    // (a `diesel::result::Error` -> `ConnectionError`), so a
                    // broken setup surfaces as a pool/connection error rather
                    // than being silently swallowed.
                    .map_err(ConnectionError::CouldntSetupConfiguration)?;
                Ok(conn)
            })
        });
        let manager =
            AsyncDieselConnectionManager::<AsyncPgConnection>::new_with_config(url, config);
        let pool = Pool::builder(manager).build().expect("pool");

        // Install the complete schema into this test's isolated namespace.
        let up = include_str!("../../migrations/0001_create_scheduler_tables/up.sql");
        let mut conn = pool.get().await.expect("conn");
        conn.batch_execute(up).await.expect("install schema");

        TestDb { pool, schema }
    }

    pub async fn cleanup(&self) {
        if let Ok(mut admin) = AsyncPgConnection::establish(&database_url()).await {
            let _ = diesel::sql_query(format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", self.schema))
                .execute(&mut admin)
                .await;
        }
    }

    /// Insert a job; returns its id. Cron validated by the DB only — pass valid cron.
    pub async fn insert_job(&self, name: &str, cron: &str, next_run_at: DateTime<Utc>) -> Uuid {
        self.insert_job_full(name, cron, next_run_at, "5 minutes", 3, false)
            .await
    }

    pub async fn insert_job_full(
        &self,
        name: &str,
        cron: &str,
        next_run_at: DateTime<Utc>,
        lease_interval: &str,
        max_attempts: i32,
        is_paused: bool,
    ) -> Uuid {
        let mut conn = self.pool.get().await.unwrap();
        diesel::sql_query(
            "INSERT INTO scheduler_jobs (name, cron_expression, next_run_at, lease_duration, max_attempts, is_paused) \
             VALUES ($1,$2,$3,$4::interval,$5,$6) RETURNING id",
        )
        .bind::<diesel::sql_types::Text, _>(name)
        .bind::<diesel::sql_types::Text, _>(cron)
        .bind::<diesel::sql_types::Timestamptz, _>(next_run_at)
        .bind::<diesel::sql_types::Text, _>(lease_interval)
        .bind::<diesel::sql_types::Integer, _>(max_attempts)
        .bind::<diesel::sql_types::Bool, _>(is_paused)
        .get_result::<IdRow>(&mut conn)
        .await
        .unwrap()
        .id
    }

    pub async fn job_next_run_at(&self, job_id: Uuid) -> DateTime<Utc> {
        let mut conn = self.pool.get().await.unwrap();
        diesel::sql_query("SELECT next_run_at FROM scheduler_jobs WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(job_id)
            .get_result::<TsRow>(&mut conn)
            .await
            .unwrap()
            .next_run_at
    }

    pub async fn is_paused(&self, job_id: Uuid) -> bool {
        let mut conn = self.pool.get().await.unwrap();
        diesel::sql_query("SELECT is_paused AS flag FROM scheduler_jobs WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(job_id)
            .get_result::<BoolRow>(&mut conn)
            .await
            .unwrap()
            .flag
    }

    /// All run ids for a job, oldest first.
    pub async fn run_ids(&self, job_id: Uuid) -> Vec<Uuid> {
        let mut conn = self.pool.get().await.unwrap();
        diesel::sql_query("SELECT id FROM scheduler_runs WHERE job_id = $1 ORDER BY scheduled_for")
            .bind::<diesel::sql_types::Uuid, _>(job_id)
            .get_results::<IdRow>(&mut conn)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.id)
            .collect()
    }

    pub async fn force_lease_expired(&self, job_id: Uuid) {
        let mut conn = self.pool.get().await.unwrap();
        diesel::sql_query(
            "UPDATE scheduler_runs SET lease_expires_at = started_at + interval '1 microsecond' \
             WHERE job_id = $1 AND state = 'running'::scheduler_run_state",
        )
        .bind::<diesel::sql_types::Uuid, _>(job_id)
        .execute(&mut conn)
        .await
        .unwrap();
    }

    /// Poll `run_ids` until at least one run has materialized for `job_id`, or
    /// panic if none appears within `timeout`.
    pub async fn run_ids_eventually(&self, job_id: Uuid, timeout: std::time::Duration) -> Uuid {
        let start = std::time::Instant::now();
        loop {
            if let Some(id) = self.run_ids(job_id).await.into_iter().next() {
                return id;
            }
            assert!(start.elapsed() < timeout, "no run materialized in time");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Poll until backend `pid` is blocked waiting on a lock, so a concurrency test
    /// deterministically exercises the contended insert before the holder commits.
    /// Panics on timeout (the backend never blocked).
    pub async fn wait_until_lock_blocked(&self, pid: i32) {
        let step = std::time::Duration::from_millis(20);
        let timeout = std::time::Duration::from_secs(5);
        let mut waited = std::time::Duration::ZERO;
        let mut conn = self.pool.get().await.expect("poll conn");
        loop {
            let n = diesel::sql_query(
                "SELECT count(*) AS n FROM pg_stat_activity \
                 WHERE pid = $1 AND wait_event_type = 'Lock'",
            )
            .bind::<diesel::sql_types::Integer, _>(pid)
            .get_result::<CountRow>(&mut conn)
            .await
            .expect("pg_stat_activity")
            .n;
            if n >= 1 {
                return;
            }
            assert!(waited < timeout, "backend {pid} never blocked on a lock");
            tokio::time::sleep(step).await;
            waited += step;
        }
    }
}

#[derive(diesel::QueryableByName)]
struct IdRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: Uuid,
}
#[derive(diesel::QueryableByName)]
struct TsRow {
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    next_run_at: DateTime<Utc>,
}
#[derive(diesel::QueryableByName)]
struct BoolRow {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    flag: bool,
}

#[derive(diesel::QueryableByName)]
struct PidRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pid: i32,
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

/// The Postgres backend PID serving `conn` (so a test can watch it in pg_stat_activity).
pub async fn backend_pid(conn: &mut AsyncPgConnection) -> i32 {
    diesel::sql_query("SELECT pg_backend_pid() AS pid")
        .get_result::<PidRow>(conn)
        .await
        .expect("pg_backend_pid")
        .pid
}
