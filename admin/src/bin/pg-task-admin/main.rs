//! Standalone admin server: `pg-task-admin --config <path>` or `pg-task-admin hash-password`.
mod config;

use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::deadpool::Pool;
use pg_task_scheduler_admin::auth::{self, with_basic_auth, with_trusted_header};
use pg_task_scheduler_admin::router;
use tracing_subscriber::EnvFilter;

use config::Auth;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("pg-task-admin: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["hash-password"] => {
            let mut line = String::new();
            std::io::stdin().read_line(&mut line)?;
            println!(
                "{}",
                auth::hash_password(line.trim_end_matches(['\r', '\n']))
            );
            Ok(())
        }
        ["--config", path] => serve(config::parse(&std::fs::read_to_string(path)?)?).await,
        _ => Err("usage: pg-task-admin --config <path> | pg-task-admin hash-password".into()),
    }
}

async fn serve(cfg: config::StandaloneConfig) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&cfg.database_url);
    let pool = Pool::builder(manager).max_size(cfg.pool_size).build()?;
    let app = router(pool, cfg.admin_config());
    let app = match cfg.auth {
        Auth::Basic(users) => {
            if !cfg.bind.ip().is_loopback() {
                tracing::warn!("basic auth over plain HTTP: terminate TLS in front of this server");
            }
            with_basic_auth(app, users)?
        }
        Auth::TrustedHeader(h) => with_trusted_header(app, h.parse()?),
    };
    let listener = tokio::net::TcpListener::bind(cfg.bind).await?;
    tracing::info!("listening on {}", cfg.bind);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await?;
    Ok(())
}

async fn shutdown() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
