mod tasks;
mod web;

use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use tokio_util::sync::CancellationToken;

pub type Pool = diesel_async::pooled_connection::deadpool::Pool<AsyncPgConnection>;

fn pool(url: &str, max_size: usize) -> Pool {
    Pool::builder(AsyncDieselConnectionManager::<AsyncPgConnection>::new(url))
        .max_size(max_size)
        .build()
        .expect("building the connection pool")
}

fn required_env(name: &str) -> String {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v,
        _ => {
            eprintln!(
                "{name} is not set: copy example-app/.env.example to .env and set it (see example-app/README.md)"
            );
            std::process::exit(1);
        }
    }
}

/// Resolves on SIGTERM (what `docker stop` sends to PID 1) or Ctrl-C.
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("installing the SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    match std::env::args().nth(1).as_deref() {
        Some("worker") => {
            let scheduler = tasks::scheduler(pool(&required_env("DATABASE_URL"), 12))?;
            let token = CancellationToken::new();
            tokio::spawn({
                let token = token.clone();
                async move {
                    shutdown_signal().await;
                    token.cancel();
                }
            });
            tracing::info!("worker started: queues default(4), bulk(1)");
            scheduler.run_until_shutdown(token).await?;
            tracing::info!("worker stopped cleanly");
            Ok(())
        }
        Some("web") => {
            let url = required_env("DATABASE_URL");
            web::run(
                pool(&url, 4),
                pool(&url, 4),
                required_env("ADMIN_USER"),
                required_env("ADMIN_PASSWORD"),
            )
            .await
        }
        _ => {
            eprintln!("usage: example-app <worker|web>");
            std::process::exit(2);
        }
    }
}
