mod handler;
mod pages;
mod rate_limit;
mod registry;

use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{Router, extract::DefaultBodyLimit, routing::get};
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use handler::{public_handler, ws_handler};
use rate_limit::RateLimiter;
use registry::Registry;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let port = env_var("PORT", "").unwrap_or_else(|| "8080".into());
    let domain = env_var("PEEK_DOMAIN", "RELAY_DOMAIN").unwrap_or_else(|| "localhost".into());
    let auth_token = env_var("PEEK_AUTH_TOKEN", "RELAY_AUTH_TOKEN")
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "PEEK_AUTH_TOKEN is required",
            )
        })?;
    let max_tunnels: usize = env_var("PEEK_MAX_TUNNELS", "MAX_TUNNELS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);
    let max_body_size_mb: usize = env_var("PEEK_MAX_BODY_SIZE_MB", "MAX_BODY_SIZE_MB")
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let max_body_size = max_body_size_mb.saturating_mul(1024 * 1024);
    let rate_limit_rpm: u32 = env_var("PEEK_RATE_LIMIT_RPM", "RATE_LIMIT_RPM")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let drain_timeout_secs: u64 = env_var("PEEK_DRAIN_TIMEOUT_SECS", "DRAIN_TIMEOUT_SECS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let trust_proxy_headers = env_bool("PEEK_TRUST_PROXY_HEADERS").unwrap_or(false);

    info!("authentication enabled");

    info!(
        port = %port,
        domain = %domain,
        max_tunnels = max_tunnels,
        max_body_size_mb = max_body_size_mb,
        rate_limit_rpm = rate_limit_rpm,
        drain_timeout_secs = drain_timeout_secs,
        trust_proxy_headers = trust_proxy_headers,
        "starting peek"
    );

    let rate_limiter = RateLimiter::new(rate_limit_rpm, Duration::from_secs(60));
    let registry = Arc::new(Registry::new(
        domain,
        Some(auth_token),
        max_tunnels,
        max_body_size,
        trust_proxy_headers,
        rate_limiter,
    ));

    {
        let registry = registry.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                registry.rate_limiter.cleanup();
            }
        });
    }

    let app = Router::new()
        .route("/tunnel", get(ws_handler))
        .fallback(public_handler)
        .layer(DefaultBodyLimit::max(max_body_size))
        .layer(TraceLayer::new_for_http())
        .with_state(registry);

    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(addr = %addr, "listening");

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    tokio::spawn(async move {
        if let Err(error) = shutdown_signal().await {
            warn!(%error, "failed to listen for shutdown signal");
        }
        let _ = shutdown_tx.send(true);
    });

    let mut force_rx = shutdown_rx.clone();
    tokio::spawn(async move {
        let _ = force_rx.changed().await;
        info!(drain_timeout_secs, "draining connections");
        tokio::time::sleep(Duration::from_secs(drain_timeout_secs)).await;
        warn!("drain timeout exceeded, forcing shutdown");
        std::process::exit(0);
    });

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        let _ = shutdown_rx.changed().await;
    })
    .await?;

    info!("server shut down gracefully");
    Ok(())
}

fn env_var(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary).ok().or_else(|| {
        (!fallback.is_empty())
            .then(|| std::env::var(fallback).ok())
            .flatten()
    })
}

fn env_bool(name: &str) -> Option<bool> {
    std::env::var(name)
        .ok()
        .and_then(|value| match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
}

async fn shutdown_signal() -> std::io::Result<()> {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    #[cfg(unix)]
    tokio::select! {
        result = ctrl_c => {
            result?;
            info!("received ctrl+c, shutting down");
        }
        _ = terminate.recv() => info!("received SIGTERM, shutting down"),
    }

    #[cfg(not(unix))]
    tokio::select! {
        result = ctrl_c => {
            result?;
            info!("received ctrl+c, shutting down");
        }
        () = terminate => {}
    }

    Ok(())
}
