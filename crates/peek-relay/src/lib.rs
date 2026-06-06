mod handler;
mod pages;
mod rate_limit;
mod registry;

use std::sync::Arc;
use std::time::Duration;

use axum::{Router, extract::DefaultBodyLimit, routing::get};
use tower_http::trace::TraceLayer;

use handler::{public_handler, ws_handler};
use rate_limit::RateLimiter;
use registry::Registry;

pub struct AppConfig {
    pub domain: String,
    pub auth_token: Option<String>,
    pub max_tunnels: usize,
    pub max_body_size: usize,
    pub rate_limit_rpm: u32,
    pub trust_proxy_headers: bool,
}

pub fn build_app(config: AppConfig) -> Router {
    let rate_limiter = RateLimiter::new(config.rate_limit_rpm, Duration::from_secs(60));
    let registry = Arc::new(Registry::new(
        config.domain,
        config.auth_token,
        config.max_tunnels,
        config.max_body_size,
        config.trust_proxy_headers,
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

    Router::new()
        .route("/tunnel", get(ws_handler))
        .fallback(public_handler)
        .layer(DefaultBodyLimit::max(config.max_body_size))
        .layer(TraceLayer::new_for_http())
        .with_state(registry)
}
