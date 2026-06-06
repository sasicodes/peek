use axum::extract::ws::Message;
use rand::RngExt as _;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex, RwLock, mpsc, oneshot};

use crate::rate_limit::RateLimiter;

pub const MAX_PENDING_PER_TUNNEL: usize = 10_000;

pub struct Registry {
    domain: String,
    auth_token: Option<String>,
    max_tunnels: usize,
    tunnels: RwLock<HashMap<String, Arc<TunnelConnection>>>,
    pub trust_proxy_headers: bool,
    pub max_body_size: usize,
    pub rate_limiter: RateLimiter,
}

pub struct TunnelConnection {
    pub write_tx: mpsc::Sender<Message>,
    pub pending: Mutex<HashMap<u32, oneshot::Sender<Vec<u8>>>>,
    pub password: Option<String>,
    next_request_id: AtomicU32,
}

impl Registry {
    pub fn new(
        domain: String,
        auth_token: Option<String>,
        max_tunnels: usize,
        max_body_size: usize,
        trust_proxy_headers: bool,
        rate_limiter: RateLimiter,
    ) -> Self {
        Self {
            domain,
            auth_token,
            max_tunnels,
            tunnels: RwLock::new(HashMap::new()),
            trust_proxy_headers,
            max_body_size,
            rate_limiter,
        }
    }

    pub fn validate_token(&self, token: Option<&str>) -> bool {
        self.auth_token.as_ref().is_none_or(|expected| {
            token.is_some_and(|provided| provided.as_bytes().ct_eq(expected.as_bytes()).into())
        })
    }

    pub async fn register(&self, subdomain: String, conn: Arc<TunnelConnection>) -> bool {
        let mut tunnels = self.tunnels.write().await;
        if tunnels.len() >= self.max_tunnels {
            return false;
        }
        match tunnels.entry(subdomain) {
            Entry::Occupied(_) => false,
            Entry::Vacant(slot) => {
                slot.insert(conn);
                true
            }
        }
    }

    pub async fn remove(&self, subdomain: &str) {
        self.tunnels.write().await.remove(subdomain);
    }

    pub async fn get(&self, subdomain: &str) -> Option<Arc<TunnelConnection>> {
        self.tunnels.read().await.get(subdomain).cloned()
    }

    pub async fn generate_subdomain(&self) -> String {
        let tunnels = self.tunnels.read().await;
        let mut rng = rand::rng();
        loop {
            let subdomain: String = (0..8)
                .map(|_| {
                    let idx = rng.random_range(0..36);
                    if idx < 10 {
                        (b'0' + idx) as char
                    } else {
                        (b'a' + idx - 10) as char
                    }
                })
                .collect();
            if !tunnels.contains_key(&subdomain) {
                return subdomain;
            }
        }
    }

    pub async fn is_taken(&self, subdomain: &str) -> bool {
        self.tunnels.read().await.contains_key(subdomain)
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }
}

impl TunnelConnection {
    pub fn new(write_tx: mpsc::Sender<Message>, password: Option<String>) -> Self {
        Self {
            write_tx,
            pending: Mutex::new(HashMap::new()),
            password,
            next_request_id: AtomicU32::new(1),
        }
    }

    pub fn next_id(&self) -> u32 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }
}
