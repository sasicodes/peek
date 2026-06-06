use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct RateLimiter {
    windows: Mutex<HashMap<IpAddr, WindowState>>,
    max_requests: u32,
    window_duration: Duration,
}

struct WindowState {
    count: u32,
    window_start: Instant,
}

impl RateLimiter {
    pub fn new(max_requests: u32, window_duration: Duration) -> Self {
        Self {
            windows: Mutex::new(HashMap::new()),
            max_requests,
            window_duration,
        }
    }

    /// Returns `true` if the request is allowed, `false` if rate-limited.
    pub fn check(&self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let mut windows = self.windows.lock().unwrap_or_else(|e| e.into_inner());

        let state = windows.entry(ip).or_insert(WindowState {
            count: 0,
            window_start: now,
        });

        if now.duration_since(state.window_start) >= self.window_duration {
            // Window expired, reset
            state.count = 1;
            state.window_start = now;
            return true;
        }

        state.count += 1;
        state.count <= self.max_requests
    }

    /// Remove expired entries to prevent unbounded memory growth.
    pub fn cleanup(&self) {
        let now = Instant::now();
        let mut windows = self.windows.lock().unwrap_or_else(|e| e.into_inner());
        windows.retain(|_, state| now.duration_since(state.window_start) < self.window_duration);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn allows_requests_within_limit() {
        let limiter = RateLimiter::new(5, Duration::from_secs(60));
        let ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));

        for _ in 0..5 {
            assert!(limiter.check(ip));
        }
        assert!(!limiter.check(ip));
    }

    #[test]
    fn separate_limits_per_ip() {
        let limiter = RateLimiter::new(2, Duration::from_secs(60));
        let ip1 = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        let ip2 = IpAddr::V4(Ipv4Addr::new(5, 6, 7, 8));

        assert!(limiter.check(ip1));
        assert!(limiter.check(ip1));
        assert!(!limiter.check(ip1));

        // ip2 has its own quota
        assert!(limiter.check(ip2));
        assert!(limiter.check(ip2));
        assert!(!limiter.check(ip2));
    }

    #[test]
    fn cleanup_removes_expired_entries() {
        let limiter = RateLimiter::new(100, Duration::from_millis(1));
        let ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));

        limiter.check(ip);
        std::thread::sleep(Duration::from_millis(5));
        limiter.cleanup();

        let windows = limiter.windows.lock().unwrap();
        assert!(windows.is_empty());
    }
}
