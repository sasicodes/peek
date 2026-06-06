use std::fmt::Write as _;
use std::net::SocketAddr;
use std::time::Duration;

use axum::{Router, routing::get};
use peek_relay::{AppConfig, build_app};
use tokio::net::TcpListener;

async fn start_relay(domain: &str) -> SocketAddr {
    let app = build_app(AppConfig {
        domain: domain.to_string(),
        auth_token: None,
        max_tunnels: 100,
        max_body_size: 10 * 1024 * 1024,
        rate_limit_rpm: 10_000,
        trust_proxy_headers: false,
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    addr
}

async fn start_local_server() -> (SocketAddr, &'static str) {
    let expected_body = "Hello from local server!";

    let app = Router::new()
        .route("/", get(|| async { "Hello from local server!" }))
        .route(
            "/echo",
            axum::routing::post(|body: String| async move { format!("Echo: {body}") }),
        )
        .route(
            "/headers",
            get(|headers: axum::http::HeaderMap| async move {
                let mut out = String::new();
                for (k, v) in &headers {
                    let _ = writeln!(out, "{}: {}", k, v.to_str().unwrap_or(""));
                }
                out
            }),
        )
        .route(
            "/large-echo",
            axum::routing::post(|body: axum::body::Bytes| async move {
                axum::response::Response::builder()
                    .header("content-type", "application/octet-stream")
                    .body(axum::body::Body::from(body))
                    .unwrap()
            }),
        )
        .route(
            "/status/{code}",
            get(
                |axum::extract::Path(code): axum::extract::Path<u16>| async move {
                    axum::response::Response::builder()
                        .status(code)
                        .body(axum::body::Body::from(format!("status {code}")))
                        .unwrap()
                },
            ),
        )
        .route(
            "/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                "slow response"
            }),
        );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (addr, expected_body)
}

#[tokio::test]
async fn test_tunnel_end_to_end() {
    let (local_addr, expected_body) = start_local_server().await;
    let relay_addr = start_relay("test.local").await;

    let client = peek_client::TunnelClient::new(&format!("ws://{relay_addr}/tunnel")).unwrap();
    let handle = client
        .connect_with_subdomain(local_addr.port(), Some("testsubdomain".into()))
        .await
        .unwrap();

    assert!(handle.url().contains("test.local"));
    tokio::time::sleep(Duration::from_millis(100)).await;

    let http_client = reqwest::Client::new();
    let resp = http_client
        .get(format!("http://{relay_addr}/"))
        .header("host", "testsubdomain.test.local")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, expected_body);

    let resp = http_client
        .post(format!("http://{relay_addr}/echo"))
        .header("host", "testsubdomain.test.local")
        .body("test data")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "Echo: test data");

    handle.close().await;
}

#[tokio::test]
async fn test_tunnel_not_found() {
    let relay_addr = start_relay("test2.local").await;

    let http_client = reqwest::Client::new();
    let resp = http_client
        .get(format!("http://{relay_addr}/"))
        .header("host", "nonexistent.test2.local")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn test_tunnel_unreachable_local_server() {
    let relay_addr = start_relay("test3.local").await;

    let client = peek_client::TunnelClient::new(&format!("ws://{relay_addr}/tunnel")).unwrap();
    let handle = client
        .connect_with_subdomain(19999, Some("testsubdomain".into()))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let http_client = reqwest::Client::new();
    let resp = http_client
        .get(format!("http://{relay_addr}/"))
        .header("host", "testsubdomain.test3.local")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 502);

    handle.close().await;
}

#[tokio::test]
async fn test_concurrent_requests_through_tunnel() {
    let (local_addr, _) = start_local_server().await;
    let relay_addr = start_relay("test-concurrent.local").await;

    let client = peek_client::TunnelClient::new(&format!("ws://{relay_addr}/tunnel")).unwrap();
    let handle = client
        .connect_with_subdomain(local_addr.port(), Some("testsubdomain".into()))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let http_client = reqwest::Client::new();
    let mut tasks = Vec::new();
    for i in 0..10 {
        let http_client = http_client.clone();
        tasks.push(tokio::spawn(async move {
            let resp = http_client
                .post(format!("http://{relay_addr}/echo"))
                .header("host", "testsubdomain.test-concurrent.local")
                .body(format!("request-{i}"))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            let body = resp.text().await.unwrap();
            assert_eq!(body, format!("Echo: request-{i}"));
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    handle.close().await;
}

#[tokio::test]
async fn test_large_request_body() {
    let (local_addr, _) = start_local_server().await;
    let relay_addr = start_relay("test-large.local").await;

    let client = peek_client::TunnelClient::new(&format!("ws://{relay_addr}/tunnel")).unwrap();
    let handle = client
        .connect_with_subdomain(local_addr.port(), Some("testsubdomain".into()))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let http_client = reqwest::Client::new();
    let large_body = vec![b'X'; 1024 * 1024];
    let resp = http_client
        .post(format!("http://{relay_addr}/large-echo"))
        .header("host", "testsubdomain.test-large.local")
        .body(large_body.clone())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let resp_body = resp.bytes().await.unwrap();
    assert_eq!(resp_body.len(), large_body.len());
    assert_eq!(&resp_body[..], &large_body[..]);

    handle.close().await;
}

#[tokio::test]
async fn test_multiple_tunnels_simultaneously() {
    let (local_addr, expected_body) = start_local_server().await;
    let relay_addr = start_relay("test-multi.local").await;
    let client1 = peek_client::TunnelClient::new(&format!("ws://{relay_addr}/tunnel")).unwrap();
    let handle1 = client1
        .connect_with_subdomain(local_addr.port(), Some("tunnel1".into()))
        .await
        .unwrap();

    let client2 = peek_client::TunnelClient::new(&format!("ws://{relay_addr}/tunnel")).unwrap();
    let handle2 = client2
        .connect_with_subdomain(local_addr.port(), Some("tunnel2".into()))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let http_client = reqwest::Client::new();
    let resp1 = http_client
        .get(format!("http://{relay_addr}/"))
        .header("host", "tunnel1.test-multi.local")
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status(), 200);
    assert_eq!(resp1.text().await.unwrap(), expected_body);
    let resp2 = http_client
        .get(format!("http://{relay_addr}/"))
        .header("host", "tunnel2.test-multi.local")
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), 200);
    assert_eq!(resp2.text().await.unwrap(), expected_body);

    handle1.close().await;
    handle2.close().await;
}

#[tokio::test]
async fn test_various_http_methods() {
    let (local_addr, _) = start_local_server().await;
    let relay_addr = start_relay("test-methods.local").await;

    let client = peek_client::TunnelClient::new(&format!("ws://{relay_addr}/tunnel")).unwrap();
    let handle = client
        .connect_with_subdomain(local_addr.port(), Some("testsubdomain".into()))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let http_client = reqwest::Client::new();
    let resp = http_client
        .get(format!("http://{relay_addr}/"))
        .header("host", "testsubdomain.test-methods.local")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let resp = http_client
        .post(format!("http://{relay_addr}/echo"))
        .header("host", "testsubdomain.test-methods.local")
        .body("hello")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "Echo: hello");

    handle.close().await;
}

#[tokio::test]
async fn test_non_200_status_codes_forwarded() {
    let (local_addr, _) = start_local_server().await;
    let relay_addr = start_relay("test-status.local").await;

    let client = peek_client::TunnelClient::new(&format!("ws://{relay_addr}/tunnel")).unwrap();
    let handle = client
        .connect_with_subdomain(local_addr.port(), Some("testsubdomain".into()))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let http_client = reqwest::Client::new();
    let resp = http_client
        .get(format!("http://{relay_addr}/status/404"))
        .header("host", "testsubdomain.test-status.local")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    let resp = http_client
        .get(format!("http://{relay_addr}/status/500"))
        .header("host", "testsubdomain.test-status.local")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 500);

    handle.close().await;
}

#[tokio::test]
async fn test_no_host_header_returns_not_found() {
    let relay_addr = start_relay("test-nohost.local").await;

    let http_client = reqwest::Client::new();
    let resp = http_client
        .get(format!("http://{relay_addr}/"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn test_slow_local_server_responds() {
    let (local_addr, _) = start_local_server().await;
    let relay_addr = start_relay("test-slow.local").await;

    let client = peek_client::TunnelClient::new(&format!("ws://{relay_addr}/tunnel")).unwrap();
    let handle = client
        .connect_with_subdomain(local_addr.port(), Some("testsubdomain".into()))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let resp = http_client
        .get(format!("http://{relay_addr}/slow"))
        .header("host", "testsubdomain.test-slow.local")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "slow response");

    handle.close().await;
}
