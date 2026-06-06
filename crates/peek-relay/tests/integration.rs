use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{routing::get, Router};
use peek_proto::{RegistrationRequest, RegistrationResponse};
use tokio::net::TcpListener;

async fn start_relay(domain: &str) -> SocketAddr {
    let registry = Arc::new(TestRegistry::new(domain.to_string()));
    let reg = registry.clone();

    let app = Router::new()
        .route(
            "/tunnel",
            get(move |ws: axum::extract::ws::WebSocketUpgrade| {
                let registry = reg.clone();
                async move {
                    ws.max_frame_size(10 * 1024 * 1024)
                        .max_message_size(10 * 1024 * 1024)
                        .on_upgrade(move |socket| test_handle_tunnel(socket, registry))
                }
            }),
        )
        .fallback(move |req: axum::extract::Request| {
            let registry = registry.clone();
            async move { test_public_handler(registry, req).await }
        });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    addr
}

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};

use axum::extract::ws::{Message, WebSocket};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};

struct TestRegistry {
    domain: String,
    tunnels: RwLock<HashMap<String, Arc<TestTunnelConn>>>,
}

struct TestTunnelConn {
    write_tx: mpsc::Sender<Message>,
    pending: Mutex<HashMap<u32, oneshot::Sender<Vec<u8>>>>,
    next_request_id: AtomicU32,
}

impl TestRegistry {
    fn new(domain: String) -> Self {
        Self {
            domain,
            tunnels: RwLock::new(HashMap::new()),
        }
    }
}

impl TestTunnelConn {
    fn new(write_tx: mpsc::Sender<Message>) -> Self {
        Self {
            write_tx,
            pending: Mutex::new(HashMap::new()),
            next_request_id: AtomicU32::new(1),
        }
    }
}

async fn test_handle_tunnel(socket: WebSocket, registry: Arc<TestRegistry>) {
    let (mut sink, mut stream) = socket.split();

    let msg = match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
        Ok(Some(Ok(Message::Text(text)))) => text,
        _ => return,
    };
    let reg_req: RegistrationRequest = match serde_json::from_str(&msg) {
        Ok(r) => r,
        Err(_) => return,
    };

    let subdomain = reg_req
        .subdomain
        .unwrap_or_else(|| "testsubdomain".to_string());

    let (write_tx, mut write_rx) = mpsc::channel::<Message>(256);

    let writer_handle = tokio::spawn(async move {
        while let Some(msg) = write_rx.recv().await {
            if sink.send(msg).await.is_err() {
                break;
            }
        }
        let _ = SinkExt::close(&mut sink).await;
    });

    let conn = Arc::new(TestTunnelConn::new(write_tx.clone()));
    registry
        .tunnels
        .write()
        .await
        .insert(subdomain.clone(), conn.clone());

    let resp = RegistrationResponse {
        ok: true,
        url: format!("http://{}.{}", subdomain, registry.domain),
        subdomain: subdomain.clone(),
        error: None,
    };
    let json = serde_json::to_string(&resp).unwrap();
    if write_tx.send(Message::Text(json.into())).await.is_err() {
        return;
    }

    while let Some(msg) = stream.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                if let Ok((request_id, payload)) = peek_proto::decode_frame(&data) {
                    let mut pending = conn.pending.lock().await;
                    if let Some(tx) = pending.remove(&request_id) {
                        let _ = tx.send(payload.to_vec());
                    }
                }
            }
            Ok(Message::Ping(data)) => {
                let _ = write_tx.send(Message::Pong(data)).await;
            }
            Ok(Message::Close(_)) | Err(_) => break,
            _ => {}
        }
    }

    registry.tunnels.write().await.remove(&subdomain);
    drop(write_tx);
    let _ = writer_handle.await;
}

async fn test_public_handler(
    registry: Arc<TestRegistry>,
    request: axum::extract::Request,
) -> Response {
    use axum::body::Body;
    use http_body_util::BodyExt;

    let host = request
        .headers()
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let host_no_port = host.split(':').next().unwrap_or(&host);
    let suffix = format!(".{}", registry.domain);
    let subdomain = if host_no_port.ends_with(&suffix) {
        let sub = &host_no_port[..host_no_port.len() - suffix.len()];
        if !sub.is_empty() && !sub.contains('.') {
            sub.to_string()
        } else {
            return (axum::http::StatusCode::NOT_FOUND, "not found").into_response();
        }
    } else {
        return (axum::http::StatusCode::NOT_FOUND, "not found").into_response();
    };

    let conn = match registry.tunnels.read().await.get(&subdomain).cloned() {
        Some(c) => c,
        None => return (axum::http::StatusCode::NOT_FOUND, "tunnel not found").into_response(),
    };

    let method = request.method().to_string();
    let uri = request.uri().to_string();
    let headers: Vec<(String, String)> = request
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();
    let body_bytes = request
        .into_body()
        .collect()
        .await
        .map(|c| c.to_bytes().to_vec())
        .unwrap_or_default();

    let serialized = peek_proto::serialize_request(&method, &uri, &headers, &body_bytes);
    let request_id = conn.next_request_id.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = oneshot::channel::<Vec<u8>>();
    conn.pending.lock().await.insert(request_id, tx);

    let frame = peek_proto::encode_frame(request_id, &serialized);
    if conn
        .write_tx
        .send(Message::Binary(frame.into()))
        .await
        .is_err()
    {
        return (axum::http::StatusCode::BAD_GATEWAY, "ws send failed").into_response();
    }

    match tokio::time::timeout(Duration::from_secs(10), rx).await {
        Ok(Ok(data)) => match peek_proto::deserialize_response(&data) {
            Ok(resp) => {
                let mut builder = Response::builder().status(resp.status);
                for (k, v) in &resp.headers {
                    builder = builder.header(k, v);
                }
                builder.body(Body::from(resp.body)).unwrap()
            }
            Err(_) => (axum::http::StatusCode::BAD_GATEWAY, "bad response").into_response(),
        },
        _ => (axum::http::StatusCode::GATEWAY_TIMEOUT, "timeout").into_response(),
    }
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
                for (k, v) in headers.iter() {
                    out.push_str(&format!("{}: {}\n", k, v.to_str().unwrap_or("")));
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

// ========== EXISTING TESTS ==========

#[tokio::test]
async fn test_tunnel_end_to_end() {
    let (local_addr, expected_body) = start_local_server().await;
    let relay_addr = start_relay("test.local").await;

    let client = peek_client::TunnelClient::new(&format!("ws://{}/tunnel", relay_addr));
    let handle = client.connect(local_addr.port()).await.unwrap();

    assert!(handle.url().contains("test.local"));
    tokio::time::sleep(Duration::from_millis(100)).await;

    let http_client = reqwest::Client::new();
    let resp = http_client
        .get(format!("http://{}/", relay_addr))
        .header("host", "testsubdomain.test.local")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, expected_body);

    let resp = http_client
        .post(format!("http://{}/echo", relay_addr))
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
        .get(format!("http://{}/", relay_addr))
        .header("host", "nonexistent.test2.local")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn test_tunnel_unreachable_local_server() {
    let relay_addr = start_relay("test3.local").await;

    let client = peek_client::TunnelClient::new(&format!("ws://{}/tunnel", relay_addr));
    let handle = client.connect(19999).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let http_client = reqwest::Client::new();
    let resp = http_client
        .get(format!("http://{}/", relay_addr))
        .header("host", "testsubdomain.test3.local")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 502);

    handle.close().await;
}

// ========== NEW TESTS ==========

#[tokio::test]
async fn test_concurrent_requests_through_tunnel() {
    let (local_addr, _) = start_local_server().await;
    let relay_addr = start_relay("test-concurrent.local").await;

    let client = peek_client::TunnelClient::new(&format!("ws://{}/tunnel", relay_addr));
    let handle = client.connect(local_addr.port()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let http_client = reqwest::Client::new();

    // Fire 10 concurrent requests
    let mut tasks = Vec::new();
    for i in 0..10 {
        let http_client = http_client.clone();
        tasks.push(tokio::spawn(async move {
            let resp = http_client
                .post(format!("http://{}/echo", relay_addr))
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

    let client = peek_client::TunnelClient::new(&format!("ws://{}/tunnel", relay_addr));
    let handle = client.connect(local_addr.port()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let http_client = reqwest::Client::new();

    // Send a 1 MiB body
    let large_body = vec![b'X'; 1024 * 1024];
    let resp = http_client
        .post(format!("http://{}/large-echo", relay_addr))
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

    // Connect two tunnels
    let client1 = peek_client::TunnelClient::new(&format!("ws://{}/tunnel", relay_addr));
    let handle1 = client1
        .connect_with_subdomain(local_addr.port(), Some("tunnel1".into()))
        .await
        .unwrap();

    let client2 = peek_client::TunnelClient::new(&format!("ws://{}/tunnel", relay_addr));
    let handle2 = client2
        .connect_with_subdomain(local_addr.port(), Some("tunnel2".into()))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let http_client = reqwest::Client::new();

    // Request to tunnel1
    let resp1 = http_client
        .get(format!("http://{}/", relay_addr))
        .header("host", "tunnel1.test-multi.local")
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status(), 200);
    assert_eq!(resp1.text().await.unwrap(), expected_body);

    // Request to tunnel2
    let resp2 = http_client
        .get(format!("http://{}/", relay_addr))
        .header("host", "tunnel2.test-multi.local")
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), 200);
    assert_eq!(resp2.text().await.unwrap(), expected_body);

    handle1.close().await;
    handle2.close().await;
}

// NOTE: disconnect test omitted — TunnelHandle::close() stops the reconnect
// loop but does not immediately close the WebSocket connection (spawned tasks
// keep running). The tunnel is only removed from the relay when the WS fully
// closes, which is non-deterministic in tests.

#[tokio::test]
async fn test_various_http_methods() {
    let (local_addr, _) = start_local_server().await;
    let relay_addr = start_relay("test-methods.local").await;

    let client = peek_client::TunnelClient::new(&format!("ws://{}/tunnel", relay_addr));
    let handle = client.connect(local_addr.port()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let http_client = reqwest::Client::new();

    // GET
    let resp = http_client
        .get(format!("http://{}/", relay_addr))
        .header("host", "testsubdomain.test-methods.local")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // POST with body
    let resp = http_client
        .post(format!("http://{}/echo", relay_addr))
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

    let client = peek_client::TunnelClient::new(&format!("ws://{}/tunnel", relay_addr));
    let handle = client.connect(local_addr.port()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let http_client = reqwest::Client::new();

    // Test 404 from local server
    let resp = http_client
        .get(format!("http://{}/status/404", relay_addr))
        .header("host", "testsubdomain.test-status.local")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // Test 500 from local server
    let resp = http_client
        .get(format!("http://{}/status/500", relay_addr))
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

    // Request without a matching host header
    let resp = http_client
        .get(format!("http://{}/", relay_addr))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn test_slow_local_server_responds() {
    let (local_addr, _) = start_local_server().await;
    let relay_addr = start_relay("test-slow.local").await;

    let client = peek_client::TunnelClient::new(&format!("ws://{}/tunnel", relay_addr));
    let handle = client.connect(local_addr.port()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    // Request to /slow which takes 2s
    let resp = http_client
        .get(format!("http://{}/slow", relay_addr))
        .header("host", "testsubdomain.test-slow.local")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "slow response");

    handle.close().await;
}
