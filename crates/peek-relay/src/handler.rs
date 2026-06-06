use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Request, State,
    },
    http::HeaderMap,
    response::{Html, IntoResponse, Response},
};
use futures_util::StreamExt;
use http_body_util::BodyExt;
use subtle::ConstantTimeEq;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

use peek_proto::{self, RegistrationRequest, RegistrationResponse};

use crate::registry::{Registry, TunnelConnection, MAX_PENDING_PER_TUNNEL};

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(10);
const WRITER_CHANNEL_SIZE: usize = 1024;

pub async fn ws_handler(
    State(registry): State<Arc<Registry>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    // Rate limit tunnel registrations
    if let Some(ip) = extract_client_ip(&headers) {
        if !registry.rate_limiter.check(ip) {
            return too_many_requests_page();
        }
    }

    let max_body_size = registry.max_body_size;
    ws.max_frame_size(max_body_size)
        .max_message_size(max_body_size)
        .on_upgrade(|socket| handle_tunnel_client(socket, registry))
}

async fn handle_tunnel_client(socket: WebSocket, registry: Arc<Registry>) {
    let (mut sink, mut stream) = socket.split();

    let reg_req: RegistrationRequest = match read_registration(&mut stream).await {
        Some(r) => r,
        None => return,
    };

    if !registry.validate_token(&reg_req.token) {
        warn!("tunnel registration rejected: auth failed");
        let resp = RegistrationResponse {
            ok: false,
            url: String::new(),
            subdomain: String::new(),
            error: Some("authentication failed".into()),
        };
        let Ok(json) = serde_json::to_string(&resp) else {
            error!("failed to serialize registration response");
            return;
        };
        use futures_util::SinkExt;
        let _ = sink.send(Message::Text(json.into())).await;
        let _ = sink
            .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                code: peek_proto::close_codes::AUTH_FAILED,
                reason: "authentication failed".into(),
            })))
            .await;
        return;
    }

    let tunnel_password = match reg_req.password {
        Some(ref p) if !p.is_empty() => p.clone(),
        _ => {
            warn!("tunnel registration rejected: password required");
            let resp = RegistrationResponse {
                ok: false,
                url: String::new(),
                subdomain: String::new(),
                error: Some("tunnel password is required".into()),
            };
            let Ok(json) = serde_json::to_string(&resp) else {
                error!("failed to serialize registration response");
                return;
            };
            use futures_util::SinkExt;
            let _ = sink.send(Message::Text(json.into())).await;
            let _ = sink
                .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                    code: peek_proto::close_codes::AUTH_FAILED,
                    reason: "tunnel password is required".into(),
                })))
                .await;
            return;
        }
    };

    let subdomain = if let Some(ref requested) = reg_req.subdomain {
        if registry.is_taken(requested).await {
            registry.generate_subdomain().await
        } else {
            requested.clone()
        }
    } else {
        registry.generate_subdomain().await
    };

    let (write_tx, mut write_rx) = mpsc::channel::<Message>(WRITER_CHANNEL_SIZE);

    let writer_handle = tokio::spawn(async move {
        use futures_util::SinkExt;
        while let Some(msg) = write_rx.recv().await {
            if sink.send(msg).await.is_err() {
                break;
            }
        }
        let _ = futures_util::SinkExt::close(&mut sink).await;
    });

    let conn = Arc::new(TunnelConnection::new(
        write_tx.clone(),
        Some(tunnel_password),
    ));

    if !registry.register(subdomain.clone(), conn.clone()).await {
        warn!("tunnel registration rejected: relay at capacity");
        let resp = RegistrationResponse {
            ok: false,
            url: String::new(),
            subdomain: String::new(),
            error: Some("relay at capacity".into()),
        };
        let Ok(json) = serde_json::to_string(&resp) else {
            error!("failed to serialize registration response");
            drop(write_tx);
            let _ = writer_handle.await;
            return;
        };
        let _ = write_tx.send(Message::Text(json.into())).await;
        let _ = write_tx
            .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                code: peek_proto::close_codes::CAPACITY_FULL,
                reason: "relay at capacity".into(),
            })))
            .await;
        drop(write_tx);
        let _ = writer_handle.await;
        return;
    }

    info!(subdomain = %subdomain, "tunnel registered");

    let domain = registry.domain();
    let url = format!("https://{subdomain}.{domain}");
    let resp = RegistrationResponse {
        ok: true,
        url,
        subdomain: subdomain.clone(),
        error: None,
    };
    let Ok(json) = serde_json::to_string(&resp) else {
        error!("failed to serialize registration response");
        registry.remove(&subdomain).await;
        drop(write_tx);
        let _ = writer_handle.await;
        return;
    };
    if write_tx.send(Message::Text(json.into())).await.is_err() {
        registry.remove(&subdomain).await;
        drop(write_tx);
        let _ = writer_handle.await;
        return;
    }

    while let Some(msg) = stream.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                warn!(subdomain = %subdomain, error = %e, "ws read error");
                break;
            }
        };

        match msg {
            Message::Binary(data) => match peek_proto::decode_frame(&data) {
                Ok((request_id, payload)) => {
                    let mut pending = conn.pending.lock().await;
                    if let Some(tx) = pending.remove(&request_id) {
                        let _ = tx.send(payload.to_vec());
                    }
                }
                Err(e) => {
                    warn!(subdomain = %subdomain, error = %e, "bad frame from client");
                }
            },
            Message::Ping(data) => {
                let _ = write_tx.send(Message::Pong(data)).await;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    info!(subdomain = %subdomain, "tunnel disconnected");
    registry.remove(&subdomain).await;
    drop(write_tx);
    let _ = writer_handle.await;
    conn.pending.lock().await.clear();
}

async fn read_registration(
    stream: &mut futures_util::stream::SplitStream<WebSocket>,
) -> Option<RegistrationRequest> {
    let timeout = tokio::time::timeout(REGISTRATION_TIMEOUT, stream.next()).await;
    match timeout {
        Ok(Some(Ok(Message::Text(text)))) => match serde_json::from_str(&text) {
            Ok(req) => Some(req),
            Err(e) => {
                warn!(error = %e, "invalid registration JSON");
                None
            }
        },
        _ => {
            warn!("no registration message received");
            None
        }
    }
}

pub async fn public_handler(State(registry): State<Arc<Registry>>, request: Request) -> Response {
    // Rate limit by client IP
    if let Some(ip) = extract_client_ip(request.headers()) {
        if !registry.rate_limiter.check(ip) {
            return too_many_requests_page();
        }
    }

    let host = request
        .headers()
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let subdomain = match extract_subdomain(&host, registry.domain()) {
        Some(s) => s,
        None => return not_found_page(),
    };

    let conn = match registry.get(&subdomain).await {
        Some(c) => c,
        None => return not_found_page(),
    };

    // --- Password protection gate ---
    if let Some(ref tunnel_password) = conn.password {
        let method = request.method().clone();
        let uri = request.uri().clone();

        // Check if this is a password form submission
        if method == axum::http::Method::POST && uri.path() == "/__peek_auth" {
            let body_bytes = match request.into_body().collect().await {
                Ok(collected) => collected.to_bytes().to_vec(),
                Err(_) => return password_page(&subdomain, Some("Invalid request")),
            };
            let form_data = String::from_utf8_lossy(&body_bytes);
            let submitted_password = form_data
                .split('&')
                .find_map(|pair| {
                    let (key, val) = pair.split_once('=')?;
                    if key == "password" {
                        Some(urlencoding::decode(val).unwrap_or_default().into_owned())
                    } else {
                        None
                    }
                })
                .unwrap_or_default();

            // Constant-time comparison to prevent timing attacks
            let password_matches: bool = submitted_password
                .as_bytes()
                .ct_eq(tunnel_password.as_bytes())
                .into();

            if password_matches {
                let cookie_value = generate_auth_cookie(tunnel_password, &subdomain);
                let cookie = format!(
                    "peek_auth_{}={}; Path=/; HttpOnly; SameSite=Lax; Secure; Max-Age=86400",
                    subdomain, cookie_value
                );
                return Response::builder()
                    .status(303)
                    .header("location", "/")
                    .header("set-cookie", cookie)
                    .body(Body::empty())
                    .unwrap_or_else(|_| bad_gateway_page());
            } else {
                return password_page(&subdomain, Some("Incorrect password"));
            }
        }

        // Check for valid auth cookie
        let cookie_name = format!("peek_auth_{}", subdomain);
        let expected_cookie = generate_auth_cookie(tunnel_password, &subdomain);
        let has_valid_cookie = request.headers().get_all("cookie").iter().any(|v| {
            let v = v.to_str().unwrap_or("");
            v.split(';').map(|c| c.trim()).any(|c| {
                if let Some((k, val)) = c.split_once('=') {
                    let name_matches = k.trim() == cookie_name;
                    // Constant-time comparison for cookie value
                    let value_matches: bool = val
                        .trim()
                        .as_bytes()
                        .ct_eq(expected_cookie.as_bytes())
                        .into();
                    name_matches && value_matches
                } else {
                    false
                }
            })
        });

        if !has_valid_cookie {
            return password_page(&subdomain, None);
        }
    }
    // --- End password gate ---

    let max_body_size = registry.max_body_size;
    let method = request.method().to_string();
    let uri = request.uri().to_string();
    let headers: Vec<(String, String)> = request
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();

    let body_bytes = match request.into_body().collect().await {
        Ok(collected) => {
            let bytes = collected.to_bytes();
            if bytes.len() > max_body_size {
                return payload_too_large_page();
            }
            bytes.to_vec()
        }
        Err(_) => Vec::new(),
    };

    let serialized = peek_proto::serialize_request(&method, &uri, &headers, &body_bytes);

    // Check pending request limit before adding
    {
        let pending = conn.pending.lock().await;
        if pending.len() >= MAX_PENDING_PER_TUNNEL {
            return service_unavailable_page();
        }
    }

    let request_id = conn.next_id();
    let (tx, rx) = oneshot::channel::<Vec<u8>>();
    conn.pending.lock().await.insert(request_id, tx);

    let frame = peek_proto::encode_frame(request_id, &serialized);
    if conn
        .write_tx
        .send(Message::Binary(frame.into()))
        .await
        .is_err()
    {
        conn.pending.lock().await.remove(&request_id);
        return bad_gateway_page();
    }

    let response_data = match tokio::time::timeout(REQUEST_TIMEOUT, rx).await {
        Ok(Ok(data)) => data,
        Ok(Err(_)) => {
            conn.pending.lock().await.remove(&request_id);
            return bad_gateway_page();
        }
        Err(_) => {
            conn.pending.lock().await.remove(&request_id);
            return gateway_timeout_page();
        }
    };

    match peek_proto::deserialize_response(&response_data) {
        Ok(resp) => {
            let mut builder = Response::builder().status(resp.status);
            for (k, v) in &resp.headers {
                builder = builder.header(k, v);
            }
            builder
                .body(Body::from(resp.body))
                .unwrap_or_else(|_| bad_gateway_page())
        }
        Err(e) => {
            error!(error = %e, "failed to deserialize tunnel response");
            bad_gateway_page()
        }
    }
}

/// Extract client IP from trusted proxy headers.
/// Priority: CF-Connecting-IP > X-Forwarded-For > X-Real-IP
fn extract_client_ip(headers: &HeaderMap) -> Option<IpAddr> {
    if let Some(ip) = headers
        .get("cf-connecting-ip")
        .and_then(|v| v.to_str().ok())
    {
        if let Ok(addr) = ip.parse() {
            return Some(addr);
        }
    }
    // X-Forwarded-For (first entry is the original client)
    if let Some(forwarded) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        if let Some(first) = forwarded.split(',').next() {
            if let Ok(addr) = first.trim().parse() {
                return Some(addr);
            }
        }
    }
    // X-Real-IP
    if let Some(ip) = headers.get("x-real-ip").and_then(|v| v.to_str().ok()) {
        if let Ok(addr) = ip.parse() {
            return Some(addr);
        }
    }
    None
}

fn generate_auth_cookie(password: &str, subdomain: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(password.as_bytes()).expect("HMAC can take key of any size");
    mac.update(subdomain.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn password_page(subdomain: &str, error: Option<&str>) -> Response {
    let error_html = match error {
        Some(msg) => format!(r#"<p style="color:#e53e3e;margin-bottom:16px">{msg}</p>"#),
        None => String::new(),
    };
    let html = format!(
        r#"<!DOCTYPE html>
<html><head>
<title>Password Required — {subdomain}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body style="font-family:system-ui,-apple-system,sans-serif;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;background:#f5f5f5">
<div style="background:white;padding:40px;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,0.08);max-width:360px;width:100%;text-align:center">
<h2 style="margin:0 0 8px">🔒 Password Required</h2>
<p style="color:#666;margin:0 0 24px;font-size:14px">This tunnel is protected</p>
{error_html}
<form method="POST" action="/__peek_auth">
<input type="password" name="password" placeholder="Enter password" required autofocus
  style="width:100%;padding:12px;border:1px solid #ddd;border-radius:8px;font-size:16px;box-sizing:border-box;margin-bottom:12px">
<button type="submit"
  style="width:100%;padding:12px;background:#111;color:white;border:none;border-radius:8px;font-size:16px;cursor:pointer">
  Continue
</button>
</form>
</div></body></html>"#
    );
    Html(html).into_response()
}

fn extract_subdomain(host: &str, domain: &str) -> Option<String> {
    let host_no_port = host.split(':').next().unwrap_or(host);
    let suffix = format!(".{domain}");
    if host_no_port.ends_with(&suffix) {
        let sub = &host_no_port[..host_no_port.len() - suffix.len()];
        if !sub.is_empty() && !sub.contains('.') {
            return Some(sub.to_string());
        }
    }
    None
}

fn not_found_page() -> Response {
    let mut resp = Html(
        r#"<!DOCTYPE html>
<html><head><title>Tunnel Not Found</title></head>
<body style="font-family:system-ui;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;background:#f5f5f5">
<div style="text-align:center">
<h2>Tunnel Not Found</h2>
<p style="color:#666">This tunnel doesn't exist or has been disconnected.</p>
</div></body></html>"#,
    )
    .into_response();
    *resp.status_mut() = axum::http::StatusCode::NOT_FOUND;
    resp
}

fn bad_gateway_page() -> Response {
    let mut resp = Html(
        r#"<!DOCTYPE html>
<html><head><title>Bad Gateway</title></head>
<body style="font-family:system-ui;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;background:#f5f5f5">
<div style="text-align:center">
<h2>502 Bad Gateway</h2>
<p style="color:#666">The tunnel client is unreachable.</p>
</div></body></html>"#,
    )
    .into_response();
    *resp.status_mut() = axum::http::StatusCode::BAD_GATEWAY;
    resp
}

fn gateway_timeout_page() -> Response {
    let mut resp = Html(
        r#"<!DOCTYPE html>
<html><head><title>Gateway Timeout</title></head>
<body style="font-family:system-ui;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;background:#f5f5f5">
<div style="text-align:center">
<h2>504 Gateway Timeout</h2>
<p style="color:#666">The local server didn't respond in time.</p>
</div></body></html>"#,
    )
    .into_response();
    *resp.status_mut() = axum::http::StatusCode::GATEWAY_TIMEOUT;
    resp
}

fn payload_too_large_page() -> Response {
    let mut resp = Html(
        r#"<!DOCTYPE html>
<html><head><title>Payload Too Large</title></head>
<body style="font-family:system-ui;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;background:#f5f5f5">
<div style="text-align:center">
<h2>413 Payload Too Large</h2>
<p style="color:#666">Request body exceeds the size limit.</p>
</div></body></html>"#,
    )
    .into_response();
    *resp.status_mut() = axum::http::StatusCode::PAYLOAD_TOO_LARGE;
    resp
}

fn too_many_requests_page() -> Response {
    let mut resp = Html(
        r#"<!DOCTYPE html>
<html><head><title>Too Many Requests</title></head>
<body style="font-family:system-ui;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;background:#f5f5f5">
<div style="text-align:center">
<h2>429 Too Many Requests</h2>
<p style="color:#666">You are sending too many requests. Please slow down.</p>
</div></body></html>"#,
    )
    .into_response();
    *resp.status_mut() = axum::http::StatusCode::TOO_MANY_REQUESTS;
    resp
}

fn service_unavailable_page() -> Response {
    let mut resp = Html(
        r#"<!DOCTYPE html>
<html><head><title>Service Unavailable</title></head>
<body style="font-family:system-ui;display:flex;justify-content:center;align-items:center;min-height:100vh;margin:0;background:#f5f5f5">
<div style="text-align:center">
<h2>503 Service Unavailable</h2>
<p style="color:#666">The tunnel has too many pending requests. Please try again later.</p>
</div></body></html>"#,
    )
    .into_response();
    *resp.status_mut() = axum::http::StatusCode::SERVICE_UNAVAILABLE;
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_subdomain() {
        assert_eq!(
            extract_subdomain("a8f3k2.example.com", "example.com"),
            Some("a8f3k2".into())
        );
        assert_eq!(
            extract_subdomain("a8f3k2.example.com:8080", "example.com"),
            Some("a8f3k2".into())
        );
        assert_eq!(extract_subdomain("example.com", "example.com"), None);
        assert_eq!(extract_subdomain("example.com:8080", "example.com"), None);
        assert_eq!(extract_subdomain("other.com", "example.com"), None);
        assert_eq!(
            extract_subdomain("sub.nested.example.com", "example.com"),
            None
        );
    }

    #[test]
    fn test_extract_client_ip() {
        let mut headers = HeaderMap::new();
        assert_eq!(extract_client_ip(&headers), None);

        // CF-Connecting-IP takes priority
        headers.insert("cf-connecting-ip", "1.2.3.4".parse().unwrap());
        headers.insert("x-forwarded-for", "5.6.7.8, 9.10.11.12".parse().unwrap());
        assert_eq!(
            extract_client_ip(&headers),
            Some("1.2.3.4".parse().unwrap())
        );

        // Falls back to X-Forwarded-For (first IP)
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "5.6.7.8, 9.10.11.12".parse().unwrap());
        assert_eq!(
            extract_client_ip(&headers),
            Some("5.6.7.8".parse().unwrap())
        );

        // Falls back to X-Real-IP
        let mut headers = HeaderMap::new();
        headers.insert("x-real-ip", "10.0.0.1".parse().unwrap());
        assert_eq!(
            extract_client_ip(&headers),
            Some("10.0.0.1".parse().unwrap())
        );
    }
}
