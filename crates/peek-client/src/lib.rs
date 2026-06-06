use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use peek_proto::{RegistrationRequest, RegistrationResponse};

const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "host",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

pub struct TunnelClient {
    server_url: String,
    token: Option<String>,
    password: Option<String>,
    http_client: reqwest::Client,
}

pub struct TunnelHandle {
    pub url: String,
    write_tx: Option<mpsc::Sender<Message>>,
}

#[derive(Debug, thiserror::Error)]
pub enum TunnelError {
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("registration failed: {0}")]
    Registration(String),
    #[error("URL parse error: {0}")]
    Url(#[from] url::ParseError),
    #[error("HTTP client error: {0}")]
    HttpClient(#[from] reqwest::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("connection closed before registration")]
    ConnectionClosed,
}

impl TunnelClient {
    pub fn new(server_url: &str) -> Result<Self, TunnelError> {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .pool_max_idle_per_host(10)
            .build()?;

        Ok(Self {
            server_url: server_url.to_string(),
            token: None,
            password: None,
            http_client,
        })
    }

    #[must_use]
    pub fn with_token(mut self, token: String) -> Self {
        self.token = Some(token);
        self
    }

    #[must_use]
    pub fn with_password(mut self, password: String) -> Self {
        self.password = Some(password);
        self
    }

    pub async fn connect(&self, port: u16) -> Result<TunnelHandle, TunnelError> {
        self.connect_with_subdomain(port, None).await
    }

    pub async fn connect_with_subdomain(
        &self,
        port: u16,
        subdomain: Option<String>,
    ) -> Result<TunnelHandle, TunnelError> {
        let _parsed = url::Url::parse(&self.server_url)?;

        let (public_url, write_tx) = self.do_first_connection(port, subdomain.as_ref()).await?;

        Ok(TunnelHandle {
            url: public_url,
            write_tx: Some(write_tx),
        })
    }

    async fn do_first_connection(
        &self,
        port: u16,
        subdomain: Option<&String>,
    ) -> Result<(String, mpsc::Sender<Message>), TunnelError> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(&self.server_url).await?;
        let (mut sink, mut stream) = ws_stream.split();

        let reg = RegistrationRequest {
            subdomain: subdomain.cloned(),
            token: self.token.clone(),
            password: self.password.clone(),
        };
        let json = serde_json::to_string(&reg)?;
        sink.send(Message::Text(json.into())).await?;

        let resp: RegistrationResponse = match stream.next().await {
            Some(Ok(Message::Text(text))) => {
                serde_json::from_str(&text).map_err(|e| TunnelError::Registration(e.to_string()))?
            }
            Some(Ok(_)) => {
                return Err(TunnelError::Registration("expected text message".into()));
            }
            Some(Err(e)) => return Err(TunnelError::WebSocket(e)),
            None => return Err(TunnelError::ConnectionClosed),
        };

        if !resp.ok {
            return Err(TunnelError::Registration(
                resp.error.unwrap_or_else(|| "unknown error".into()),
            ));
        }

        let public_url = resp.url;
        info!(url = %public_url, "tunnel established");

        let (write_tx, write_rx) = mpsc::channel::<Message>(256);

        spawn_connection_tasks(
            sink,
            stream,
            write_tx.clone(),
            write_rx,
            port,
            &self.http_client,
        );

        Ok((public_url, write_tx))
    }
}

fn spawn_connection_tasks<S, K>(
    mut sink: S,
    mut stream: K,
    write_tx: mpsc::Sender<Message>,
    mut write_rx: mpsc::Receiver<Message>,
    port: u16,
    http_client: &reqwest::Client,
) where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error>
        + Unpin
        + Send
        + 'static,
    K: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin
        + Send
        + 'static,
{
    tokio::spawn(async move {
        while let Some(msg) = write_rx.recv().await {
            if sink.send(msg).await.is_err() {
                break;
            }
        }
    });

    let write_tx_read = write_tx.clone();
    let http_client = http_client.clone();
    tokio::spawn(async move {
        loop {
            let msg = match stream.next().await {
                Some(Ok(m)) => m,
                Some(Err(e)) => {
                    warn!(error = %e, "ws read error");
                    break;
                }
                None => break,
            };

            match msg {
                Message::Binary(data) => {
                    let write_tx = write_tx_read.clone();
                    let http_client = http_client.clone();
                    tokio::spawn(async move {
                        handle_request(&data, port, &http_client, &write_tx).await;
                    });
                }
                Message::Ping(data) => {
                    let _ = write_tx_read.send(Message::Pong(data)).await;
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    });

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(20));
        loop {
            interval.tick().await;
            if write_tx.send(Message::Ping(vec![].into())).await.is_err() {
                break;
            }
        }
    });
}

impl TunnelHandle {
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    pub async fn close(mut self) {
        if let Some(tx) = self.write_tx.take() {
            let _ = tx.send(Message::Close(None)).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

impl Drop for TunnelHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.write_tx.take() {
            let _ = tx.try_send(Message::Close(None));
        }
    }
}

async fn handle_request(
    data: &[u8],
    port: u16,
    http_client: &reqwest::Client,
    write_tx: &mpsc::Sender<Message>,
) {
    let (request_id, payload) = match peek_proto::decode_frame(data) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "failed to decode frame");
            return;
        }
    };

    let req = match peek_proto::deserialize_request(payload) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "failed to deserialize request");
            send_error_response(request_id, 400, "Bad Request", write_tx).await;
            return;
        }
    };

    let local_url = format!("http://127.0.0.1:{port}{}", req.uri);

    let Ok(method) = req.method.parse::<reqwest::Method>() else {
        send_error_response(request_id, 400, "Invalid method", write_tx).await;
        return;
    };

    let mut builder = http_client.request(method, &local_url);

    for (k, v) in &req.headers {
        if is_hop_by_hop_header(k) {
            continue;
        }
        builder = builder.header(k, v);
    }

    if !req.body.is_empty() {
        builder = builder.body(req.body.clone());
    }

    let response = match builder.send().await {
        Ok(r) => r,
        Err(e) if e.is_timeout() => {
            warn!(error = %e, url = %local_url, "local server timed out");
            send_error_response(
                request_id,
                504,
                &format!(
                    "<html><body style=\"font-family:system-ui;text-align:center;padding:40px\">\
                     <h1>504 Gateway Timeout</h1>\
                     <p>localhost:{port} did not respond within 30 seconds</p></body></html>"
                ),
                write_tx,
            )
            .await;
            return;
        }
        Err(e) => {
            warn!(error = %e, url = %local_url, "failed to reach local server");
            send_error_response(
                request_id,
                502,
                &format!(
                    "<html><body style=\"font-family:system-ui;text-align:center;padding:40px\">\
                     <h1>502 Bad Gateway</h1>\
                     <p>Could not connect to localhost:{port}</p>\
                     <p style=\"color:#999\">{e}</p></body></html>"
                ),
                write_tx,
            )
            .await;
            return;
        }
    };

    let status = response.status().as_u16();
    let resp_headers: Vec<(String, String)> = response
        .headers()
        .iter()
        .filter(|(k, _)| !is_hop_by_hop_header(k.as_str()))
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();
    let body = response.bytes().await.unwrap_or_default();

    let serialized = peek_proto::serialize_response(status, &resp_headers, &body);
    let frame = peek_proto::encode_frame(request_id, &serialized);
    let _ = write_tx.send(Message::Binary(frame.into())).await;
}

async fn send_error_response(
    request_id: u32,
    status: u16,
    body: &str,
    write_tx: &mpsc::Sender<Message>,
) {
    let headers = vec![("content-type".into(), "text/html".into())];
    let serialized = peek_proto::serialize_response(status, &headers, body.as_bytes());
    let frame = peek_proto::encode_frame(request_id, &serialized);
    let _ = write_tx.send(Message::Binary(frame.into())).await;
}

fn is_hop_by_hop_header(name: &str) -> bool {
    HOP_BY_HOP_HEADERS
        .iter()
        .any(|header| name.eq_ignore_ascii_case(header))
}
