use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtoError {
    #[error("frame too short: need at least 4 bytes, got {0}")]
    FrameTooShort(usize),
    #[error("invalid HTTP data: {0}")]
    InvalidHttp(String),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

pub fn encode_frame(request_id: u32, data: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(4 + data.len());
    frame.extend_from_slice(&request_id.to_be_bytes());
    frame.extend_from_slice(data);
    frame
}

pub fn decode_frame(frame: &[u8]) -> Result<(u32, &[u8]), ProtoError> {
    if frame.len() < 4 {
        return Err(ProtoError::FrameTooShort(frame.len()));
    }
    let id = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]);
    Ok((id, &frame[4..]))
}

const CRLF: &[u8] = b"\r\n";
const HEADER_END: &[u8] = b"\r\n\r\n";
const MAX_HEADERS: usize = 100;

pub fn serialize_request(
    method: &str,
    uri: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(method.as_bytes());
    buf.push(b' ');
    buf.extend_from_slice(uri.as_bytes());
    buf.extend_from_slice(CRLF);
    for (k, v) in headers {
        buf.extend_from_slice(k.as_bytes());
        buf.extend_from_slice(b": ");
        buf.extend_from_slice(v.as_bytes());
        buf.extend_from_slice(CRLF);
    }
    buf.extend_from_slice(CRLF);
    buf.extend_from_slice(body);
    buf
}

pub struct DeserializedRequest {
    pub method: String,
    pub uri: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

pub fn deserialize_request(data: &[u8]) -> Result<DeserializedRequest, ProtoError> {
    let (head, body) = split_head_body(data)?;
    let mut lines = head.split(|&b| b == b'\n');

    let request_line = lines
        .next()
        .ok_or_else(|| ProtoError::InvalidHttp("missing request line".into()))?;
    let request_line = strip_cr(request_line);
    let space = request_line
        .iter()
        .position(|&b| b == b' ')
        .ok_or_else(|| ProtoError::InvalidHttp("no space in request line".into()))?;
    let method = String::from_utf8_lossy(&request_line[..space]).to_string();
    let uri = String::from_utf8_lossy(&request_line[space + 1..]).to_string();

    let headers = parse_headers(lines);

    Ok(DeserializedRequest {
        method,
        uri,
        headers,
        body: body.to_vec(),
    })
}

pub fn serialize_response(status: u16, headers: &[(String, String)], body: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(status.to_string().as_bytes());
    buf.extend_from_slice(CRLF);
    for (k, v) in headers {
        buf.extend_from_slice(k.as_bytes());
        buf.extend_from_slice(b": ");
        buf.extend_from_slice(v.as_bytes());
        buf.extend_from_slice(CRLF);
    }
    buf.extend_from_slice(CRLF);
    buf.extend_from_slice(body);
    buf
}

pub struct DeserializedResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

pub fn deserialize_response(data: &[u8]) -> Result<DeserializedResponse, ProtoError> {
    let (head, body) = split_head_body(data)?;
    let mut lines = head.split(|&b| b == b'\n');

    let status_line = lines
        .next()
        .ok_or_else(|| ProtoError::InvalidHttp("missing status line".into()))?;
    let status_line = strip_cr(status_line);
    let status_str = String::from_utf8_lossy(status_line);
    let status: u16 = status_str
        .trim()
        .parse()
        .map_err(|_| ProtoError::InvalidHttp(format!("invalid status code: {status_str}")))?;

    let headers = parse_headers(lines);

    Ok(DeserializedResponse {
        status,
        headers,
        body: body.to_vec(),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistrationRequest {
    pub subdomain: Option<String>,
    pub token: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistrationResponse {
    pub ok: bool,
    pub url: String,
    pub subdomain: String,
    pub error: Option<String>,
}

pub mod close_codes {
    pub const TUNNEL_EXPIRED: u16 = 4000;
    pub const TUNNEL_EVICTED: u16 = 4001;
    pub const AUTH_FAILED: u16 = 4002;
    pub const CAPACITY_FULL: u16 = 4003;

    pub fn is_permanent(code: u16) -> bool {
        matches!(
            code,
            TUNNEL_EXPIRED | TUNNEL_EVICTED | AUTH_FAILED | CAPACITY_FULL
        )
    }
}

fn split_head_body(data: &[u8]) -> Result<(&[u8], &[u8]), ProtoError> {
    if let Some(pos) = data.windows(HEADER_END.len()).position(|w| w == HEADER_END) {
        Ok((&data[..pos], &data[pos + HEADER_END.len()..]))
    } else {
        Ok((data, &[]))
    }
}

fn strip_cr(line: &[u8]) -> &[u8] {
    if line.last() == Some(&b'\r') {
        &line[..line.len() - 1]
    } else {
        line
    }
}

fn parse_headers<'a>(lines: impl Iterator<Item = &'a [u8]>) -> Vec<(String, String)> {
    let mut headers = Vec::new();
    for line in lines {
        if headers.len() >= MAX_HEADERS {
            break;
        }
        let line = strip_cr(line);
        if line.is_empty() {
            continue;
        }
        if let Some(colon) = line.iter().position(|&b| b == b':') {
            let key = String::from_utf8_lossy(&line[..colon]).trim().to_string();
            let val = String::from_utf8_lossy(&line[colon + 1..])
                .trim()
                .to_string();
            headers.push((key, val));
        }
    }
    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip() {
        let data = b"hello world";
        let frame = encode_frame(42, data);
        let (id, payload) = decode_frame(&frame).unwrap();
        assert_eq!(id, 42);
        assert_eq!(payload, data);
    }

    #[test]
    fn frame_max_id() {
        let frame = encode_frame(u32::MAX, b"x");
        let (id, payload) = decode_frame(&frame).unwrap();
        assert_eq!(id, u32::MAX);
        assert_eq!(payload, b"x");
    }

    #[test]
    fn frame_too_short() {
        assert!(decode_frame(&[0, 1, 2]).is_err());
        assert!(decode_frame(&[]).is_err());
    }

    #[test]
    fn frame_empty_payload() {
        let frame = encode_frame(1, b"");
        let (id, payload) = decode_frame(&frame).unwrap();
        assert_eq!(id, 1);
        assert!(payload.is_empty());
    }

    #[test]
    fn request_roundtrip() {
        let headers = vec![
            ("Content-Type".into(), "text/plain".into()),
            ("X-Custom".into(), "value".into()),
        ];
        let body = b"request body";
        let serialized = serialize_request("POST", "/api/data", &headers, body);
        let req = deserialize_request(&serialized).unwrap();
        assert_eq!(req.method, "POST");
        assert_eq!(req.uri, "/api/data");
        assert_eq!(req.headers.len(), 2);
        assert_eq!(req.headers[0], ("Content-Type".into(), "text/plain".into()));
        assert_eq!(req.headers[1], ("X-Custom".into(), "value".into()));
        assert_eq!(req.body, body);
    }

    #[test]
    fn request_no_body() {
        let serialized = serialize_request("GET", "/", &[], b"");
        let req = deserialize_request(&serialized).unwrap();
        assert_eq!(req.method, "GET");
        assert_eq!(req.uri, "/");
        assert!(req.headers.is_empty());
        assert!(req.body.is_empty());
    }

    #[test]
    fn response_roundtrip() {
        let headers = vec![("Content-Type".into(), "application/json".into())];
        let body = b"{\"ok\":true}";
        let serialized = serialize_response(200, &headers, body);
        let resp = deserialize_response(&serialized).unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.headers.len(), 1);
        assert_eq!(
            resp.headers[0],
            ("Content-Type".into(), "application/json".into())
        );
        assert_eq!(resp.body, body);
    }

    #[test]
    fn response_no_body() {
        let serialized = serialize_response(204, &[], b"");
        let resp = deserialize_response(&serialized).unwrap();
        assert_eq!(resp.status, 204);
        assert!(resp.headers.is_empty());
        assert!(resp.body.is_empty());
    }

    #[test]
    fn close_codes_permanent() {
        assert!(crate::close_codes::is_permanent(4000));
        assert!(crate::close_codes::is_permanent(4001));
        assert!(crate::close_codes::is_permanent(4002));
        assert!(crate::close_codes::is_permanent(4003));
        assert!(!crate::close_codes::is_permanent(1000));
        assert!(!crate::close_codes::is_permanent(1001));
    }

    #[test]
    fn registration_json_roundtrip() {
        let req = RegistrationRequest {
            subdomain: Some("myapp".into()),
            token: None,
            password: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: RegistrationRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.subdomain.as_deref(), Some("myapp"));
        assert!(decoded.token.is_none());
        assert!(decoded.password.is_none());

        let resp = RegistrationResponse {
            ok: true,
            url: "https://myapp.example.com".into(),
            subdomain: "myapp".into(),
            error: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: RegistrationResponse = serde_json::from_str(&json).unwrap();
        assert!(decoded.ok);
        assert_eq!(decoded.url, "https://myapp.example.com");
    }
}
