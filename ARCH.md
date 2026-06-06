# Architecture

peek is a localhost-to-public proxy.

The product is easiest to describe as a proxy: public HTTP traffic enters the server, crosses a WebSocket tunnel, and exits through the client to a local port.

Internally, the server binary is called `peek-relay` because it relays framed messages between the public side and the local side.

## Flow

```text
Visitor HTTP request
  -> peek-relay public server
  -> WebSocket frame
  -> peek-client
  -> localhost app
  -> peek-client
  -> WebSocket frame
  -> peek-relay public server
  -> Visitor HTTP response
```

## Crates

| Crate | Type | Purpose |
| --- | --- | --- |
| `peek-proto` | Library | Shared binary frame format, HTTP serialization, registration types, close codes |
| `peek-client` | Library and CLI | Opens the tunnel and forwards requests to `127.0.0.1:{port}` |
| `peek-relay` | Binary | Accepts tunnel connections and proxies public HTTP requests |

## Tunnel Setup

1. The client opens a WebSocket connection to `/tunnel`.
2. The client sends a JSON registration request with optional subdomain, auth token, and tunnel password.
3. The server validates the auth token when `RELAY_AUTH_TOKEN` is set.
4. The server requires a tunnel password.
5. The server assigns a subdomain and stores the tunnel connection.
6. The server responds with the public URL.

## Request Proxying

Each proxied request uses a binary frame:

```text
[4 bytes request_id][serialized HTTP request or response]
```

The request ID lets multiple HTTP requests share one WebSocket connection at the same time.

## HTTP Serialization

Requests:

```text
METHOD URI
Header: Value

body
```

Responses:

```text
STATUS_CODE
Header: Value

body
```

The wire format is intentionally simple and owned by `peek-proto`.

## Password Gate

When a visitor opens a protected tunnel URL:

1. The server checks for a valid tunnel cookie.
2. If the cookie is missing, the server serves a password form.
3. A correct password sets a 24-hour HTTP-only cookie.
4. Requests with a valid cookie are proxied to the tunnel.

## Limits

- WebSocket frames are capped by `MAX_BODY_SIZE_MB`.
- HTTP request bodies are capped by `MAX_BODY_SIZE_MB`.
- Each tunnel has a maximum number of pending requests.
- Tunnel responses time out after 30 seconds.
- The server enforces a maximum active tunnel count.

## Close Codes

| Code | Name | Meaning |
| --- | --- | --- |
| `4000` | `TUNNEL_EXPIRED` | Tunnel expired |
| `4001` | `TUNNEL_EVICTED` | Tunnel was removed |
| `4002` | `AUTH_FAILED` | Auth token failed |
| `4003` | `CAPACITY_FULL` | Server is at capacity |

These close codes are treated as permanent by the client.
