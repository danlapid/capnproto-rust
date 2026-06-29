# capnp-http

`http-over-capnp` for Rust: a bridge between HTTP services and the Cap'n Proto
[`HttpService`](https://github.com/capnproto/capnproto/blob/master/c%2B%2B/src/capnp/compat/http-over-capnp.capnp)
RPC protocol. This is the Rust counterpart of the C++
`capnp/compat/http-over-capnp.{h,c++}` library.

The core is **HTTP-framework-agnostic**: it is written against the crate's own
[`HttpService`] trait and the neutral interchange crates (`http`, `http-body`,
`bytes`), not against any particular HTTP library. Framework adapters ship
behind feature flags, and any other library can be supported by implementing
`HttpService` for its service type. It is built on top of
[`capnp-byte-stream`](../capnp-byte-stream), which carries HTTP bodies.

## Plugging in a framework

Expose a service over Cap'n Proto with `service_to_capnp`, and consume a remote
capability as an `HttpService` with `capnp_to_service`:

```rust,ignore
// Server: expose any `HttpService` over Cap'n Proto.
let cap: http_service::Client = capnp_http::service_to_capnp(my_service);

// Client: call a remote HttpService. The spawner drives a background
// per-request task (e.g. `tokio::task::spawn_local`).
let svc: capnp_http::CapnpHttpService =
    capnp_http::capnp_to_service(cap, |f| { tokio::task::spawn_local(f); });
let response = svc.call(request).await?; // capnp_http::HttpService
```

`my_service` can be anything implementing [`HttpService`]. Adapters make the
common ecosystems drop-in:

| feature | what you get |
| --- | --- |
| `hyper` *(default)* | a blanket impl so every `hyper::service::Service` **is** an `HttpService`, plus `HyperService`, which wraps any `HttpService` back into a `hyper::service::Service` (for a hyper server/client). |
| `tower` | `TowerService`, wrapping any `tower::Service` — so `tower` middleware stacks / `ServiceBuilder`, `axum::Router`, `tonic`, etc. can be served over capnp. |

Disable default features for a framework-free core (`default-features = false`)
and either enable an adapter or implement `HttpService` yourself. See the
`pluggable_frameworks` example for `tower` and `axum` served over capnp, and
`inmemory_roundtrip` for a hyper round-trip.

## Streaming and lifecycle

Request and response bodies stream with backpressure; the received body is
`capnp_http::IncomingBody` (an `http_body::Body`). Dropping the response (or the
`call()` future) cancels the in-flight `request()`. Like `capnp-rpc`, the whole
stack is single-threaded (`!Send`); run it on a `tokio::task::LocalSet`. Because
it is `!Send`, `Send`-requiring frameworks (e.g. `axum`) need a small streaming
shim at their boundary — a *bounded stream*, never a full-body buffer; the
`pluggable_frameworks` example shows one.

## WebSocket (`websocket` feature)

Bridges the capnp `WebSocket` interface to `tungstenite::Message`
(compatible with `hyper-tungstenite` / `tokio-tungstenite`): on the server,
`accept_websocket()` returns an upgrade token to place in a 101 response's
extensions plus a pending `WebSocket` (a `Stream<Message>` + `Sink<Message>`);
on the client, use `svc.open_websocket(request)`.

## CONNECT (`connect` feature)

Bridges the capnp `connect()` method to a `Tunnel` (`AsyncRead` + `AsyncWrite`):
on the client, `svc.connect(host, headers, settings)`; on the server, implement
`ConnectService` and use `connect_service_to_capnp()`, or
`service_to_capnp_with_connect()` to serve both requests and CONNECT on one
capability. The `startTls` signal is carried across the tunnel; performing the
actual TLS handshake is left to the application.

## Limitations

Only the modern `request()` method is implemented (not the deprecated
`startRequest()`), matching current C++. HTTP trailers are dropped, as the capnp
`ByteStream` protocol cannot represent them. `ByteStream` path-shortening is not
implemented.
