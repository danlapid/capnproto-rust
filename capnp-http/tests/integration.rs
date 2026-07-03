// Copyright (c) 2026 the capnproto-rust contributors
// Licensed under the MIT License:
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#![cfg(feature = "hyper")]

//! Full-stack integration tests for http-over-capnp using the `hyper` adapter,
//! in the "edge proxy" topology:
//!
//!   real hyper client --TCP/HTTP1--> [edge: hyper http1 server running
//!   capnp_to_service] --capnp--> [backend: service_to_capnp(hyper service)]
//!
//! Every request goes through hyper's real HTTP/1 codec on both ends, with the
//! Cap'n Proto RPC layer in the middle.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body::Frame;
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Empty, Full, StreamBody};
use hyper::body::Incoming;
use hyper::service::Service;
use hyper_util::rt::TokioIo;

use capnp_http::http_over_capnp_capnp::http_service;
use capnp_http::{capnp_to_service, service_to_capnp, HyperService, IncomingBody};
use capnp_rpc::{rpc_twoparty_capnp::Side, twoparty, RpcSystem};
use futures::FutureExt;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BackendBody = UnsyncBoxBody<Bytes, Infallible>;

fn pattern(n: usize) -> Bytes {
    Bytes::from((0..n).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
}

fn empty_body() -> BackendBody {
    Empty::<Bytes>::new().map_err(|e| match e {}).boxed_unsync()
}
fn full_body(b: Bytes) -> BackendBody {
    Full::new(b).boxed_unsync()
}

// The backend hyper service, routed by path.
#[derive(Clone)]
struct Backend;

impl Service<Request<IncomingBody>> for Backend {
    type Response = Response<BackendBody>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, BoxError>>>>;

    fn call(&self, req: Request<IncomingBody>) -> Self::Future {
        Box::pin(handle_backend(req))
    }
}

async fn handle_backend(req: Request<IncomingBody>) -> Result<Response<BackendBody>, BoxError> {
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_owned())
        .unwrap_or_default();
    let req_headers = req.headers().clone();

    // Routes that must not read the body up front.
    match path.as_str() {
        // Echo the exact request target (path + query) to verify it is preserved.
        "/uri" => {
            return Ok(Response::builder()
                .status(200)
                .body(full_body(Bytes::from(path_and_query)))?);
        }
        "/ignore" => {
            return Ok(Response::builder()
                .status(200)
                .body(full_body(Bytes::from_static(b"ignored")))?);
        }
        "/error" => {
            return Err("backend deliberately failed".into());
        }
        "/headers" => {
            // Echo every request header back, prefixed, to verify propagation.
            let mut builder = Response::builder().status(200);
            for (name, value) in req_headers.iter() {
                let echoed = format!("x-echo-{name}");
                builder = builder.header(echoed, value.clone());
            }
            return Ok(builder.body(empty_body())?);
        }
        "/interleave" => {
            use futures::{channel::mpsc, SinkExt};
            let (mut tx, rx) = mpsc::channel::<Result<Frame<Bytes>, Infallible>>(1);
            let mut body = req.into_body();
            tokio::task::spawn_local(async move {
                while let Some(Ok(frame)) = body.frame().await {
                    if let Ok(data) = frame.into_data() {
                        let up: Bytes = data.iter().map(|b| b.to_ascii_uppercase()).collect();
                        if tx.send(Ok(Frame::data(up))).await.is_err() {
                            break;
                        }
                    }
                }
            });
            return Ok(Response::builder()
                .status(200)
                .body(StreamBody::new(rx).boxed_unsync())?);
        }
        _ => {}
    }

    // Routes that consume the body.
    let body = req.into_body().collect().await?.to_bytes();

    if let Some(rest) = path.strip_prefix("/status/") {
        let code: u16 = rest.parse().unwrap_or(200);
        let status = StatusCode::from_u16(code)?;
        // 204/205/304 and HEAD must have empty bodies; otherwise echo a marker.
        let no_body = matches!(code, 204 | 205 | 304) || method == Method::HEAD;
        let b = if no_body {
            empty_body()
        } else {
            full_body(Bytes::from(format!("status {code}")))
        };
        return Ok(Response::builder().status(status).body(b)?);
    }

    if let Some(rest) = path.strip_prefix("/bytes/") {
        let n: usize = rest.parse().unwrap_or(0);
        return Ok(Response::builder()
            .status(200)
            .body(full_body(pattern(n)))?);
    }

    if let Some(rest) = path.strip_prefix("/chunked/") {
        let n: usize = rest.parse().unwrap_or(0);
        let data = pattern(n);
        let chunk = (n / 4).max(1);
        let mut frames = vec![];
        let mut off = 0;
        while off < data.len() {
            let end = (off + chunk).min(data.len());
            frames.push(Ok::<_, Infallible>(Frame::data(data.slice(off..end))));
            off = end;
        }
        return Ok(Response::builder()
            .status(200)
            .body(StreamBody::new(futures::stream::iter(frames)).boxed_unsync())?);
    }

    if path == "/echo" {
        let mut builder = Response::builder().status(200);
        if let Some(ct) = req_headers.get("content-type") {
            builder = builder.header("content-type", ct.clone());
        }
        return Ok(builder.body(full_body(body))?);
    }

    Ok(Response::builder()
        .status(200)
        .body(full_body(Bytes::from_static(b"ok")))?)
}

/// A running proxy: a hyper HTTP/1 edge (backed by capnp -> backend) on a TCP port.
struct Proxy {
    addr: SocketAddr,
}

impl Proxy {
    /// Sends one request over a fresh HTTP/1 connection and returns the response.
    async fn send<B>(&self, req: Request<B>) -> hyper::Result<Response<Incoming>>
    where
        B: http_body::Body + 'static,
        B::Data: bytes::Buf + Send,
        B::Error: Into<BoxError>,
    {
        let stream = tokio::net::TcpStream::connect(self.addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        tokio::task::spawn_local(async move {
            let _ = conn.with_upgrades().await;
        });
        sender.send_request(req).await
    }
}

/// Spins up backend + edge on the current `LocalSet` and returns the edge address.
async fn start_proxy() -> Proxy {
    // In-memory capnp link between edge (client) and backend (server).
    let (to_backend_w, to_backend_r) = async_byte_channel::channel();
    let (to_edge_w, to_edge_r) = async_byte_channel::channel();

    let backend_net = Box::new(twoparty::VatNetwork::new(
        to_backend_r,
        to_edge_w,
        Side::Server,
        Default::default(),
    ));
    let backend_rpc = RpcSystem::new(backend_net, Some(service_to_capnp(Backend).client));

    let edge_net = Box::new(twoparty::VatNetwork::new(
        to_edge_r,
        to_backend_w,
        Side::Client,
        Default::default(),
    ));
    let mut edge_rpc = RpcSystem::new(edge_net, None);
    let http_client: http_service::Client = edge_rpc.bootstrap(Side::Server);

    tokio::task::spawn_local(backend_rpc.map(|_| ()));
    tokio::task::spawn_local(edge_rpc.map(|_| ()));

    let edge_service = capnp_to_service(http_client, |f| {
        tokio::task::spawn_local(f);
    });

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::task::spawn_local(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(x) => x,
                Err(_) => break,
            };
            stream.set_nodelay(true).ok();
            let svc = edge_service.clone();
            tokio::task::spawn_local(async move {
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), HyperService(svc))
                    .with_upgrades()
                    .await;
            });
        }
    });

    Proxy { addr }
}

/// Runs `body` on a current-thread runtime + LocalSet with a live proxy.
fn with_proxy<F, Fut>(body: F)
where
    F: FnOnce(Proxy) -> Fut + 'static,
    Fut: Future<Output = ()>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        let proxy = start_proxy().await;
        body(proxy).await;
    });
}

async fn collect(resp: Response<Incoming>) -> (StatusCode, http::HeaderMap, Bytes) {
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, headers, bytes)
}

#[test]
fn post_echo() {
    with_proxy(|proxy| async move {
        let payload = Bytes::from_static(b"hello integration");
        let resp = proxy
            .send(
                Request::builder()
                    .method(Method::POST)
                    .uri("/echo")
                    .header("content-type", "application/octet-stream")
                    .body(Full::new(payload.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, headers, body) = collect(resp).await;
        assert_eq!(status, 200);
        assert_eq!(
            headers.get("content-type").unwrap(),
            "application/octet-stream"
        );
        assert_eq!(body, payload);

        // An empty POST body takes a separate path (no request-body stream at all).
        let resp = proxy
            .send(
                Request::builder()
                    .method(Method::POST)
                    .uri("/echo")
                    .body(Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, _h, body) = collect(resp).await;
        assert_eq!(status, 200);
        assert!(body.is_empty());
    });
}

#[test]
fn methods_put_delete_patch_options() {
    with_proxy(|proxy| async move {
        for method in [Method::PUT, Method::DELETE, Method::PATCH, Method::OPTIONS] {
            let resp = proxy
                .send(
                    Request::builder()
                        .method(method.clone())
                        .uri("/echo")
                        .body(Full::new(Bytes::from_static(b"x")))
                        .unwrap(),
                )
                .await
                .unwrap();
            let (status, _h, body) = collect(resp).await;
            assert_eq!(status, 200, "method {method}");
            assert_eq!(&body[..], b"x", "method {method}");
        }
    });
}

#[test]
fn head_has_no_body() {
    with_proxy(|proxy| async move {
        // The /bytes route attaches a 1000-byte body regardless of method. For a
        // HEAD request the bridge must declare `bodySize fixed(0)` and not pump
        // the body, and the exchange must still complete without stalling on an
        // unconsumed body pump.
        let resp = proxy
            .send(
                Request::builder()
                    .method(Method::HEAD)
                    .uri("/bytes/1000")
                    .body(Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, _h, body) = collect(resp).await;
        assert_eq!(status, 200);
        assert!(body.is_empty());

        // The same route via GET still yields the body.
        let resp = proxy
            .send(
                Request::builder()
                    .method(Method::GET)
                    .uri("/bytes/1000")
                    .body(Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, _h, body) = collect(resp).await;
        assert_eq!(status, 200);
        assert_eq!(body, pattern(1000));
    });
}

#[test]
fn status_codes() {
    with_proxy(|proxy| async move {
        for code in [201u16, 202, 400, 404, 418, 500, 503] {
            let resp = proxy
                .send(
                    Request::builder()
                        .uri(format!("/status/{code}"))
                        .body(Empty::<Bytes>::new())
                        .unwrap(),
                )
                .await
                .unwrap();
            let (status, _h, body) = collect(resp).await;
            assert_eq!(status.as_u16(), code);
            assert_eq!(body, Bytes::from(format!("status {code}")));
        }
    });
}

#[test]
fn statuses_without_bodies() {
    // RFC 9110: 204, 205, and 304 responses never have content.
    with_proxy(|proxy| async move {
        for code in [204u16, 205, 304] {
            let resp = proxy
                .send(
                    Request::builder()
                        .uri(format!("/status/{code}"))
                        .body(Empty::<Bytes>::new())
                        .unwrap(),
                )
                .await
                .unwrap();
            let (status, _h, body) = collect(resp).await;
            assert_eq!(status.as_u16(), code);
            assert!(body.is_empty(), "status {code}");
        }
    });
}

#[test]
fn chunked_download() {
    with_proxy(|proxy| async move {
        let n = 100_003;
        let resp = proxy
            .send(
                Request::builder()
                    .uri(format!("/chunked/{n}"))
                    .body(Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, _h, body) = collect(resp).await;
        assert_eq!(status, 200);
        assert_eq!(body, pattern(n));
    });
}

#[test]
fn large_round_trip_echo() {
    with_proxy(|proxy| async move {
        let n = 2 * 1024 * 1024;
        let payload = pattern(n);
        let resp = proxy
            .send(
                Request::builder()
                    .method(Method::POST)
                    .uri("/echo")
                    .body(Full::new(payload.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, _h, body) = collect(resp).await;
        assert_eq!(status, 200);
        assert_eq!(body, payload);
    });
}

#[test]
fn header_round_trips() {
    // Custom, duplicated, non-UTF-8, empty-valued, and numerous headers must all
    // survive the trip through the capnp header encoding.
    with_proxy(|proxy| async move {
        let raw = &[0xe2u8, 0x98, 0x83]; // UTF-8 snowman bytes
        let mut builder = Request::builder()
            .uri("/headers")
            .header("x-custom-one", "alpha")
            .header("x-dup", "a")
            .header("x-dup", "b")
            .header("x-bin", http::HeaderValue::from_bytes(raw).unwrap())
            .header("x-empty", "");
        for i in 0..64u32 {
            builder = builder.header(format!("x-h-{i}"), format!("v{i}"));
        }
        let resp = proxy
            .send(builder.body(Empty::<Bytes>::new()).unwrap())
            .await
            .unwrap();
        let (status, headers, _b) = collect(resp).await;
        assert_eq!(status, 200);
        assert_eq!(headers.get("x-echo-x-custom-one").unwrap(), "alpha");
        let mut values: Vec<_> = headers
            .get_all("x-echo-x-dup")
            .iter()
            .map(|v| v.to_str().unwrap().to_owned())
            .collect();
        values.sort();
        assert_eq!(values, vec!["a".to_owned(), "b".to_owned()]);
        assert_eq!(headers.get("x-echo-x-bin").unwrap().as_bytes(), raw);
        assert_eq!(headers.get("x-echo-x-empty").unwrap().as_bytes(), b"");
        for i in 0..64u32 {
            assert_eq!(
                headers.get(format!("x-echo-x-h-{i}")).unwrap(),
                &format!("v{i}"),
                "header {i} did not round-trip"
            );
        }
    });
}

#[test]
fn server_ignores_request_body() {
    with_proxy(|proxy| async move {
        // The backend route /ignore never reads the request body even though the
        // client streams a large one. This must still complete cleanly.
        let resp = proxy
            .send(
                Request::builder()
                    .method(Method::POST)
                    .uri("/ignore")
                    .body(Full::new(pattern(1024 * 1024)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, _h, body) = collect(resp).await;
        assert_eq!(status, 200);
        assert_eq!(&body[..], b"ignored");
    });
}

#[test]
fn backend_service_error_aborts_connection() {
    with_proxy(|proxy| async move {
        let result = proxy
            .send(
                Request::builder()
                    .uri("/error")
                    .body(Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await;
        // A service error must surface as a failed exchange, not a bogus 200.
        match result {
            Err(_) => {}
            Ok(resp) => {
                let body = resp.into_body().collect().await;
                assert!(body.is_err(), "expected body error after service failure");
            }
        }
    });
}

#[test]
fn bidirectional_streaming_interleave() {
    use futures::{channel::mpsc, SinkExt};

    /// Reads body frames until exactly `n` more bytes have been accumulated.
    async fn read_n(body: &mut Incoming, n: usize) -> Vec<u8> {
        let mut got = Vec::new();
        while got.len() < n {
            let frame = body
                .frame()
                .await
                .expect("body ended before expected bytes arrived")
                .unwrap();
            if let Ok(data) = frame.into_data() {
                got.extend_from_slice(&data);
            }
        }
        assert_eq!(got.len(), n, "backend echoed more bytes than were sent");
        got
    }

    with_proxy(|proxy| async move {
        let (mut tx, rx) = mpsc::channel::<Result<Frame<Bytes>, Infallible>>(1);
        let req = Request::builder()
            .method(Method::POST)
            .uri("/interleave")
            .body(StreamBody::new(rx))
            .unwrap();

        // Send the first word before issuing the request, then require each
        // echoed response chunk to arrive while the request body is still open.
        // This asserts true bidirectional interleaving, not just that the
        // concatenated response is correct after buffering everything.
        tx.send(Ok(Frame::data(Bytes::from_static(b"one"))))
            .await
            .unwrap();
        let resp = proxy.send(req).await.unwrap();
        assert_eq!(resp.status(), 200);
        let mut body = resp.into_body();

        assert_eq!(read_n(&mut body, 3).await, b"ONE");
        tx.send(Ok(Frame::data(Bytes::from_static(b"two"))))
            .await
            .unwrap();
        assert_eq!(read_n(&mut body, 3).await, b"TWO");
        tx.send(Ok(Frame::data(Bytes::from_static(b"three"))))
            .await
            .unwrap();
        assert_eq!(read_n(&mut body, 5).await, b"THREE");

        // Close the request body; the response must then end cleanly.
        drop(tx);
        while let Some(frame) = body.frame().await {
            let frame = frame.unwrap();
            assert!(frame.into_data().is_err(), "unexpected trailing data");
        }
    });
}

#[test]
fn many_concurrent_requests() {
    with_proxy(|proxy| async move {
        let proxy = &proxy;
        let mut futs = Vec::new();
        for i in 0..32u32 {
            let n = (i as usize) * 1000 + 1;
            futs.push(async move {
                let resp = proxy
                    .send(
                        Request::builder()
                            .uri(format!("/bytes/{n}"))
                            .body(Empty::<Bytes>::new())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                let (status, _h, body) = collect(resp).await;
                assert_eq!(status, 200);
                assert_eq!(body, pattern(n));
            });
        }
        futures::future::join_all(futs).await;
    });
}

#[test]
fn uri_query_string_preserved() {
    with_proxy(|proxy| async move {
        let target = "/uri?foo=bar&baz=qux%20quux&n=1";
        let resp = proxy
            .send(
                Request::builder()
                    .uri(target)
                    .body(Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, _h, body) = collect(resp).await;
        assert_eq!(status, 200);
        assert_eq!(body, Bytes::from(target));
    });
}

#[test]
fn single_content_length_header() {
    // A fixed-length response must carry exactly one correct Content-Length header
    // (the length is propagated via the body's size hint; it must not be duplicated
    // or contradicted).
    with_proxy(|proxy| async move {
        let n = 1234;
        let resp = proxy
            .send(
                Request::builder()
                    .uri(format!("/bytes/{n}"))
                    .body(Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, headers, body) = collect(resp).await;
        assert_eq!(status, 200);
        let cls: Vec<_> = headers.get_all("content-length").iter().collect();
        assert_eq!(cls.len(), 1, "expected exactly one content-length header");
        assert_eq!(cls[0], &n.to_string());
        assert_eq!(body.len(), n);
    });
}

#[test]
fn request_trailers_are_dropped_not_fatal() {
    // The capnp ByteStream protocol has no trailer representation, so trailer
    // frames are documented to be silently dropped by the body pump. Verify
    // that a chunked request carrying trailers still completes cleanly and the
    // data frames all arrive.
    with_proxy(|proxy| async move {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-trailer", "trailing-value".parse().unwrap());
        let frames = vec![
            Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"data-before-"))),
            Ok(Frame::data(Bytes::from_static(b"trailers"))),
            Ok(Frame::trailers(trailers)),
        ];
        let body = StreamBody::new(futures::stream::iter(frames));
        let resp = proxy
            .send(
                Request::builder()
                    .method(Method::POST)
                    .uri("/echo")
                    .body(body)
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, _h, body) = collect(resp).await;
        assert_eq!(status, 200);
        assert_eq!(&body[..], b"data-before-trailers");
    });
}
