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

//! Demonstrates that `capnp-http` is HTTP-framework-agnostic by serving two
//! *different* ecosystems over the exact same capnp `HttpService` protocol,
//! using only the `tower` adapter — the `hyper` feature is not required here.
//!
//! 1. A hand-built `tower` stack (`tower::service_fn` + a `ServiceBuilder`
//!    middleware layer), and
//! 2. An `axum::Router` with real handlers/extractors.
//!
//! Both are wrapped in [`capnp_http::TowerService`] and handed to
//! [`service_to_capnp`], then driven from the client side through the neutral
//! [`capnp_http::HttpService`] trait. Run with:
//!
//! ```text
//! cargo run -p capnp-http --example pluggable_frameworks --features tower
//! ```

use std::convert::Infallible;

use bytes::Bytes;
use http::{Method, Request, Response};
use http_body::Frame;
use http_body_util::{BodyExt, Either, Full, StreamBody};

use capnp_http::http_over_capnp_capnp::http_service;
use capnp_http::{capnp_to_service, service_to_capnp, HttpService, IncomingBody, TowerService};
use capnp_rpc::{rpc_twoparty_capnp::Side, twoparty, RpcSystem};
use futures::FutureExt;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Serves `backend` (any capnp `HttpService` capability) to `body`, wiring an
/// in-memory capnp link between a client and server `RpcSystem`. Keeps this
/// example focused on the *service* side rather than transport plumbing.
fn roundtrip<F, Fut>(backend: http_service::Client, body: F)
where
    F: FnOnce(capnp_http::CapnpHttpService) -> Fut + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        let (c2s_writer, c2s_reader) = async_byte_channel::channel();
        let (s2c_writer, s2c_reader) = async_byte_channel::channel();

        let server_net = Box::new(twoparty::VatNetwork::new(
            c2s_reader,
            s2c_writer,
            Side::Server,
            Default::default(),
        ));
        let server_rpc = RpcSystem::new(server_net, Some(backend.client));

        let client_net = Box::new(twoparty::VatNetwork::new(
            s2c_reader,
            c2s_writer,
            Side::Client,
            Default::default(),
        ));
        let mut client_rpc = RpcSystem::new(client_net, None);
        let http_client: http_service::Client = client_rpc.bootstrap(Side::Server);

        tokio::task::spawn_local(client_rpc.map(|_| ()));
        tokio::task::spawn_local(server_rpc.map(|_| ()));

        let svc = capnp_to_service(http_client, |f| {
            tokio::task::spawn_local(f);
        });
        body(svc).await;
    });
}

/// Issues a request through the capnp-backed client and returns `(status, body)`.
async fn get(svc: &capnp_http::CapnpHttpService, method: Method, uri: &str, body: &'static [u8]) -> (u16, String) {
    let resp = svc
        .call(
            Request::builder()
                .method(method)
                .uri(uri)
                .body(Full::new(Bytes::from_static(body)))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// Sends a large, multi-frame POST body and drains the echoed response
/// frame-by-frame, counting bytes without ever holding the whole body. This
/// exercises the streaming path end-to-end: for the axum backend, 8 MiB flows
/// through a bounded 4-frame channel, proving nothing buffers the full body.
async fn streaming_echo_check(svc: &capnp_http::CapnpHttpService, label: &str) {
    const FRAMES: usize = 128;
    const FRAME_LEN: usize = 64 * 1024;
    let chunk = Bytes::from(vec![b'x'; FRAME_LEN]);
    let stream =
        futures::stream::iter((0..FRAMES).map(move |_| Ok::<_, Infallible>(Frame::data(chunk.clone()))));

    let resp = svc
        .call(
            Request::builder()
                .method(Method::POST)
                .uri("/echo")
                .body(StreamBody::new(stream))
                .unwrap(),
        )
        .await
        .unwrap();

    let mut body = resp.into_body();
    let mut echoed = 0usize;
    while let Some(frame) = body.frame().await {
        if let Some(data) = frame.unwrap().data_ref() {
            echoed += data.len();
        }
    }
    println!(
        "  {label}: streamed {} bytes in {FRAMES} frames, echoed {echoed} bytes back \
         (bounded, no full-body buffering)",
        FRAMES * FRAME_LEN
    );
}

// ---------------------------------------------------------------------------
// 1. A plain `tower` service with a middleware layer.
// ---------------------------------------------------------------------------

fn tower_backend() -> http_service::Client {
    use tower::{service_fn, ServiceBuilder};

    // A leaf service over the neutral `http` types. The POST handler streams the
    // request body straight back out as the response body: the `IncomingBody`
    // (itself an `http_body::Body` fed frame-by-frame off the capnp `ByteStream`)
    // *becomes* the response body verbatim. `Either` unifies the two body types
    // without boxing, so nothing is collected or buffered.
    let leaf = service_fn(|req: Request<IncomingBody>| async move {
        let body: Either<Full<Bytes>, IncomingBody> = if req.method() == Method::POST {
            Either::Right(req.into_body())
        } else {
            Either::Left(Full::new(Bytes::from_static(b"hello from a tower service_fn")))
        };
        Ok::<_, Infallible>(Response::new(body))
    });

    // A real tower middleware stack around it (here: a concurrency limit). Any
    // `tower::Layer` composes the same way.
    let stack = ServiceBuilder::new().concurrency_limit(64).service(leaf);

    service_to_capnp(TowerService(stack))
}

// ---------------------------------------------------------------------------
// 2. An `axum` application.
// ---------------------------------------------------------------------------

/// Re-frames a `!Send` capnp [`IncomingBody`] as a `Send` streaming body without
/// buffering. A pump task (necessarily `!Send`, so it stays on the current
/// thread's `LocalSet`) forwards frames through a *bounded* channel; the returned
/// [`StreamBody`] hands them to the framework. The bound provides backpressure —
/// if the consumer stops reading, the pump blocks and stops pulling from capnp —
/// so at most a few frames are ever in flight, never the whole body.
fn stream_incoming(body: IncomingBody) -> StreamBody<futures::channel::mpsc::Receiver<Result<Frame<Bytes>, BoxError>>> {
    let (mut tx, rx) = futures::channel::mpsc::channel::<Result<Frame<Bytes>, BoxError>>(4);
    tokio::task::spawn_local(async move {
        use futures::SinkExt;
        let mut body = body;
        while let Some(frame) = body.frame().await {
            let item = frame.map_err(|e| -> BoxError { format!("request body: {e}").into() });
            let was_err = item.is_err();
            // `Err` here means the consumer was dropped; stop pumping either way.
            if tx.send(item).await.is_err() || was_err {
                break;
            }
        }
    });
    StreamBody::new(rx)
}

fn axum_backend() -> http_service::Client {
    use axum::routing::{get, post};
    use axum::Router;
    use tower::{service_fn, ServiceExt};

    let app: Router = Router::new()
        .route("/", get(|| async { "hello from axum over capnp" }))
        // Streaming echo: take the body as an `axum::body::Body` and return it
        // unchanged. No extractor buffers it.
        .route("/echo", post(|body: axum::body::Body| async move { body }));

    // `axum::Router` speaks `http::Request<axum::body::Body>`, and
    // `axum::body::Body` requires `Send`, while capnp's `IncomingBody` is `!Send`
    // (single-threaded capnp-rpc). `stream_incoming` bridges the two by *streaming*
    // frames across a bounded channel — no full-body buffering — then we dispatch
    // to the router via `oneshot`. This is the kind of small, streaming adapter
    // the pluggable design expects at a framework's boundary.
    let adapter = service_fn(move |req: Request<IncomingBody>| {
        let app = app.clone();
        async move {
            let (parts, body) = req.into_parts();
            let axum_req = Request::from_parts(parts, axum::body::Body::new(stream_incoming(body)));
            // `axum::Router`'s service error is `Infallible`.
            let resp = app.oneshot(axum_req).await.unwrap();
            Ok::<_, BoxError>(resp)
        }
    });

    service_to_capnp(TowerService(adapter))
}

fn main() {
    println!("== tower (service_fn + concurrency-limit layer) ==");
    roundtrip(tower_backend(), |svc| async move {
        let (s, b) = get(&svc, Method::GET, "/", b"").await;
        println!("  GET  /      -> {s} {b:?}");
        let (s, b) = get(&svc, Method::POST, "/echo", b"tower echo").await;
        println!("  POST /echo  -> {s} {b:?}");
        streaming_echo_check(&svc, "tower").await;
    });

    println!("== axum (Router with handlers/extractors) ==");
    roundtrip(axum_backend(), |svc| async move {
        let (s, b) = get(&svc, Method::GET, "/", b"").await;
        println!("  GET  /      -> {s} {b:?}");
        let (s, b) = get(&svc, Method::POST, "/echo", b"axum echo").await;
        println!("  POST /echo  -> {s} {b:?}");
        streaming_echo_check(&svc, "axum").await;
    });
}
