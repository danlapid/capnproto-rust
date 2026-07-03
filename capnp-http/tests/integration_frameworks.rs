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

#![cfg(feature = "tower")]

//! Per-framework integration tests.
//!
//! These prove that non-hyper HTTP stacks plug into capnp-http through the
//! [`HttpService`] abstraction, and — crucially — that request/response bodies
//! *stream* across the bridge rather than being buffered whole.
//!
//! hyper is already exercised by the other suites (hyper service input +
//! [`HyperService`](capnp_http::HyperService) output), so these focus on the
//! `tower` adapter and, layered on top of it, a real `axum::Router`, including
//! the `!Send` capnp body -> `Send` framework body streaming bridge that axum
//! requires.
//!
//! Each test runs over a real loopback-TCP capnp link (see `common`), on a
//! current-thread runtime + `LocalSet`, because capnp-rpc is single-threaded.

mod common;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body::Frame;
use http_body_util::{BodyExt, Either, Empty, Full, StreamBody};

use capnp_http::http_over_capnp_capnp::http_service;
use capnp_http::{service_to_capnp, CapnpHttpService, HttpService, IncomingBody, TowerService};
use common::start_client;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const FRAMES: usize = 128;
const FRAME_LEN: usize = 64 * 1024; // 8 MiB total, far larger than any sane buffer.

/// Runs `body` against a client `CapnpHttpService` backed by `backend`, on a
/// current-thread runtime + `LocalSet`.
fn run<F, Fut>(backend: http_service::Client, body: F)
where
    F: FnOnce(CapnpHttpService) -> Fut + 'static,
    Fut: std::future::Future<Output = ()>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        let svc = start_client(backend).await;
        body(svc).await;
    });
}

async fn get(svc: &CapnpHttpService, uri: &str) -> (StatusCode, Bytes) {
    let resp = svc
        .call(
            Request::builder()
                .method(Method::GET)
                .uri(uri)
                .body(Empty::<Bytes>::new())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, body)
}

async fn post(svc: &CapnpHttpService, uri: &str, payload: &'static [u8]) -> (StatusCode, Bytes) {
    let resp = svc
        .call(
            Request::builder()
                .method(Method::POST)
                .uri(uri)
                .body(Full::new(Bytes::from_static(payload)))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, body)
}

/// POSTs an 8 MiB multi-frame body to `/echo` and drains the echoed response
/// frame-by-frame, asserting integrity. If any layer buffered the whole body the
/// bounded bridge (capacity 4) would deadlock or the size would mismatch; a clean
/// pass shows the body streamed through.
async fn assert_streaming_echo(svc: &CapnpHttpService) {
    let chunk = Bytes::from(vec![b'x'; FRAME_LEN]);
    let stream = futures::stream::iter(
        (0..FRAMES).map(move |_| Ok::<_, std::convert::Infallible>(Frame::data(chunk.clone()))),
    );
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
    assert_eq!(resp.status(), StatusCode::OK);

    let mut body = resp.into_body();
    let mut echoed = 0usize;
    while let Some(frame) = body.frame().await {
        if let Some(data) = frame.unwrap().data_ref() {
            assert!(data.iter().all(|&b| b == b'x'), "payload corrupted in transit");
            echoed += data.len();
        }
    }
    assert_eq!(echoed, FRAMES * FRAME_LEN);
}

// ---------------------------------------------------------------------------
// tower: a `service_fn` leaf inside a middleware stack, exposed via TowerService.
// The POST handler streams the request body straight back out (`Either` unifies
// the two response body types without boxing or buffering).
// ---------------------------------------------------------------------------

fn tower_backend() -> http_service::Client {
    use tower::{service_fn, ServiceBuilder};

    let leaf = service_fn(|req: Request<IncomingBody>| async move {
        let body: Either<Full<Bytes>, IncomingBody> = if req.method() == Method::POST {
            Either::Right(req.into_body())
        } else {
            Either::Left(Full::new(Bytes::from_static(b"tower greeting")))
        };
        Ok::<_, std::convert::Infallible>(Response::new(body))
    });
    let stack = ServiceBuilder::new().concurrency_limit(32).service(leaf);
    service_to_capnp(TowerService(stack))
}

#[test]
fn tower_service_round_trips() {
    run(tower_backend(), |svc| async move {
        let (status, body) = get(&svc, "/").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(&body[..], b"tower greeting");

        let (status, body) = post(&svc, "/echo", b"hello tower").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(&body[..], b"hello tower");
    });
}

#[test]
fn tower_streams_large_body() {
    run(tower_backend(), |svc| async move {
        assert_streaming_echo(&svc).await;
    });
}

// ---------------------------------------------------------------------------
// axum: a real Router. `axum::body::Body` requires `Send` but capnp's
// `IncomingBody` is `!Send`, so the request body is re-framed onto a `Send`
// `StreamBody` fed by a `!Send` pump task over a *bounded* channel — streaming,
// never buffering the whole body.
// ---------------------------------------------------------------------------

fn stream_incoming(
    body: IncomingBody,
) -> StreamBody<futures::channel::mpsc::Receiver<Result<Frame<Bytes>, BoxError>>> {
    let (mut tx, rx) = futures::channel::mpsc::channel::<Result<Frame<Bytes>, BoxError>>(4);
    tokio::task::spawn_local(async move {
        use futures::SinkExt;
        let mut body = body;
        while let Some(frame) = body.frame().await {
            let item = frame.map_err(|e| -> BoxError { format!("request body: {e}").into() });
            let was_err = item.is_err();
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
        .route("/", get(|| async { "axum greeting" }))
        .route("/echo", post(|body: axum::body::Body| async move { body }));

    let adapter = service_fn(move |req: Request<IncomingBody>| {
        let app = app.clone();
        async move {
            let (parts, body) = req.into_parts();
            let axum_req = Request::from_parts(parts, axum::body::Body::new(stream_incoming(body)));
            let resp = app.oneshot(axum_req).await.unwrap(); // Router error is Infallible
            Ok::<_, BoxError>(resp)
        }
    });
    service_to_capnp(TowerService(adapter))
}

#[test]
fn axum_router_round_trips() {
    run(axum_backend(), |svc| async move {
        let (status, body) = get(&svc, "/").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(&body[..], b"axum greeting");

        let (status, body) = post(&svc, "/echo", b"hello axum").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(&body[..], b"hello axum");
    });
}

#[test]
fn axum_streams_large_body_through_bounded_bridge() {
    run(axum_backend(), |svc| async move {
        assert_streaming_echo(&svc).await;
    });
}
