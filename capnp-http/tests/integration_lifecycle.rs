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

//! Lifecycle / robustness tests for http-over-capnp, over an in-memory capnp
//! transport so a test can deterministically hold and drop responses and observe
//! what the backend sees: cancellation propagation, truncation surfacing as
//! errors, declared-size enforcement, and flow control / ordering. These use the
//! `hyper` adapter as the sample service frontend.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response};
use http_body::{Body, Frame};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Empty, Full, StreamBody};
use hyper::service::{service_fn, Service};

use capnp_http::http_over_capnp_capnp::http_service::client_request_context;
use capnp_http::http_over_capnp_capnp::HttpMethod;
use capnp_http::{capnp_to_service, service_to_capnp, CapnpHttpService, HttpService, IncomingBody};
use capnp_rpc::{new_client, rpc_twoparty_capnp::Side, twoparty, RpcSystem};
use futures::channel::oneshot;
use futures::FutureExt;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type RespBody = UnsyncBoxBody<Bytes, BoxError>;

fn full(b: impl Into<Bytes>) -> RespBody {
    Full::new(b.into())
        .map_err(|e: Infallible| -> BoxError { match e {} })
        .boxed_unsync()
}

fn pattern(n: usize) -> Bytes {
    Bytes::from((0..n).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
}

/// Fires a oneshot when dropped, so a test can observe teardown/cancellation.
struct Sentinel(Option<oneshot::Sender<()>>);
impl Drop for Sentinel {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

/// Yields the queued frames, then parks forever (never produces EOF). Dropping it
/// fires its [`Sentinel`]. Used to keep a response open until it is cancelled.
struct PendingAfter {
    frames: VecDeque<Bytes>,
    _sentinel: Sentinel,
}

impl Body for PendingAfter {
    type Data = Bytes;
    type Error = BoxError;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        match self.frames.pop_front() {
            Some(b) => Poll::Ready(Some(Ok(Frame::data(b)))),
            None => Poll::Pending,
        }
    }
}

/// Yields the queued frames, then yields an error (simulating a backend whose body
/// fails mid-stream / a truncated upstream).
struct ErrAfter {
    frames: VecDeque<Bytes>,
}

impl Body for ErrAfter {
    type Data = Bytes;
    type Error = BoxError;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        match self.frames.pop_front() {
            Some(b) => Poll::Ready(Some(Ok(Frame::data(b)))),
            None => Poll::Ready(Some(Err("backend body failed mid-stream".into()))),
        }
    }
}

/// A body that *lies* about its size: `size_hint` claims `claimed` bytes exactly,
/// while the frames actually yield something else. Used to verify that the
/// receiving side enforces the declared fixed `bodySize` instead of letting the
/// mismatch surface later as an HTTP framing error.
struct LyingBody {
    frames: VecDeque<Bytes>,
    claimed: u64,
}

impl Body for LyingBody {
    type Data = Bytes;
    type Error = BoxError;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        match self.frames.pop_front() {
            Some(b) => Poll::Ready(Some(Ok(Frame::data(b)))),
            None => Poll::Ready(None),
        }
    }
    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.claimed)
    }
}

/// A request body (client side) that yields one chunk then errors.
struct ErrReqBody {
    chunk: Option<Bytes>,
}
impl Body for ErrReqBody {
    type Data = Bytes;
    type Error = BoxError;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        match self.chunk.take() {
            Some(b) => Poll::Ready(Some(Ok(Frame::data(b)))),
            None => Poll::Ready(Some(Err("client request body failed".into()))),
        }
    }
}

// The harness: an in-memory capnp link, handing the client `CapnpHttpService`
// to the test body.
fn run<S, RespB, F, Fut>(service: S, body: F)
where
    S: Service<Request<IncomingBody>, Response = Response<RespB>> + 'static,
    S::Error: Into<BoxError>,
    S::Future: 'static,
    RespB: Body + 'static,
    RespB::Data: bytes::Buf,
    RespB::Error: Into<BoxError>,
    F: FnOnce(CapnpHttpService) -> Fut + 'static,
    Fut: Future<Output = ()>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        let (c2s_w, c2s_r) = async_byte_channel::channel();
        let (s2c_w, s2c_r) = async_byte_channel::channel();

        let server_net = Box::new(twoparty::VatNetwork::new(
            c2s_r,
            s2c_w,
            Side::Server,
            Default::default(),
        ));
        let server_rpc = RpcSystem::new(server_net, Some(service_to_capnp(service).client));

        let client_net = Box::new(twoparty::VatNetwork::new(
            s2c_r,
            c2s_w,
            Side::Client,
            Default::default(),
        ));
        let mut client_rpc = RpcSystem::new(client_net, None);
        let http_client = client_rpc.bootstrap(Side::Server);

        tokio::task::spawn_local(server_rpc.map(|_| ()));
        tokio::task::spawn_local(client_rpc.map(|_| ()));

        let svc = capnp_to_service(http_client, |f| {
            tokio::task::spawn_local(f);
        });
        body(svc).await;
    });
}

fn get(uri: &str) -> Request<Empty<Bytes>> {
    Request::builder()
        .uri(uri)
        .body(Empty::<Bytes>::new())
        .unwrap()
}

#[test]
fn cancel_propagates_when_response_dropped() {
    // The backend's response body stays open (parked) after one chunk; when the
    // client drops the response, the backend body must be dropped (cancelled).
    let sentinel_slot: Rc<RefCell<Option<oneshot::Sender<()>>>> = Rc::new(RefCell::new(None));
    let (sentinel_tx, sentinel_rx) = oneshot::channel();
    *sentinel_slot.borrow_mut() = Some(sentinel_tx);

    let slot = sentinel_slot.clone();
    let service = service_fn(move |_req: Request<IncomingBody>| {
        let tx = slot.borrow_mut().take();
        async move {
            let mut frames = VecDeque::new();
            frames.push_back(Bytes::from_static(b"partial"));
            let body = PendingAfter {
                frames,
                _sentinel: Sentinel(tx),
            };
            Ok::<_, BoxError>(
                Response::builder()
                    .status(200)
                    .body(body.boxed_unsync())
                    .unwrap(),
            )
        }
    });

    run(service, |svc| async move {
        let mut resp = svc.call(get("/hang")).await.unwrap();
        // Read the first chunk so we know the exchange is live.
        let frame = resp.body_mut().frame().await.unwrap().unwrap();
        assert_eq!(&frame.into_data().unwrap()[..], b"partial");

        // Drop the response: this must cancel the backend.
        drop(resp);

        tokio::time::timeout(Duration::from_secs(5), sentinel_rx)
            .await
            .expect("backend was not cancelled when the response was dropped")
            .expect("sentinel sender dropped without firing");
    });
}

#[test]
fn cancel_propagates_when_call_dropped_before_response() {
    // The backend never responds; dropping the in-flight call must cancel it.
    let called_slot: Rc<RefCell<Option<oneshot::Sender<()>>>> = Rc::new(RefCell::new(None));
    let done_slot: Rc<RefCell<Option<oneshot::Sender<()>>>> = Rc::new(RefCell::new(None));
    let (called_tx, called_rx) = oneshot::channel();
    let (done_tx, done_rx) = oneshot::channel();
    *called_slot.borrow_mut() = Some(called_tx);
    *done_slot.borrow_mut() = Some(done_tx);

    let cs = called_slot.clone();
    let ds = done_slot.clone();
    let service = service_fn(move |_req: Request<IncomingBody>| {
        let called = cs.borrow_mut().take();
        let sentinel = Sentinel(ds.borrow_mut().take());
        async move {
            if let Some(c) = called {
                let _ = c.send(());
            }
            // Hold the sentinel until cancellation drops this future.
            let _sentinel = sentinel;
            std::future::pending::<()>().await;
            Ok::<_, BoxError>(Response::builder().status(200).body(full("never")).unwrap())
        }
    });

    run(service, |svc| async move {
        let handle = tokio::task::spawn_local(async move {
            let _ = svc.call(get("/hang")).await;
        });
        // Wait until the backend has actually been invoked.
        tokio::time::timeout(Duration::from_secs(5), called_rx)
            .await
            .expect("backend was never called")
            .unwrap();
        // Cancel the in-flight call.
        handle.abort();
        // The backend future must be dropped as a result.
        tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("backend was not cancelled when the call was dropped")
            .expect("sentinel dropped without firing");
    });
}

#[test]
fn response_truncation_surfaces_as_error() {
    // The backend's response body yields some bytes then errors. The client must
    // see the bytes followed by an error, never a clean EOF that would make a
    // truncated body look complete.
    let service = service_fn(move |_req: Request<IncomingBody>| async move {
        let mut frames = VecDeque::new();
        frames.push_back(Bytes::from_static(b"good-data"));
        let body = ErrAfter { frames };
        Ok::<_, BoxError>(
            Response::builder()
                .status(200)
                .body(body.boxed_unsync())
                .unwrap(),
        )
    });

    run(service, |svc| async move {
        let mut resp = svc.call(get("/truncate")).await.unwrap();
        assert_eq!(resp.status(), 200);
        // First frame is the good data.
        let first = resp.body_mut().frame().await.unwrap().unwrap();
        assert_eq!(&first.into_data().unwrap()[..], b"good-data");
        // The next poll must be an error (truncation), not None (clean EOF).
        match resp.body_mut().frame().await {
            Some(Err(_)) => {}
            Some(Ok(_)) => panic!("unexpected extra data after truncation"),
            None => panic!("truncated body surfaced as a clean EOF"),
        }
    });
}

#[test]
fn request_body_error_fails_request_and_cancels_backend() {
    // The client's request body errors mid-stream while the backend is reading it.
    // A capnp ByteStream has no abort message, so the request must be failed
    // (cancelling the backend) rather than deadlocking it. We verify both: the
    // call fails, and the backend service future is dropped (cancelled).
    let done_slot: Rc<RefCell<Option<oneshot::Sender<()>>>> = Rc::new(RefCell::new(None));
    let (done_tx, done_rx) = oneshot::channel();
    *done_slot.borrow_mut() = Some(done_tx);

    let ds = done_slot.clone();
    let service = service_fn(move |req: Request<IncomingBody>| {
        let sentinel = Sentinel(ds.borrow_mut().take());
        async move {
            // The sentinel fires if this future is dropped (cancelled).
            let _sentinel = sentinel;
            let _ = req.into_body().collect().await;
            Ok::<_, BoxError>(Response::builder().status(200).body(full("ok")).unwrap())
        }
    });

    run(service, |svc| async move {
        let req = Request::builder()
            .method("POST")
            .uri("/sink")
            .body(ErrReqBody {
                chunk: Some(Bytes::from_static(b"partial")),
            })
            .unwrap();
        // A truncated request body must surface as a failed request, not a hang.
        let result = svc.call(req).await;
        assert!(
            result.is_err(),
            "a truncated request body must fail the request"
        );
        // ...and the backend must have been cancelled rather than left hanging.
        tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("backend was not cancelled after request-body truncation")
            .expect("sentinel dropped without firing");
    });
}

#[test]
fn slow_consumer_large_body_in_order() {
    // A large response read by a deliberately slow consumer must arrive intact and
    // in order (exercising backpressure without deadlock or loss).
    let n = 2 * 1024 * 1024;
    let service = service_fn(move |_req: Request<IncomingBody>| async move {
        Ok::<_, BoxError>(
            Response::builder()
                .status(200)
                .body(full(pattern(n)))
                .unwrap(),
        )
    });

    run(service, move |svc| async move {
        let mut resp = svc.call(get("/big")).await.unwrap();
        let mut got = Vec::new();
        let mut reads = 0u32;
        while let Some(frame) = resp.body_mut().frame().await {
            let data = frame.unwrap().into_data().unwrap();
            got.extend_from_slice(&data);
            // Occasionally yield to simulate a slow consumer.
            reads += 1;
            if reads % 8 == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        assert_eq!(got.len(), n);
        assert_eq!(Bytes::from(got), pattern(n));
    });
}

#[test]
fn slow_server_reader_large_upload_in_order() {
    // Mirror of `slow_consumer_large_body_in_order` for the *request* direction:
    // the client streams a 2 MiB upload while the backend reads it slowly. This
    // exercises request-body flow control (client writes must block on the
    // stream's backpressure rather than buffer unboundedly) and byte ordering.
    let n = 2 * 1024 * 1024;
    let service = service_fn(move |req: Request<IncomingBody>| async move {
        let mut body = req.into_body();
        let mut got = Vec::new();
        let mut reads = 0u32;
        while let Some(frame) = body.frame().await {
            let data = frame.unwrap().into_data().unwrap();
            got.extend_from_slice(&data);
            // Occasionally yield to simulate a slow reader.
            reads += 1;
            if reads % 8 == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        // Report the verdict to the client (asserting here would only panic a
        // spawned server task; a status code is deterministic to observe).
        let ok = got.len() == n && got == pattern(n);
        Ok::<_, BoxError>(
            Response::builder()
                .status(if ok { 200 } else { 500 })
                .body(full("done"))
                .unwrap(),
        )
    });

    run(service, move |svc| async move {
        let data = pattern(n);
        let chunk = 8 * 1024;
        let frames: Vec<Result<Frame<Bytes>, Infallible>> = (0..n)
            .step_by(chunk)
            .map(|off| Ok(Frame::data(data.slice(off..(off + chunk).min(n)))))
            .collect();
        let body = StreamBody::new(futures::stream::iter(frames))
            .map_err(|e: Infallible| -> BoxError { match e {} })
            .boxed_unsync();
        let req = Request::builder().uri("/upload").body(body).unwrap();
        let resp = svc.call(req).await.unwrap();
        assert_eq!(resp.status(), 200, "backend saw corrupted/short upload");
    });
}

#[test]
fn response_body_shorter_than_declared_is_error() {
    // The backend's body claims exactly 10 bytes but yields only 5 before a
    // clean EOF. The declared size was already promised to the client (it
    // becomes Content-Length), so the shortfall must surface as a body error,
    // never as a silent, clean 5-byte EOF.
    let service = service_fn(move |_req: Request<IncomingBody>| async move {
        let body = LyingBody {
            frames: VecDeque::from([Bytes::from_static(b"12345")]),
            claimed: 10,
        }
        .boxed_unsync();
        Ok::<_, BoxError>(Response::builder().status(200).body(body).unwrap())
    });

    run(service, move |svc| async move {
        let resp = svc.call(get("/short")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let result = resp.into_body().collect().await;
        assert!(
            result.is_err(),
            "a body shorter than its declared size must error, not EOF cleanly"
        );
    });
}

#[test]
fn response_body_longer_than_declared_is_error() {
    // The inverse: the body claims exactly 5 bytes but yields 10. The receiver
    // must reject the excess rather than pass it through (it would corrupt
    // Content-Length framing downstream).
    let service = service_fn(move |_req: Request<IncomingBody>| async move {
        let body = LyingBody {
            frames: VecDeque::from([Bytes::from_static(b"1234567890")]),
            claimed: 5,
        }
        .boxed_unsync();
        Ok::<_, BoxError>(Response::builder().status(200).body(body).unwrap())
    });

    run(service, move |svc| async move {
        let resp = svc.call(get("/long")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let result = resp.into_body().collect().await;
        assert!(
            result.is_err(),
            "a body longer than its declared size must error, not pass through"
        );
    });
}

#[test]
fn many_tiny_frames_preserved_in_order() {
    // 5000 single-byte frames must all arrive, in order, with none merged away
    // incorrectly or lost.
    let count = 5000usize;
    let service = service_fn(move |_req: Request<IncomingBody>| async move {
        let frames = (0..count)
            .map(|i| Ok::<_, Infallible>(Frame::data(Bytes::from(vec![(i % 251) as u8]))));
        let body = StreamBody::new(futures::stream::iter(frames))
            .map_err(|e: Infallible| -> BoxError { match e {} })
            .boxed_unsync();
        Ok::<_, BoxError>(Response::builder().status(200).body(body).unwrap())
    });

    run(service, move |svc| async move {
        let resp = svc.call(get("/tiny")).await.unwrap();
        let got = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(got.len(), count);
        for (i, b) in got.iter().enumerate() {
            assert_eq!(*b, (i % 251) as u8, "byte {i} out of order");
        }
    });
}

#[test]
fn interleaved_streaming_requests_on_one_connection() {
    // Several streaming responses in flight concurrently over the SAME capnp
    // connection, read in an interleaved fashion, must not corrupt each other.
    let service = service_fn(move |req: Request<IncomingBody>| async move {
        let n: usize = req
            .uri()
            .path()
            .trim_start_matches("/bytes/")
            .parse()
            .unwrap_or(0);
        Ok::<_, BoxError>(
            Response::builder()
                .status(200)
                .body(full(pattern(n)))
                .unwrap(),
        )
    });

    run(service, |svc| async move {
        let svc = &svc;
        let sizes = [1usize, 1000, 50_000, 7, 200_000, 333];
        let mut futs = Vec::new();
        for &n in &sizes {
            futs.push(async move {
                let resp = svc.call(get(&format!("/bytes/{n}"))).await.unwrap();
                let got = resp.into_body().collect().await.unwrap().to_bytes();
                assert_eq!(got, pattern(n));
            });
        }
        futures::future::join_all(futs).await;
    });
}

// The remaining pieces drive the raw capnp `request()` interface directly, to
// assert the `HttpService` lifetime contract: the service stays alive while a
// call is outstanding and is freed once that call is cancelled.

/// A service that never responds and records when it is called and dropped.
struct HangingService {
    called: Rc<Cell<bool>>,
    dropped: Rc<Cell<bool>>,
}

impl Drop for HangingService {
    fn drop(&mut self) {
        self.dropped.set(true);
    }
}

impl Service<Request<IncomingBody>> for HangingService {
    type Response = Response<Full<Bytes>>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>>>>;

    fn call(&self, _req: Request<IncomingBody>) -> Self::Future {
        self.called.set(true);
        Box::pin(std::future::pending())
    }
}

/// A ClientRequestContext that is never actually invoked (the service hangs).
struct DummyContext;
impl client_request_context::Server for DummyContext {}

#[test]
fn service_outlives_then_dies_with_outstanding_call() {
    // Mirrors the C++ "HttpService isn't destroyed while call outstanding" test,
    // plus the cancellation case: dropping the in-flight call cancels the request
    // and frees the service.
    let called = Rc::new(Cell::new(false));
    let dropped = Rc::new(Cell::new(false));

    let client = service_to_capnp(HangingService {
        called: called.clone(),
        dropped: dropped.clone(),
    });

    futures::executor::block_on(async move {
        // Build a bare request() call directly against the capnp interface.
        let mut req = client.request_request();
        {
            let mut r = req.get().init_request();
            r.set_method(HttpMethod::Get);
            r.set_url("/");
            r.reborrow().init_headers(0);
            r.reborrow().get_body_size().set_fixed(0);
        }
        req.get().set_context(new_client(DummyContext));

        // Box::pin so that `drop(promise)` actually drops the future (a `pin_mut!`
        // would only drop the borrow, not the underlying call).
        let mut promise = Box::pin(req.send().promise);

        // Drive the call far enough to reach the (hanging) service.
        assert!(futures::poll!(promise.as_mut()).is_pending());
        assert!(called.get(), "service should have been called");

        // Dropping the client capability must NOT destroy the service while the
        // call is still outstanding.
        drop(client);
        assert!(futures::poll!(promise.as_mut()).is_pending());
        assert!(
            !dropped.get(),
            "service must stay alive while a call is outstanding"
        );

        // Cancelling the call (dropping the promise) frees the service.
        drop(promise);
        assert!(
            dropped.get(),
            "service must be dropped once the outstanding call is cancelled"
        );
    });
}

#[test]
fn response_body_delivered_before_request_completes() {
    // The backend responds with a body that emits one frame then parks forever
    // (never EOF), so the server's `request()` call cannot complete: its body pump
    // blocks waiting for the next frame. If the first frame still reaches the
    // client, response bytes are being delivered via promise pipelining while
    // `request()` is in flight, not queued until the call returns.
    let sentinel_slot: Rc<RefCell<Option<oneshot::Sender<()>>>> = Rc::new(RefCell::new(None));
    let (gone_tx, gone_rx) = oneshot::channel();
    *sentinel_slot.borrow_mut() = Some(gone_tx);

    let slot = sentinel_slot.clone();
    let service = service_fn(move |_req: Request<IncomingBody>| {
        let tx = slot.borrow_mut().take();
        async move {
            let mut frames = VecDeque::new();
            frames.push_back(Bytes::from_static(b"first"));
            let body = PendingAfter {
                frames,
                _sentinel: Sentinel(tx),
            };
            Ok::<_, BoxError>(
                Response::builder()
                    .status(200)
                    .body(body.boxed_unsync())
                    .unwrap(),
            )
        }
    });

    run(service, |svc| async move {
        let mut resp = svc.call(get("/stream")).await.unwrap();
        assert_eq!(resp.status(), 200);

        // `request()` is still in flight here (the backend body is parked, so the
        // server's pump cannot finish). The first frame must still arrive promptly.
        let frame = tokio::time::timeout(Duration::from_secs(5), resp.body_mut().frame())
            .await
            .expect(
                "first response frame did not arrive before request() completed: \
                 response bytes were queued, not pipelined",
            )
            .expect("expected a response frame")
            .unwrap();
        assert_eq!(&frame.into_data().unwrap()[..], b"first");

        // Tidy teardown: dropping the response cancels the in-flight request() and
        // drops the backend body (firing the sentinel).
        drop(resp);
        tokio::time::timeout(Duration::from_secs(5), gone_rx)
            .await
            .expect("backend was not torn down after the response was dropped")
            .expect("sentinel dropped without firing");
    });
}

#[test]
fn server_error_before_response_surfaces_real_error() {
    // The backend fails before sending any response. This races two events on the
    // client: request() returns an exception, and the context cap drops (cancelling
    // the response channel). The client must surface the *real* server error, not a
    // generic "completed without a response" message. (Regression guard for the
    // swallowed-`request()`-error fix.)
    let service = service_fn(move |_req: Request<IncomingBody>| async move {
        Err::<Response<RespBody>, BoxError>("backend exploded".into())
    });

    run(service, |svc| async move {
        let err = match svc.call(get("/boom")).await {
            Ok(_) => panic!("request must fail when the backend errors before responding"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("backend exploded"),
            "client should see the real server error, got: {msg}"
        );
        assert!(
            !msg.contains("without a response"),
            "client got the generic no-response message instead of the real error: {msg}"
        );
    });
}
