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

//! The two top-level adapters between an [`HttpService`] and the capnp
//! `HttpService` RPC interface.
//!
//! * [`service_to_capnp`]: wrap a local service, expose it as `HttpService::Client`.
//! * [`capnp_to_service`]: wrap an `HttpService::Client`, expose it as an [`HttpService`].
//!
//! The core is framework-agnostic: it is written against the local
//! [`HttpService`] trait (plus the neutral `http` / `http-body` / `bytes`
//! interchange types), not against any particular HTTP library. Enable the
//! `hyper` feature to get blanket compatibility with `hyper::service::Service`.
//!
//! Only the modern `request()` method is implemented (not the deprecated
//! `startRequest()`). WebSocket upgrades are handled here under the `websocket`
//! feature; CONNECT tunnelling lives in the `connect` module under the `connect`
//! feature.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use bytes::Buf;
use capnp::Error;
use futures::channel::oneshot;
use futures::future::{AbortHandle, LocalBoxFuture};
use futures::FutureExt;
use http_body::Body;

use crate::body::{incoming_body_with_len, pump_body_to_byte_stream, IncomingBody, PumpOutcome};
use crate::http_over_capnp_capnp::http_service::{self, client_request_context};
use crate::http_over_capnp_capnp::HttpMethod;
use crate::{headers, method};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A spawner for the background per-request driver task. The future is `!Send`
/// and `'static`; the spawner must drive it to completion on the current thread
/// (e.g. `tokio::task::spawn_local`, or a `futures::executor::LocalPool`
/// spawner). The core stays executor-agnostic by taking this explicitly.
pub(crate) type Spawner = Rc<dyn Fn(LocalBoxFuture<'static, ()>)>;

fn not_in_schema(_e: ::capnp::NotInSchema) -> Error {
    Error::failed("unknown enumerant in http-over-capnp message".to_string())
}

/// The HTTP service abstraction this crate bridges to Cap'n Proto.
///
/// It deliberately mirrors the shape of [`hyper::service::Service`] and
/// `tower::Service`: a single [`call`](HttpService::call) taking an
/// [`http::Request`] and returning a future of an [`http::Response`]. By
/// depending only on this local trait (and the neutral `http` / `http-body`
/// interchange types), the core stays free of any particular HTTP framework.
///
/// Adapters for specific ecosystems are provided behind feature flags — with the
/// `hyper` feature, every `hyper::service::Service` implements this trait
/// automatically, and [`HyperService`] wraps any `HttpService` back into a
/// `hyper::service::Service`. Any other library can be supported by implementing
/// this trait for its service type.
pub trait HttpService<ReqB> {
    /// The body type of the response produced by this service.
    type ResBody: Body;
    /// The error type produced by this service. Must convert into a boxed error.
    type Error: Into<BoxError>;
    /// The future returned by [`call`](HttpService::call).
    type Future: Future<Output = Result<http::Response<Self::ResBody>, Self::Error>>;

    /// Handles a single request.
    fn call(&self, req: http::Request<ReqB>) -> Self::Future;
}

/// Wraps an [`HttpService`] so it can be exposed over Cap'n Proto RPC as an
/// `HttpService` capability.
///
/// The service consumes `http::Request<IncomingBody>` (the request body arrives
/// over capnp) and produces `http::Response<B>` for any `http_body::Body`. With
/// the `hyper` feature enabled, any `hyper::service::Service` of the right shape
/// can be passed directly.
pub fn service_to_capnp<S>(service: S) -> http_service::Client
where
    S: HttpService<IncomingBody> + 'static,
    S::Future: 'static,
    S::ResBody: 'static,
    <S::ResBody as Body>::Data: Buf,
    <S::ResBody as Body>::Error: Into<BoxError>,
{
    capnp_rpc::new_client(HttpServiceServer {
        service: Rc::new(service),
    })
}

struct HttpServiceServer<S> {
    service: Rc<S>,
}

impl<S> http_service::Server for HttpServiceServer<S>
where
    S: HttpService<IncomingBody> + 'static,
    S::Future: 'static,
    S::ResBody: 'static,
    <S::ResBody as Body>::Data: Buf,
    <S::ResBody as Body>::Error: Into<BoxError>,
{
    async fn request(
        self: Rc<Self>,
        params: http_service::RequestParams,
        results: http_service::RequestResults,
    ) -> Result<(), Error> {
        serve_request(self.service.clone(), params, results).await
    }
}

/// Handles a capnp `request()` by invoking the wrapped [`HttpService`]. Shared by
/// [`service_to_capnp`] and `service_to_capnp_with_connect`.
pub(crate) async fn serve_request<S>(
    service: Rc<S>,
    params: http_service::RequestParams,
    mut results: http_service::RequestResults,
) -> Result<(), Error>
where
    S: HttpService<IncomingBody> + 'static,
    S::Future: 'static,
    S::ResBody: 'static,
    <S::ResBody as Body>::Data: Buf,
    <S::ResBody as Body>::Error: Into<BoxError>,
{
    use crate::http_over_capnp_capnp::http_request::body_size::Which as BodySize;

    // Read request metadata into owned values (no borrows across await).
    let params_reader = params.get()?;
    let req_reader = params_reader.get_request()?;
    let method_capnp = req_reader.get_method().map_err(not_in_schema)?;
    let http_method = method::from_capnp(method_capnp);
    let is_head = matches!(method_capnp, HttpMethod::Head);
    let url = req_reader
        .get_url()?
        .to_str()
        .map_err(|e| Error::failed(format!("request url is not valid UTF-8: {e}")))?
        .to_owned();
    let req_headers = headers::from_capnp(req_reader.get_headers()?)?;
    let req_body_len = match req_reader.get_body_size().which().map_err(not_in_schema)? {
        BodySize::Fixed(n) => Some(n),
        BodySize::Unknown(()) => None,
    };
    let context = params_reader.get_context()?;

    let body = if req_body_len == Some(0) {
        IncomingBody::empty()
    } else {
        let (client, body) = incoming_body_with_len(req_body_len);
        // Publish the request-body cap via setPipeline so the peer can begin
        // streaming the body before this call returns.
        results.get().set_request_body(client);
        results.set_pipeline()?;
        body
    };

    let mut request = http::Request::builder()
        .method(http_method)
        .uri(url)
        .body(body)
        .map_err(|e| Error::failed(format!("invalid request: {e}")))?;
    *request.headers_mut() = req_headers;

    let response = service
        .call(request)
        .await
        .map_err(|e| Error::failed(format!("http service error: {}", e.into())))?;

    let (resp_parts, resp_body) = response.into_parts();

    // WebSocket upgrade: the service signalled it via a `WebSocketUpgrade`
    // extension on a 101 response. Use startWebSocket instead of startResponse.
    #[cfg(feature = "websocket")]
    if let Some(fulfiller) = resp_parts
        .extensions
        .get::<crate::websocket::WebSocketUpgrade>()
        .and_then(|u| u.take_fulfiller())
    {
        let mut req = context.start_web_socket_request();
        headers::to_capnp(
            &resp_parts.headers,
            req.get().init_headers(resp_parts.headers.len() as u32),
        )?;
        let (up_client, rx) = crate::websocket::web_socket_receiver();
        req.get().set_up_socket(up_client);
        let promise = req.send();
        let down_socket = promise.pipeline.get_down_socket();
        let ws = crate::websocket::WebSocket::new(down_socket, rx);
        let _ = fulfiller.send(ws);
        return match promise.promise.await {
            Ok(_) => Ok(()),
            Err(e) if e.kind == capnp::ErrorKind::Disconnected => Ok(()),
            Err(e) => Err(e),
        };
    }

    let has_resp_body = response_has_body(is_head, resp_parts.status, &resp_body);

    let mut start = context.start_response_request();
    {
        let mut r = start.get().init_response();
        r.set_status_code(resp_parts.status.as_u16());
        if let Some(reason) = resp_parts.status.canonical_reason() {
            r.set_status_text(reason);
        }
        headers::to_capnp(
            &resp_parts.headers,
            r.reborrow().init_headers(resp_parts.headers.len() as u32),
        )?;
        let mut bs = r.reborrow().get_body_size();
        if has_resp_body {
            match resp_body.size_hint().exact() {
                Some(n) => bs.set_fixed(n),
                None => bs.set_unknown(()),
            }
        } else {
            bs.set_fixed(0);
        }
    }

    let resp_promise = start.send();
    if has_resp_body {
        let body_stream = resp_promise.pipeline.get_body();
        match pump_body_to_byte_stream(resp_body, body_stream).await {
            // Clean EOF, or the client abandoned the response (dropped it /
            // cancelled). The latter is not a server error: we simply stop,
            // mirroring the C++ implementation which swallows DISCONNECTED.
            PumpOutcome::Ended | PumpOutcome::SinkClosed => {}
            // The service's own response body failed: fail the call so the
            // response stream is torn down without end() and the client sees a
            // truncation error rather than a silent clean EOF.
            PumpOutcome::SourceFailed(e) => return Err(e),
        }
    }
    // The exchange is complete once the client has acknowledged startResponse.
    // Again, a DISCONNECTED here just means the client went away after we sent
    // the response; treat it as a clean completion.
    match resp_promise.promise.await {
        Ok(_) => Ok(()),
        Err(e) if e.kind == capnp::ErrorKind::Disconnected => Ok(()),
        Err(e) => Err(e),
    }
}

fn response_has_body<B: Body>(is_head: bool, status: http::StatusCode, body: &B) -> bool {
    if is_head {
        return false;
    }
    // RFC 9110: 1xx (informational), 204, 205, and 304 responses never have
    // content. (A 101 WebSocket upgrade takes the startWebSocket path and never
    // reaches this check.)
    if status.is_informational() {
        return false;
    }
    match status.as_u16() {
        204 | 205 | 304 => return false,
        _ => {}
    }
    body.size_hint().exact() != Some(0)
}

/// Wraps an `HttpService::Client` so it can be called as an [`HttpService`]
/// (i.e. used as an HTTP client). With the `hyper` feature, wrap the result in
/// [`HyperService`] to use it anywhere a `hyper::service::Service` is expected.
///
/// `spawn` drives the background per-request task (request-body upload +
/// completion) that must outlive each `call()`. It must run a `!Send` `'static`
/// future to completion on the current thread; pass e.g.
/// `|f| { tokio::task::spawn_local(f); }`, or, with the `tokio` feature,
/// [`tokio_local_spawn`]. The core itself does not depend on any executor.
pub fn capnp_to_service(
    client: http_service::Client,
    spawn: impl Fn(LocalBoxFuture<'static, ()>) + 'static,
) -> CapnpHttpService {
    CapnpHttpService {
        client,
        spawn: Rc::new(spawn),
    }
}

/// An [`HttpService`] backed by a capnp `HttpService` capability.
#[derive(Clone)]
pub struct CapnpHttpService {
    pub(crate) client: http_service::Client,
    pub(crate) spawn: Spawner,
}

impl<ReqB> HttpService<ReqB> for CapnpHttpService
where
    ReqB: Body + 'static,
    ReqB::Data: Buf,
    ReqB::Error: Into<BoxError>,
{
    type ResBody = IncomingBody;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<http::Response<IncomingBody>, Error>>>>;

    fn call(&self, req: http::Request<ReqB>) -> Self::Future {
        let client = self.client.clone();
        let spawn = self.spawn.clone();
        Box::pin(async move {
            match do_request(client, spawn, req).await? {
                ClientOutcome::Response(response) => Ok(response),
                #[cfg(feature = "websocket")]
                ClientOutcome::WebSocket(_) => Err(Error::failed(
                    "server upgraded to a WebSocket; use open_websocket() instead".to_string(),
                )),
            }
        })
    }
}

/// A `tokio::task::spawn_local`-based spawner, for callers running on a tokio
/// `LocalSet`. Pass it as the `spawn` argument to [`capnp_to_service`].
#[cfg(feature = "tokio")]
pub fn tokio_local_spawn(future: LocalBoxFuture<'static, ()>) {
    tokio::task::spawn_local(future);
}

#[cfg(feature = "websocket")]
impl CapnpHttpService {
    /// Performs a WebSocket handshake request, returning the negotiated
    /// [`crate::WebSocket`] if the server accepted the upgrade.
    pub async fn open_websocket<ReqB>(
        &self,
        req: http::Request<ReqB>,
    ) -> Result<crate::websocket::WebSocket, Error>
    where
        ReqB: Body + 'static,
        ReqB::Data: Buf,
        ReqB::Error: Into<BoxError>,
    {
        match do_request(self.client.clone(), self.spawn.clone(), req).await? {
            ClientOutcome::WebSocket(ws) => Ok(ws),
            ClientOutcome::Response(response) => Err(Error::failed(format!(
                "server did not upgrade to a WebSocket (status {})",
                response.status()
            ))),
        }
    }
}

/// What the client's `ClientRequestContext` was asked to deliver.
enum ClientOutcome {
    Response(http::Response<IncomingBody>),
    #[cfg(feature = "websocket")]
    WebSocket(crate::websocket::WebSocket),
}

impl ClientOutcome {
    /// Attaches the background request driver guard to whichever object carries
    /// the ongoing exchange, so dropping it cancels the request.
    fn attach_guard(self, guard: AbortGuard) -> Self {
        match self {
            ClientOutcome::Response(mut response) => {
                response.body_mut().attach_keepalive(Box::new(guard));
                ClientOutcome::Response(response)
            }
            #[cfg(feature = "websocket")]
            ClientOutcome::WebSocket(mut ws) => {
                ws.attach_keepalive(Box::new(guard));
                ClientOutcome::WebSocket(ws)
            }
        }
    }
}

async fn do_request<ReqB>(
    client: http_service::Client,
    spawn: Spawner,
    req: http::Request<ReqB>,
) -> Result<ClientOutcome, Error>
where
    ReqB: Body + 'static,
    ReqB::Data: Buf,
    ReqB::Error: Into<BoxError>,
{
    let (parts, req_body) = req.into_parts();
    let method_capnp = method::to_capnp(&parts.method)?;
    let exact_len = req_body.size_hint().exact();
    let has_req_body = exact_len != Some(0);
    let url = parts.uri.to_string();

    let mut rpc = client.request_request();
    {
        let mut r = rpc.get().init_request();
        r.set_method(method_capnp);
        r.set_url(url.as_str());
        headers::to_capnp(
            &parts.headers,
            r.reborrow().init_headers(parts.headers.len() as u32),
        )?;
        let mut bs = r.reborrow().get_body_size();
        match exact_len {
            Some(n) => bs.set_fixed(n),
            None => bs.set_unknown(()),
        }
    }

    // The ClientRequestContext receives the server's response callback.
    let (resp_tx, resp_rx) = oneshot::channel::<Result<ClientOutcome, Error>>();
    let context: client_request_context::Client =
        capnp_rpc::new_client(ClientRequestContextServer {
            resp_tx: RefCell::new(Some(resp_tx)),
        });
    rpc.get().set_context(context);

    let capnp::capability::RemotePromise {
        promise: req_promise,
        pipeline,
    } = rpc.send();
    let request_body_stream = pipeline.get_request_body();

    // Lets the background driver report a request-body source failure back to
    // `do_request` so it can fail the call. This is necessary because cancelling
    // the `request()` call requires dropping BOTH `req_promise` and `pipeline`, and
    // `pipeline` lives here (it must, to expose the request-body stream). On a
    // body failure the driver drops `req_promise` and signals us; we then stop
    // awaiting the response and return, dropping `pipeline` (and `guard`, which
    // aborts the driver), fully tearing the call down.
    let (fail_tx, fail_rx) = oneshot::channel::<Error>();

    // Drive the request-body upload and wait for request() completion in the
    // background. This must outlive `do_request` (which returns as soon as the
    // response *headers* arrive via startResponse), but it must NOT outlive the
    // caller's interest in the request. We therefore make the driver abortable
    // and tie the `AbortHandle` to the ongoing exchange via an `AbortGuard`:
    //
    //   * If `do_request` is dropped before the response arrives (the caller
    //     cancelled the `call()` future), the guard is dropped here and aborts the
    //     driver, cancelling the in-flight `request()`.
    //   * Otherwise the guard is moved into the response body, so dropping the
    //     response cancels the request, while reading it to completion lets the
    //     (by-then-finished) driver's abort be a no-op.
    let driver = async move {
        let result = if has_req_body {
            match pump_body_to_byte_stream(req_body, request_body_stream).await {
                // Body fully uploaded, or the server stopped reading it (e.g. it
                // responded without consuming the request). Either way the request
                // is valid: wait for `request()` to complete and propagate its error.
                PumpOutcome::Ended | PumpOutcome::SinkClosed => req_promise.await.map(|_| ()),
                // The client's request body failed mid-stream. A capnp `ByteStream`
                // has no abort message, so dropping the stream cannot unblock a
                // server already reading the body, and `request()` would deadlock.
                // Drop `req_promise` to tear the call down and report the source error.
                PumpOutcome::SourceFailed(e) => {
                    drop(req_promise);
                    Err(e)
                }
            }
        } else {
            req_promise.await.map(|_| ())
        };
        // Report a failure (a request-body source error, or a `request()` that
        // failed without ever delivering a response). If a response already arrived,
        // `do_request` has returned and this send is a harmless no-op; otherwise it
        // surfaces the real error instead of a generic "no response" message.
        if let Err(e) = result {
            let _ = fail_tx.send(e);
        }
    };
    let (driver, abort_handle) = futures::future::abortable(driver);
    spawn(
        async move {
            let _ = driver.await;
        }
        .boxed_local(),
    );
    let guard = AbortGuard(abort_handle);

    // Resolve as soon as the response headers (or WebSocket) are delivered, or
    // the request fails.
    let outcome = response_or_failure(
        resp_rx,
        fail_rx,
        "request completed without a response being sent",
    )
    .await;
    outcome.map(|o| o.attach_guard(guard))
}

/// Awaits either a delivered response (`resp_rx`) or a reported call failure
/// (`fail_rx`). The two signals can race: a handler that fails before responding
/// both errors the call and drops the context capability, cancelling `resp_rx`.
/// So when one channel yields nothing, consult the other rather than reporting a
/// generic message, preserving the real server-side error.
pub(crate) async fn response_or_failure<T>(
    resp_rx: oneshot::Receiver<Result<T, Error>>,
    fail_rx: oneshot::Receiver<Error>,
    no_response_msg: &str,
) -> Result<T, Error> {
    match futures::future::select(resp_rx, fail_rx).await {
        futures::future::Either::Left((resp, fail_rx)) => match resp {
            Ok(resp) => resp,
            Err(_canceled) => Err(fail_rx
                .await
                .unwrap_or_else(|_| Error::failed(no_response_msg.to_string()))),
        },
        futures::future::Either::Right((failed, resp_rx)) => match failed {
            Ok(e) => Err(e),
            Err(_canceled) => resp_rx
                .await
                .unwrap_or_else(|_| Err(Error::failed(no_response_msg.to_string()))),
        },
    }
}

/// Aborts the spawned request driver when dropped. Used to cancel the in-flight
/// `request()` when the caller loses interest (drops the response / `call()`).
pub(crate) struct AbortGuard(pub(crate) AbortHandle);

impl Drop for AbortGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct ClientRequestContextServer {
    resp_tx: RefCell<Option<oneshot::Sender<Result<ClientOutcome, Error>>>>,
}

impl client_request_context::Server for ClientRequestContextServer {
    async fn start_response(
        self: Rc<Self>,
        params: client_request_context::StartResponseParams,
        mut results: client_request_context::StartResponseResults,
    ) -> Result<(), Error> {
        // No `set_pipeline()` needed while this handler stays synchronous (no `.await`):
        // the body pipeline resolves this turn. If you add an await, call
        // `results.set_pipeline()` before it so response-body writes aren't queued.
        let response = build_response(params, &mut results)?;
        if let Some(tx) = self.resp_tx.borrow_mut().take() {
            let _ = tx.send(Ok(ClientOutcome::Response(response)));
        }
        Ok(())
    }

    #[cfg(feature = "websocket")]
    async fn start_web_socket(
        self: Rc<Self>,
        params: client_request_context::StartWebSocketParams,
        mut results: client_request_context::StartWebSocketResults,
    ) -> Result<(), Error> {
        // `upSocket` is the capability we call to send client->server frames.
        let up_socket = params.get()?.get_up_socket()?;
        // We host `downSocket` to receive server->client frames.
        let (down_client, rx) = crate::websocket::web_socket_receiver();
        results.get().set_down_socket(down_client);
        let ws = crate::websocket::WebSocket::new(up_socket, rx);
        if let Some(tx) = self.resp_tx.borrow_mut().take() {
            let _ = tx.send(Ok(ClientOutcome::WebSocket(ws)));
        }
        Ok(())
    }
}

fn build_response(
    params: client_request_context::StartResponseParams,
    results: &mut client_request_context::StartResponseResults,
) -> Result<http::Response<IncomingBody>, Error> {
    use crate::http_over_capnp_capnp::http_response::body_size::Which as BodySize;

    let resp_reader = params.get()?.get_response()?;
    let status = http::StatusCode::from_u16(resp_reader.get_status_code())
        .map_err(|e| Error::failed(format!("invalid status code: {e}")))?;
    let resp_headers = headers::from_capnp(resp_reader.get_headers()?)?;
    let body = match resp_reader.get_body_size().which().map_err(not_in_schema)? {
        BodySize::Fixed(0) => IncomingBody::empty(),
        BodySize::Fixed(n) => {
            let (client, body) = incoming_body_with_len(Some(n));
            results.get().set_body(client);
            body
        }
        BodySize::Unknown(()) => {
            let (client, body) = incoming_body_with_len(None);
            results.get().set_body(client);
            body
        }
    };

    let mut response = http::Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = resp_headers;
    Ok(response)
}

/// Adapters bridging the local [`HttpService`] trait to the `hyper` ecosystem.
///
/// Enabled by the `hyper` feature (on by default). This provides two directions:
///
/// * a blanket `impl HttpService for S` for every `hyper::service::Service<..>`
///   producing an `http::Response`, so hyper services (e.g. `service_fn`, an
///   `axum::Router`) can be handed straight to [`service_to_capnp`]; and
/// * [`HyperService`], which wraps any [`HttpService`] (such as
///   [`CapnpHttpService`]) so it can be used wherever a `hyper::service::Service`
///   is expected (a hyper server or client).
#[cfg(feature = "hyper")]
mod hyper_compat {
    use super::{BoxError, HttpService};
    use http_body::Body;

    impl<S, ReqB, ResB> HttpService<ReqB> for S
    where
        S: hyper::service::Service<http::Request<ReqB>, Response = http::Response<ResB>>,
        ResB: Body,
        S::Error: Into<BoxError>,
    {
        type ResBody = ResB;
        type Error = S::Error;
        type Future = S::Future;

        fn call(&self, req: http::Request<ReqB>) -> Self::Future {
            hyper::service::Service::call(self, req)
        }
    }

    /// Wraps any [`HttpService`] so it implements `hyper::service::Service`.
    ///
    /// Use this to plug a [`CapnpHttpService`](super::CapnpHttpService) (or any
    /// other `HttpService`) into hyper's server/client machinery:
    /// `HyperService(capnp_to_service(client, spawn))`.
    #[derive(Clone)]
    pub struct HyperService<S>(pub S);

    impl<S, ReqB> hyper::service::Service<http::Request<ReqB>> for HyperService<S>
    where
        S: HttpService<ReqB>,
    {
        type Response = http::Response<S::ResBody>;
        type Error = S::Error;
        type Future = S::Future;

        fn call(&self, req: http::Request<ReqB>) -> Self::Future {
            self.0.call(req)
        }
    }
}

#[cfg(feature = "hyper")]
pub use hyper_compat::HyperService;

/// Adapter bridging the local [`HttpService`] trait to the `tower` ecosystem.
///
/// Enabled by the `tower` feature. Because a `tower::Service` takes `&mut self`
/// and must be driven through `poll_ready` before `call`, the wrapped service
/// must be `Clone`: each request drives a fresh clone to readiness and then
/// dispatches (the same clone-per-call approach `hyper_util`'s
/// `TowerToHyperService` uses). This makes whole tower stacks — `ServiceBuilder`
/// layers, `axum::Router`, `tonic` services, and so on — usable with
/// [`service_to_capnp`].
#[cfg(feature = "tower")]
mod tower_compat {
    use std::future::Future;
    use std::pin::Pin;

    use http_body::Body;

    use super::{BoxError, HttpService};

    /// Wraps any [`tower::Service`] producing an `http::Response` so it
    /// implements [`HttpService`]. The service must be `Clone` (see the module
    /// note above).
    #[derive(Clone)]
    pub struct TowerService<S>(pub S);

    impl<S, ReqB, ResB> HttpService<ReqB> for TowerService<S>
    where
        S: tower::Service<http::Request<ReqB>, Response = http::Response<ResB>> + Clone + 'static,
        S::Future: 'static,
        S::Error: Into<BoxError>,
        ReqB: 'static,
        ResB: Body,
    {
        type ResBody = ResB;
        type Error = S::Error;
        type Future = Pin<Box<dyn Future<Output = Result<http::Response<ResB>, S::Error>>>>;

        fn call(&self, req: http::Request<ReqB>) -> Self::Future {
            let mut svc = self.0.clone();
            Box::pin(async move {
                futures::future::poll_fn(|cx| tower::Service::poll_ready(&mut svc, cx)).await?;
                tower::Service::call(&mut svc, req).await
            })
        }
    }
}

#[cfg(feature = "tower")]
pub use tower_compat::TowerService;
