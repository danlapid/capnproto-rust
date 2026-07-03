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

//! HTTP `CONNECT` tunnel support.
//!
//! A CONNECT tunnel is a bidirectional byte stream. This module exposes it as a
//! [`Tunnel`] implementing [`futures::io::AsyncRead`] + [`futures::io::AsyncWrite`],
//! built on the `capnp-byte-stream` adapters.
//!
//! Because HTTP libraries typically model CONNECT via a connection-bound upgrade
//! mechanism (which cannot cross the capnp hop), CONNECT is exposed here through
//! dedicated APIs rather than the [`HttpService`](crate::HttpService) trait:
//!
//! * client: [`CapnpHttpService::connect`](crate::CapnpHttpService::connect).
//! * server: implement [`ConnectService`] and use [`connect_service_to_capnp`].
//!
//! Note: the `startTls` *signal* is carried across the tunnel (see
//! [`Tunnel::start_tls`] and [`Tunnel::next_tls_request`]); performing the actual
//! TLS handshake over the tunnel is left to the application.

use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use capnp::Error;
use futures::channel::oneshot;
use futures::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use capnp_byte_stream::{
    byte_stream_reader, byte_stream_reader_with_tls, byte_stream_to_async_write, ByteStreamReader,
    ByteStreamWriter,
};
use futures::channel::mpsc;
use futures::StreamExt;

use crate::body::{incoming_body_with_len, IncomingBody};
use crate::http_over_capnp_capnp::http_service::{self, connect_client_request_context};
use crate::service::AbortGuard;
use crate::{headers, CapnpHttpService};
use futures::FutureExt;

/// Settings for a CONNECT request.
#[derive(Clone, Copy, Debug, Default)]
pub struct ConnectSettings {
    pub use_tls: bool,
}

/// A bidirectional CONNECT tunnel: read = peer→here, write = here→peer.
pub struct Tunnel {
    reader: ByteStreamReader,
    writer: ByteStreamWriter,
    /// Server side only: receives `startTls` requests (expected server hostname).
    tls_requests: Option<mpsc::UnboundedReceiver<String>>,
    _keepalive: Option<Box<dyn core::any::Any>>,
}

impl Tunnel {
    fn new(
        reader: ByteStreamReader,
        writer: ByteStreamWriter,
        tls_requests: Option<mpsc::UnboundedReceiver<String>>,
    ) -> Self {
        Self {
            reader,
            writer,
            tls_requests,
            _keepalive: None,
        }
    }

    fn attach_keepalive(&mut self, keepalive: Box<dyn core::any::Any>) {
        self._keepalive = Some(keepalive);
    }

    /// Client side: requests that the peer begin a TLS handshake, expecting
    /// `expected_server_hostname`. The tunnel remains usable; the application is
    /// responsible for performing the actual TLS handshake over it.
    pub async fn start_tls(&mut self, expected_server_hostname: &str) -> Result<(), Error> {
        self.writer.start_tls(expected_server_hostname).await
    }

    /// Server side: awaits the next `startTls` request from the peer (returns the
    /// expected server hostname), or `None` if no more will arrive.
    pub async fn next_tls_request(&mut self) -> Option<String> {
        match &mut self.tls_requests {
            Some(rx) => rx.next().await,
            None => None,
        }
    }
}

impl AsyncRead for Tunnel {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().reader).poll_read(cx, buf)
    }
}

impl AsyncWrite for Tunnel {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().writer).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().writer).poll_flush(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().writer).poll_close(cx)
    }
}

/// The result of a CONNECT request.
pub enum ConnectOutcome {
    /// The server accepted the tunnel.
    Accepted {
        status: http::StatusCode,
        headers: http::HeaderMap,
        tunnel: Tunnel,
    },
    /// The server rejected the tunnel, with an error response + body.
    Rejected {
        status: http::StatusCode,
        headers: http::HeaderMap,
        body: IncomingBody,
    },
}

enum ConnectSignal {
    Accept(http::StatusCode, http::HeaderMap),
    Reject(http::StatusCode, http::HeaderMap, IncomingBody),
}

impl CapnpHttpService {
    /// Opens an HTTP CONNECT tunnel.
    pub async fn connect(
        &self,
        host: &str,
        headers: &http::HeaderMap,
        settings: ConnectSettings,
    ) -> Result<ConnectOutcome, Error> {
        let mut req = self.client.connect_request();
        {
            let mut p = req.get();
            p.set_host(host);
            headers::to_capnp(headers, p.reborrow().init_headers(headers.len() as u32))?;
            p.reborrow().init_settings().set_use_tls(settings.use_tls);
        }

        // `down` carries server->client bytes (we host the server, read from it).
        let (down_client, down_reader) = byte_stream_reader();
        req.get().set_down(down_client);

        let (tx, rx) = oneshot::channel::<Result<ConnectSignal, Error>>();
        let context: connect_client_request_context::Client =
            capnp_rpc::new_client(ConnectContextServer {
                tx: RefCell::new(Some(tx)),
            });
        req.get().set_context(context);

        let capnp::capability::RemotePromise { promise, pipeline } = req.send();
        // `up` carries client->server bytes (server hosts it, we write to it).
        let up_writer = byte_stream_to_async_write(pipeline.get_up());
        let tunnel = Tunnel::new(down_reader, up_writer, None);

        // Lets the background driver report a `connect()` that failed without ever
        // delivering an accept/reject signal, so the real server-side error is
        // surfaced instead of a generic message.
        let (fail_tx, fail_rx) = oneshot::channel::<Error>();
        let (driver, abort_handle) = futures::future::abortable(async move {
            if let Err(e) = promise.await {
                let _ = fail_tx.send(e);
            }
        });
        (self.spawn)(
            async move {
                let _ = driver.await;
            }
            .boxed_local(),
        );
        let guard = AbortGuard(abort_handle);

        let signal = crate::service::response_or_failure(
            rx,
            fail_rx,
            "CONNECT completed without a response",
        )
        .await;

        match signal {
            Ok(ConnectSignal::Accept(status, headers)) => {
                let mut tunnel = tunnel;
                tunnel.attach_keepalive(Box::new(guard));
                Ok(ConnectOutcome::Accepted {
                    status,
                    headers,
                    tunnel,
                })
            }
            Ok(ConnectSignal::Reject(status, headers, mut body)) => {
                body.attach_keepalive(Box::new(guard));
                Ok(ConnectOutcome::Rejected {
                    status,
                    headers,
                    body,
                })
            }
            Err(e) => Err(e),
        }
    }
}

struct ConnectContextServer {
    tx: RefCell<Option<oneshot::Sender<Result<ConnectSignal, Error>>>>,
}

impl ConnectContextServer {
    fn deliver(&self, signal: ConnectSignal) {
        if let Some(tx) = self.tx.borrow_mut().take() {
            let _ = tx.send(Ok(signal));
        }
    }
}

impl connect_client_request_context::Server for ConnectContextServer {
    async fn start_connect(
        self: Rc<Self>,
        params: connect_client_request_context::StartConnectParams,
        _results: connect_client_request_context::StartConnectResults,
    ) -> Result<(), Error> {
        let (status, headers) = read_response_meta(params.get()?.get_response()?)?;
        self.deliver(ConnectSignal::Accept(status, headers));
        Ok(())
    }

    async fn start_error(
        self: Rc<Self>,
        params: connect_client_request_context::StartErrorParams,
        mut results: connect_client_request_context::StartErrorResults,
    ) -> Result<(), Error> {
        let (status, headers) = read_response_meta(params.get()?.get_response()?)?;
        let (body_client, body) = incoming_body_with_len(None);
        results.get().set_body(body_client);
        self.deliver(ConnectSignal::Reject(status, headers, body));
        Ok(())
    }
}

/// Lets a [`ConnectService`] accept or reject a CONNECT request.
pub struct ConnectResponse {
    context: connect_client_request_context::Client,
}

impl ConnectResponse {
    /// Accepts the tunnel with the given response status/headers.
    pub async fn accept(
        self,
        status: http::StatusCode,
        headers: &http::HeaderMap,
    ) -> Result<(), Error> {
        let mut req = self.context.start_connect_request();
        write_response_meta(req.get().init_response(), status, headers, Some(0))?;
        req.send().promise.await.map(|_| ())
    }

    /// Rejects the tunnel, sending an error response with `body`.
    pub async fn reject(
        self,
        status: http::StatusCode,
        headers: &http::HeaderMap,
        body: &[u8],
    ) -> Result<(), Error> {
        let mut req = self.context.start_error_request();
        write_response_meta(
            req.get().init_response(),
            status,
            headers,
            Some(body.len() as u64),
        )?;
        let promise = req.send();
        let mut writer = byte_stream_to_async_write(promise.pipeline.get_body());
        writer
            .write_all(body)
            .await
            .map_err(|e| Error::failed(format!("failed writing CONNECT error body: {e}")))?;
        writer
            .close()
            .await
            .map_err(|e| Error::failed(format!("failed ending CONNECT error body: {e}")))?;
        promise.promise.await.map(|_| ())
    }
}

/// A handler for incoming CONNECT requests.
pub trait ConnectService: 'static {
    fn connect(
        self: Rc<Self>,
        host: String,
        headers: http::HeaderMap,
        settings: ConnectSettings,
        tunnel: Tunnel,
        response: ConnectResponse,
    ) -> impl std::future::Future<Output = Result<(), Error>> + 'static;
}

/// Exposes a [`ConnectService`] over Cap'n Proto as an `HttpService` capability
/// that supports `connect()` (regular `request()` calls return "unimplemented").
pub fn connect_service_to_capnp<C: ConnectService>(handler: C) -> http_service::Client {
    capnp_rpc::new_client(ConnectServer {
        handler: Rc::new(handler),
    })
}

struct ConnectServer<C> {
    handler: Rc<C>,
}

impl<C: ConnectService> http_service::Server for ConnectServer<C> {
    async fn connect(
        self: Rc<Self>,
        params: http_service::ConnectParams,
        results: http_service::ConnectResults,
    ) -> Result<(), Error> {
        serve_connect(self.handler.clone(), params, results).await
    }
}

/// Handles a capnp `connect()` by invoking a [`ConnectService`]. Shared by
/// [`connect_service_to_capnp`] and [`service_to_capnp_with_connect`].
pub(crate) async fn serve_connect<C: ConnectService>(
    handler: Rc<C>,
    params: http_service::ConnectParams,
    mut results: http_service::ConnectResults,
) -> Result<(), Error> {
    let p = params.get()?;
    let host = p
        .get_host()?
        .to_str()
        .map_err(|e| Error::failed(format!("CONNECT host is not valid UTF-8: {e}")))?
        .to_owned();
    let headers = headers::from_capnp(p.get_headers()?)?;
    let use_tls = p.get_settings()?.get_use_tls();
    let down = p.get_down()?;
    let context = p.get_context()?;

    // `down` = server->client; `up` = client->server.
    let down_writer = byte_stream_to_async_write(down);
    let (up_client, up_reader, tls_rx) = byte_stream_reader_with_tls();
    results.get().set_up(up_client);
    results.set_pipeline()?;

    let tunnel = Tunnel::new(up_reader, down_writer, Some(tls_rx));
    let response = ConnectResponse { context };
    handler
        .connect(host, headers, ConnectSettings { use_tls }, tunnel, response)
        .await
}

/// Exposes both a request [`HttpService`](crate::HttpService) and a
/// [`ConnectService`] over a single `HttpService` capability (handles both
/// `request()` and `connect()`).
pub fn service_to_capnp_with_connect<S, C>(service: S, connect: C) -> http_service::Client
where
    S: crate::HttpService<IncomingBody> + 'static,
    S::Future: 'static,
    S::ResBody: 'static,
    <S::ResBody as http_body::Body>::Data: bytes::Buf,
    <S::ResBody as http_body::Body>::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    C: ConnectService,
{
    capnp_rpc::new_client(HttpAndConnectServer {
        service: Rc::new(service),
        connect: Rc::new(connect),
    })
}

struct HttpAndConnectServer<S, C> {
    service: Rc<S>,
    connect: Rc<C>,
}

impl<S, C> http_service::Server for HttpAndConnectServer<S, C>
where
    S: crate::HttpService<IncomingBody> + 'static,
    S::Future: 'static,
    S::ResBody: 'static,
    <S::ResBody as http_body::Body>::Data: bytes::Buf,
    <S::ResBody as http_body::Body>::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    C: ConnectService,
{
    async fn request(
        self: Rc<Self>,
        params: http_service::RequestParams,
        results: http_service::RequestResults,
    ) -> Result<(), Error> {
        crate::service::serve_request(self.service.clone(), params, results).await
    }

    async fn connect(
        self: Rc<Self>,
        params: http_service::ConnectParams,
        results: http_service::ConnectResults,
    ) -> Result<(), Error> {
        serve_connect(self.connect.clone(), params, results).await
    }
}

fn read_response_meta(
    reader: crate::http_over_capnp_capnp::http_response::Reader,
) -> Result<(http::StatusCode, http::HeaderMap), Error> {
    let status = http::StatusCode::from_u16(reader.get_status_code())
        .map_err(|e| Error::failed(format!("invalid status code: {e}")))?;
    let headers = headers::from_capnp(reader.get_headers()?)?;
    Ok((status, headers))
}

fn write_response_meta(
    mut builder: crate::http_over_capnp_capnp::http_response::Builder,
    status: http::StatusCode,
    headers: &http::HeaderMap,
    body_len: Option<u64>,
) -> Result<(), Error> {
    builder.set_status_code(status.as_u16());
    if let Some(reason) = status.canonical_reason() {
        builder.set_status_text(reason);
    }
    headers::to_capnp(
        headers,
        builder.reborrow().init_headers(headers.len() as u32),
    )?;
    let mut bs = builder.reborrow().get_body_size();
    match body_len {
        Some(n) => bs.set_fixed(n),
        None => bs.set_unknown(()),
    }
    Ok(())
}
