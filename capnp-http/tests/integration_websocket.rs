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

//! WebSocket integration tests, driven over a loopback TCP capnp session:
//!
//!   Rust WebSocket client (`open_websocket`) --capnp/TCP--> service_to_capnp(WsBackend)
//!
//! Ping/Pong control frames are deliberately not tunnelled across the capnp
//! WebSocket interface (they stay local to each real-socket hop);
//! `ping_pong_kept_local` pins that down.

#![cfg(feature = "websocket")]

mod common;

use std::cell::RefCell;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request, Response};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Empty, Full};

use capnp_http::tungstenite::protocol::frame::coding::CloseCode;
use capnp_http::tungstenite::protocol::CloseFrame;
use capnp_http::tungstenite::Message;
use capnp_http::{service_to_capnp, CapnpHttpService, HttpService, IncomingBody};
use futures::channel::oneshot;
use futures::{SinkExt, StreamExt};

use common::start_client;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BackendBody = UnsyncBoxBody<Bytes, Infallible>;

fn empty_body() -> BackendBody {
    Empty::<Bytes>::new().map_err(|e| match e {}).boxed_unsync()
}

// A backend WebSocket service, routed by path. The control handle lets a test
// observe server-side lifecycle events (e.g. the inbound stream ending when the
// client drops its WebSocket).
#[derive(Clone, Default)]
struct WsControl {
    client_gone_tx: Rc<RefCell<Option<oneshot::Sender<()>>>>,
}

#[derive(Clone)]
struct WsBackend {
    ctl: WsControl,
}

impl HttpService<IncomingBody> for WsBackend {
    type ResBody = BackendBody;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Response<BackendBody>, BoxError>>>>;

    fn call(&self, req: Request<IncomingBody>) -> Self::Future {
        let ctl = self.ctl.clone();
        Box::pin(async move { handle(req, ctl).await })
    }
}

async fn handle(
    req: Request<IncomingBody>,
    ctl: WsControl,
) -> Result<Response<BackendBody>, BoxError> {
    let path = req.uri().path().to_owned();
    match path.as_str() {
        // Echo text/binary; stop on Close.
        "/echo" => {
            let (upgrade, pending) = capnp_http::accept_websocket();
            tokio::task::spawn_local(async move {
                if let Ok(mut ws) = pending.await {
                    while let Some(Ok(msg)) = ws.next().await {
                        match msg {
                            Message::Text(_) | Message::Binary(_) => {
                                if ws.send(msg).await.is_err() {
                                    break;
                                }
                            }
                            Message::Close(_) => break,
                            _ => {}
                        }
                    }
                }
            });
            Ok(Response::builder()
                .status(101)
                .extension(upgrade)
                .body(empty_body())?)
        }
        // Server pushes 5 text messages then closes, ignoring client input.
        "/push" => {
            let (upgrade, pending) = capnp_http::accept_websocket();
            tokio::task::spawn_local(async move {
                if let Ok(mut ws) = pending.await {
                    for i in 0..5u32 {
                        if ws.send(Message::text(format!("msg-{i}"))).await.is_err() {
                            return;
                        }
                    }
                    let _ = ws.send(Message::Close(None)).await;
                }
            });
            Ok(Response::builder()
                .status(101)
                .extension(upgrade)
                .body(empty_body())?)
        }
        // Server sends a Close frame carrying a non-default code + reason.
        "/close" => {
            let (upgrade, pending) = capnp_http::accept_websocket();
            tokio::task::spawn_local(async move {
                if let Ok(mut ws) = pending.await {
                    let _ = ws
                        .send(Message::Close(Some(CloseFrame {
                            code: CloseCode::Away,
                            reason: "see ya".into(),
                        })))
                        .await;
                }
            });
            Ok(Response::builder()
                .status(101)
                .extension(upgrade)
                .body(empty_body())?)
        }
        // Reads until the inbound stream ends, then signals the control handle.
        "/dropobserve" => {
            let (upgrade, pending) = capnp_http::accept_websocket();
            let tx = ctl.client_gone_tx.borrow_mut().take();
            tokio::task::spawn_local(async move {
                if let Ok(mut ws) = pending.await {
                    while let Some(Ok(msg)) = ws.next().await {
                        if msg.is_close() {
                            break;
                        }
                    }
                }
                if let Some(tx) = tx {
                    let _ = tx.send(());
                }
            });
            Ok(Response::builder()
                .status(101)
                .extension(upgrade)
                .body(empty_body())?)
        }
        _ => Ok(Response::builder().status(404).body(empty_body())?),
    }
}

fn with_ws<F, Fut>(body: F)
where
    F: FnOnce(CapnpHttpService) -> Fut + 'static,
    Fut: Future<Output = ()>,
{
    with_ws_ctl(|svc, _ctl| body(svc));
}

fn with_ws_ctl<F, Fut>(body: F)
where
    F: FnOnce(CapnpHttpService, WsControl) -> Fut + 'static,
    Fut: Future<Output = ()>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        let ctl = WsControl::default();
        let svc = start_client(service_to_capnp(WsBackend { ctl: ctl.clone() })).await;
        body(svc, ctl).await;
    });
}

fn ws_request(path: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .method(Method::GET)
        .uri(path)
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .body(Full::new(Bytes::new()))
        .unwrap()
}

#[test]
fn echo_round_trips() {
    with_ws(|svc| async move {
        let mut ws = svc.open_websocket(ws_request("/echo")).await.unwrap();
        let large: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
        for msg in [
            Message::text("hello"),
            Message::binary(vec![1u8, 2, 3, 4, 5]),
            Message::text(""),
            Message::binary(Vec::new()),
            Message::binary(large),
        ] {
            ws.send(msg.clone()).await.unwrap();
            match ws.next().await {
                Some(Ok(echoed)) => assert_eq!(echoed, msg),
                other => panic!("expected {msg:?} echoed, got {other:?}"),
            }
        }
        ws.send(Message::Close(None)).await.unwrap();
    });
}

#[test]
fn many_messages_in_order() {
    with_ws(|svc| async move {
        let mut ws = svc.open_websocket(ws_request("/echo")).await.unwrap();
        for i in 0..100u32 {
            ws.send(Message::text(format!("m{i}"))).await.unwrap();
            match ws.next().await {
                Some(Ok(Message::Text(t))) => assert_eq!(t.as_str(), format!("m{i}")),
                other => panic!("expected m{i}, got {other:?}"),
            }
        }
        ws.send(Message::Close(None)).await.unwrap();
    });
}

#[test]
fn server_push_then_close() {
    with_ws(|svc| async move {
        let mut ws = svc.open_websocket(ws_request("/push")).await.unwrap();
        let mut texts = Vec::new();
        let mut saw_close = false;
        while let Some(item) = ws.next().await {
            match item {
                Ok(Message::Text(t)) => texts.push(t.as_str().to_owned()),
                Ok(Message::Close(_)) => {
                    saw_close = true;
                    break;
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        assert_eq!(
            texts,
            (0..5).map(|i| format!("msg-{i}")).collect::<Vec<_>>()
        );
        assert!(saw_close, "expected a Close frame from the server");
    });
}

#[test]
fn bidirectional_concurrent() {
    with_ws(|svc| async move {
        let ws = svc.open_websocket(ws_request("/echo")).await.unwrap();
        let (mut sink, mut stream) = ws.split();

        // Send 50 messages from a background task while we read concurrently.
        let sender = tokio::task::spawn_local(async move {
            for i in 0..50u32 {
                sink.send(Message::text(format!("c{i}"))).await.unwrap();
            }
            sink
        });

        let mut got = 0u32;
        while got < 50 {
            match stream.next().await {
                Some(Ok(Message::Text(t))) => {
                    assert_eq!(t.as_str(), format!("c{got}"));
                    got += 1;
                }
                other => panic!("unexpected {other:?}"),
            }
        }
        let mut sink = sender.await.unwrap();
        sink.send(Message::Close(None)).await.unwrap();
    });
}

#[test]
fn close_with_code_and_reason() {
    with_ws(|svc| async move {
        let mut ws = svc.open_websocket(ws_request("/close")).await.unwrap();
        match ws.next().await {
            Some(Ok(Message::Close(Some(cf)))) => {
                assert_eq!(u16::from(cf.code), 1001, "CloseCode::Away");
                assert_eq!(cf.reason.as_str(), "see ya");
            }
            other => panic!("expected close with code+reason, got {other:?}"),
        }
    });
}

#[test]
fn open_websocket_rejected_when_server_does_not_upgrade() {
    with_ws(|svc| async move {
        // The backend answers /nosuch with a plain 404 (no upgrade extension);
        // `open_websocket` must surface that as an error, not a WebSocket.
        let err = match svc.open_websocket(ws_request("/nosuch")).await {
            Err(e) => e,
            Ok(_) => panic!("expected the upgrade to be rejected"),
        };
        assert!(
            err.to_string().contains("404"),
            "error should mention the status, got: {err}"
        );
    });
}

#[test]
fn ping_pong_kept_local() {
    with_ws(|svc| async move {
        // Ping/Pong are not representable in the capnp WebSocket interface and
        // are kept local to each hop. Sending them must be a no-op that neither
        // errors nor disturbs the ordering of real frames.
        let mut ws = svc.open_websocket(ws_request("/echo")).await.unwrap();
        ws.send(Message::Ping(Bytes::from_static(b"ping")))
            .await
            .unwrap();
        ws.send(Message::text("after-ping")).await.unwrap();
        ws.send(Message::Pong(Bytes::from_static(b"pong")))
            .await
            .unwrap();
        ws.send(Message::text("after-pong")).await.unwrap();
        match ws.next().await {
            Some(Ok(Message::Text(t))) => assert_eq!(t.as_str(), "after-ping"),
            other => panic!("expected after-ping, got {other:?}"),
        }
        match ws.next().await {
            Some(Ok(Message::Text(t))) => assert_eq!(t.as_str(), "after-pong"),
            other => panic!("expected after-pong, got {other:?}"),
        }
        ws.send(Message::Close(None)).await.unwrap();
    });
}

#[test]
fn client_drop_seen_by_server() {
    with_ws_ctl(|svc, ctl| async move {
        let (tx, rx) = oneshot::channel();
        *ctl.client_gone_tx.borrow_mut() = Some(tx);

        let ws = svc
            .open_websocket(ws_request("/dropobserve"))
            .await
            .unwrap();
        // Drop the client end without sending Close; the server's inbound stream
        // must terminate as a result.
        drop(ws);

        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("server did not observe client drop within timeout")
            .expect("control sender dropped without signalling");
    });
}
