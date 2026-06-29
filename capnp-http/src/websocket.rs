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

//! WebSocket bridging between the capnp `WebSocket` interface and
//! [`tungstenite::Message`] (the frame type used by `tokio-tungstenite` /
//! `hyper-tungstenite`).
//!
//! The capnp `WebSocket` interface is message-oriented (`sendText` / `sendData` /
//! `close`), so it maps cleanly onto `tungstenite::Message`. Control frames
//! (`Ping` / `Pong`) are not representable in the capnp interface and are kept
//! local at each hop (tungstenite auto-replies to pings at real socket edges),
//! matching the C++ / `kj::WebSocket` semantics.
//!
//! The [`WebSocket`] type implements both [`futures::Stream`] (incoming frames)
//! and [`futures::Sink`] (outgoing frames), mirroring
//! `tokio_tungstenite::WebSocketStream`.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use bytes::Bytes;
use capnp::capability::Promise;
use capnp::Error;
use futures::channel::{mpsc, oneshot};
use futures::{Sink, SinkExt, Stream};
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::protocol::CloseFrame;
use tungstenite::Message;

use crate::http_over_capnp_capnp::web_socket;

/// How many inbound frames may be buffered before backpressure is applied to the
/// peer's streaming `sendText` / `sendData` calls.
const INBOUND_QUEUE_CAPACITY: usize = 8;

/// A bidirectional WebSocket carried over Cap'n Proto.
///
/// Outgoing frames (via the [`Sink`] impl) are sent as method calls on a capnp
/// `WebSocket` client; incoming frames (via the [`Stream`] impl) arrive as method
/// calls on a hosted capnp `WebSocket` server.
pub struct WebSocket {
    out: Option<web_socket::Client>,
    out_in_flight: Option<Promise<(), Error>>,
    incoming: mpsc::Receiver<Message>,
    /// Opaque value kept alive for the lifetime of the WebSocket (used by the
    /// client adapter to attach the background request driver).
    _keepalive: Option<Box<dyn core::any::Any>>,
}

impl WebSocket {
    pub(crate) fn new(out: web_socket::Client, incoming: mpsc::Receiver<Message>) -> Self {
        Self {
            out: Some(out),
            out_in_flight: None,
            incoming,
            _keepalive: None,
        }
    }

    pub(crate) fn attach_keepalive(&mut self, keepalive: Box<dyn core::any::Any>) {
        self._keepalive = Some(keepalive);
    }

    fn poll_in_flight(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        match &mut self.out_in_flight {
            None => Poll::Ready(Ok(())),
            Some(fut) => match Pin::new(fut).poll(cx) {
                Poll::Ready(r) => {
                    self.out_in_flight = None;
                    Poll::Ready(r)
                }
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

impl Stream for WebSocket {
    type Item = Result<Message, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().incoming)
            .poll_next(cx)
            .map(|opt| opt.map(Ok))
    }
}

impl Sink<Message> for WebSocket {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.get_mut().poll_in_flight(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Error> {
        let this = self.get_mut();
        if this.out_in_flight.is_some() {
            // The `Sink` contract requires `poll_ready` before each `start_send`.
            // Overwriting the in-flight promise would *cancel* that RPC and
            // silently lose the frame, so reject the misuse instead.
            return Err(Error::failed(
                "start_send called with a frame still in flight (poll_ready was not awaited)"
                    .to_string(),
            ));
        }
        let out = this
            .out
            .as_ref()
            .ok_or_else(|| Error::disconnected("WebSocket already closed".to_string()))?;
        match item {
            Message::Text(text) => {
                let mut req = out.send_text_request();
                req.get().set_text(text.as_str());
                this.out_in_flight = Some(req.send());
            }
            Message::Binary(data) => {
                let mut req = out.send_data_request();
                req.get().set_data(data.as_ref());
                this.out_in_flight = Some(req.send());
            }
            Message::Close(frame) => {
                let mut req = out.close_request();
                {
                    let mut b = req.get();
                    // A code-less close cannot be represented in the capnp `close`
                    // message, and RFC 6455 reserves 1005 ("no status received")
                    // for local reporting only: it must never be sent on the wire,
                    // and a peer relaying our code to a real socket would do just
                    // that. Map a code-less close (and an explicit 1005, which
                    // means the same thing) to 1000 (normal closure) instead,
                    // which is how a bare close is conventionally relayed.
                    let (code, reason) = match &frame {
                        Some(cf) if u16::from(cf.code) != 1005 => {
                            (u16::from(cf.code), cf.reason.as_str())
                        }
                        _ => (1000, ""),
                    };
                    b.set_code(code);
                    b.set_reason(reason);
                }
                let remote = req.send();
                this.out_in_flight = Some(Promise::from_future(async move {
                    remote.promise.await.map(|_| ())
                }));
                // No more frames may be sent after close.
                this.out = None;
            }
            // Control frames are kept local and not forwarded across capnp.
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.get_mut().poll_in_flight(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let this = self.get_mut();
        match this.poll_in_flight(cx) {
            Poll::Ready(Ok(())) => {
                this.out = None;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

// The hosted capnp WebSocket server that feeds inbound frames into a channel.
struct WebSocketReceiver {
    tx: RefCell<Option<mpsc::Sender<Message>>>,
}

impl WebSocketReceiver {
    async fn forward(&self, message: Message) -> Result<(), Error> {
        let sender = self.tx.borrow().clone();
        match sender {
            Some(mut tx) => tx
                .send(message)
                .await
                .map_err(|_| Error::disconnected("WebSocket consumer dropped".to_string())),
            None => Err(Error::disconnected("WebSocket already closed".to_string())),
        }
    }
}

impl web_socket::Server for WebSocketReceiver {
    async fn send_text(self: Rc<Self>, params: web_socket::SendTextParams) -> Result<(), Error> {
        let text = params
            .get()?
            .get_text()?
            .to_str()
            .map_err(|e| Error::failed(format!("WebSocket text frame is not valid UTF-8: {e}")))?
            .to_owned();
        self.forward(Message::text(text)).await
    }

    async fn send_data(self: Rc<Self>, params: web_socket::SendDataParams) -> Result<(), Error> {
        let data = Bytes::copy_from_slice(params.get()?.get_data()?);
        self.forward(Message::binary(data)).await
    }

    async fn close(
        self: Rc<Self>,
        params: web_socket::CloseParams,
        _results: web_socket::CloseResults,
    ) -> Result<(), Error> {
        let reader = params.get()?;
        let code = reader.get_code();
        let reason = reader.get_reason()?.to_str().unwrap_or("").to_owned();
        // 1005 is reserved for local "no status" reporting and must never be
        // relayed onto a real socket, so deliver it as a code-less close. (Our
        // own sender never emits 1005, see `start_send`, but another
        // implementation might.)
        let frame = if code == 1005 {
            None
        } else {
            Some(CloseFrame {
                code: CloseCode::from(code),
                reason: reason.into(),
            })
        };
        self.forward(Message::Close(frame)).await?;
        // End the inbound stream after delivering the close frame.
        *self.tx.borrow_mut() = None;
        Ok(())
    }
}

/// Creates a capnp `WebSocket` client whose inbound frames are delivered to the
/// returned receiver. Used to construct the "incoming" half of a [`WebSocket`].
pub(crate) fn web_socket_receiver() -> (web_socket::Client, mpsc::Receiver<Message>) {
    let (tx, rx) = mpsc::channel(INBOUND_QUEUE_CAPACITY);
    let client = capnp_rpc::new_client(WebSocketReceiver {
        tx: RefCell::new(Some(tx)),
    });
    (client, rx)
}

// Server-side upgrade signalling: an `HttpService` signals a WebSocket upgrade
// by returning a `101` response whose extensions contain a `WebSocketUpgrade`.
// Because `WebSocket` is `!Send` (it holds capnp capabilities) it cannot itself
// live in `http::Extensions` (which requires `Send + Sync`). Instead, the
// extension carries a small `Send` token, and the (`!Send`) one-shot fulfiller is
// held in a thread-local registry. This is sound because the whole stack is
// single-threaded (capnp-rpc). `service_to_capnp` removes the registry entry when
// it processes the upgrade; an entry only lingers if a `101` response built via
// `accept_websocket()` is never routed back through `service_to_capnp`.

thread_local! {
    static UPGRADES: RefCell<HashMap<u64, oneshot::Sender<WebSocket>>> =
        RefCell::new(HashMap::new());
    static NEXT_TOKEN: Cell<u64> = const { Cell::new(1) };
}

/// Placed in a `101` response's extensions to signal a WebSocket upgrade. Created
/// by [`accept_websocket`].
///
/// `Clone` is provided only for `http::Extensions` compatibility. Do **not** rely
/// on cloning to fan a single upgrade into multiple `101` responses: claiming the
/// fulfiller is destructive (it removes the entry from a thread-local registry),
/// so only the first clone to be consumed succeeds and any others silently fail
/// to upgrade.
#[derive(Clone)]
pub struct WebSocketUpgrade {
    token: u64,
}

impl WebSocketUpgrade {
    /// Used by the server adapter to claim the fulfiller for this upgrade.
    ///
    /// Destructive: removes the fulfiller from the thread-local registry, so a
    /// second call (e.g. via a clone of this token) returns `None`.
    pub(crate) fn take_fulfiller(&self) -> Option<oneshot::Sender<WebSocket>> {
        UPGRADES.with(|u| u.borrow_mut().remove(&self.token))
    }
}

/// A future that resolves to the negotiated server-side [`WebSocket`] once the
/// adapter has completed the `startWebSocket` handshake.
pub struct PendingWebSocket {
    rx: oneshot::Receiver<WebSocket>,
}

impl Future for PendingWebSocket {
    type Output = Result<WebSocket, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.rx).poll(cx).map(|r| {
            r.map_err(|_| Error::failed("WebSocket upgrade was not completed".to_string()))
        })
    }
}

/// Begins accepting a WebSocket from within an [`HttpService`](crate::HttpService).
///
/// Returns a [`WebSocketUpgrade`] to place in the `101` response's extensions, and
/// a [`PendingWebSocket`] that resolves to the negotiated [`WebSocket`] (typically
/// awaited in a spawned task). Mirrors `hyper_tungstenite::upgrade`.
pub fn accept_websocket() -> (WebSocketUpgrade, PendingWebSocket) {
    let token = NEXT_TOKEN.with(|c| {
        let t = c.get();
        c.set(t.wrapping_add(1));
        t
    });
    let (tx, rx) = oneshot::channel();
    UPGRADES.with(|u| u.borrow_mut().insert(token, tx));
    (WebSocketUpgrade { token }, PendingWebSocket { rx })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    /// Two cross-wired `WebSocket`s sharing local capnp capabilities, for testing
    /// the frame bridge without the full HTTP request flow.
    fn loopback() -> (WebSocket, WebSocket) {
        let (a_in, a_rx) = web_socket_receiver();
        let (b_in, b_rx) = web_socket_receiver();
        // `a` sends via b_in (arriving at b_rx) and receives a_rx.
        let a = WebSocket::new(b_in, a_rx);
        let b = WebSocket::new(a_in, b_rx);
        (a, b)
    }

    #[test]
    fn text_binary_and_close() {
        futures::executor::block_on(async {
            let (mut a, mut b) = loopback();

            a.send(Message::text("hello")).await.unwrap();
            match b.next().await {
                Some(Ok(Message::Text(t))) => assert_eq!(t.as_str(), "hello"),
                other => panic!("expected text, got {other:?}"),
            }

            b.send(Message::binary(vec![1u8, 2, 3])).await.unwrap();
            match a.next().await {
                Some(Ok(Message::Binary(d))) => assert_eq!(&d[..], &[1, 2, 3]),
                other => panic!("expected binary, got {other:?}"),
            }

            a.send(Message::Close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "bye".into(),
            })))
            .await
            .unwrap();
            match b.next().await {
                Some(Ok(Message::Close(Some(cf)))) => {
                    assert_eq!(u16::from(cf.code), 1000);
                    assert_eq!(cf.reason.as_str(), "bye");
                }
                other => panic!("expected close, got {other:?}"),
            }

            // After the close frame, the inbound stream ends.
            assert!(b.next().await.is_none());
        });
    }

    #[test]
    fn codeless_close_is_relayed_as_1000() {
        // RFC 6455 forbids sending 1005 on the wire; a `Close(None)` must cross
        // the capnp hop as a normal closure (1000), not as 1005.
        futures::executor::block_on(async {
            let (mut a, mut b) = loopback();
            a.send(Message::Close(None)).await.unwrap();
            match b.next().await {
                Some(Ok(Message::Close(Some(cf)))) => {
                    assert_eq!(u16::from(cf.code), 1000);
                    assert_eq!(cf.reason.as_str(), "");
                }
                other => panic!("expected close(1000), got {other:?}"),
            }

            // An *explicit* 1005 means the same thing and is remapped likewise.
            let (mut a, mut b) = loopback();
            a.send(Message::Close(Some(CloseFrame {
                code: CloseCode::from(1005),
                reason: "".into(),
            })))
            .await
            .unwrap();
            match b.next().await {
                Some(Ok(Message::Close(Some(cf)))) => {
                    assert_eq!(u16::from(cf.code), 1000);
                }
                other => panic!("expected close(1000), got {other:?}"),
            }
        });
    }
}
