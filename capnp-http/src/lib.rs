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

//! `http-over-capnp` for Rust: a bridge between HTTP services and the Cap'n Proto
//! `HttpService` RPC protocol.
//!
//! This is the Rust counterpart of the C++ `capnp/compat/http-over-capnp.{h,c++}`
//! library. The core is HTTP-framework-agnostic: it is written against the local
//! [`HttpService`] trait and the neutral interchange crates (`http`, `http-body`,
//! `bytes`), not against any particular HTTP library. It is built on top of the
//! [`capnp_byte_stream`] crate, which carries HTTP bodies.
//!
//! # Plugging in an HTTP library
//!
//! Implement [`HttpService`] for your service type and pass it to
//! [`service_to_capnp`]; consume a remote capability as an [`HttpService`] via
//! [`capnp_to_service`]. With the `hyper` feature (enabled by default) every
//! `hyper::service::Service` implements [`HttpService`] automatically, and
//! [`HyperService`] adapts any [`HttpService`] back into a
//! `hyper::service::Service`. With the `tower` feature, [`TowerService`] wraps
//! any `tower::Service` — unlocking `tower` middleware stacks, `axum::Router`,
//! `tonic`, and the wider ecosystem. See the `pluggable_frameworks` example.

/// Code generated from
/// [`http-over-capnp.capnp`](https://github.com/capnproto/capnproto/blob/master/c%2B%2B/src/capnp/compat/http-over-capnp.capnp).
///
/// This is committed, generated code (regenerate with
/// `regenerate-http-over-capnp-schema-code.sh`). Because the schema imports
/// `byte-stream.capnp`, the generator is told (via `crate_provides`) that the
/// `ByteStream` types live in the `capnp-byte-stream` crate, so this code
/// references `capnp_byte_stream::byte_stream_capnp::*`.
#[allow(clippy::all)]
pub mod http_over_capnp_capnp;

// Conversions between capnp wire types and the `http` crate types; building
// blocks for the service adapters.
mod body;
#[cfg(feature = "connect")]
mod connect;
mod headers;
mod method;
mod service;
#[cfg(feature = "websocket")]
mod websocket;

pub use body::{pump_body_to_byte_stream, IncomingBody, PumpOutcome};
#[cfg(feature = "connect")]
pub use connect::{
    connect_service_to_capnp, service_to_capnp_with_connect, ConnectOutcome, ConnectResponse,
    ConnectService, ConnectSettings, Tunnel,
};
#[cfg(feature = "hyper")]
pub use service::HyperService;
#[cfg(feature = "tokio")]
pub use service::tokio_local_spawn;
#[cfg(feature = "tower")]
pub use service::TowerService;
pub use service::{capnp_to_service, service_to_capnp, CapnpHttpService, HttpService};
#[cfg(feature = "websocket")]
pub use websocket::{accept_websocket, PendingWebSocket, WebSocket, WebSocketUpgrade};

/// Re-export of the `tungstenite` crate, whose [`Message`](tungstenite::Message)
/// type is the WebSocket frame representation used by this crate.
#[cfg(feature = "websocket")]
pub use tungstenite;
