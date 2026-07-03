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

use capnp_http::http_over_capnp_capnp::http_service;
use capnp_http::{capnp_to_service, CapnpHttpService};
use capnp_rpc::{rpc_twoparty_capnp::Side, twoparty, RpcSystem};
use futures::FutureExt;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

/// Wires `bootstrap` as the server capability over a loopback TCP socket and
/// returns the client-side `CapnpHttpService`. Must be called on a tokio
/// `LocalSet`.
pub(crate) async fn start_client(bootstrap: http_service::Client) -> CapnpHttpService {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let connect = tokio::net::TcpStream::connect(addr);
    let accept = async { listener.accept().await.unwrap().0 };
    let (client_stream, server_stream) = tokio::join!(connect, accept);
    let client_stream = client_stream.unwrap();
    client_stream.set_nodelay(true).unwrap();
    server_stream.set_nodelay(true).unwrap();

    let (cr, cw) = client_stream.into_split();
    let (sr, sw) = server_stream.into_split();

    let server_net = Box::new(twoparty::VatNetwork::new(
        sr.compat(),
        sw.compat_write(),
        Side::Server,
        Default::default(),
    ));
    let server_rpc = RpcSystem::new(server_net, Some(bootstrap.client));

    let client_net = Box::new(twoparty::VatNetwork::new(
        cr.compat(),
        cw.compat_write(),
        Side::Client,
        Default::default(),
    ));
    let mut client_rpc = RpcSystem::new(client_net, None);
    let http_client = client_rpc.bootstrap(Side::Server);

    tokio::task::spawn_local(server_rpc.map(|_| ()));
    tokio::task::spawn_local(client_rpc.map(|_| ()));

    capnp_to_service(http_client, |f| {
        tokio::task::spawn_local(f);
    })
}
