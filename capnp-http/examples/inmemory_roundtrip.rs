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
//
// A self-contained demo of `http-over-capnp` using the `hyper` adapter: a hyper
// service is exposed over a Cap'n Proto RPC connection and called as a hyper
// client, all in one process over an in-memory two-party connection. See the
// `pluggable_frameworks` example for the same idea with `tower` and `axum`.
//
// Run with:  cargo run -p capnp-http --example inmemory_roundtrip

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use http::{Method, Request, Response};
use http_body_util::{BodyExt, Empty, Full};
use hyper::service::Service;

use capnp_http::http_over_capnp_capnp::http_service;
use capnp_http::{capnp_to_service, service_to_capnp, HttpService, IncomingBody};
use capnp_rpc::{rpc_twoparty_capnp::Side, twoparty, RpcSystem};
use futures::FutureExt;

/// A tiny backend: greets on GET, echoes the body on POST.
#[derive(Clone)]
struct DemoService;

impl Service<Request<IncomingBody>> for DemoService {
    type Response = Response<Full<Bytes>>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>>>>;

    fn call(&self, req: Request<IncomingBody>) -> Self::Future {
        Box::pin(async move {
            let is_post = req.method() == Method::POST;
            let body = req.into_body().collect().await.unwrap().to_bytes();
            let out = if is_post {
                body
            } else {
                Bytes::from_static(b"hello from the capnp-backed service")
            };
            Ok(Response::new(Full::new(out)))
        })
    }
}

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        // In-memory duplex transport between the two RPC endpoints.
        let (c2s_writer, c2s_reader) = async_byte_channel::channel();
        let (s2c_writer, s2c_reader) = async_byte_channel::channel();

        // Server: expose the hyper service over capnp.
        let server_net = Box::new(twoparty::VatNetwork::new(
            c2s_reader,
            s2c_writer,
            Side::Server,
            Default::default(),
        ));
        let server_rpc = RpcSystem::new(server_net, Some(service_to_capnp(DemoService).client));

        // Client: bootstrap the remote HttpService and present it as a hyper service.
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

        // GET /
        let resp = svc
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri("/")
                    .body(Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        println!(
            "GET  /      -> {status} {:?}",
            String::from_utf8_lossy(&body)
        );

        // POST /echo
        let resp = svc
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri("/echo")
                    .body(Full::new(Bytes::from_static(b"hello over capnp")))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        println!(
            "POST /echo  -> {status} {:?}",
            String::from_utf8_lossy(&body)
        );
    });
}
