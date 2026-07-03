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

//! Cross-implementation interop test: drives the Rust `capnp-http` client
//! against the reference C++ `http-over-capnp` echo server (see `interop/`).
//!
//! Ignored by default because it requires a built C++ capnproto checkout and a
//! C++23 toolchain. Run it explicitly with:
//!
//!   cargo test -p capnp-http --test interop -- --ignored --nocapture

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

use bytes::Bytes;
use http::{Method, Request};
use http_body_util::{BodyExt, Empty, Full};
use capnp_http::HttpService;

use capnp_http::capnp_to_service;
use capnp_http::http_over_capnp_capnp::http_service;
use capnp_rpc::{rpc_twoparty_capnp::Side, twoparty, RpcSystem};

#[test]
#[ignore = "requires a built C++ capnproto checkout (see interop/README.md)"]
fn interop_with_cpp_server() {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let build_sh = format!("{manifest}/interop/build.sh");
    let echo_server = format!("{manifest}/../target/interop/echo_server");

    // 1. Build the C++ echo server.
    let status = Command::new("sh")
        .arg(&build_sh)
        .status()
        .expect("failed to run interop/build.sh");
    assert!(
        status.success(),
        "interop/build.sh failed; see interop/README.md (a consistently built \
         capnproto checkout is required)"
    );

    // 2. Spawn it and read the bound port from its first stdout line.
    let mut child = Command::new(&echo_server)
        .arg("0")
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to spawn echo_server");

    // Ensure the child is killed when we leave this scope. Constructed before
    // anything below can panic (e.g. a malformed port line), so a failing test
    // never orphans the C++ server.
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let stdout = child.stdout.take().unwrap();
    let _guard = Kill(child);

    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line).expect("read port line");
    let port: u16 = line.trim().parse().expect("parse port");

    // 3. Drive the Rust client against it.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        use tokio_util::compat::TokioAsyncReadCompatExt;

        let stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect to C++ server");
        stream.set_nodelay(true).unwrap();
        let (reader, writer) = futures::AsyncReadExt::split(stream.compat());

        let network = Box::new(twoparty::VatNetwork::new(
            futures::io::BufReader::new(reader),
            futures::io::BufWriter::new(writer),
            Side::Client,
            Default::default(),
        ));
        let mut rpc = RpcSystem::new(network, None);
        let http_client: http_service::Client = rpc.bootstrap(Side::Server);
        tokio::task::spawn_local(async move {
            let _ = rpc.await;
        });

        let svc = capnp_to_service(http_client, |f| {
            tokio::task::spawn_local(f);
        });

        // GET / -> the C++ server replies with a fixed body for empty requests.
        let resp = svc
            .call(
                Request::builder()
                    .method(Method::GET)
                    .uri("/")
                    .body(Empty::<Bytes>::new())
                    .unwrap(),
            )
            .await
            .expect("GET should succeed");
        assert_eq!(resp.status(), 200);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"hello-from-c++", "GET body mismatch");

        // POST /echo -> the C++ server echoes the request body.
        let payload = Bytes::from_static(b"rust-client -> c++ server");
        let resp = svc
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri("/echo")
                    .body(Full::new(payload.clone()))
                    .unwrap(),
            )
            .await
            .expect("POST should succeed");
        assert_eq!(resp.status(), 200);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, payload, "echo body mismatch");

        println!("interop OK: Rust client <-> C++ http-over-capnp server");
    });
}
