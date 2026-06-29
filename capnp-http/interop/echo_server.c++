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
// A minimal C++ `http-over-capnp` server used to verify cross-implementation
// (and cross-version) wire compatibility with the Rust `capnp-http` client.
//
// It serves a `capnp::HttpService` (an echo service) over a two-party RPC
// connection on a TCP port, using the reference C++ `HttpOverCapnpFactory`.
//
// Build with `interop/build.sh` (requires a built `../capnproto` checkout). The
// program prints the bound port number on the first line of stdout, then serves
// until killed.

#include <capnp/compat/http-over-capnp.h>
#include <capnp/rpc-twoparty.h>
#include <kj/async-io.h>
#include <kj/compat/http.h>
#include <kj/debug.h>
#include <stdio.h>

namespace {

class EchoHttpService final : public kj::HttpService {
  // Echoes the request body back as the response body (200 OK). For an empty
  // request body, replies with a fixed "hello-from-c++" payload so that simple
  // GETs have something to assert on.
public:
  explicit EchoHttpService(kj::HttpHeaderTable& table) : table(table) {}

  kj::Promise<void> request(kj::HttpMethod method, kj::StringPtr url,
                            const kj::HttpHeaders& headers,
                            kj::AsyncInputStream& requestBody,
                            Response& response) override {
    auto body = co_await requestBody.readAllText();
    kj::String payload = body.size() > 0 ? kj::mv(body) : kj::str("hello-from-c++");

    kj::HttpHeaders respHeaders(table);
    respHeaders.set(kj::HttpHeaderId::CONTENT_TYPE, "text/plain");
    auto out = response.send(200, "OK", respHeaders, payload.size());
    co_await out->write(payload.asBytes());
  }

private:
  kj::HttpHeaderTable& table;
};

}  // namespace

int main(int argc, char* argv[]) {
  kj::StringPtr port = argc >= 2 ? argv[1] : "0";

  auto io = kj::setupAsyncIo();

  kj::HttpHeaderTable::Builder tableBuilder;
  capnp::ByteStreamFactory streamFactory;
  capnp::HttpOverCapnpFactory factory(
      streamFactory, capnp::HttpOverCapnpFactory::HeaderIdBundle(tableBuilder),
      capnp::HttpOverCapnpFactory::LEVEL_2);
  auto headerTable = tableBuilder.build();

  auto service = kj::heap<EchoHttpService>(*headerTable);
  capnp::HttpService::Client capnpService = factory.kjToCapnp(kj::mv(service));

  auto& network = io.provider->getNetwork();
  auto addr = network.parseAddress(kj::str("127.0.0.1:", port)).wait(io.waitScope);
  auto listener = addr->listen();

  // Announce the bound port so the test harness can connect.
  printf("%u\n", listener->getPort());
  fflush(stdout);

  capnp::TwoPartyServer server(kj::mv(capnpService));
  server.listen(*listener).wait(io.waitScope);
  return 0;
}
