#include "geerpc/server/server.h"
#include "geerpc/codec/protobuf_codec.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/in.h>

#include <nlohmann/json.hpp>
#include <httplib.h>

#include <thread>
#include <chrono>
#include <future>
#include <sstream>
#include <cstring>
#include <iostream>

namespace geerpc {

using json = nlohmann::json;

// ── helpers ───────────────────────────────────────────────────────────────────

static bool readExact(int fd, void* buf, size_t n) {
    char* p = static_cast<char*>(buf);
    while (n > 0) {
        ssize_t r = ::read(fd, p, n);
        if (r <= 0) return false;
        p += r; n -= r;
    }
    return true;
}

// Read a line terminated by '\n' from fd (strips \r).
static std::string readLineFromFd(int fd) {
    std::string line;
    char c;
    while (true) {
        ssize_t r = ::read(fd, &c, 1);
        if (r <= 0) break;
        if (c == '\n') break;
        if (c != '\r') line += c;
    }
    return line;
}

// Write all bytes of s to fd.
static bool writeAllToFd(int fd, const std::string& s) {
    size_t n = s.size();
    const char* p = s.data();
    while (n > 0) {
        ssize_t w = ::write(fd, p, n);
        if (w <= 0) return false;
        p += w; n -= w;
    }
    return true;
}

// Read a JSON object terminated by '\n' from fd.
static bool readJsonLine(int fd, json& out) {
    std::string line;
    char c;
    while (true) {
        ssize_t r = ::read(fd, &c, 1);
        if (r <= 0) return false;
        if (c == '\n') break;
        line += c;
    }
    try { out = json::parse(line); return true; }
    catch (...) { return false; }
}

// ── Server ────────────────────────────────────────────────────────────────────

Server::Server(ServerOption opt) : opt_(std::move(opt)) {}
Server::~Server() {}

bool Server::RegisterService(ServicePtr svc) {
    std::lock_guard<std::mutex> lk(service_mu_);
    auto [it, ok] = services_.emplace(svc->name(), svc);
    return ok;
}

// Accept: create TCP listener, spawn a thread per connection.
void Server::Accept(int port, std::function<void()> on_listening) {
    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        std::cerr << "rpc server: socket error\n";
        return;
    }
    int yes = 1;
    ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(static_cast<uint16_t>(port));
    addr.sin_addr.s_addr = INADDR_ANY;

    if (::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "rpc server: bind error\n";
        ::close(listen_fd);
        return;
    }
    if (::listen(listen_fd, 128) < 0) {
        std::cerr << "rpc server: listen error\n";
        ::close(listen_fd);
        return;
    }
    std::cout << "rpc server: listening on port " << port << "\n";

    // Bug8 fix: notify caller only after listen() succeeds.
    if (on_listening) on_listening();

    for (;;) {
        int conn_fd = ::accept(listen_fd, nullptr, nullptr);
        if (conn_fd < 0) {
            if (errno == EINTR) continue;  // Bug6 fix: retry on signal interrupt
            std::cerr << "rpc server: accept error\n";
            break;
        }
        std::thread([this, conn_fd]() { ServeConn(conn_fd); }).detach();
    }
    ::close(listen_fd);
}

// ServeConn: negotiate option then dispatch to serveCodec.
void Server::ServeConn(int fd) {
    geerpc::Option opt;
    if (!readOption(fd, opt)) {
        ::close(fd);
        return;
    }
    if (opt.magic_number() != MagicNumber) {
        std::cerr << "rpc server: invalid magic number\n";
        ::close(fd);
        return;
    }
    auto factory_fn = codec::CodecFactory::instance().Get(opt.codec_type());
    if (!factory_fn) {
        std::cerr << "rpc server: unknown codec type: " << opt.codec_type() << "\n";
        ::close(fd);
        return;
    }
    auto handle_ms = std::chrono::milliseconds(opt.handle_timeout() / 1000000);
    serveCodec(factory_fn(fd), handle_ms);
}

// ServeConnFromHTTP: handle an HTTP CONNECT tunnel, then hand off to ServeConn.
void Server::ServeConnFromHTTP(int fd) {
    // Read the request line, e.g. "CONNECT /_geerpc_ HTTP/1.0"
    std::string request_line = readLineFromFd(fd);
    if (request_line.find("CONNECT") == std::string::npos) {
        writeAllToFd(fd, "HTTP/1.0 405 Method Not Allowed\r\n\r\n");
        ::close(fd);
        return;
    }
    // Drain remaining request headers
    while (true) {
        std::string line = readLineFromFd(fd);
        if (line.empty()) break;
    }
    // Send 200 to complete the HTTP CONNECT handshake
    if (!writeAllToFd(fd, "HTTP/1.0 200 Connected to Gee RPC\r\n\r\n")) {
        ::close(fd);
        return;
    }
    // Now treat the connection as a plain RPC connection
    ServeConn(fd);
}

// ServeDebugHTTP: start a debug HTTP server in a background thread.
void Server::ServeDebugHTTP(int port) {
    std::thread([this, port]() {
        httplib::Server svr;
        svr.Get("/debug/geerpc", [this](const httplib::Request&, httplib::Response& res) {
            std::string html;
            html += "<html><body><title>GeeRPC Services</title>\n";
            std::lock_guard<std::mutex> lk(service_mu_);
            for (auto& [svc_name, svc] : services_) {
                html += "<hr><h2>Service: " + svc_name + "</h2>\n";
                html += "<table border=1><tr><th>Method</th><th>Calls</th></tr>\n";
                // Iterate methods via FindMethod is not possible directly;
                // expose via a helper that returns all method stats.
                // We access the internal map via a friend or public helper.
                for (auto& [method_name, minfo] : svc->methods_) {
                    html += "<tr><td>" + method_name + "</td>";
                    html += "<td>" + std::to_string(minfo.num_calls.load()) + "</td></tr>\n";
                }
                html += "</table>\n";
            }
            html += "</body></html>\n";
            res.set_content(html, "text/html");
        });
        std::cout << "rpc debug server: listening on port " << port
                  << " at /debug/geerpc\n";
        svr.listen("0.0.0.0", port);
    }).detach();
}

// readOption: read a JSON line and decode into geerpc::Option proto.
bool Server::readOption(int fd, geerpc::Option& opt) {
    json j;
    if (!readJsonLine(fd, j)) return false;
    opt.set_magic_number(j.value("magic_number", 0));
    opt.set_codec_type(j.value("codec_type", std::string(codec::ProtobufType)));
    opt.set_connect_timeout(j.value("connect_timeout", int64_t(0)));
    opt.set_handle_timeout(j.value("handle_timeout", int64_t(0)));
    return true;
}

// serveCodec: main read-dispatch-reply loop.
void Server::serveCodec(codec::CodecPtr cc,
                        std::chrono::milliseconds handle_timeout) {
    std::mutex sending;

    for (;;) {
        geerpc::Header h;
        if (!cc->ReadHeader(h)) break;  // EOF or error

        std::string body;
        if (!cc->ReadBody(body)) break;

        ServicePtr svc;
        MethodInfo* minfo = nullptr;
        if (!findService(h.service_method(), svc, minfo)) {
            geerpc::Header eh = h;
            eh.set_error("rpc server: can't find service " + h.service_method());
            sendResponse(cc, eh, "", sending);
            continue;
        }

        // Execute handler (possibly with timeout).
        // Bug1+2 fix: use packaged_task + detached thread instead of std::async.
        // std::async(launch::async) blocks in its future destructor, negating
        // the timeout. Capture body by value and minfo by pointer (safe: svc
        // keeps the Service alive) to avoid dangling references after detach.
        auto do_call = [body_copy = body, minfo]() -> std::pair<std::string, std::string> {
            std::string reply_bytes;
            minfo->num_calls.fetch_add(1, std::memory_order_relaxed);
            std::string err = minfo->handler(body_copy, reply_bytes);
            return {reply_bytes, err};
        };

        if (handle_timeout.count() == 0) {
            auto [reply, err] = do_call();
            geerpc::Header rh = h;
            rh.set_error(err);
            sendResponse(cc, rh, err.empty() ? reply :"", sending);
        } else {
            std::packaged_task<std::pair<std::string,std::string>()> task(do_call);
            auto fut = task.get_future();
            std::thread(std::move(task)).detach();
            if (fut.wait_for(handle_timeout) == std::future_status::timeout) {
                geerpc::Header eh = h;
                eh.set_error("rpc server: request handle timeout");
                sendResponse(cc, eh, "", sending);
            } else {
                auto [reply, err] = fut.get();
                geerpc::Header rh = h;
                rh.set_error(err);
                sendResponse(cc, rh, err.empty() ? reply : "", sending);
            }
        }
    }
    cc->Close();
}

bool Server::findService(const std::string& service_method,
                         ServicePtr& svc, MethodInfo*& minfo) {
    auto dot = service_method.rfind('.');
    if (dot == std::string::npos) return false;
    std::string svc_name    = service_method.substr(0, dot);
    std::string method_name = service_method.substr(dot + 1);

    std::lock_guard<std::mutex> lk(service_mu_);
    auto it = services_.find(svc_name);
    if (it == services_.end()) return false;
    svc   = it->second;
    minfo = svc->FindMethod(method_name);
    return minfo != nullptr;
}

void Server::sendResponse(codec::CodecPtr cc,
                          const geerpc::Header& h,
                          const std::string& body,
                          std::mutex& sending) {
    std::lock_guard<std::mutex> lk(sending);
    if (!cc->Write(h, body)) {
        std::cerr << "rpc server: write response error\n";
    }
}

// ── Global defaults ───────────────────────────────────────────────────────────

Server& DefaultServer() {
    static Server s;
    return s;
}

bool Register(ServicePtr svc) { return DefaultServer().RegisterService(svc); }
void Accept(int port)         { DefaultServer().Accept(port); }

} // namespace geerpc
