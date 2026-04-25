#include "geerpc/client/client.h"
#include "geerpc/codec/protobuf_codec.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netdb.h>
#include <fcntl.h>
#include <poll.h>

#include <nlohmann/json.hpp>

#include <thread>
#include <iostream>
#include <sstream>
#include <cstring>

namespace geerpc {

using json = nlohmann::json;

// ── static helpers ────────────────────────────────────────────────────────────

static int connectTCP(const std::string& host, int port,
                      std::chrono::milliseconds timeout) {
    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    std::string port_str = std::to_string(port);
    if (::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res) != 0)
        return -1;

    int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) { ::freeaddrinfo(res); return -1; }

    if (timeout.count() <= 0) {
        // No timeout: blocking connect.
        if (::connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
            ::close(fd); ::freeaddrinfo(res); return -1;
        }
        ::freeaddrinfo(res);
        return fd;
    }

    // Bug3 fix: non-blocking connect + poll to honour connect_timeout.
    int flags = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    int rc = ::connect(fd, res->ai_addr, res->ai_addrlen);
    ::freeaddrinfo(res);

    if (rc == 0) {
        // Connected immediately.
        ::fcntl(fd, F_SETFL, flags);  // restore blocking
        return fd;
    }
    if (errno != EINPROGRESS) {
        ::close(fd); return -1;
    }

    struct pollfd pfd{};
    pfd.fd     = fd;
    pfd.events = POLLOUT;
    int ms = static_cast<int>(timeout.count());
    int ret = ::poll(&pfd, 1, ms);
    if (ret <= 0) {
        // Timeout or error.
        ::close(fd); return -1;
    }
    // Check for async connect error.
    int err = 0;
    socklen_t len = sizeof(err);
    ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
    if (err != 0) {
        ::close(fd); return -1;
    }
    ::fcntl(fd, F_SETFL, flags);  // restore blocking
    return fd;
}

static bool writeStr(int fd, const std::string& s) {
    size_t n = s.size();
    const char* p = s.data();
    while (n > 0) {
        ssize_t w = ::write(fd, p, n);
        if (w <= 0) return false;
        p += w; n -= w;
    }
    return true;
}

static std::string readLine(int fd) {
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

// ── Client construction ───────────────────────────────────────────────────────

Client::Client(codec::CodecPtr cc, ClientOption opt)
    : cc_(std::move(cc)), opt_(std::move(opt)) {
    // Start background receive loop.
    std::thread([this]() { receiveLoop(); }).detach();
}

Client::~Client() {
    Close();
}

bool Client::sendOption(int fd, const ClientOption& opt) {
    json j;
    j["magic_number"]    = opt.magic_number;
    j["codec_type"]      = opt.codec_type;
    j["connect_timeout"] = opt.connect_timeout.count() * 1000000LL; // ms -> ns
    j["handle_timeout"]  = opt.handle_timeout.count()  * 1000000LL;
    std::string line = j.dump() + "\n";
    return writeStr(fd, line);
}

// ── Dial variants ─────────────────────────────────────────────────────────────

ClientPtr Client::Dial(const std::string& host, int port, ClientOption opt) {
    int fd = connectTCP(host, port, opt.connect_timeout);
    if (fd < 0) {
        std::cerr << "rpc client: connect failed\n";
        return nullptr;
    }
    if (!sendOption(fd, opt)) {
        std::cerr << "rpc client: send option failed\n";
        ::close(fd);
        return nullptr;
    }
    auto factory_fn = codec::CodecFactory::instance().Get(opt.codec_type);
    if (!factory_fn) {
        std::cerr << "rpc client: unknown codec type\n";
        ::close(fd);
        return nullptr;
    }
    return std::shared_ptr<Client>(new Client(factory_fn(fd), std::move(opt)));
}

ClientPtr Client::DialHTTP(const std::string& host, int port, ClientOption opt) {
    int fd = connectTCP(host, port, opt.connect_timeout);
    if (fd < 0) return nullptr;

    // Send HTTP CONNECT
    std::string req = "CONNECT /_geerpc_ HTTP/1.0\r\n\r\n";
    if (!writeStr(fd, req)) { ::close(fd); return nullptr; }

    // Read response line: "HTTP/1.0 200 Connected to Gee RPC"
    std::string status = readLine(fd);
    if (status.find("200") == std::string::npos) {
        std::cerr << "rpc client: unexpected HTTP response: " << status << "\n";
        ::close(fd);
        return nullptr;
    }
    // Drain remaining headers
    while (true) {
        std::string line = readLine(fd);
        if (line.empty()) break;
    }

    if (!sendOption(fd, opt)) { ::close(fd); return nullptr; }

    auto factory_fn = codec::CodecFactory::instance().Get(opt.codec_type);
    if (!factory_fn) { ::close(fd); return nullptr; }
    return std::shared_ptr<Client>(new Client(factory_fn(fd), std::move(opt)));
}

ClientPtr Client::XDial(const std::string& rpc_addr, ClientOption opt) {
    auto at = rpc_addr.find('@');
    if (at == std::string::npos) {
        std::cerr << "rpc client: wrong format '" << rpc_addr << "', expect protocol@host:port\n";
        return nullptr;
    }
    std::string protocol = rpc_addr.substr(0, at);
    std::string addr     = rpc_addr.substr(at + 1);

    // Split host:port
    auto colon = addr.rfind(':');
    if (colon == std::string::npos) return nullptr;
    std::string host = addr.substr(0, colon);
    int port = std::stoi(addr.substr(colon + 1));

    if (protocol == "http") return DialHTTP(host, port, std::move(opt));
    return Dial(host, port, std::move(opt));
}

// ── availability / close ──────────────────────────────────────────────────────

bool Client::IsAvailable() const {
    std::lock_guard<std::mutex> lk(const_cast<std::mutex&>(mu_));
    return !shutdown_ && !closing_;
}

void Client::Close() {
    std::lock_guard<std::mutex> lk(mu_);
    if (closing_) return;
    closing_ = true;
    cc_->Close();
}

// ── call management ───────────────────────────────────────────────────────────

void Client::registerCall(CallPtr call) {
    std::lock_guard<std::mutex> lk(mu_);
    call->seq = seq_++;
    pending_[call->seq] = call;
}

CallPtr Client::removeCall(uint64_t seq) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = pending_.find(seq);
    if (it == pending_.end()) return nullptr;
    auto call = it->second;
    pending_.erase(it);
    return call;
}

void Client::terminateCalls(const std::string& err) {
    std::lock_guard<std::mutex> lk1(sending_);
    std::lock_guard<std::mutex> lk2(mu_);
    shutdown_ = true;
    for (auto& [seq, call] : pending_) {
        call->error = err;
        call->promise.set_value();
    }
    pending_.clear();
}

// ── receive loop ──────────────────────────────────────────────────────────────

void Client::receiveLoop() {
    std::string err;
    for (;;) {
        geerpc::Header h;
        if (!cc_->ReadHeader(h)) {
            err = "rpc client: connection closed";
            break;
        }
        auto call = removeCall(h.seq());
        if (!call) {
            // No matching call; drain the body.
            std::string dummy;
            cc_->ReadBody(dummy);
            continue;
        }
        if (!h.error().empty()) {
            call->error = h.error();
            std::string dummy;
            cc_->ReadBody(dummy);
            call->promise.set_value();
            continue;
        }
        if (!cc_->ReadBody(call->reply_bytes)) {
            call->error = "rpc client: error reading body";
        }
        call->promise.set_value();
    }
    terminateCalls(err);
}

// ── Go / Call ─────────────────────────────────────────────────────────────────

std::future<void> Client::Go(const std::string& service_method,
                              std::string args_bytes,
                              CallPtr& out_call) {
    auto call = std::make_shared<RpcCall>();
    call->service_method = service_method;
    call->args_bytes     = std::move(args_bytes);
    auto fut = call->promise.get_future();
    out_call = call;

    {
        std::lock_guard<std::mutex> lk(mu_);
        if (closing_ || shutdown_) {
            call->error = "rpc client: connection is shut down";
            call->promise.set_value();
            return fut;
        }
    }

    registerCall(call);

    std::lock_guard<std::mutex> lk(sending_);
    geerpc::Header h;
    h.set_service_method(service_method);
    h.set_seq(call->seq);
    h.set_error("");

    if (!cc_->Write(h, call->args_bytes)) {
        auto c = removeCall(call->seq);
        if (c) {
            c->error = "rpc client: write error";
            c->promise.set_value();
        }
    }
    return fut;
}

std::string Client::Call(const std::string& service_method,
                         const std::string& args_bytes,
                         std::string& reply_bytes,
                         std::chrono::milliseconds timeout) {
    CallPtr call;
    auto fut = Go(service_method, args_bytes, call);

    if (timeout.count() == 0) {
        fut.wait();
    } else {
        if (fut.wait_for(timeout) == std::future_status::timeout) {
            removeCall(call->seq);
            return "rpc client: call timed out";
        }
    }
    reply_bytes = call->reply_bytes;
    return call->error;
}

} // namespace geerpc
