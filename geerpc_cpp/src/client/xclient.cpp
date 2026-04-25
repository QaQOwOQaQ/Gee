#include "geerpc/client/xclient.h"

#include <random>
#include <thread>
#include <future>
#include <iostream>
#include <sstream>
#include <stdexcept>

// For HTTP GET to registry
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>

namespace geerpc {
namespace xclient {

// ── MultiServersDiscovery ─────────────────────────────────────────────────────

MultiServersDiscovery::MultiServersDiscovery(std::vector<std::string> servers)
    : servers_(std::move(servers)), rng_(std::random_device{}()) {
    // Initialise round-robin cursor to a random position.
    if (!servers_.empty())
        index_ = static_cast<int>(rng_() % servers_.size());
}

void MultiServersDiscovery::Update(std::vector<std::string> servers) {
    std::lock_guard<std::mutex> lk(mu_);
    servers_ = std::move(servers);
}

std::string MultiServersDiscovery::Get(SelectMode mode) {
    std::lock_guard<std::mutex> lk(mu_);
    int n = static_cast<int>(servers_.size());
    if (n == 0) throw std::runtime_error("rpc discovery: no available servers");

    switch (mode) {
    case SelectMode::Random: {
        return servers_[rng_() % static_cast<size_t>(n)];
    }
    case SelectMode::RoundRobin: {
        std::string s = servers_[index_ % n];
        index_ = (index_ + 1) % n;
        return s;
    }
    default:
        throw std::runtime_error("rpc discovery: unsupported select mode");
    }
}

std::vector<std::string> MultiServersDiscovery::GetAll() {
    std::lock_guard<std::mutex> lk(mu_);
    return servers_;
}

// ── GeeRegistryDiscovery ──────────────────────────────────────────────────────

GeeRegistryDiscovery::GeeRegistryDiscovery(std::string registry_url,
                                           std::chrono::seconds timeout)
    : MultiServersDiscovery({}),
      registry_url_(std::move(registry_url)),
      timeout_(timeout),
      last_update_(std::chrono::steady_clock::time_point::min()) {}

// Simple HTTP GET — returns the value of X-Geerpc-Servers header.
static std::string httpGetHeader(const std::string& url,
                                 const std::string& header_name) {
    // Minimal raw HTTP/1.0 GET implementation (no third-party lib needed).
    auto scheme_end = url.find("://");
    std::string rest = (scheme_end == std::string::npos)
                           ? url : url.substr(scheme_end + 3);
    auto slash = rest.find('/');
    std::string host_port = (slash == std::string::npos) ? rest : rest.substr(0, slash);
    std::string path      = (slash == std::string::npos) ? "/" : rest.substr(slash);

    auto colon = host_port.rfind(':');
    std::string host = (colon == std::string::npos) ? host_port : host_port.substr(0, colon);
    std::string port = (colon == std::string::npos) ? "80" : host_port.substr(colon + 1);

    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    if (::getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0) return "";

    int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) { ::freeaddrinfo(res); return ""; }
    if (::connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
        ::close(fd); ::freeaddrinfo(res); return "";
    }
    ::freeaddrinfo(res);

    std::string req = "GET " + path + " HTTP/1.0\r\nHost: " + host_port + "\r\n\r\n";
    const char* p = req.data(); size_t n = req.size();
    while (n > 0) { ssize_t w = ::write(fd, p, n); if (w <= 0) break; p += w; n -= w; }

    // Read response headers.
    std::string value;
    std::string line;
    char c;
    while (true) {
        line.clear();
        while (true) {
            ssize_t r = ::read(fd, &c, 1);
            if (r <= 0) goto done;
            if (c == '\n') break;
            if (c != '\r') line += c;
        }
        if (line.empty()) break;  // end of headers
        // Check for our header (case-insensitive prefix match).
        if (line.size() > header_name.size() &&
            line[header_name.size()] == ':') {
            std::string h = line.substr(0, header_name.size());
            // simple tolower comparison
            bool match = true;
            for (size_t i = 0; i < header_name.size(); ++i)
                if (std::tolower((unsigned char)h[i]) !=
                    std::tolower((unsigned char)header_name[i])) { match = false; break; }
            if (match)
                value = line.substr(header_name.size() + 1 +
                                    (line[header_name.size()+1]==' ' ? 1 : 0));
        }
    }
done:
    ::close(fd);
    return value;
}

bool GeeRegistryDiscovery::Refresh() {
    // NOTE: must NOT hold mu_ when calling this — we will lock inside after
    // fetching from network, so we first check/update under a separate scope.
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto now = std::chrono::steady_clock::now();
        if (last_update_ != std::chrono::steady_clock::time_point::min() &&
            now - last_update_ < timeout_) {
            return true;  // cache still valid, no network call needed
        }
    }

    // Network call outside the lock to avoid holding mu_ during blocking I/O.
    std::string raw = httpGetHeader(registry_url_, "X-Geerpc-Servers");
    if (raw.empty()) {
        std::cerr << "rpc registry: refresh failed from " << registry_url_ << "\n";
        return false;
    }

    std::vector<std::string> new_servers;
    std::istringstream ss(raw);
    std::string token;
    while (std::getline(ss, token, ',')) {
        auto b = token.find_first_not_of(" \t\r\n");
        auto e = token.find_last_not_of(" \t\r\n");
        if (b != std::string::npos)
            new_servers.push_back(token.substr(b, e - b + 1));
    }

    {
        std::lock_guard<std::mutex> lk(mu_);
        servers_ = std::move(new_servers);
        last_update_ = std::chrono::steady_clock::now();
    }
    return true;
}

void GeeRegistryDiscovery::Update(std::vector<std::string> servers) {
    std::lock_guard<std::mutex> lk(mu_);
    servers_ = std::move(servers);
    last_update_ = std::chrono::steady_clock::now();
}

std::string GeeRegistryDiscovery::Get(SelectMode mode) {
    Refresh();
    return MultiServersDiscovery::Get(mode);
}

std::vector<std::string> GeeRegistryDiscovery::GetAll() {
    Refresh();
    return MultiServersDiscovery::GetAll();
}

// ── XClient ───────────────────────────────────────────────────────────────────

XClient::XClient(DiscoveryPtr d, SelectMode mode, ClientOption opt)
    : d_(std::move(d)), mode_(mode), opt_(std::move(opt)) {}

XClient::~XClient() { Close(); }

void XClient::Close() {
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& [addr, c] : clients_) c->Close();
    clients_.clear();
}

ClientPtr XClient::dial(const std::string& rpc_addr) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = clients_.find(rpc_addr);
    if (it != clients_.end()) {
        if (it->second->IsAvailable()) return it->second;
        it->second->Close();
        clients_.erase(it);
    }
    auto c = Client::XDial(rpc_addr, opt_);
    if (!c) return nullptr;
    clients_[rpc_addr] = c;
    return c;
}

std::string XClient::Call(const std::string& service_method,
                           const std::string& args_bytes,
                           std::string& reply_bytes,
                           std::chrono::milliseconds timeout) {
    std::string addr;
    try { addr = d_->Get(mode_); }
    catch (const std::exception& e) { return e.what(); }

    auto c = dial(addr);
    if (!c) return "rpc xclient: dial failed for " + addr;
    return c->Call(service_method, args_bytes, reply_bytes, timeout);
}

std::string XClient::Broadcast(const std::string& service_method,
                                const std::string& args_bytes,
                                std::string& reply_bytes,
                                std::chrono::milliseconds timeout) {
    auto servers = d_->GetAll();
    if (servers.empty()) return "rpc xclient: no available servers";

    std::mutex mu;
    std::string first_err;
    std::string first_reply;
    bool reply_done = false;
    std::atomic<bool> cancelled{false};

    std::vector<std::future<void>> futs;
    futs.reserve(servers.size());

    for (auto& addr : servers) {
        futs.push_back(std::async(std::launch::async, [&, addr]() {
            if (cancelled.load()) return;
            std::string rep;
            auto c = dial(addr);
            std::string err;
            if (!c) {
                err = "rpc xclient: dial failed for " + addr;
            } else {
                err = c->Call(service_method, args_bytes, rep, timeout);
            }
            std::lock_guard<std::mutex> lk(mu);
            if (!err.empty() && first_err.empty()) {
                first_err = err;
                cancelled.store(true);
            }
            if (err.empty() && !reply_done) {
                first_reply = rep;
                reply_done  = true;
            }
        }));
    }

    for (auto& f : futs) f.wait();

    reply_bytes = first_reply;
    return first_err;
}

} // namespace xclient
} // namespace geerpc
