#include "geerpc/registry/registry.h"

#include <algorithm>
#include <iostream>
#include <sstream>
#include <thread>

// For Heartbeat HTTP POST (raw socket, no external dependency)
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

namespace geerpc {
namespace registry {

constexpr const char* kRegistryPath = "/_geerpc_/registry";

GeeRegistry::GeeRegistry(std::chrono::seconds timeout) : timeout_(timeout) {}
GeeRegistry::~GeeRegistry() { Stop(); }

void GeeRegistry::putServer(const std::string& addr) {
    std::lock_guard<std::mutex> lk(mu_);
    auto& item = servers_[addr];
    item.addr      = addr;
    item.last_beat = std::chrono::steady_clock::now();
}

std::vector<std::string> GeeRegistry::aliveServers() {
    std::lock_guard<std::mutex> lk(mu_);
    auto now = std::chrono::steady_clock::now();
    std::vector<std::string> alive;
    for (auto it = servers_.begin(); it != servers_.end(); ) {
        bool expired = (timeout_.count() > 0) &&
                       (now - it->second.last_beat > timeout_);
        if (expired) {
            it = servers_.erase(it);
        } else {
            alive.push_back(it->second.addr);
            ++it;
        }
    }
    std::sort(alive.begin(), alive.end());
    return alive;
}

void GeeRegistry::Start(int port) {
    svr_.Get(kRegistryPath, [this](const httplib::Request&, httplib::Response& res) {
        auto servers = aliveServers();
        std::string val;
        for (size_t i = 0; i < servers.size(); ++i) {
            if (i) val += ',';
            val += servers[i];
        }
        res.set_header("X-Geerpc-Servers", val);
        res.status = 200;
    });

    svr_.Post(kRegistryPath, [this](const httplib::Request& req, httplib::Response& res) {
        std::string addr = req.get_header_value("X-Geerpc-Server");
        if (addr.empty()) {
            res.status = 500;
            return;
        }
        putServer(addr);
        res.status = 200;
    });

    std::cout << "rpc registry: listening on port " << port
              << " at " << kRegistryPath << "\n";
    svr_.listen("0.0.0.0", port);
}

void GeeRegistry::Stop() {
    svr_.stop();
}

// ── Heartbeat ─────────────────────────────────────────────────────────────────
// Send a raw HTTP POST to registry_url with "X-Geerpc-Server: addr" header.

static bool rawHttpPost(const std::string& url, const std::string& header_name,
                        const std::string& header_value) {
    auto scheme_end = url.find("://");
    std::string rest = (scheme_end == std::string::npos)
                           ? url : url.substr(scheme_end + 3);
    auto slash     = rest.find('/');
    std::string host_port = (slash == std::string::npos) ? rest : rest.substr(0, slash);
    std::string path      = (slash == std::string::npos) ? "/" : rest.substr(slash);

    auto colon = host_port.rfind(':');
    std::string host = (colon == std::string::npos) ? host_port : host_port.substr(0, colon);
    std::string port = (colon == std::string::npos) ? "80" : host_port.substr(colon + 1);

    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    if (::getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0) return false;

    int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) { ::freeaddrinfo(res); return false; }
    if (::connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
        ::close(fd); ::freeaddrinfo(res); return false;
    }
    ::freeaddrinfo(res);

    std::string req =
        "POST " + path + " HTTP/1.0\r\n"
        "Host: " + host_port + "\r\n" +
        header_name + ": " + header_value + "\r\n"
        "Content-Length: 0\r\n\r\n";

    const char* p = req.data(); size_t n = req.size();
    while (n > 0) { ssize_t w = ::write(fd, p, n); if (w <= 0) { ::close(fd); return false; } p += w; n -= w; }

    // Bug4+5 fix: read HTTP response status line to detect server-side errors.
    std::string status_line;
    char c;
    while (true) {
        ssize_t r = ::read(fd, &c, 1);
        if (r <= 0) break;
        if (c == '\n') break;
        if (c != '\r') status_line += c;
    }
    ::close(fd);
    return status_line.find("200") != std::string::npos;
}

bool GeeRegistry::Heartbeat(const std::string& registry_url,
                             const std::string& addr) {
    std::cout << addr << " send heartbeat to registry " << registry_url << "\n";
    return rawHttpPost(registry_url, "X-Geerpc-Server", addr);
}

void GeeRegistry::HeartbeatAuto(const std::string& registry_url,
                                const std::string& addr,
                                std::chrono::seconds interval) {
    // Default interval: timeout - 1 minute (matching Go version)
    if (interval.count() == 0)
        interval = std::chrono::seconds(5 * 60 - 60);  // 4 minutes

    // Send first heartbeat immediately
    Heartbeat(registry_url, addr);

    // Spawn background thread for periodic heartbeat
    std::thread([registry_url, addr, interval]() {
        while (true) {
            std::this_thread::sleep_for(interval);
            if (!Heartbeat(registry_url, addr)) {
                std::cerr << addr << " heartbeat failed, retrying...\n";
            }
        }
    }).detach();
}

} // namespace registry
} // namespace geerpc
