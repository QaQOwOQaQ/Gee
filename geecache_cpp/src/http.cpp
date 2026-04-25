#include "http.h"

#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include "geecache.h"
#include "geecachepb.pb.h"

namespace geecache {

// --- URL helpers ---

void HTTPPool::ParseURL(const std::string& url, std::string& host, int& port) {
    // Parse "http://host:port" or "http://host"
    std::string stripped = url;
    auto pos = stripped.find("://");
    if (pos != std::string::npos) {
        stripped = stripped.substr(pos + 3);
    }
    // Remove trailing slash
    if (!stripped.empty() && stripped.back() == '/') {
        stripped.pop_back();
    }
    auto colon = stripped.find(':');
    if (colon != std::string::npos) {
        host = stripped.substr(0, colon);
        port = std::stoi(stripped.substr(colon + 1));
    } else {
        host = stripped;
        port = 80;
    }
}

std::string HTTPPool::UrlEncode(const std::string& value) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;
    for (char c : value) {
        if (isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_' ||
            c == '.' || c == '~') {
            escaped << c;
        } else {
            escaped << '%' << std::setw(2)
                    << static_cast<int>(static_cast<unsigned char>(c));
        }
    }
    return escaped.str();
}

// --- HTTPPool ---

HTTPPool::HTTPPool(const std::string& self)
    : self_(self), base_path_(kDefaultBasePath) {}

HTTPPool::~HTTPPool() {
    Stop();
}

void HTTPPool::Set(const std::vector<std::string>& peers) {
    std::lock_guard<std::mutex> lock(mu_);
    peers_ = std::make_unique<consistenthash::Map>(kDefaultReplicas);
    peers_->Add(peers);
    std::unordered_map<std::string, bool> next_health;
    std::unordered_map<std::string, bool> next_seen_live;
    http_getters_.clear();
    for (const auto& peer : peers) {
        auto health_it = peer_health_.find(peer);
        auto seen_live_it = peer_seen_live_.find(peer);
        bool healthy =
            peer == self_ ? true : (health_it == peer_health_.end() ? true : health_it->second);
        bool seen_live = peer == self_ ? true
                                       : (seen_live_it == peer_seen_live_.end()
                                              ? false
                                              : seen_live_it->second);
        next_health[peer] = healthy;
        next_seen_live[peer] = seen_live;
        http_getters_[peer] = std::make_shared<HttpGetter>(this, peer, peer + base_path_);
    }
    peer_health_ = std::move(next_health);
    peer_seen_live_ = std::move(next_seen_live);
}

void HTTPPool::SetHealthCheckInterval(std::chrono::milliseconds interval) {
    if (interval <= std::chrono::milliseconds::zero()) {
        interval = std::chrono::milliseconds(1);
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        health_check_interval_ = interval;
    }
    health_wait_cv_.notify_all();
}

bool HTTPPool::IsPeerHealthy(const std::string& peer) const {
    if (peer == self_) {
        return true;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = peer_health_.find(peer);
    if (it == peer_health_.end()) {
        return false;
    }
    return it->second;
}

std::pair<std::shared_ptr<PeerGetter>, bool> HTTPPool::PickPeer(
    const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!peers_ || peers_->IsEmpty()) {
        return {nullptr, false};
    }
    std::string peer = peers_->Get(key);
    if (!peer.empty() && peer != self_) {
        auto health = peer_health_.find(peer);
        if (health != peer_health_.end() && !health->second) {
            return {nullptr, false};
        }
        auto it = http_getters_.find(peer);
        if (it != http_getters_.end()) {
            std::cerr << "Pick peer " << peer << std::endl;
            return {it->second, true};
        }
    }
    return {nullptr, false};
}

std::vector<std::shared_ptr<PeerGetter>> HTTPPool::GetAllPeers() {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<std::shared_ptr<PeerGetter>> peers;
    peers.reserve(http_getters_.size());
    for (const auto& [peer, getter] : http_getters_) {
        if (peer == self_) {
            continue;
        }
        peers.push_back(getter);
    }
    return peers;
}

void HTTPPool::RegisterGroup(const std::shared_ptr<Group>& group) {
    std::lock_guard<std::mutex> lock(mu_);
    served_groups_[group->Name()] = group;
}

void HTTPPool::HandleRequest(const httplib::Request& req, httplib::Response& res) {
    std::string path = req.path;

    if (path.find(base_path_) != 0) {
        res.status = 400;
        res.set_content("bad request", "text/plain");
        return;
    }

    if (path == std::string(base_path_) + "healthz") {
        res.status = 200;
        res.set_content("ok", "text/plain");
        return;
    }

    std::string remainder = path.substr(base_path_.size());
    auto slash = remainder.find('/');
    if (slash == std::string::npos) {
        res.status = 400;
        res.set_content("bad request: missing key", "text/plain");
        return;
    }

    std::string group_name = remainder.substr(0, slash);
    std::string key = remainder.substr(slash + 1);

    std::shared_ptr<Group> group;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = served_groups_.find(group_name);
        if (it != served_groups_.end()) {
            group = it->second;
        }
    }
    if (!group) {
        group = Group::GetGroup(group_name);
    }
    if (!group) {
        res.status = 404;
        res.set_content("no such group: " + group_name, "text/plain");
        return;
    }

    if (req.method == "DELETE") {
        uint64_t version = 0;
        auto header = req.get_header_value(kVersionHeader);
        if (!header.empty()) {
            version = std::stoull(header);
        }
        std::string err = group->ApplyInvalidation(key, version);
        if (!err.empty()) {
            res.status = 500;
            res.set_content(err, "text/plain");
            return;
        }
        res.status = 200;
        res.set_content("ok", "text/plain");
        return;
    }

    bool local_only = req.has_header(kPeerRequestHeader);
    auto [view, err] = local_only ? group->GetLocallyOwned(key) : group->Get(key);
    if (!err.empty()) {
        res.status = 500;
        res.set_content(err, "text/plain");
        return;
    }

    geecachepb::Response resp;
    resp.set_value(view.ByteSlice());
    std::string body;
    if (!resp.SerializeToString(&body)) {
        res.status = 500;
        res.set_content("failed to serialize response", "text/plain");
        return;
    }

    res.set_content(body, "application/octet-stream");
}

void HTTPPool::Start() {
    start_failed_.store(false);
    stop_health_checks_.store(false);
    std::string pattern = std::string(base_path_) + "(.+)";
    server_.Get(pattern, [this](const httplib::Request& req, httplib::Response& res) {
        HandleRequest(req, res);
    });
    server_.Delete(pattern, [this](const httplib::Request& req, httplib::Response& res) {
        HandleRequest(req, res);
    });

    std::string host;
    int port;
    ParseURL(self_, host, port);

    std::cerr << "[HTTPPool] serving at " << self_ << std::endl;
    server_thread_ = std::thread([this, host, port]() {
        if (!server_.listen(host.c_str(), port)) {
            start_failed_.store(true);
        }
    });

    while (!server_.is_running() && !start_failed_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (start_failed_.load()) {
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
        throw std::runtime_error("failed to start HTTPPool at " + self_);
    }

    if (health_thread_.joinable()) {
        health_thread_.join();
    }
    health_thread_ = std::thread([this]() { HealthCheckLoop(); });
}

void HTTPPool::Stop() {
    stop_health_checks_.store(true);
    health_wait_cv_.notify_all();
    if (health_thread_.joinable()) {
        health_thread_.join();
    }
    if (server_.is_running()) {
        server_.stop();
    }
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
}

void HTTPPool::HealthCheckLoop() {
    while (!stop_health_checks_.load()) {
        std::vector<std::string> peers;
        std::chrono::milliseconds interval;
        {
            std::lock_guard<std::mutex> lock(mu_);
            peers.reserve(http_getters_.size());
            for (const auto& [peer, _] : http_getters_) {
                if (peer == self_) {
                    continue;
                }
                peers.push_back(peer);
            }
            interval = health_check_interval_;
        }

        for (const auto& peer : peers) {
            bool healthy = CheckPeerHealth(peer);
            UpdatePeerHealthFromProbe(peer, healthy);
        }

        std::unique_lock<std::mutex> lock(health_wait_mu_);
        health_wait_cv_.wait_for(lock, interval,
                                 [this]() { return stop_health_checks_.load(); });
    }
}

bool HTTPPool::CheckPeerHealth(const std::string& peer) {
    std::string host;
    int port;
    ParseURL(peer, host, port);

    httplib::Client client(host.c_str(), port);
    client.set_connection_timeout(std::chrono::milliseconds(200));
    client.set_read_timeout(std::chrono::milliseconds(200));

    auto result = client.Get((std::string(base_path_) + "healthz").c_str());
    return result && result->status == 200;
}

void HTTPPool::UpdatePeerHealth(const std::string& peer, bool healthy) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = peer_health_.find(peer);
    if (it != peer_health_.end()) {
        it->second = healthy;
        if (healthy) {
            peer_seen_live_[peer] = true;
        }
    }
}

void HTTPPool::UpdatePeerHealthFromProbe(const std::string& peer, bool healthy) {
    std::lock_guard<std::mutex> lock(mu_);
    auto health_it = peer_health_.find(peer);
    if (health_it == peer_health_.end()) {
        return;
    }
    if (healthy) {
        health_it->second = true;
        peer_seen_live_[peer] = true;
        return;
    }

    auto seen_live_it = peer_seen_live_.find(peer);
    if (seen_live_it != peer_seen_live_.end() && seen_live_it->second) {
        health_it->second = false;
    }
}

// --- HttpGetter ---

HTTPPool::HttpGetter::HttpGetter(HTTPPool* owner, std::string peer,
                                 const std::string& base_url)
    : owner_(owner), peer_(std::move(peer)) {
    // Parse "http://host:port/path/"
    std::string stripped = base_url;
    auto pos = stripped.find("://");
    if (pos != std::string::npos) {
        stripped = stripped.substr(pos + 3);
    }
    auto slash = stripped.find('/');
    std::string host_port;
    if (slash != std::string::npos) {
        host_port = stripped.substr(0, slash);
        path_prefix_ = stripped.substr(slash);
    } else {
        host_port = stripped;
        path_prefix_ = "/";
    }
    auto colon = host_port.find(':');
    if (colon != std::string::npos) {
        host_ = host_port.substr(0, colon);
        port_ = std::stoi(host_port.substr(colon + 1));
    } else {
        host_ = host_port;
        port_ = 80;
    }
}

std::string HTTPPool::HttpGetter::Get(const geecachepb::Request& req,
                                       geecachepb::Response* resp) {
    std::string url = path_prefix_ + UrlEncode(req.group()) + "/" + UrlEncode(req.key());

    httplib::Client client(host_.c_str(), port_);
    client.set_connection_timeout(std::chrono::seconds(5));
    client.set_read_timeout(std::chrono::seconds(5));

    httplib::Headers headers = {
        {kPeerRequestHeader, "1"},
    };
    auto result = client.Get(url.c_str(), headers);
    if (!result) {
        owner_->UpdatePeerHealth(peer_, false);
        return "http request failed: " + httplib::to_string(result.error());
    }
    owner_->UpdatePeerHealth(peer_, true);
    if (result->status != 200) {
        return "server returned: " + std::to_string(result->status);
    }

    if (!resp->ParseFromString(result->body)) {
        return "failed to parse response body";
    }
    return "";
}

std::string HTTPPool::HttpGetter::Invalidate(const geecachepb::Request& req) {
    std::string url = path_prefix_ + UrlEncode(req.group()) + "/" + UrlEncode(req.key());

    httplib::Client client(host_.c_str(), port_);
    client.set_connection_timeout(std::chrono::seconds(5));
    client.set_read_timeout(std::chrono::seconds(5));

    httplib::Headers headers = {
        {kVersionHeader, std::to_string(req.version())},
    };
    auto result = client.Delete(url.c_str(), headers);
    if (!result) {
        owner_->UpdatePeerHealth(peer_, false);
        return "http invalidation failed: " + httplib::to_string(result.error());
    }
    owner_->UpdatePeerHealth(peer_, true);
    if (result->status != 200) {
        return "invalidation returned: " + std::to_string(result->status);
    }
    return "";
}

}  // namespace geecache
