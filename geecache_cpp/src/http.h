#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "httplib.h"
#include "consistenthash/consistenthash.h"
#include "peers.h"

namespace geecache {

class Group;

inline constexpr const char* kDefaultBasePath = "/_geecache/";
inline constexpr int kDefaultReplicas = 50;
inline constexpr const char* kPeerRequestHeader = "X-Geecache-Peer";
inline constexpr const char* kVersionHeader = "X-Geecache-Version";

class HTTPPool : public PeerPicker, public PeerManager {
public:
    explicit HTTPPool(const std::string& self);
    ~HTTPPool();

    // Set updates the pool's list of peers.
    void Set(const std::vector<std::string>& peers);

    // SetHealthCheckInterval updates how often remote peers are probed.
    void SetHealthCheckInterval(std::chrono::milliseconds interval);

    // IsPeerHealthy reports the latest known health status for a peer.
    bool IsPeerHealthy(const std::string& peer) const;

    // PickPeer picks a peer for the given key.
    std::pair<std::shared_ptr<PeerGetter>, bool> PickPeer(
        const std::string& key) override;

    std::vector<std::shared_ptr<PeerGetter>> GetAllPeers() override;

    // RegisterGroup binds a local group instance to this HTTP server.
    void RegisterGroup(const std::shared_ptr<Group>& group);

    // Start starts the HTTP server in a background thread.
    void Start();

    // Stop gracefully shuts down the HTTP server.
    void Stop();

private:
    // HttpGetter implements PeerGetter via HTTP.
    class HttpGetter : public PeerGetter {
    public:
        HttpGetter(HTTPPool* owner, std::string peer, const std::string& base_url);
        std::string Get(const geecachepb::Request& req,
                        geecachepb::Response* resp) override;
        std::string Invalidate(const geecachepb::Request& req) override;

    private:
        HTTPPool* owner_;
        std::string peer_;
        std::string host_;
        int port_;
        std::string path_prefix_;  // e.g., "/_geecache/"
    };

    void HandleRequest(const httplib::Request& req, httplib::Response& res);

    // Parse "http://host:port" into host and port.
    static void ParseURL(const std::string& url, std::string& host, int& port);

    // URL-encode a string.
    static std::string UrlEncode(const std::string& value);

    void HealthCheckLoop();
    bool CheckPeerHealth(const std::string& peer);
    void UpdatePeerHealth(const std::string& peer, bool healthy);
    void UpdatePeerHealthFromProbe(const std::string& peer, bool healthy);

    std::string self_;
    std::string base_path_;
    mutable std::mutex mu_;
    std::unique_ptr<consistenthash::Map> peers_;
    std::unordered_map<std::string, std::shared_ptr<HttpGetter>> http_getters_;
    std::unordered_map<std::string, bool> peer_health_;
    std::unordered_map<std::string, bool> peer_seen_live_;
    std::unordered_map<std::string, std::shared_ptr<Group>> served_groups_;
    std::chrono::milliseconds health_check_interval_{std::chrono::seconds(1)};
    std::mutex health_wait_mu_;
    std::condition_variable health_wait_cv_;
    std::thread health_thread_;
    std::atomic<bool> stop_health_checks_{false};
    httplib::Server server_;
    std::thread server_thread_;
    std::atomic<bool> start_failed_{false};
};

}  // namespace geecache
