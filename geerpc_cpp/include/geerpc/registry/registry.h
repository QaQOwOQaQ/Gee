#pragma once

#include <httplib.h>
#include <string>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <vector>
#include <thread>

namespace geerpc {
namespace registry {

// ServerItem tracks a single registered server.
struct ServerItem {
    std::string addr;
    std::chrono::steady_clock::time_point last_beat;
};

// GeeRegistry is an HTTP-based registry center.
// Mirrors Go's registry.GeeRegistry.
//
// HTTP API:
//   GET  /_geerpc_/registry  -> X-Geerpc-Servers: addr1,addr2,...
//   POST /_geerpc_/registry  -> X-Geerpc-Server: addr   (heartbeat / register)
class GeeRegistry {
public:
    explicit GeeRegistry(
        std::chrono::seconds timeout = std::chrono::seconds(5 * 60));
    ~GeeRegistry();

    // Start the HTTP server on the given port.  Blocks until Stop() is called.
    void Start(int port);

    // Stop the HTTP server.
    void Stop();

    // Heartbeat: send a POST to registry_url advertising addr.
    // Call this from each RPC server periodically.
    static bool Heartbeat(const std::string& registry_url,
                          const std::string& addr);

    // HeartbeatAuto: send the first heartbeat immediately, then spawn a
    // background thread that repeats every `interval` seconds.
    // If interval == 0, defaults to (timeout - 1 minute).
    // Returns immediately; the background thread runs until process exit.
    static void HeartbeatAuto(const std::string& registry_url,
                              const std::string& addr,
                              std::chrono::seconds interval = std::chrono::seconds(0));

private:
    void putServer(const std::string& addr);
    std::vector<std::string> aliveServers();

    std::chrono::seconds timeout_;
    std::mutex mu_;
    std::unordered_map<std::string, ServerItem> servers_;
    httplib::Server svr_;  // HTTP server instance, controlled by Start/Stop
};

} // namespace registry
} // namespace geerpc
