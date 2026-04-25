#pragma once

#include "geerpc/client/client.h"

#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <chrono>
#include <random>

namespace geerpc {
namespace xclient {

enum class SelectMode {
    Random,      // select randomly
    RoundRobin,  // select using round-robin
};

// Discovery abstracts server list retrieval.
// Mirrors Go's xclient.Discovery interface.
class Discovery {
public:
    virtual ~Discovery() = default;

    // Refresh server list from remote registry.
    virtual bool Refresh() = 0;

    // Update server list manually.
    virtual void Update(std::vector<std::string> servers) = 0;

    // Get one server address according to SelectMode.
    virtual std::string Get(SelectMode mode) = 0;

    // GetAll returns a copy of all known servers.
    virtual std::vector<std::string> GetAll() = 0;
};

using DiscoveryPtr = std::shared_ptr<Discovery>;

// MultiServersDiscovery: static list, no registry.
// Mirrors Go's xclient.MultiServersDiscovery.
class MultiServersDiscovery : public Discovery {
public:
    explicit MultiServersDiscovery(std::vector<std::string> servers);

    bool Refresh() override { return true; }
    void Update(std::vector<std::string> servers) override;
    std::string Get(SelectMode mode) override;
    std::vector<std::string> GetAll() override;

protected:
    mutable std::mutex mu_;
    std::vector<std::string> servers_;
    int index_{0};  // round-robin cursor
    mutable std::mt19937 rng_;  // Bug7 fix: reuse RNG across Get() calls
};

// GeeRegistryDiscovery: pulls server list from GeeRegistry via HTTP.
// Mirrors Go's xclient.GeeRegistryDiscovery.
class GeeRegistryDiscovery : public MultiServersDiscovery {
public:
    GeeRegistryDiscovery(std::string registry_url,
                         std::chrono::seconds timeout = std::chrono::seconds(10));

    bool Refresh() override;
    void Update(std::vector<std::string> servers) override;
    std::string Get(SelectMode mode) override;
    std::vector<std::string> GetAll() override;

private:
    std::string registry_url_;
    std::chrono::seconds timeout_;
    std::chrono::steady_clock::time_point last_update_;
};

// XClient: load-balanced client over multiple servers.
// Mirrors Go's xclient.XClient.
class XClient {
public:
    XClient(DiscoveryPtr d, SelectMode mode, ClientOption opt = {});
    ~XClient();

    // Call selects one server and invokes serviceMethod.
    std::string Call(const std::string& service_method,
                     const std::string& args_bytes,
                     std::string& reply_bytes,
                     std::chrono::milliseconds timeout = std::chrono::milliseconds(0));

    // Broadcast invokes serviceMethod on all servers concurrently.
    // Returns the first error encountered, or empty string on success.
    std::string Broadcast(const std::string& service_method,
                          const std::string& args_bytes,
                          std::string& reply_bytes,
                          std::chrono::milliseconds timeout = std::chrono::milliseconds(0));

    void Close();

private:
    ClientPtr dial(const std::string& rpc_addr);

    DiscoveryPtr d_;
    SelectMode   mode_;
    ClientOption opt_;

    std::mutex mu_;
    std::unordered_map<std::string, ClientPtr> clients_;
};

} // namespace xclient
} // namespace geerpc
