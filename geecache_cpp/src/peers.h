#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "geecachepb.pb.h"

namespace geecache {

// PeerGetter is the interface that must be implemented by a peer.
class PeerGetter {
public:
    virtual ~PeerGetter() = default;
    // Get fetches a value from a remote peer.
    // Returns empty string on success, error message on failure.
    virtual std::string Get(const geecachepb::Request& req,
                            geecachepb::Response* resp) = 0;

    // Invalidate removes a key from a remote peer.
    virtual std::string Invalidate(const geecachepb::Request& req) = 0;
};

// PeerPicker is the interface that must be implemented to locate
// the peer that owns a specific key.
class PeerPicker {
public:
    virtual ~PeerPicker() = default;
    virtual std::pair<std::shared_ptr<PeerGetter>, bool> PickPeer(
        const std::string& key) = 0;
};

// PeerManager exposes all remote peers so invalidations can be broadcast.
class PeerManager {
public:
    virtual ~PeerManager() = default;
    virtual std::vector<std::shared_ptr<PeerGetter>> GetAllPeers() = 0;
};

}  // namespace geecache
