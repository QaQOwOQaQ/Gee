#pragma once

#include <any>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "byteview.h"
#include "cache.h"
#include "peers.h"
#include "singleflight/singleflight.h"

namespace geecache {

struct GroupOptions {
    int64_t hot_cache_bytes = 0;
    int hot_cache_denominator = 8;
    std::chrono::milliseconds cache_ttl = std::chrono::milliseconds::zero();
    std::shared_ptr<class Setter> setter;
};

// Getter loads data for a key (the slow backend / data source).
class Getter {
public:
    virtual ~Getter() = default;
    // Returns {bytes, error}. Empty error means success.
    virtual std::pair<std::string, std::string> Get(const std::string& key) = 0;
};

// GetterFunc implements Getter with a function.
class GetterFunc : public Getter {
public:
    using Func = std::function<std::pair<std::string, std::string>(const std::string&)>;
    explicit GetterFunc(Func fn) : fn_(std::move(fn)) {}
    std::pair<std::string, std::string> Get(const std::string& key) override {
        return fn_(key);
    }

private:
    Func fn_;
};

// Setter persists data for a key on the write path.
class Setter {
public:
    virtual ~Setter() = default;
    // Returns empty string on success, error message on failure.
    virtual std::string Set(const std::string& key, const std::string& value) = 0;
};

// SetterFunc implements Setter with a function.
class SetterFunc : public Setter {
public:
    using Func = std::function<std::string(const std::string&, const std::string&)>;
    explicit SetterFunc(Func fn) : fn_(std::move(fn)) {}
    std::string Set(const std::string& key, const std::string& value) override {
        return fn_(key, value);
    }

private:
    Func fn_;
};

// Group is a cache namespace and associated data loading logic.
class Group {
public:
    // NewGroup creates a new Group and registers it globally.
    static std::shared_ptr<Group> NewGroup(
        const std::string& name,
        int64_t cacheBytes,
        std::shared_ptr<Getter> getter,
        GroupOptions options = {});

    // NewStandaloneGroup creates a group without adding it to the global registry.
    static std::shared_ptr<Group> NewStandaloneGroup(
        const std::string& name,
        int64_t cacheBytes,
        std::shared_ptr<Getter> getter,
        GroupOptions options = {});

    // GetGroup returns a previously created group by name, or nullptr.
    static std::shared_ptr<Group> GetGroup(const std::string& name);

    // DestroyAllGroups clears the global group registry (for testing).
    static void DestroyAllGroups();

    // Get retrieves a value for a key.
    std::pair<ByteView, std::string> Get(const std::string& key);

    // GetLocallyOwned serves a peer request without forwarding again.
    std::pair<ByteView, std::string> GetLocallyOwned(const std::string& key);

    // Set writes through to the backing store and invalidates caches.
    std::string Set(const std::string& key, const std::string& value);

    // Invalidate removes a key from caches and broadcasts the invalidation.
    std::string Invalidate(const std::string& key);

    // ApplyInvalidation removes a key locally and advances its version.
    std::string ApplyInvalidation(const std::string& key, uint64_t version);

    // RegisterPeers registers a PeerPicker for this group.
    void RegisterPeers(std::shared_ptr<PeerPicker> peers);

    const std::string& Name() const { return name_; }

private:
    Group(const std::string& name, int64_t cacheBytes,
          std::shared_ptr<Getter> getter, GroupOptions options);

    enum class LoadMode {
        kAllowPeer,
        kLocalOnly,
    };

    std::pair<ByteView, std::string> GetInternal(const std::string& key, LoadMode mode);
    std::pair<ByteView, std::string> Load(const std::string& key, LoadMode mode);
    std::pair<ByteView, std::string> LoadFromSourceWithVersion(const std::string& key);
    std::pair<ByteView, std::string> GetFromPeer(
        std::shared_ptr<PeerGetter> peer, const std::string& key);
    void PopulateMainCache(const std::string& key, const ByteView& value);
    void MaybePopulateHotCache(const std::string& key, const ByteView& value);
    std::pair<ByteView, bool> LookupCaches(const std::string& key);
    uint64_t GetVersion(const std::string& key) const;
    uint64_t NextVersion();
    bool IsVersionCurrent(const std::string& key, uint64_t version) const;

    std::string name_;
    std::shared_ptr<Getter> getter_;
    std::shared_ptr<Setter> setter_;
    ConcurrentCache main_cache_;
    ConcurrentCache hot_cache_;
    int hot_cache_denominator_;
    std::mutex peers_mu_;
    std::shared_ptr<PeerPicker> peers_;
    singleflight::Group loader_;
    mutable std::shared_mutex versions_mu_;
    std::unordered_map<std::string, uint64_t> versions_;

    // Global registry
    static std::shared_mutex groups_mu_;
    static std::unordered_map<std::string, std::shared_ptr<Group>> groups_;
};

}  // namespace geecache
