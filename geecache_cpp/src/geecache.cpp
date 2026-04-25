#include "geecache.h"

#include <algorithm>
#include <atomic>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "geecachepb.pb.h"

namespace geecache {

// Static members
std::shared_mutex Group::groups_mu_;
std::unordered_map<std::string, std::shared_ptr<Group>> Group::groups_;
namespace {
std::atomic<uint64_t> g_version_clock{1};
}

Group::Group(const std::string& name, int64_t cacheBytes,
             std::shared_ptr<Getter> getter, GroupOptions options)
    : name_(name),
      getter_(std::move(getter)),
      setter_(std::move(options.setter)),
      main_cache_(cacheBytes, options.cache_ttl),
      hot_cache_(options.hot_cache_bytes > 0 ? options.hot_cache_bytes
                                             : std::max<int64_t>(cacheBytes / 8, 0),
                 options.cache_ttl),
      hot_cache_denominator_(options.hot_cache_denominator <= 0
                                 ? 1
                                 : options.hot_cache_denominator) {}

std::shared_ptr<Group> Group::NewGroup(
    const std::string& name,
    int64_t cacheBytes,
    std::shared_ptr<Getter> getter,
    GroupOptions options) {
    if (!getter) {
        throw std::runtime_error("nil Getter");
    }
    std::unique_lock<std::shared_mutex> lock(groups_mu_);
    auto g = std::shared_ptr<Group>(new Group(name, cacheBytes, std::move(getter), options));
    groups_[name] = g;
    return g;
}

std::shared_ptr<Group> Group::NewStandaloneGroup(
    const std::string& name,
    int64_t cacheBytes,
    std::shared_ptr<Getter> getter,
    GroupOptions options) {
    if (!getter) {
        throw std::runtime_error("nil Getter");
    }
    return std::shared_ptr<Group>(new Group(name, cacheBytes, std::move(getter), options));
}

std::shared_ptr<Group> Group::GetGroup(const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(groups_mu_);
    auto it = groups_.find(name);
    if (it == groups_.end()) {
        return nullptr;
    }
    return it->second;
}

void Group::DestroyAllGroups() {
    std::unique_lock<std::shared_mutex> lock(groups_mu_);
    groups_.clear();
}

std::pair<ByteView, std::string> Group::Get(const std::string& key) {
    return GetInternal(key, LoadMode::kAllowPeer);
}

std::pair<ByteView, std::string> Group::GetLocallyOwned(const std::string& key) {
    return GetInternal(key, LoadMode::kLocalOnly);
}

std::string Group::Set(const std::string& key, const std::string& value) {
    if (key.empty()) {
        return "key is required";
    }
    if (!setter_) {
        return "setter is not configured";
    }

    std::string err = setter_->Set(key, value);
    if (!err.empty()) {
        return err;
    }
    return Invalidate(key);
}

std::string Group::Invalidate(const std::string& key) {
    if (key.empty()) {
        return "key is required";
    }

    uint64_t version = NextVersion();
    std::string err = ApplyInvalidation(key, version);
    if (!err.empty()) {
        return err;
    }

    std::shared_ptr<PeerPicker> peers;
    {
        std::lock_guard<std::mutex> lock(peers_mu_);
        peers = peers_;
    }
    auto manager = std::dynamic_pointer_cast<PeerManager>(peers);
    if (!manager) {
        return "";
    }

    geecachepb::Request req;
    req.set_group(name_);
    req.set_key(key);
    req.set_version(version);

    std::vector<std::string> errors;
    for (const auto& peer : manager->GetAllPeers()) {
        std::string peer_err = peer->Invalidate(req);
        if (!peer_err.empty()) {
            errors.push_back(peer_err);
        }
    }
    if (errors.empty()) {
        return "";
    }

    std::ostringstream oss;
    oss << "failed to invalidate " << errors.size() << " peer(s): ";
    for (size_t i = 0; i < errors.size(); ++i) {
        if (i > 0) {
            oss << "; ";
        }
        oss << errors[i];
    }
    return oss.str();
}

std::string Group::ApplyInvalidation(const std::string& key, uint64_t version) {
    if (key.empty()) {
        return "key is required";
    }

    {
        std::unique_lock<std::shared_mutex> lock(versions_mu_);
        uint64_t& current = versions_[key];
        if (version > current) {
            current = version;
        }
    }
    main_cache_.Remove(key);
    hot_cache_.Remove(key);
    return "";
}

std::pair<ByteView, std::string> Group::GetInternal(const std::string& key, LoadMode mode) {
    if (key.empty()) {
        return {ByteView{}, "key is required"};
    }

    auto [view, ok] = LookupCaches(key);
    if (ok) {
        return {view, ""};
    }

    return Load(key, mode);
}

void Group::RegisterPeers(std::shared_ptr<PeerPicker> peers) {
    std::lock_guard<std::mutex> lock(peers_mu_);
    if (peers_) {
        throw std::runtime_error("RegisterPeers called more than once");
    }
    peers_ = std::move(peers);
}

std::pair<ByteView, std::string> Group::Load(const std::string& key, LoadMode mode) {
    auto [result, err] = loader_.Do(
        key, [this, &key, mode]() -> std::pair<std::any, std::string> {
        constexpr int kMaxAttempts = 2;
        for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
            uint64_t version = GetVersion(key);
            std::shared_ptr<PeerPicker> peers;
            {
                std::lock_guard<std::mutex> lock(peers_mu_);
                peers = peers_;
            }
            if (mode == LoadMode::kAllowPeer && peers) {
                auto [peer, ok] = peers->PickPeer(key);
                if (ok) {
                    auto [value, peer_err] = GetFromPeer(peer, key);
                    if (peer_err.empty()) {
                        if (!IsVersionCurrent(key, version)) {
                            continue;
                        }
                        MaybePopulateHotCache(key, value);
                        return {std::any(value), ""};
                    }
                    std::cerr << "[GeeCache] Failed to get from peer: " << peer_err << std::endl;
                }
            }
            auto [value, local_err] = LoadFromSourceWithVersion(key);
            if (!local_err.empty()) {
                return {std::any{}, local_err};
            }
            return {std::any(value), ""};
        }
        return {std::any{}, "stale value discarded during concurrent invalidation"};
    });

    if (!err.empty()) {
        return {ByteView{}, err};
    }

    return {std::any_cast<ByteView>(result), ""};
}

std::pair<ByteView, std::string> Group::GetFromPeer(
    std::shared_ptr<PeerGetter> peer, const std::string& key) {
    geecachepb::Request req;
    req.set_group(name_);
    req.set_key(key);

    geecachepb::Response resp;
    std::string err = peer->Get(req, &resp);
    if (!err.empty()) {
        return {ByteView{}, err};
    }
    return {ByteView(resp.value()), ""};
}

std::pair<ByteView, std::string> Group::LoadFromSourceWithVersion(const std::string& key) {
    constexpr int kMaxAttempts = 2;
    for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
        uint64_t version = GetVersion(key);
        auto [bytes, err] = getter_->Get(key);
        if (!err.empty()) {
            return {ByteView{}, err};
        }
        ByteView value(std::move(bytes));
        if (!IsVersionCurrent(key, version)) {
            continue;
        }
        PopulateMainCache(key, value);
        return {value, ""};
    }
    return {ByteView{}, "stale value discarded during concurrent invalidation"};
}

void Group::PopulateMainCache(const std::string& key, const ByteView& value) {
    main_cache_.Add(key, value);
}

void Group::MaybePopulateHotCache(const std::string& key, const ByteView& value) {
    if (hot_cache_denominator_ <= 1) {
        hot_cache_.Add(key, value);
        return;
    }
    size_t bucket = std::hash<std::string>{}(key) %
                    static_cast<size_t>(hot_cache_denominator_);
    if (bucket == 0) {
        hot_cache_.Add(key, value);
    }
}

std::pair<ByteView, bool> Group::LookupCaches(const std::string& key) {
    auto [view, ok] = main_cache_.Get(key);
    if (ok) {
        return {view, true};
    }
    return hot_cache_.Get(key);
}

uint64_t Group::GetVersion(const std::string& key) const {
    std::shared_lock<std::shared_mutex> lock(versions_mu_);
    auto it = versions_.find(key);
    if (it == versions_.end()) {
        return 0;
    }
    return it->second;
}

uint64_t Group::NextVersion() {
    return g_version_clock.fetch_add(1, std::memory_order_relaxed);
}

bool Group::IsVersionCurrent(const std::string& key, uint64_t version) const {
    return GetVersion(key) == version;
}

}  // namespace geecache
