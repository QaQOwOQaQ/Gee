#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>

#include "byteview.h"
#include "lru/lru.h"

namespace geecache {

// ConcurrentCache is a thread-safe wrapper around lru::Cache<ByteView>.
class ConcurrentCache {
public:
    explicit ConcurrentCache(
        int64_t cacheBytes = 0,
        std::chrono::milliseconds ttl = std::chrono::milliseconds::zero())
        : cache_bytes_(cacheBytes), ttl_(ttl) {}

    void Add(const std::string& key, const ByteView& value) {
        std::unique_lock<std::shared_mutex> lock(mu_);
        if (!lru_) {
            lru_ = std::make_unique<lru::Cache<CacheEntry>>(cache_bytes_);
        }
        lru_->Add(key, MakeEntry(value));
    }

    std::pair<ByteView, bool> Get(const std::string& key) {
        auto now = std::chrono::steady_clock::now();
        {
            std::shared_lock<std::shared_mutex> lock(mu_);
            if (!lru_) {
                return {ByteView{}, false};
            }
            auto [entry, ok] = lru_->Peek(key);
            if (!ok) {
                return {ByteView{}, false};
            }
            if (IsExpired(entry, now)) {
                // Upgrade to a write lock before removing the expired entry.
                // The entry is rechecked after the lock handoff.
            }
        }

        std::unique_lock<std::shared_mutex> lock(mu_);
        if (!lru_) {
            return {ByteView{}, false};
        }
        auto [entry, ok] = lru_->Peek(key);
        if (!ok) {
            return {ByteView{}, false};
        }
        if (IsExpired(entry, std::chrono::steady_clock::now())) {
            lru_->Remove(key);
            return {ByteView{}, false};
        }
        auto [fresh_entry, hit] = lru_->Get(key);
        if (!hit) {
            return {ByteView{}, false};
        }
        return {fresh_entry.value, true};
    }

    void Remove(const std::string& key) {
        std::unique_lock<std::shared_mutex> lock(mu_);
        if (!lru_) {
            return;
        }
        lru_->Remove(key);
    }

private:
    struct CacheEntry {
        ByteView value;
        std::chrono::steady_clock::time_point expires_at{};
        bool has_expiration = false;

        int Len() const { return value.Len(); }
    };

    CacheEntry MakeEntry(const ByteView& value) const {
        CacheEntry entry;
        entry.value = value;
        entry.has_expiration = ttl_ > std::chrono::milliseconds::zero();
        if (entry.has_expiration) {
            entry.expires_at = std::chrono::steady_clock::now() + ttl_;
        }
        return entry;
    }

    static bool IsExpired(const CacheEntry& entry,
                          std::chrono::steady_clock::time_point now) {
        return entry.has_expiration && now >= entry.expires_at;
    }

    mutable std::shared_mutex mu_;
    std::unique_ptr<lru::Cache<CacheEntry>> lru_;
    int64_t cache_bytes_;
    std::chrono::milliseconds ttl_;
};

}  // namespace geecache
