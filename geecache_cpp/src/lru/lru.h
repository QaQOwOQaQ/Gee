#pragma once

#include <cstdint>
#include <functional>
#include <list>
#include <string>
#include <unordered_map>
#include <utility>

namespace geecache {
namespace lru {

// Cache is a LRU cache. It is NOT safe for concurrent access.
// V must have a Len() method returning int.
template <typename V>
class Cache {
public:
    using EvictedCallback = std::function<void(const std::string&, const V&)>;
    using Entry = std::pair<std::string, V>;
    using ListType = std::list<Entry>;
    using MapType = std::unordered_map<std::string, typename ListType::iterator>;

    explicit Cache(int64_t maxBytes, EvictedCallback onEvicted = nullptr)
        : max_bytes_(maxBytes), nbytes_(0), on_evicted_(std::move(onEvicted)) {}

    // Add adds or updates a value in the cache.
    void Add(const std::string& key, const V& value) {
        auto it = cache_.find(key);
        if (it != cache_.end()) {
            // Update existing entry: move to front
            ll_.splice(ll_.begin(), ll_, it->second);
            nbytes_ += value.Len() - it->second->second.Len();
            it->second->second = value;
        } else {
            // Insert new entry at front
            ll_.push_front({key, value});
            cache_[key] = ll_.begin();
            nbytes_ += static_cast<int64_t>(key.size()) + value.Len();
        }
        // Evict until under capacity
        while (max_bytes_ != 0 && max_bytes_ < nbytes_) {
            RemoveOldest();
        }
    }

    // Get looks up a key's value from the cache.
    std::pair<V, bool> Get(const std::string& key) {
        auto it = cache_.find(key);
        if (it == cache_.end()) {
            return {V{}, false};
        }
        // Move to front (most recently used)
        ll_.splice(ll_.begin(), ll_, it->second);
        return {it->second->second, true};
    }

    // Peek looks up a key without updating its recency.
    std::pair<V, bool> Peek(const std::string& key) const {
        auto it = cache_.find(key);
        if (it == cache_.end()) {
            return {V{}, false};
        }
        return {it->second->second, true};
    }

    // Remove deletes a key if present.
    bool Remove(const std::string& key) {
        auto it = cache_.find(key);
        if (it == cache_.end()) {
            return false;
        }
        nbytes_ -= static_cast<int64_t>(it->second->first.size()) +
                   it->second->second.Len();
        ll_.erase(it->second);
        cache_.erase(it);
        return true;
    }

    // RemoveOldest removes the oldest item.
    void RemoveOldest() {
        if (ll_.empty()) {
            return;
        }
        auto& back = ll_.back();
        cache_.erase(back.first);
        nbytes_ -= static_cast<int64_t>(back.first.size()) + back.second.Len();
        if (on_evicted_) {
            on_evicted_(back.first, back.second);
        }
        ll_.pop_back();
    }

    // Len returns the number of cache entries.
    int Len() const {
        return static_cast<int>(ll_.size());
    }

private:
    int64_t max_bytes_;
    int64_t nbytes_;
    ListType ll_;
    MapType cache_;
    EvictedCallback on_evicted_;
};

}  // namespace lru
}  // namespace geecache
