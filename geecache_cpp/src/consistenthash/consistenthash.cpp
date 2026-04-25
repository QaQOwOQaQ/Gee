#include "consistenthash.h"

#include <zlib.h>

#include <algorithm>
#include <string>

namespace geecache {
namespace consistenthash {

static uint32_t defaultHash(const std::string& data) {
    return crc32(0, reinterpret_cast<const Bytef*>(data.data()),
                 static_cast<uInt>(data.size()));
}

Map::Map(int replicas, HashFunc fn)
    : replicas_(replicas), hash_(fn ? std::move(fn) : defaultHash) {}

void Map::Add(const std::vector<std::string>& keys) {
    for (const auto& key : keys) {
        for (int i = 0; i < replicas_; ++i) {
            std::string vnode = std::to_string(i) + key;
            uint32_t h = hash_(vnode);
            ring_[h] = key;
        }
    }
}

std::string Map::Get(const std::string& key) const {
    if (ring_.empty()) {
        return "";
    }
    uint32_t h = hash_(key);
    auto it = ring_.lower_bound(h);
    if (it == ring_.end()) {
        it = ring_.begin();
    }
    return it->second;
}

bool Map::IsEmpty() const {
    return ring_.empty();
}

}  // namespace consistenthash
}  // namespace geecache
