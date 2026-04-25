#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <vector>

namespace geecache {
namespace consistenthash {

using HashFunc = std::function<uint32_t(const std::string&)>;

class Map {
public:
    explicit Map(int replicas, HashFunc fn = nullptr);

    // Add adds node names to the hash ring.
    void Add(const std::vector<std::string>& keys);

    // Get gets the closest node in the hash ring for the given key.
    std::string Get(const std::string& key) const;

    // IsEmpty returns true if there are no nodes in the ring.
    bool IsEmpty() const;

private:
    HashFunc hash_;
    int replicas_;
    std::map<uint32_t, std::string> ring_;  // sorted hash -> node name
};

}  // namespace consistenthash
}  // namespace geecache
