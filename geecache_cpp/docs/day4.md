# Day 4: 一致性哈希

## 1. 背景知识

### 1.1 分布式缓存面临的问题

假设我们有 3 个缓存节点，现在需要决定 key `"Tom"` 应该存到哪个节点上。最简单的方法是取模：

```
node = hash("Tom") % 3
```

这看起来很完美，但当节点数量发生变化时，灾难就来了：

```
原来 3 个节点:  hash("Tom") % 3 = 1  → 节点 1
增加到 4 个节点: hash("Tom") % 4 = 2  → 节点 2  ← 变了！
```

几乎所有 key 的映射都会改变，导致**大量缓存失效**——这就是所谓的"缓存雪崩"。

### 1.2 一致性哈希如何解决这个问题

一致性哈希的核心思想是：**将节点和 key 都映射到同一个哈希环上**。

想象一个从 0 到 2^32-1 的环形空间（首尾相接）：

```
                    0
                    |
               -----+-----
              /     |     \
             /      |      \
            |    node A     |
            |       |       |
   2^32 ----+       |       +---- 2^32/4
            |       |       |
            |    node C     |
             \      |      /
              \     |     /
               -----+-----
                    |
                 2^32/2
                 node B
```

- **添加节点**：计算节点名的哈希值，放到环上
- **查找 key**：计算 key 的哈希值，沿环**顺时针**找到第一个节点，该节点就是负责存储这个 key 的节点

**当增删节点时**，只有该节点附近的 key 会被重新分配，其他 key 不受影响。这就是一致性哈希的优势——节点变化时，影响的 key 范围最小（约 1/n）。

### 1.3 虚拟节点

在实际使用中，如果节点数量较少，可能出现**数据倾斜**问题：某些节点分配到的 key 远多于其他节点。

解决方案是**虚拟节点（Virtual Nodes）**：每个真实节点在环上放置多个虚拟节点。

```
真实节点: A, B
虚拟节点 (replicas=3):
  A -> hash("0A"), hash("1A"), hash("2A")
  B -> hash("0B"), hash("1B"), hash("2B")
```

虚拟节点越多，key 的分布越均匀。我们的项目默认使用 50 个虚拟节点。

### 1.4 文件结构

```
geecache_cpp/
└── src/
    └── consistenthash/
        ├── consistenthash.h     # [新增] 一致性哈希定义
        └── consistenthash.cpp   # [新增] 一致性哈希实现
```

## 2. 代码实现

### 2.1 类定义

创建文件 `src/consistenthash/consistenthash.h`：

```cpp
#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <vector>

namespace geecache {
namespace consistenthash {

// 哈希函数类型：接收一个字符串，返回 uint32_t
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
    HashFunc hash_;                    // 哈希函数
    int replicas_;                     // 每个真实节点的虚拟节点数
    std::map<int, std::string> ring_;  // 哈希环：哈希值 -> 节点名
};

}  // namespace consistenthash
}  // namespace geecache
```

**关键设计决策：**

**为什么用 `std::map` 而不是 `std::unordered_map`？**

因为我们需要**顺时针查找**——即找到第一个 `>=` 某个哈希值的节点。`std::map` 是有序的（红黑树），提供了 `lower_bound` 方法，可以高效地完成这个操作。`unordered_map` 是无序的，无法做到。

**为什么允许自定义哈希函数？**

- 默认使用 CRC32（通过 zlib 库提供），分布均匀且速度快
- 允许自定义主要是为了**测试**——使用可预测的哈希函数，方便验证结果

### 2.2 实现

创建文件 `src/consistenthash/consistenthash.cpp`：

```cpp
#include "consistenthash.h"

#include <zlib.h>

#include <algorithm>
#include <string>

namespace geecache {
namespace consistenthash {

// 默认哈希函数：使用 zlib 提供的 CRC32
static uint32_t defaultHash(const std::string& data) {
    return crc32(0, reinterpret_cast<const Bytef*>(data.data()),
                 static_cast<uInt>(data.size()));
}

Map::Map(int replicas, HashFunc fn)
    : replicas_(replicas), hash_(fn ? std::move(fn) : defaultHash) {}
```

构造函数：如果用户传入了自定义哈希函数就使用它，否则使用 CRC32。

**Add — 添加节点到哈希环：**

```cpp
void Map::Add(const std::vector<std::string>& keys) {
    for (const auto& key : keys) {
        for (int i = 0; i < replicas_; ++i) {
            // 为每个虚拟节点生成不同的哈希值
            std::string vnode = std::to_string(i) + key;
            uint32_t h = hash_(vnode);
            ring_[static_cast<int>(h)] = key;  // 映射到真实节点名
        }
    }
}
```

**示例**：假设 `replicas=3`，添加节点 `"A"`：
- 虚拟节点 `"0A"` → hash → 放到环上，映射到 `"A"`
- 虚拟节点 `"1A"` → hash → 放到环上，映射到 `"A"`
- 虚拟节点 `"2A"` → hash → 放到环上，映射到 `"A"`

注意 `ring_` 的 value 始终是**真实节点名**（`"A"`），不是虚拟节点名。这样 `Get` 返回的就是真实节点。

**Get — 查找 key 对应的节点：**

```cpp
std::string Map::Get(const std::string& key) const {
    if (ring_.empty()) {
        return "";
    }
    uint32_t h = hash_(key);
    // lower_bound: 找到第一个 >= h 的位置（顺时针方向）
    auto it = ring_.lower_bound(static_cast<int>(h));
    if (it == ring_.end()) {
        it = ring_.begin();  // 环形：超过最大值则回到起点
    }
    return it->second;
}
```

**核心操作解析：**

1. 计算 key 的哈希值 `h`
2. 在有序 map 中调用 `lower_bound(h)`，找到第一个哈希值 `>= h` 的节点
3. 如果到了 map 末尾（没有 `>= h` 的节点），**回到 map 开头**——这就实现了"环形"效果
4. 返回该位置对应的真实节点名

```
          哈希环示意图

     ring_: {2 -> "2", 4 -> "4", 6 -> "6", ...}

     查找 key="11" (hash=11):
       lower_bound(11) → 找到 12 → "2"

     查找 key="27" (hash=27):
       lower_bound(27) → end() → 回到 begin() → 2 → "2"
```

**IsEmpty：**

```cpp
bool Map::IsEmpty() const {
    return ring_.empty();
}

}  // namespace consistenthash
}  // namespace geecache
```

## 3. 测试验证

创建文件 `tests/consistenthash_test.cpp`：

```cpp
#include <gtest/gtest.h>

#include <string>

#include "consistenthash/consistenthash.h"
```

### 测试 1：一致性哈希的基本功能

```cpp
TEST(ConsistentHashTest, Hashing) {
    // 使用自定义哈希函数：直接返回字符串的数值
    // 这样哈希值可预测，方便测试
    auto hash_fn = [](const std::string& key) -> uint32_t {
        return static_cast<uint32_t>(std::stoi(key));
    };

    geecache::consistenthash::Map m(3, hash_fn);

    // 添加节点 "6", "4", "2"
    // replicas=3，虚拟节点为：
    // "06"->6, "16"->16, "26"->26  (真实节点 "6")
    // "04"->4, "14"->14, "24"->24  (真实节点 "4")
    // "02"->2, "12"->12, "22"->22  (真实节点 "2")
    m.Add({"6", "4", "2"});

    // 哈希环上的节点（按哈希值排序）：
    // 2, 4, 6, 12, 14, 16, 22, 24, 26

    struct TestCase {
        std::string key;
        std::string expected;
    };

    std::vector<TestCase> cases = {
        {"2", "2"},    // hash=2, lower_bound(2)=2 -> "2"
        {"11", "2"},   // hash=11, lower_bound(11)=12 -> "2"
        {"23", "4"},   // hash=23, lower_bound(23)=24 -> "4"
        {"27", "2"},   // hash=27, lower_bound(27)=end() -> begin()=2 -> "2"
    };

    for (const auto& tc : cases) {
        std::string got = m.Get(tc.key);
        EXPECT_EQ(got, tc.expected)
            << "Asking for " << tc.key << ", should have yielded " << tc.expected;
    }

    // 新增节点 "8"：虚拟节点 "08"->8, "18"->18, "28"->28
    m.Add({"8"});

    // "27" 现在应该映射到 "8"（因为 28 比 2 更近）
    EXPECT_EQ(m.Get("27"), "8");
}
```

**这个测试验证了一致性哈希的三个关键特性：**

1. **正确路由**：key 被分配到顺时针方向最近的节点
2. **环形回绕**：hash=27 超过最大值 26 后回到起点
3. **最小影响**：新增节点 "8" 后，只有 "27" 的映射改变了（从 "2" 变为 "8"），其他 key 不受影响

### 测试 2：空哈希环

```cpp
TEST(ConsistentHashTest, Empty) {
    geecache::consistenthash::Map m(3);
    EXPECT_TRUE(m.IsEmpty());
    EXPECT_EQ(m.Get("key"), "");  // 空环返回空字符串
}
```

## 4. 小结

Day 4 我们实现了分布式缓存的关键算法——一致性哈希。核心要点：

- **哈希环**：将节点和 key 都映射到同一个环上，顺时针查找最近节点
- **虚拟节点**：每个真实节点对应多个虚拟节点（`replicas` 个），解决数据倾斜
- **`std::map` + `lower_bound`**：利用有序 map 的 `lower_bound` 实现 O(log n) 的顺时针查找
- **环形回绕**：`lower_bound` 到 `end()` 时回到 `begin()`
- **最小影响**：节点增删时，只有约 1/n 的 key 需要重新分配
- **CRC32 默认哈希**：使用 zlib 提供的 CRC32，分布均匀

Day 5 预告：我们将把一致性哈希集成到 `HTTPPool` 中，实现真正的**分布式节点**——当缓存未命中时，通过一致性哈希找到负责该 key 的远程节点，并通过 HTTP 获取数据。
