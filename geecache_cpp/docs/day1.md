# Day 1: LRU 缓存淘汰策略

## 1. 背景知识

### 1.1 什么是缓存淘汰策略？

缓存的容量是有限的。当缓存满了，再往里面添加新数据时，就需要"淘汰"掉一些旧数据，为新数据腾出空间。淘汰哪些数据，就是**缓存淘汰策略**要解决的问题。

常见的淘汰策略有三种：

| 策略 | 全称 | 淘汰规则 |
|------|------|----------|
| FIFO | First In First Out | 淘汰最早被添加的数据 |
| LFU  | Least Frequently Used | 淘汰被访问次数最少的数据 |
| **LRU** | **Least Recently Used** | **淘汰最近最少被使用的数据** |

我们的项目选择 **LRU**，因为它实现简单、效果好，是工业界最常用的缓存淘汰策略（Redis、Memcached 等都使用了 LRU 或其变种）。

### 1.2 LRU 的核心思想

LRU 的核心假设是：**如果一个数据最近被访问过，那么它将来被访问的概率也更高。**

举个例子：假设缓存容量为 3，依次访问数据 `A B C D`：

```
操作       缓存状态（左边是最新）     说明
Add A  ->  [A]                       缓存未满，直接添加
Add B  ->  [B, A]                    缓存未满，直接添加
Add C  ->  [C, B, A]                 缓存已满
Add D  ->  [D, C, B]                 缓存已满，淘汰最久未使用的 A
Get B  ->  [B, D, C]                 访问 B，B 移到最前面
Add E  ->  [E, B, D]                 缓存已满，淘汰最久未使用的 C
```

### 1.3 LRU 的数据结构选型

要高效实现 LRU，需要两个操作都是 O(1)：
- **查找**：给定 key，快速找到对应的数据 -> 需要**哈希表**（`unordered_map`）
- **淘汰 + 移动**：快速将元素移到队首，快速删除队尾元素 -> 需要**双向链表**（`list`）

所以 LRU 的经典实现是：**哈希表 + 双向链表**。

```
                    哈希表 (unordered_map)
                   +--------+--------+
                   | key1   | iter1 -------+
                   | key2   | iter2 -----+ |
                   | key3   | iter3 ---+ | |
                   +--------+--------+ | | |
                                       | | |
           双向链表 (list)              | | |
     front                        back | | |
       |                            |  | | |
       v                            v  v v v
      [key3,val3] <-> [key2,val2] <-> [key1,val1]
       (最新)                          (最旧，优先淘汰)
```

- 哈希表的 value 存储的是链表的**迭代器**（指针），实现 O(1) 定位
- 链表头部是最近使用的，尾部是最久未使用的
- 淘汰时直接删除链表尾部元素即可

## 2. 设计思路

### 2.1 整体结构

我们要实现一个 `Cache` 类，它需要：

1. **模板化**：缓存的 value 类型应该是泛型的，用模板参数 `V` 表示
2. **容量控制**：通过 `maxBytes` 限制缓存占用的总字节数（而不是条目数）
3. **淘汰回调**：当数据被淘汰时，允许用户注册一个回调函数
4. **三个核心方法**：
   - `Add(key, value)` - 添加/更新缓存
   - `Get(key)` - 查找缓存
   - `RemoveOldest()` - 淘汰最旧的条目

### 2.2 字节数计算

为什么用字节数而不是条目数来限制容量？因为不同的 value 占用的内存大小可能差别很大。我们要求 value 类型 `V` 必须提供一个 `Len()` 方法，返回其占用的字节数。

每个条目占用的字节数 = `key.size() + value.Len()`

### 2.3 文件结构

Day 1 我们只需要创建一个文件：

```
geecache_cpp/
└── src/
    └── lru/
        └── lru.h      # LRU 缓存实现（header-only）
```

由于使用了 C++ 模板，整个实现放在头文件中即可（header-only）。

## 3. 代码实现

### 3.1 头文件与命名空间

首先创建文件 `src/lru/lru.h`，引入需要的头文件并定义命名空间：

```cpp
#pragma once

#include <cstdint>
#include <functional>
#include <list>
#include <string>
#include <unordered_map>
#include <utility>

namespace geecache {
namespace lru {

// ... Cache 类的实现将放在这里

}  // namespace lru
}  // namespace geecache
```

各头文件的用途：

| 头文件 | 用途 |
|--------|------|
| `<cstdint>` | 提供 `int64_t`，用于字节数计算 |
| `<functional>` | 提供 `std::function`，用于淘汰回调 |
| `<list>` | 提供 `std::list`，作为双向链表 |
| `<string>` | 提供 `std::string`，作为 key 的类型 |
| `<unordered_map>` | 提供 `std::unordered_map`，作为哈希表 |
| `<utility>` | 提供 `std::pair` 和 `std::move` |

### 3.2 类定义与类型别名

```cpp
template <typename V>
class Cache {
public:
    // 淘汰回调函数类型：接收被淘汰的 key 和 value
    using EvictedCallback = std::function<void(const std::string&, const V&)>;
    // 链表中的每个节点存储一个 key-value 对
    using Entry = std::pair<std::string, V>;
    // 双向链表类型
    using ListType = std::list<Entry>;
    // 哈希表类型：key -> 链表迭代器
    using MapType = std::unordered_map<std::string, typename ListType::iterator>;
```

**为什么哈希表的 value 是链表迭代器？**

`std::list` 的迭代器本质上就是指向链表节点的指针。通过哈希表存储迭代器，我们可以在 O(1) 时间内定位到链表中的任意节点，然后用 `splice` 将其移动到链表头部。

### 3.3 构造函数

```cpp
    explicit Cache(int64_t maxBytes, EvictedCallback onEvicted = nullptr)
        : max_bytes_(maxBytes), nbytes_(0), on_evicted_(std::move(onEvicted)) {}
```

- `maxBytes`：最大容量（字节数）。传入 0 表示不限制容量
- `onEvicted`：可选的淘汰回调，默认为 `nullptr`
- `nbytes_`：初始化为 0，记录当前已使用的字节数

### 3.4 Get 方法 - 查找缓存

```cpp
    // Get looks up a key's value from the cache.
    std::pair<V, bool> Get(const std::string& key) {
        auto it = cache_.find(key);
        if (it == cache_.end()) {
            return {V{}, false};     // 未找到，返回默认值 + false
        }
        // 找到了，将该节点移到链表头部（标记为最近使用）
        ll_.splice(ll_.begin(), ll_, it->second);
        return {it->second->second, true};  // 返回 value + true
    }
```

**关键操作解析：**

1. `cache_.find(key)` — 在哈希表中查找 key，时间复杂度 O(1)
2. `ll_.splice(ll_.begin(), ll_, it->second)` — 这是 `std::list` 的核心操作：
   - 将 `it->second`（链表迭代器）指向的节点，从当前位置移动到 `ll_.begin()`（链表头部）
   - 这个操作是 O(1) 的，不会导致迭代器失效
3. `it->second->second` — `it->second` 是链表迭代器，指向 `Entry`（即 `pair<string, V>`），再取 `.second` 就是 value

### 3.5 Add 方法 - 添加/更新缓存

```cpp
    // Add adds or updates a value in the cache.
    void Add(const std::string& key, const V& value) {
        auto it = cache_.find(key);
        if (it != cache_.end()) {
            // key 已存在 -> 更新：移到链表头部，更新 value 和字节数
            ll_.splice(ll_.begin(), ll_, it->second);
            nbytes_ += value.Len() - it->second->second.Len();
            it->second->second = value;
        } else {
            // key 不存在 -> 新增：插入链表头部，更新哈希表和字节数
            ll_.push_front({key, value});
            cache_[key] = ll_.begin();
            nbytes_ += static_cast<int64_t>(key.size()) + value.Len();
        }
        // 如果超出容量，循环淘汰最旧的条目
        while (max_bytes_ != 0 && max_bytes_ < nbytes_) {
            RemoveOldest();
        }
    }
```

**分两种情况：**

**情况 1：key 已存在（更新）**
- 先把节点移到链表头部（`splice`）
- 更新字节数差值：新 value 的大小减去旧 value 的大小
- 替换 value

**情况 2：key 不存在（新增）**
- 在链表头部插入新节点
- 在哈希表中记录 key 到链表头部迭代器的映射
- 累加字节数（key + value）

**最后的淘汰循环：**
- 当 `max_bytes_` 不为 0（有容量限制）且当前字节数超过上限时，循环调用 `RemoveOldest()` 淘汰最旧的数据

### 3.6 RemoveOldest 方法 - 淘汰最旧条目

```cpp
    // RemoveOldest removes the oldest item.
    void RemoveOldest() {
        if (ll_.empty()) {
            return;
        }
        auto& back = ll_.back();                    // 取链表尾部（最旧的条目）
        cache_.erase(back.first);                   // 从哈希表中删除
        nbytes_ -= static_cast<int64_t>(back.first.size()) + back.second.Len();  // 扣减字节数
        if (on_evicted_) {
            on_evicted_(back.first, back.second);   // 触发淘汰回调
        }
        ll_.pop_back();                             // 从链表中删除
    }
```

淘汰流程：
1. 取链表尾部元素（最久未使用）
2. 从哈希表中删除对应的 key
3. 扣减字节数
4. 如果注册了淘汰回调，调用它
5. 从链表中删除尾部节点

### 3.7 Len 方法 - 获取缓存条目数

```cpp
    // Len returns the number of cache entries.
    int Len() const {
        return static_cast<int>(ll_.size());
    }
```

### 3.8 私有成员变量

```cpp
private:
    int64_t max_bytes_;              // 最大容量（字节），0 表示无限制
    int64_t nbytes_;                 // 当前已使用字节数
    ListType ll_;                    // 双向链表
    MapType cache_;                  // 哈希表：key -> 链表迭代器
    EvictedCallback on_evicted_;     // 淘汰回调函数
};
```

### 3.9 完整代码

将以上所有部分组合在一起，完整的 `src/lru/lru.h` 如下：

```cpp
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
```

## 4. 测试验证

为了验证我们的 LRU 实现是否正确，编写以下测试（使用 GoogleTest）。

创建文件 `tests/lru_test.cpp`：

```cpp
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "lru/lru.h"

// 一个简单的 value 类型，满足 Len() 方法的要求
struct StringValue {
    std::string s;
    int Len() const { return static_cast<int>(s.size()); }
};
```

### 测试 1：基本的 Get 操作

```cpp
TEST(LruTest, Get) {
    geecache::lru::Cache<StringValue> cache(0);  // 0 表示不限容量
    cache.Add("key1", StringValue{"1234"});

    auto [val, ok] = cache.Get("key1");
    ASSERT_TRUE(ok);
    EXPECT_EQ(val.s, "1234");

    auto [val2, ok2] = cache.Get("key2");
    EXPECT_FALSE(ok2);  // key2 不存在
}
```

### 测试 2：淘汰最旧条目

```cpp
TEST(LruTest, RemoveOldest) {
    // maxBytes = len("key1") + 4 + len("key2") + 4 = 16
    // 刚好容纳两个条目
    geecache::lru::Cache<StringValue> cache(16);
    cache.Add("key1", StringValue{"1234"});
    cache.Add("key2", StringValue{"1234"});
    // 此时已满 16 字节，添加 key3 应该淘汰最旧的 key1
    cache.Add("key3", StringValue{"1234"});

    auto [val, ok] = cache.Get("key1");
    EXPECT_FALSE(ok);  // key1 应该已经被淘汰
}
```

### 测试 3：淘汰回调

```cpp
TEST(LruTest, OnEvicted) {
    std::vector<std::string> evicted_keys;

    geecache::lru::Cache<StringValue> cache(16,
        [&evicted_keys](const std::string& key, const StringValue&) {
            evicted_keys.push_back(key);
        });

    cache.Add("key1", StringValue{"1234"});
    cache.Add("key2", StringValue{"1234"});
    cache.Add("key3", StringValue{"1234"});  // 淘汰 key1
    cache.Add("key4", StringValue{"1234"});  // 淘汰 key2

    ASSERT_EQ(evicted_keys.size(), 2u);
    EXPECT_EQ(evicted_keys[0], "key1");
    EXPECT_EQ(evicted_keys[1], "key2");
}
```

### 测试 4：更新已有 key

```cpp
TEST(LruTest, Add) {
    geecache::lru::Cache<StringValue> cache(0);
    cache.Add("key", StringValue{"1"});
    cache.Add("key", StringValue{"111"});  // 更新 value

    auto [val, ok] = cache.Get("key");
    ASSERT_TRUE(ok);
    EXPECT_EQ(val.s, "111");    // value 已更新
    EXPECT_EQ(cache.Len(), 1);  // 条目数不变
}
```

## 5. 小结

Day 1 我们实现了 GeeCache 的基石——LRU 缓存淘汰策略。核心要点：

- **数据结构**：哈希表（`unordered_map`）+ 双向链表（`list`），保证查找和淘汰都是 O(1)
- **容量控制**：通过字节数（而非条目数）限制缓存大小，更贴近实际场景
- **`splice` 操作**：`std::list::splice` 是实现 LRU 的关键，它能在 O(1) 时间内将节点移动到链表头部
- **淘汰回调**：通过 `std::function` 提供灵活的回调机制
- **线程安全**：当前的 LRU Cache **不是线程安全的**，这将在 Day 2 中通过加锁来解决

Day 2 预告：我们将在 LRU 缓存的基础上，实现 `ByteView`（不可变的值包装器）和 `Cache`（并发安全的缓存），并引入 `Group` 的概念来管理缓存。
