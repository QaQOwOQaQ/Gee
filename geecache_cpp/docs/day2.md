# Day 2: 单机并发缓存

## 1. 背景知识

### 1.1 Day 1 回顾

在 Day 1 中，我们实现了 LRU 缓存淘汰策略。但它有两个问题：

1. **不是线程安全的**：多个线程同时读写会导致数据竞争
2. **只是一个底层数据结构**：缺少更高层的抽象来管理缓存（比如缓存未命中时从哪里加载数据？）

Day 2 的目标是解决这两个问题，构建一个完整的单机缓存系统。

### 1.2 Day 2 要实现的三个组件

| 组件 | 文件 | 职责 |
|------|------|------|
| `ByteView` | `byteview.h` / `byteview.cpp` | 不可变的字节视图，作为缓存值的统一类型 |
| `ConcurrentCache` | `cache.h` / `cache.cpp` | 在 LRU 外面加一层锁，实现并发安全 |
| `Group` | `geecache.h` / `geecache.cpp` | 最核心的结构：缓存命名空间 + 回源逻辑 |

它们的关系是这样的：

```
Group（缓存管理者）
  ├── name_            : 缓存的名字（如 "scores"）
  ├── getter_          : 缓存未命中时的回源回调
  └── main_cache_      : ConcurrentCache（并发安全缓存）
                            └── lru_ : lru::Cache<ByteView>（Day 1 实现的 LRU）
```

### 1.3 为什么需要 ByteView？

在缓存系统中，我们需要一个**通用的值类型**来存储任意数据（字符串、序列化的 protobuf、JSON 等）。`ByteView` 就是这样一个类型：

- **不可变**：一旦创建就不能修改，防止缓存值被意外篡改
- **统一接口**：提供 `Len()` 方法满足 LRU 的要求，提供 `ByteSlice()` 和 `String()` 方法获取数据
- **轻量**：底层就是一个 `std::string`

### 1.4 为什么需要 Group？

想象一下实际使用场景：一个应用可能需要多种缓存：

- "学生成绩" 缓存 —— 未命中时从成绩数据库加载
- "用户信息" 缓存 —— 未命中时从用户数据库加载

每种缓存都是一个 **Group**，它包含：

1. 一个**唯一的名字**（如 "scores"）
2. 一个**回源回调**（getter）：当缓存未命中时，告诉系统如何获取数据
3. 一个**并发安全缓存**（ConcurrentCache）：存储已加载的数据

### 1.5 文件结构

Day 2 新增以下文件：

```
geecache_cpp/
└── src/
    ├── lru/
    │   └── lru.h            # Day 1 已完成
    ├── byteview.h           # [新增] 不可变字节视图
    ├── byteview.cpp         # [新增] 占位文件
    ├── cache.h              # [新增] 并发安全缓存
    ├── cache.cpp            # [新增] 占位文件
    ├── geecache.h           # [新增] Group 定义（Day 2 简化版）
    └── geecache.cpp         # [新增] Group 实现（Day 2 简化版）
```

> **注意**：`geecache.h` 和 `geecache.cpp` 在后续的 Day 中会逐步扩展（加入分布式节点、singleflight、protobuf 等功能）。Day 2 我们先实现一个**单机版本**。

## 2. 代码实现

### 2.1 ByteView - 不可变字节视图

创建文件 `src/byteview.h`：

```cpp
#pragma once

#include <string>

namespace geecache {

// ByteView holds an immutable view of bytes.
class ByteView {
public:
    ByteView() = default;
    explicit ByteView(std::string bytes) : b_(std::move(bytes)) {}
    ByteView(const char* data, size_t len) : b_(data, len) {}

    // Len returns the byte length. Required by lru::Cache.
    int Len() const { return static_cast<int>(b_.size()); }

    // ByteSlice returns a copy of the data as a byte slice (string).
    std::string ByteSlice() const { return b_; }

    // String returns the data as a string.
    std::string String() const { return b_; }

private:
    std::string b_;
};

}  // namespace geecache
```

**逐行解析：**

**三个构造函数：**
- `ByteView()` — 默认构造，创建空的 ByteView
- `ByteView(std::string bytes)` — 从字符串构造，使用 `std::move` 避免拷贝
- `ByteView(const char* data, size_t len)` — 从原始字节数组构造

**三个方法：**
- `Len()` — 返回字节长度。这个方法是 **必须的**，因为 Day 1 的 `lru::Cache<V>` 要求 `V` 提供 `Len()` 方法
- `ByteSlice()` — 返回数据的拷贝（保证不可变性：返回的是副本，调用者修改不会影响缓存中的值）
- `String()` — 与 `ByteSlice()` 功能相同，语义上更明确

再创建一个占位文件 `src/byteview.cpp`（构建系统需要它）：

```cpp
// byteview.cpp — trivial, all inlined in header.
// This file exists for consistency; the build system expects it.

#include "byteview.h"
```

### 2.2 ConcurrentCache - 并发安全缓存

创建文件 `src/cache.h`：

```cpp
#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "byteview.h"
#include "lru/lru.h"

namespace geecache {

// ConcurrentCache is a thread-safe wrapper around lru::Cache<ByteView>.
class ConcurrentCache {
public:
    explicit ConcurrentCache(int64_t cacheBytes = 0)
        : cache_bytes_(cacheBytes) {}

    void Add(const std::string& key, const ByteView& value) {
        std::lock_guard<std::mutex> lock(mu_);
        if (!lru_) {
            lru_ = std::make_unique<lru::Cache<ByteView>>(cache_bytes_);
        }
        lru_->Add(key, value);
    }

    std::pair<ByteView, bool> Get(const std::string& key) {
        std::lock_guard<std::mutex> lock(mu_);
        if (!lru_) {
            return {ByteView{}, false};
        }
        return lru_->Get(key);
    }

private:
    std::mutex mu_;
    std::unique_ptr<lru::Cache<ByteView>> lru_;
    int64_t cache_bytes_;
};

}  // namespace geecache
```

**设计要点：**

**1. 互斥锁保证线程安全**

```cpp
std::mutex mu_;
```

所有对 LRU 缓存的操作都在 `std::lock_guard` 保护下进行。`lock_guard` 利用 RAII 机制，在构造时加锁、析构时解锁，不会遗忘解锁。

**2. 延迟初始化（Lazy Initialization）**

```cpp
if (!lru_) {
    lru_ = std::make_unique<lru::Cache<ByteView>>(cache_bytes_);
}
```

LRU 缓存使用 `std::unique_ptr` 管理，直到第一次 `Add` 时才真正创建。这样做的好处是：
- 如果某个 Group 从未被写入数据，就不会浪费内存创建 LRU 对象
- 构造 `ConcurrentCache` 的成本很低

**3. Get 方法中的空指针检查**

```cpp
if (!lru_) {
    return {ByteView{}, false};
}
```

如果还没有任何数据被 `Add` 过，`lru_` 为空，直接返回"未找到"。

再创建占位文件 `src/cache.cpp`：

```cpp
// cache.cpp — ConcurrentCache methods are inlined in cache.h.
// This file exists for consistency; the build system expects it.

#include "cache.h"
```

### 2.3 Getter - 回源接口

当缓存未命中时，我们需要从"数据源"（数据库、文件、API 等）获取数据。`Getter` 定义了这个回源接口。

在 `src/geecache.h` 中定义：

```cpp
#pragma once

#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "byteview.h"
#include "cache.h"

namespace geecache {

// Getter loads data for a key (the slow backend / data source).
class Getter {
public:
    virtual ~Getter() = default;
    // Returns {bytes, error}. Empty error means success.
    virtual std::pair<std::string, std::string> Get(const std::string& key) = 0;
};
```

`Getter` 是一个纯虚基类（接口），只有一个方法：

- `Get(key)` — 返回 `{数据, 错误信息}`。错误信息为空字符串表示成功

**为什么用接口而不是直接传函数？**

为了灵活性，我们同时提供了一个 `GetterFunc` 适配器，允许用户用 lambda 或普通函数作为 getter：

```cpp
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
```

这是一个经典的**接口型函数**设计模式：`GetterFunc` 将一个普通函数包装成 `Getter` 接口的实现。使用时可以这样写：

```cpp
auto getter = std::make_shared<GetterFunc>(
    [](const std::string& key) -> std::pair<std::string, std::string> {
        // 从数据库查询...
        return {"data_for_" + key, ""};  // 返回 {数据, ""}，""表示无错误
    });
```

### 2.4 Group - 缓存管理核心

`Group` 是整个 GeeCache 的核心结构。Day 2 的单机版本流程如下：

```
Group::Get(key)
    |
    +--[缓存命中]--> 直接返回 ByteView
    |
    +--[缓存未命中]
            |
            v
        GetLocally(key)
            |
            v
        调用 getter_->Get(key) 从数据源获取
            |
            v
        PopulateCache(key, value) 写入缓存
            |
            v
        返回 ByteView
```

#### Group 类定义（Day 2 简化版）

继续在 `src/geecache.h` 中添加：

```cpp
// Group is a cache namespace and associated data loading logic.
class Group {
public:
    // NewGroup creates a new Group and registers it globally.
    static std::shared_ptr<Group> NewGroup(
        const std::string& name,
        int64_t cacheBytes,
        std::shared_ptr<Getter> getter);

    // GetGroup returns a previously created group by name, or nullptr.
    static std::shared_ptr<Group> GetGroup(const std::string& name);

    // DestroyAllGroups clears the global group registry (for testing).
    static void DestroyAllGroups();

    // Get retrieves a value for a key.
    std::pair<ByteView, std::string> Get(const std::string& key);

    const std::string& Name() const { return name_; }

private:
    Group(const std::string& name, int64_t cacheBytes,
          std::shared_ptr<Getter> getter);

    std::pair<ByteView, std::string> GetLocally(const std::string& key);
    void PopulateCache(const std::string& key, const ByteView& value);

    std::string name_;
    std::shared_ptr<Getter> getter_;
    ConcurrentCache main_cache_;

    // Global registry
    static std::shared_mutex groups_mu_;
    static std::unordered_map<std::string, std::shared_ptr<Group>> groups_;
};

}  // namespace geecache
```

**设计要点：**

**1. 全局注册表**

```cpp
static std::shared_mutex groups_mu_;
static std::unordered_map<std::string, std::shared_ptr<Group>> groups_;
```

所有的 Group 都注册在一个全局的 `unordered_map` 中，通过名字索引。使用 `std::shared_mutex`（读写锁）保护：
- 创建 Group 时加**写锁**（`unique_lock`）
- 查找 Group 时加**读锁**（`shared_lock`），允许多个线程同时读

**2. 构造函数是 private 的**

```cpp
private:
    Group(const std::string& name, int64_t cacheBytes,
          std::shared_ptr<Getter> getter);
```

只能通过静态方法 `NewGroup()` 创建 Group，确保每个 Group 都被注册到全局表中。

**3. 使用 `shared_ptr` 管理生命周期**

Group 使用 `shared_ptr` 管理，这样即使从全局表中移除，只要还有代码持有引用，Group 就不会被销毁。

#### Group 实现

创建文件 `src/geecache.cpp`：

```cpp
#include "geecache.h"

#include <iostream>
#include <stdexcept>

namespace geecache {

// Static members
std::shared_mutex Group::groups_mu_;
std::unordered_map<std::string, std::shared_ptr<Group>> Group::groups_;

Group::Group(const std::string& name, int64_t cacheBytes,
             std::shared_ptr<Getter> getter)
    : name_(name), getter_(std::move(getter)), main_cache_(cacheBytes) {}
```

**NewGroup — 创建并注册一个新的 Group：**

```cpp
std::shared_ptr<Group> Group::NewGroup(
    const std::string& name,
    int64_t cacheBytes,
    std::shared_ptr<Getter> getter) {
    if (!getter) {
        throw std::runtime_error("nil Getter");
    }
    std::unique_lock<std::shared_mutex> lock(groups_mu_);
    auto g = std::shared_ptr<Group>(new Group(name, cacheBytes, std::move(getter)));
    groups_[name] = g;
    return g;
}
```

注意这里用 `new Group(...)` 而不是 `std::make_shared<Group>(...)`，因为 Group 的构造函数是 private 的，`make_shared` 无法访问。

**GetGroup — 按名字查找 Group：**

```cpp
std::shared_ptr<Group> Group::GetGroup(const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(groups_mu_);
    auto it = groups_.find(name);
    if (it == groups_.end()) {
        return nullptr;
    }
    return it->second;
}
```

使用读锁（`shared_lock`），多个线程可以同时查找。

**DestroyAllGroups — 清空所有 Group（测试用）：**

```cpp
void Group::DestroyAllGroups() {
    std::unique_lock<std::shared_mutex> lock(groups_mu_);
    groups_.clear();
}
```

**Get — 获取缓存值（核心方法）：**

```cpp
std::pair<ByteView, std::string> Group::Get(const std::string& key) {
    if (key.empty()) {
        return {ByteView{}, "key is required"};
    }

    // 1. 先查本地缓存
    auto [view, ok] = main_cache_.Get(key);
    if (ok) {
        std::cerr << "[GeeCache] hit" << std::endl;
        return {view, ""};
    }

    // 2. 缓存未命中，从数据源加载
    return GetLocally(key);
}
```

这是 Day 2 的简化版本。在后续的 Day 中，缓存未命中时会先尝试从远程节点获取，再从本地数据源加载。

**GetLocally — 从本地数据源加载：**

```cpp
std::pair<ByteView, std::string> Group::GetLocally(const std::string& key) {
    auto [bytes, err] = getter_->Get(key);
    if (!err.empty()) {
        return {ByteView{}, err};
    }
    ByteView value(std::move(bytes));
    PopulateCache(key, value);
    return {value, ""};
}
```

流程：调用 getter 获取数据 → 如果出错就返回错误 → 包装成 ByteView → 写入缓存 → 返回

**PopulateCache — 写入缓存：**

```cpp
void Group::PopulateCache(const std::string& key, const ByteView& value) {
    main_cache_.Add(key, value);
}

}  // namespace geecache
```

## 3. 测试验证

创建文件 `tests/geecache_test.cpp`：

```cpp
#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <unordered_map>

#include "geecache.h"

// 测试夹具：每个测试前后清空全局 Group 注册表
class GeeCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        geecache::Group::DestroyAllGroups();
    }
    void TearDown() override {
        geecache::Group::DestroyAllGroups();
    }
};

// 模拟数据库
static std::unordered_map<std::string, std::string> db = {
    {"Tom", "630"},
    {"Jack", "589"},
    {"Sam", "567"},
};
```

### 测试 1：Getter 接口

```cpp
TEST_F(GeeCacheTest, Getter) {
    geecache::GetterFunc getter([](const std::string& key)
        -> std::pair<std::string, std::string> {
        return {"value_for_" + key, ""};
    });
    auto [val, err] = getter.Get("test");
    EXPECT_TRUE(err.empty());
    EXPECT_EQ(val, "value_for_test");
}
```

### 测试 2：缓存命中与回源

```cpp
TEST_F(GeeCacheTest, Get) {
    std::atomic<int> load_counts{0};

    auto getter = std::make_shared<geecache::GetterFunc>(
        [&load_counts](const std::string& key)
            -> std::pair<std::string, std::string> {
            auto it = db.find(key);
            if (it != db.end()) {
                load_counts++;
                return {it->second, ""};
            }
            return {"", "key not found: " + key};
        });

    auto group = geecache::Group::NewGroup("scores", 2 << 10, getter);

    // 第一次访问：触发回源（getter 被调用）
    for (const auto& [key, expected] : db) {
        auto [view, err] = group->Get(key);
        ASSERT_TRUE(err.empty()) << "Failed to get key " << key << ": " << err;
        EXPECT_EQ(view.String(), expected);
    }

    // 第二次访问：命中缓存（getter 不再被调用）
    int prev_count = load_counts.load();
    for (const auto& [key, expected] : db) {
        auto [view, err] = group->Get(key);
        ASSERT_TRUE(err.empty());
        EXPECT_EQ(view.String(), expected);
    }
    EXPECT_EQ(load_counts.load(), prev_count);  // getter 没有被额外调用

    // 访问不存在的 key
    auto [view, err] = group->Get("unknown");
    EXPECT_FALSE(err.empty());  // 应该返回错误
}
```

这个测试验证了两个关键行为：
1. **缓存未命中时回源**：第一次访问 key 时，getter 被调用，`load_counts` 递增
2. **缓存命中时不回源**：第二次访问相同的 key 时，直接从缓存返回，`load_counts` 不再变化

### 测试 3：全局 Group 注册

```cpp
TEST_F(GeeCacheTest, GetGroup) {
    auto getter = std::make_shared<geecache::GetterFunc>(
        [](const std::string& key) -> std::pair<std::string, std::string> {
            return {key, ""};
        });

    auto group = geecache::Group::NewGroup("test_group", 1024, getter);
    EXPECT_EQ(group->Name(), "test_group");

    // 通过名字查找已注册的 Group
    auto found = geecache::Group::GetGroup("test_group");
    EXPECT_NE(found, nullptr);
    EXPECT_EQ(found->Name(), "test_group");

    // 查找不存在的 Group
    auto not_found = geecache::Group::GetGroup("nonexistent");
    EXPECT_EQ(not_found, nullptr);
}
```

## 4. 小结

Day 2 我们构建了一个完整的单机缓存系统，包含三层抽象：

| 层级 | 组件 | 职责 |
|------|------|------|
| 底层 | `lru::Cache<V>` | LRU 淘汰策略（Day 1） |
| 中间 | `ConcurrentCache` | 加锁实现并发安全 |
| 上层 | `Group` | 缓存命名空间 + 回源逻辑 |

核心要点：

- **ByteView**：不可变字节视图，作为缓存值的统一类型，提供 `Len()` 满足 LRU 的要求
- **ConcurrentCache**：用 `std::mutex` + `std::lock_guard` 包装 LRU，并采用延迟初始化
- **Group**：使用全局注册表（`shared_mutex` 保护的 `unordered_map`）管理所有缓存组
- **Getter 接口**：通过接口 + `GetterFunc` 适配器，支持 lambda 作为回源回调
- **Get 流程**：先查缓存 → 未命中则调用 getter 回源 → 结果写入缓存

Day 3 预告：我们将实现 HTTP 服务端，让其他节点可以通过 HTTP 协议访问缓存数据。
