# Day 6: 防止缓存击穿

## 1. 背景知识

### 1.1 什么是缓存击穿？

想象这样一个场景：某个热门 key（如 "Tom" 的成绩）在缓存中过期或不存在，此时有 **1000 个并发请求**同时查询这个 key。

**没有 singleflight 的情况：**

```
请求 1 --> Get("Tom") --> 缓存未命中 --> Getter 回源（查数据库）
请求 2 --> Get("Tom") --> 缓存未命中 --> Getter 回源（查数据库）
请求 3 --> Get("Tom") --> 缓存未命中 --> Getter 回源（查数据库）
...
请求 1000 --> Get("Tom") --> 缓存未命中 --> Getter 回源（查数据库）
```

1000 个请求全部穿透缓存，打到数据库上！这就是**缓存击穿**（也叫缓存穿透、缓存雪崩），会导致数据库瞬间负载飙升甚至崩溃。

### 1.2 singleflight 的解决思路

**核心思想：对于同一个 key 的并发请求，只让第一个请求去执行回源操作，其他请求等待并共享结果。**

```
请求 1 --> Get("Tom") --> 缓存未命中 --> Getter 回源（查数据库）-- 结果 --+
请求 2 --> Get("Tom") --> 缓存未命中 --> 等待...                        |
请求 3 --> Get("Tom") --> 缓存未命中 --> 等待...                        |
...                                                                    |
请求 1000 --> Get("Tom") --> 缓存未命中 --> 等待...                     |
                                                                       v
                                                          所有请求共享同一个结果
```

无论有多少并发请求，数据库只被查询了 **1 次**。

### 1.3 文件结构

```
geecache_cpp/
└── src/
    └── singleflight/
        ├── singleflight.h     # [新增] singleflight 定义
        └── singleflight.cpp   # [新增] singleflight 实现
```

## 2. 代码实现

### 2.1 类定义

创建文件 `src/singleflight/singleflight.h`：

```cpp
#pragma once

#include <any>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

namespace geecache {
namespace singleflight {

class Group {
public:
    // Do executes fn() for the given key, deduplicating concurrent calls.
    // Returns {value, error_string}. Empty error_string means success.
    std::pair<std::any, std::string> Do(
        const std::string& key,
        std::function<std::pair<std::any, std::string>()> fn);

private:
    // Call represents an in-flight or completed call to Do.
    struct Call {
        std::mutex mu;
        std::condition_variable cv;
        bool done = false;        // 是否已完成
        std::any val;             // 结果值
        std::string err;          // 错误信息
    };

    std::mutex mu_;               // 保护 calls_ 的互斥锁
    std::unordered_map<std::string, std::shared_ptr<Call>> calls_;  // 正在进行中的调用
};

}  // namespace singleflight
}  // namespace geecache
```

**关键数据结构：**

**`Call` — 表示一次正在进行（或已完成）的调用**

| 成员 | 用途 |
|------|------|
| `mu` + `cv` | 互斥锁和条件变量，用于等待/通知 |
| `done` | 标记调用是否已完成 |
| `val` | 调用结果（使用 `std::any` 存储任意类型） |
| `err` | 错误信息 |

**`calls_` — 记录当前正在执行的调用**

key 是缓存的 key（如 `"Tom"`），value 是对应的 `Call` 对象。当一个 key 的调用正在执行时，其他相同 key 的请求可以找到这个 `Call` 并等待它完成。

**为什么使用 `std::any`？**

因为 singleflight 是一个通用的去重组件，它不关心具体的值类型。使用 `std::any` 可以存储任意类型的结果。调用者需要用 `std::any_cast<T>()` 取出具体类型的值。

### 2.2 Do 方法实现

创建文件 `src/singleflight/singleflight.cpp`：

```cpp
#include "singleflight.h"

namespace geecache {
namespace singleflight {

std::pair<std::any, std::string> Group::Do(
    const std::string& key,
    std::function<std::pair<std::any, std::string>()> fn) {

    std::unique_lock<std::mutex> lock(mu_);

    // 1. 检查是否已有相同 key 的调用正在执行
    auto it = calls_.find(key);
    if (it != calls_.end()) {
        // 已有调用在执行 -> 等待它完成，共享结果
        auto c = it->second;
        lock.unlock();

        std::unique_lock<std::mutex> call_lock(c->mu);
        c->cv.wait(call_lock, [&c] { return c->done; });
        return {c->val, c->err};
    }

    // 2. 第一个请求：创建 Call，注册到 calls_
    auto c = std::make_shared<Call>();
    calls_[key] = c;
    lock.unlock();

    // 3. 执行实际的函数（在锁外执行，不阻塞其他 key）
    auto [val, err] = fn();

    // 4. 存储结果，通知所有等待者
    {
        std::lock_guard<std::mutex> call_lock(c->mu);
        c->val = val;
        c->err = err;
        c->done = true;
    }
    c->cv.notify_all();

    // 5. 清理：从 calls_ 中移除
    {
        std::lock_guard<std::mutex> guard(mu_);
        calls_.erase(key);
    }

    return {val, err};
}

}  // namespace singleflight
}  // namespace geecache
```

**逐步解析：**

**步骤 1：检查是否有正在执行的调用**

```cpp
auto it = calls_.find(key);
if (it != calls_.end()) {
    auto c = it->second;
    lock.unlock();                                          // 先释放全局锁
    std::unique_lock<std::mutex> call_lock(c->mu);
    c->cv.wait(call_lock, [&c] { return c->done; });       // 等待完成
    return {c->val, c->err};                                // 共享结果
}
```

如果已经有人在为这个 key 执行回源，当前线程就**释放全局锁**，然后在 `Call` 的条件变量上等待。一旦第一个请求完成并调用 `notify_all()`，所有等待的线程都会被唤醒，获得相同的结果。

**步骤 2-3：第一个请求执行回源**

```cpp
auto c = std::make_shared<Call>();
calls_[key] = c;
lock.unlock();
auto [val, err] = fn();       // 在锁外执行！
```

关键：`fn()` 的执行在全局锁外面。这意味着：
- 其他 key 的请求不会被阻塞
- 只有相同 key 的请求会等待

**步骤 4：通知等待者**

```cpp
{
    std::lock_guard<std::mutex> call_lock(c->mu);
    c->val = val;
    c->err = err;
    c->done = true;
}
c->cv.notify_all();
```

将结果写入 `Call`，设置 `done = true`，然后 `notify_all()` 唤醒所有等待的线程。

**步骤 5：清理**

```cpp
{
    std::lock_guard<std::mutex> guard(mu_);
    calls_.erase(key);
}
```

移除已完成的 `Call`，释放内存。之后如果有新的请求来查询同一个 key，会重新执行一次（因为结果可能已经过期）。

### 2.3 集成到 Group::Load

在 Day 5 的 `Group::Load` 方法中集成 singleflight。只需要用 `loader_.Do()` 包裹加载逻辑：

**修改 `geecache.h`，新增成员：**

```cpp
#include "singleflight/singleflight.h"

class Group {
    // ... 之前的成员 ...
    singleflight::Group loader_;   // [新增]
};
```

**修改 `geecache.cpp` 中的 Load 方法：**

```cpp
std::pair<ByteView, std::string> Group::Load(const std::string& key) {
    // 用 singleflight 包裹，确保并发请求只执行一次
    auto [result, err] = loader_.Do(key, [this, &key]()
        -> std::pair<std::any, std::string> {
        // 原来的加载逻辑
        if (peers_) {
            auto [peer, ok] = peers_->PickPeer(key);
            if (ok) {
                auto [value, peer_err] = GetFromPeer(peer, key);
                if (peer_err.empty()) {
                    return {std::any(value), ""};
                }
                std::cerr << "[GeeCache] Failed to get from peer: "
                          << peer_err << std::endl;
            }
        }
        auto [value, local_err] = GetLocally(key);
        if (!local_err.empty()) {
            return {std::any{}, local_err};
        }
        return {std::any(value), ""};
    });

    if (!err.empty()) {
        return {ByteView{}, err};
    }

    return {std::any_cast<ByteView>(result), ""};
}
```

**改动非常小**——只是把原来的加载逻辑包裹在 `loader_.Do(key, [...]() { ... })` 里面。singleflight 会自动处理去重。

## 3. 测试验证

创建文件 `tests/singleflight_test.cpp`：

```cpp
#include <gtest/gtest.h>

#include <any>
#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "singleflight/singleflight.h"
```

### 测试 1：基本功能

```cpp
TEST(SingleflightTest, Do) {
    geecache::singleflight::Group g;

    auto [val, err] = g.Do("key", []() -> std::pair<std::any, std::string> {
        return {std::any(std::string("bar")), ""};
    });

    ASSERT_TRUE(err.empty());
    EXPECT_EQ(std::any_cast<std::string>(val), "bar");
}
```

### 测试 2：并发去重（核心测试）

```cpp
TEST(SingleflightTest, ConcurrentDo) {
    geecache::singleflight::Group g;
    std::atomic<int> call_count{0};

    constexpr int num_threads = 10;
    std::vector<std::thread> threads;
    std::vector<std::string> results(num_threads);
    std::vector<std::string> errors(num_threads);

    // 启动 10 个线程，同时请求同一个 key
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            auto [val, err] = g.Do("key",
                [&call_count]() -> std::pair<std::any, std::string> {
                    call_count++;
                    // 模拟耗时操作（如查数据库）
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    return {std::any(std::string("result")), ""};
                });
            errors[i] = err;
            if (err.empty()) {
                results[i] = std::any_cast<std::string>(val);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // 关键断言：虽然有 10 个线程并发请求，但函数只被执行了 1 次
    EXPECT_EQ(call_count.load(), 1);

    // 所有线程都应该得到相同的结果
    for (int i = 0; i < num_threads; ++i) {
        EXPECT_TRUE(errors[i].empty()) << "Thread " << i << " got error: " << errors[i];
        EXPECT_EQ(results[i], "result") << "Thread " << i << " got wrong result";
    }
}
```

**这个测试验证了 singleflight 的核心价值：**
- 10 个线程同时调用 `Do("key", fn)`
- `fn` 只被执行了 **1 次**（`call_count == 1`）
- 所有 10 个线程都得到了相同的结果 `"result"`

## 4. 小结

Day 6 我们实现了 singleflight 机制，解决了缓存击穿问题。核心要点：

- **缓存击穿**：大量并发请求同一个未缓存的 key，导致回源操作重复执行，压垮数据源
- **singleflight 原理**：对于同一个 key，只让第一个请求执行，其他请求等待并共享结果
- **关键技术**：
  - `std::mutex` + `std::condition_variable` 实现等待/通知
  - `std::any` 存储任意类型的结果
  - 全局锁外执行 `fn()`，不阻塞其他 key 的请求
- **集成方式**：只需在 `Group::Load` 中用 `loader_.Do()` 包裹原有逻辑，改动极小

**改造前后对比：**

| | 无 singleflight | 有 singleflight |
|---|---|---|
| 10 个并发请求同一个 key | 10 次数据库查询 | **1 次**数据库查询 |
| 数据库压力 | 高 | 低 |
| 响应时间 | 10 次独立查询 | 1 次查询，其他等待共享 |

Day 7 预告：目前节点间的 HTTP 通信使用的是简单的字节传输。Day 7 我们将引入 **Protobuf**（Protocol Buffers）来规范化通信格式，使节点间的数据交换更高效、更安全。
