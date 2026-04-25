# Day 5: 分布式节点

## 1. 背景知识

### 1.1 从单机到分布式

在前四天中，我们实现了：
- Day 1: LRU 缓存淘汰
- Day 2: 单机并发缓存（Group + ConcurrentCache）
- Day 3: HTTP 服务端（接收请求）
- Day 4: 一致性哈希（决定 key 归属哪个节点）

现在要把它们组合起来，实现**真正的分布式缓存**。核心流程：

```
客户端请求 Group::Get("Tom")
         |
    [本地缓存命中] --> 直接返回
         |
    [未命中] --> 一致性哈希决定 key 归属哪个节点
         |
    [是远程节点] --> HTTP 请求远程节点获取数据
         |
    [是本节点]   --> 调用 Getter 从数据源加载
```

### 1.2 需要哪些新组件？

要实现上述流程，我们需要定义两个接口：

| 接口 | 职责 |
|------|------|
| `PeerGetter` | 从**某一个**远程节点获取数据（HTTP 客户端） |
| `PeerPicker` | 根据 key **选择**应该访问哪个远程节点（一致性哈希） |

然后让 `HTTPPool` 同时实现这两个角色：
- 作为 `PeerPicker`：使用一致性哈希选择节点
- 内部的 `HttpGetter` 作为 `PeerGetter`：通过 HTTP 从远程节点获取数据

### 1.3 文件结构

```
geecache_cpp/
└── src/
    ├── peers.h              # [新增] PeerGetter / PeerPicker 接口
    ├── http.h               # [修改] HTTPPool 扩展为完整版
    ├── http.cpp             # [修改] 加入一致性哈希 + HttpGetter
    ├── geecache.h           # [修改] Group 加入 RegisterPeers / Load
    └── geecache.cpp         # [修改] Group::Get 支持远程获取
```

## 2. 代码实现

### 2.1 PeerGetter 和 PeerPicker 接口

创建文件 `src/peers.h`：

```cpp
#pragma once

#include <memory>
#include <string>
#include <utility>

#include "geecachepb.pb.h"

namespace geecache {

// PeerGetter is the interface that must be implemented by a peer.
class PeerGetter {
public:
    virtual ~PeerGetter() = default;
    // Get fetches a value from a remote peer.
    // Returns empty string on success, error message on failure.
    virtual std::string Get(const geecachepb::Request& req,
                            geecachepb::Response* resp) = 0;
};

// PeerPicker is the interface that must be implemented to locate
// the peer that owns a specific key.
class PeerPicker {
public:
    virtual ~PeerPicker() = default;
    virtual std::pair<std::shared_ptr<PeerGetter>, bool> PickPeer(
        const std::string& key) = 0;
};

}  // namespace geecache
```

**PeerGetter 接口：**

```cpp
virtual std::string Get(const geecachepb::Request& req,
                        geecachepb::Response* resp) = 0;
```

- 入参：`geecachepb::Request`（包含 group 名和 key）
- 出参：`geecachepb::Response*`（通过指针写入结果）
- 返回值：空字符串表示成功，非空表示错误信息

> **注意**：这里使用了 protobuf 的 `Request` 和 `Response` 类型。Day 5 先用它们作为接口参数，Day 7 会详细讲解 protobuf 的定义和序列化。

**PeerPicker 接口：**

```cpp
virtual std::pair<std::shared_ptr<PeerGetter>, bool> PickPeer(
    const std::string& key) = 0;
```

- 入参：要查找的 key
- 返回值：`{PeerGetter指针, 是否找到远程节点}`
  - 如果 key 归属远程节点，返回 `{对应的PeerGetter, true}`
  - 如果 key 归属本节点（或没有节点），返回 `{nullptr, false}`

### 2.2 HTTPPool 完整版（集成一致性哈希 + HttpGetter）

Day 3 我们实现了 HTTPPool 的服务端功能。现在扩展它，加入：
- 实现 `PeerPicker` 接口（通过一致性哈希选择节点）
- 内部类 `HttpGetter` 实现 `PeerGetter` 接口（HTTP 客户端）
- `Set` 方法：设置所有节点列表

修改文件 `src/http.h`：

```cpp
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "httplib.h"
#include "consistenthash/consistenthash.h"
#include "peers.h"

namespace geecache {

inline constexpr const char* kDefaultBasePath = "/_geecache/";
inline constexpr int kDefaultReplicas = 50;

class HTTPPool : public PeerPicker {     // <-- 实现 PeerPicker 接口
public:
    explicit HTTPPool(const std::string& self);
    ~HTTPPool();

    // Set updates the pool's list of peers.
    void Set(const std::vector<std::string>& peers);

    // PickPeer picks a peer for the given key.
    std::pair<std::shared_ptr<PeerGetter>, bool> PickPeer(
        const std::string& key) override;

    // Start starts the HTTP server in a background thread.
    void Start();

    // Stop gracefully shuts down the HTTP server.
    void Stop();

private:
    // HttpGetter implements PeerGetter via HTTP.
    class HttpGetter : public PeerGetter {
    public:
        explicit HttpGetter(const std::string& base_url);
        std::string Get(const geecachepb::Request& req,
                        geecachepb::Response* resp) override;

    private:
        std::string host_;
        int port_;
        std::string path_prefix_;  // e.g., "/_geecache/"
    };

    void HandleRequest(const httplib::Request& req, httplib::Response& res);
    static void ParseURL(const std::string& url, std::string& host, int& port);
    static std::string UrlEncode(const std::string& value);

    std::string self_;           // 本节点地址
    std::string base_path_;      // 基础路径
    std::mutex mu_;              // 保护 peers_ 和 http_getters_
    std::unique_ptr<consistenthash::Map> peers_;   // 一致性哈希环
    std::unordered_map<std::string, std::shared_ptr<HttpGetter>> http_getters_;  // 节点 -> HttpGetter
    httplib::Server server_;
    std::thread server_thread_;
};

}  // namespace geecache
```

**相比 Day 3 新增的成员：**

| 新增成员 | 类型 | 用途 |
|----------|------|------|
| `mu_` | `std::mutex` | 保护并发访问 |
| `peers_` | `unique_ptr<consistenthash::Map>` | 一致性哈希环，用于路由 key |
| `http_getters_` | `unordered_map<string, shared_ptr<HttpGetter>>` | 每个节点对应一个 HTTP 客户端 |

#### Set 方法 — 设置节点列表

```cpp
void HTTPPool::Set(const std::vector<std::string>& peers) {
    std::lock_guard<std::mutex> lock(mu_);
    // 创建一致性哈希环，每个节点 50 个虚拟节点
    peers_ = std::make_unique<consistenthash::Map>(kDefaultReplicas);
    peers_->Add(peers);
    // 为每个节点创建对应的 HttpGetter
    http_getters_.clear();
    for (const auto& peer : peers) {
        http_getters_[peer] = std::make_shared<HttpGetter>(peer + base_path_);
    }
}
```

例如调用 `pool->Set({"http://localhost:8001", "http://localhost:8002", "http://localhost:8003"})`：

1. 创建一致性哈希环，将三个节点地址加入
2. 为每个节点创建一个 `HttpGetter`，其 URL 为 `http://localhost:800x/_geecache/`

#### PickPeer 方法 — 选择节点

```cpp
std::pair<std::shared_ptr<PeerGetter>, bool> HTTPPool::PickPeer(
    const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!peers_ || peers_->IsEmpty()) {
        return {nullptr, false};
    }
    std::string peer = peers_->Get(key);       // 一致性哈希查找
    if (!peer.empty() && peer != self_) {       // 不是本节点
        auto it = http_getters_.find(peer);
        if (it != http_getters_.end()) {
            std::cerr << "Pick peer " << peer << std::endl;
            return {it->second, true};          // 返回对应的 HttpGetter
        }
    }
    return {nullptr, false};                    // 归属本节点，返回 false
}
```

**关键逻辑：`peer != self_`**

如果一致性哈希算出的节点就是本节点（`self_`），不应该再走 HTTP 请求自己，而是应该返回 `false`，让 `Group` 走本地数据源加载。

#### HttpGetter — HTTP 客户端（内部类）

```cpp
HTTPPool::HttpGetter::HttpGetter(const std::string& base_url) {
    // 解析 "http://host:port/path/"
    std::string stripped = base_url;
    auto pos = stripped.find("://");
    if (pos != std::string::npos) {
        stripped = stripped.substr(pos + 3);
    }
    auto slash = stripped.find('/');
    std::string host_port;
    if (slash != std::string::npos) {
        host_port = stripped.substr(0, slash);
        path_prefix_ = stripped.substr(slash);  // 如 "/_geecache/"
    } else {
        host_port = stripped;
        path_prefix_ = "/";
    }
    auto colon = host_port.find(':');
    if (colon != std::string::npos) {
        host_ = host_port.substr(0, colon);
        port_ = std::stoi(host_port.substr(colon + 1));
    } else {
        host_ = host_port;
        port_ = 80;
    }
}
```

构造函数解析 URL，例如 `"http://localhost:8001/_geecache/"` 会被解析为：
- `host_` = `"localhost"`
- `port_` = `8001`
- `path_prefix_` = `"/_geecache/"`

```cpp
std::string HTTPPool::HttpGetter::Get(const geecachepb::Request& req,
                                       geecachepb::Response* resp) {
    // 构建请求 URL: /_geecache/<group>/<key>
    std::string url = path_prefix_ + UrlEncode(req.group()) + "/" + UrlEncode(req.key());

    // 发送 HTTP GET 请求
    httplib::Client client(host_.c_str(), port_);
    client.set_connection_timeout(5);
    client.set_read_timeout(5);

    auto result = client.Get(url.c_str());
    if (!result) {
        return "http request failed: " + httplib::to_string(result.error());
    }
    if (result->status != 200) {
        return "server returned: " + std::to_string(result->status);
    }

    // 反序列化 protobuf 响应
    if (!resp->ParseFromString(result->body)) {
        return "failed to parse response body";
    }

    return "";  // 成功
}
```

**HttpGetter::Get 流程：**
1. 拼接 URL：`/_geecache/` + URL编码的 group 名 + `/` + URL编码的 key
2. 创建 HTTP 客户端，设置 5 秒超时
3. 发送 GET 请求
4. 检查响应状态码
5. 将响应 body 反序列化为 protobuf `Response`

#### UrlEncode — URL 编码工具

```cpp
std::string HTTPPool::UrlEncode(const std::string& value) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;
    for (char c : value) {
        if (isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_' ||
            c == '.' || c == '~') {
            escaped << c;
        } else {
            escaped << '%' << std::setw(2)
                    << static_cast<int>(static_cast<unsigned char>(c));
        }
    }
    return escaped.str();
}
```

对 group 名和 key 进行 URL 编码，防止特殊字符（如空格、`/`、中文等）破坏 URL 结构。

### 2.3 Group 扩展 — 支持分布式获取

在 Day 2 中，`Group::Get` 的流程是：缓存未命中 → 调用 Getter 从本地数据源加载。现在我们要扩展它，加入远程节点获取的逻辑。

修改 `src/geecache.h`，给 Group 新增以下成员和方法：

```cpp
class Group {
public:
    // ... Day 2 已有的方法 ...

    // RegisterPeers registers a PeerPicker for this group.
    void RegisterPeers(std::shared_ptr<PeerPicker> peers);

private:
    // ... Day 2 已有的成员 ...

    // [新增] 从远程节点加载
    std::pair<ByteView, std::string> Load(const std::string& key);
    std::pair<ByteView, std::string> GetFromPeer(
        std::shared_ptr<PeerGetter> peer, const std::string& key);

    // [新增] 节点选择器
    std::shared_ptr<PeerPicker> peers_;
};
```

修改 `src/geecache.cpp`，更新 `Get` 方法并新增 `Load`、`GetFromPeer`、`RegisterPeers`：

#### RegisterPeers — 注册节点选择器

```cpp
void Group::RegisterPeers(std::shared_ptr<PeerPicker> peers) {
    if (peers_) {
        throw std::runtime_error("RegisterPeers called more than once");
    }
    peers_ = std::move(peers);
}
```

每个 Group 只能注册一次 PeerPicker，重复注册会抛异常。

#### Get 方法（修改）

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

    // 2. 缓存未命中，调用 Load（Day 2 是直接调用 GetLocally）
    return Load(key);
}
```

唯一的改动：缓存未命中时从直接调 `GetLocally` 改为调 `Load`。

#### Load — 加载逻辑（核心变更）

```cpp
std::pair<ByteView, std::string> Group::Load(const std::string& key) {
    // 1. 如果有远程节点，先尝试从远程获取
    if (peers_) {
        auto [peer, ok] = peers_->PickPeer(key);
        if (ok) {
            auto [value, peer_err] = GetFromPeer(peer, key);
            if (peer_err.empty()) {
                return {value, ""};   // 远程获取成功
            }
            std::cerr << "[GeeCache] Failed to get from peer: "
                      << peer_err << std::endl;
            // 远程失败，降级到本地加载
        }
    }

    // 2. 归属本节点或远程失败，从本地数据源加载
    return GetLocally(key);
}
```

**Load 的流程图：**

```
Load(key)
  |
  +--[peers_ 存在?]
  |     |
  |     +--[PickPeer(key) 找到远程节点]
  |     |     |
  |     |     +--[GetFromPeer 成功] --> 返回远程结果
  |     |     |
  |     |     +--[GetFromPeer 失败] --> 降级到本地
  |     |
  |     +--[PickPeer 返回 false（归属本节点）]
  |
  +--[peers_ 不存在（单机模式）]
  |
  v
 GetLocally(key) --> Getter 回源 --> 写缓存 --> 返回
```

#### GetFromPeer — 从远程节点获取

```cpp
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
```

流程：
1. 构建 protobuf `Request`（设置 group 名和 key）
2. 调用 `peer->Get()` 发送 HTTP 请求
3. 从 protobuf `Response` 中取出 value，包装为 `ByteView` 返回

## 3. 测试验证

创建文件 `tests/http_test.cpp`：

```cpp
#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>
#include <unordered_map>

#include "geecache.h"
#include "http.h"

class HTTPTest : public ::testing::Test {
protected:
    void SetUp() override {
        geecache::Group::DestroyAllGroups();
    }
    void TearDown() override {
        geecache::Group::DestroyAllGroups();
    }
};
```

### 测试 1：单节点 HTTP 通信

```cpp
TEST_F(HTTPTest, PeerCommunication) {
    std::unordered_map<std::string, std::string> db = {
        {"Tom", "630"},
        {"Jack", "589"},
        {"Sam", "567"},
    };

    auto getter = std::make_shared<geecache::GetterFunc>(
        [&db](const std::string& key) -> std::pair<std::string, std::string> {
            auto it = db.find(key);
            if (it != db.end()) {
                return {it->second, ""};
            }
            return {"", "key not found: " + key};
        });

    auto group = geecache::Group::NewGroup("scores", 2 << 10, getter);

    // 创建 HTTPPool（单节点，self-serving）
    auto pool = std::make_shared<geecache::HTTPPool>("http://localhost:9999");
    pool->Set({"http://localhost:9999"});
    group->RegisterPeers(pool);
    pool->Start();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 通过 Group 获取（因为只有一个节点 = self，走本地加载）
    auto [view1, err1] = group->Get("Tom");
    ASSERT_TRUE(err1.empty()) << err1;
    EXPECT_EQ(view1.String(), "630");

    // 直接通过 HTTP 客户端请求服务端
    httplib::Client client("localhost", 9999);
    auto result = client.Get("/_geecache/scores/Sam");
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->status, 200);

    // 解析 protobuf 响应
    geecachepb::Response resp;
    ASSERT_TRUE(resp.ParseFromString(result->body));
    EXPECT_EQ(resp.value(), "567");

    // 请求不存在的 group -> 404
    auto result2 = client.Get("/_geecache/unknown_group/key");
    ASSERT_NE(result2, nullptr);
    EXPECT_EQ(result2->status, 404);

    pool->Stop();
}
```

### 测试 2：多节点分布式通信

```cpp
TEST_F(HTTPTest, MultiNodePeerCommunication) {
    std::unordered_map<std::string, std::string> db = {
        {"Tom", "630"},
        {"Jack", "589"},
        {"Sam", "567"},
    };

    auto getter = std::make_shared<geecache::GetterFunc>(
        [&db](const std::string& key) -> std::pair<std::string, std::string> {
            auto it = db.find(key);
            if (it != db.end()) {
                return {it->second, ""};
            }
            return {"", "key not found: " + key};
        });

    auto group = geecache::Group::NewGroup("scores", 2 << 10, getter);

    // 创建两个 HTTP 节点
    auto pool1 = std::make_shared<geecache::HTTPPool>("http://localhost:9001");
    auto pool2 = std::make_shared<geecache::HTTPPool>("http://localhost:9002");

    std::vector<std::string> peers = {
        "http://localhost:9001",
        "http://localhost:9002"
    };

    pool1->Set(peers);
    pool2->Set(peers);

    // 注册 pool1 作为 Group 的 PeerPicker
    group->RegisterPeers(pool1);

    pool1->Start();
    pool2->Start();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 所有 key 都应该可以获取到
    for (const auto& [key, expected] : db) {
        auto [view, err] = group->Get(key);
        ASSERT_TRUE(err.empty()) << "Failed to get " << key << ": " << err;
        EXPECT_EQ(view.String(), expected);
    }

    pool1->Stop();
    pool2->Stop();
}
```

**这个测试验证了分布式的关键场景：**
- 两个节点都知道完整的节点列表
- 当 `pool1` 调用 `PickPeer(key)` 时，一致性哈希可能返回 `pool2`，此时 `pool1` 会通过 HTTP 向 `pool2` 发请求
- `pool2` 收到请求后从 Getter 加载数据并返回

## 4. 小结

Day 5 我们实现了分布式缓存的核心——将前四天的所有组件组合在一起：

**新增组件：**

| 组件 | 职责 |
|------|------|
| `PeerGetter` 接口 | 定义从远程节点获取数据的方法 |
| `PeerPicker` 接口 | 定义根据 key 选择节点的方法 |
| `HTTPPool`（完整版） | 同时扮演 HTTP 服务端 + PeerPicker + PeerGetter |
| `Group::Load` | 分布式加载逻辑（远程优先，本地降级） |

**完整的数据流：**

```
Group::Get(key)
  |
  [缓存命中] --> 返回
  |
  [未命中] --> Load(key)
                |
                +-- PickPeer(key)  [一致性哈希]
                |      |
                |   [远程节点] --> HttpGetter::Get()
                |      |              |
                |      |         HTTP GET /_geecache/<group>/<key>
                |      |              |
                |      |         远程节点的 HandleRequest()
                |      |              |
                |      |         远程 Group::Get(key) --> 返回 protobuf
                |      |
                |   [本节点]
                |
                +-- GetLocally(key) --> Getter 回源 --> 写缓存
```

Day 6 预告：目前的系统在高并发场景下有一个问题——如果大量请求同时查询同一个不存在的 key，每个请求都会触发一次 Getter 回源，给数据源造成巨大压力。这就是**缓存击穿**问题。Day 6 我们将用 `singleflight` 机制来解决它。
