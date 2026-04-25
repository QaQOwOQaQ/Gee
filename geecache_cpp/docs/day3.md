# Day 3: HTTP 服务端

## 1. 背景知识

### 1.1 为什么需要 HTTP 服务端？

在 Day 2 中，我们实现了一个功能完整的单机缓存系统。但在实际生产环境中，单机的内存是有限的，我们需要将缓存分布到多台机器上。

要实现分布式缓存，节点之间需要通信。HTTP 是最简单、最通用的通信方式：

```
客户端 A                    节点 1 (HTTP Server)
   |                            |
   |-- GET /_geecache/scores/Tom -->|
   |                            |-- 查找本地缓存
   |<-- 200 OK  {"value":"630"} --|
```

Day 3 的目标：为 GeeCache 提供 HTTP 服务端能力，让其他节点可以通过 HTTP 协议访问缓存数据。

### 1.2 cpp-httplib 简介

我们使用 [cpp-httplib](https://github.com/yhirose/cpp-httplib) 作为 HTTP 库。它是一个**单头文件**的 C++ HTTP 库，使用非常简单：

```cpp
// 创建服务端
httplib::Server server;
server.Get("/hello", [](const httplib::Request& req, httplib::Response& res) {
    res.set_content("Hello World!", "text/plain");
});
server.listen("0.0.0.0", 8080);

// 创建客户端
httplib::Client client("localhost", 8080);
auto result = client.Get("/hello");
// result->body == "Hello World!"
```

项目中已将 `httplib.h` 放在 `third_party/cpp-httplib/` 目录下。

### 1.3 URL 设计

GeeCache 的 HTTP 接口 URL 格式：

```
/_geecache/<groupname>/<key>
```

例如：
- `/_geecache/scores/Tom` — 从 "scores" 缓存组获取 key 为 "Tom" 的值
- `/_geecache/users/alice` — 从 "users" 缓存组获取 key 为 "alice" 的值

`/_geecache/` 是默认的基础路径（base path），用于区分缓存请求和其他普通 HTTP 请求。

### 1.4 Day 3 的文件结构

```
geecache_cpp/
├── third_party/
│   └── cpp-httplib/
│       └── httplib.h         # 第三方 HTTP 库（已提供）
└── src/
    ├── http.h                # [新增] HTTPPool 定义
    └── http.cpp              # [新增] HTTPPool 实现
```

> **注意**：Day 3 我们只实现 HTTPPool 的**服务端**功能（接收请求、返回缓存数据）。客户端功能（向远程节点发请求）将在 Day 5（分布式节点）中实现。

## 2. 代码实现

### 2.1 HTTPPool 类定义

创建文件 `src/http.h`：

```cpp
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "httplib.h"

namespace geecache {

inline constexpr const char* kDefaultBasePath = "/_geecache/";

class HTTPPool {
public:
    explicit HTTPPool(const std::string& self);
    ~HTTPPool();

    // Start starts the HTTP server in a background thread.
    void Start();

    // Stop gracefully shuts down the HTTP server.
    void Stop();

private:
    void HandleRequest(const httplib::Request& req, httplib::Response& res);

    // Parse "http://host:port" into host and port.
    static void ParseURL(const std::string& url, std::string& host, int& port);

    std::string self_;           // 本节点地址，如 "http://localhost:8001"
    std::string base_path_;      // 基础路径，默认 "/_geecache/"
    httplib::Server server_;     // HTTP 服务端
    std::thread server_thread_;  // 服务端运行线程
};

}  // namespace geecache
```

**Day 3 简化版说明：**

这里展示的是 Day 3 的简化版，只包含服务端功能。在后续的 Day 5 中，`HTTPPool` 会扩展为同时实现 `PeerPicker` 接口（加入一致性哈希、`HttpGetter` 客户端等），成为完整的分布式节点。

**关键成员：**

| 成员 | 类型 | 用途 |
|------|------|------|
| `self_` | `string` | 本节点的地址（如 `"http://localhost:8001"`） |
| `base_path_` | `string` | URL 基础路径，默认 `"/_geecache/"` |
| `server_` | `httplib::Server` | HTTP 服务端实例 |
| `server_thread_` | `std::thread` | 服务端后台运行线程 |

### 2.2 URL 解析工具

在 `src/http.cpp` 中实现：

```cpp
#include "http.h"

#include <iostream>
#include <sstream>
#include <stdexcept>

#include "geecache.h"

namespace geecache {

void HTTPPool::ParseURL(const std::string& url, std::string& host, int& port) {
    // Parse "http://host:port" or "http://host"
    std::string stripped = url;
    auto pos = stripped.find("://");
    if (pos != std::string::npos) {
        stripped = stripped.substr(pos + 3);  // 去掉 "http://"
    }
    // Remove trailing slash
    if (!stripped.empty() && stripped.back() == '/') {
        stripped.pop_back();
    }
    auto colon = stripped.find(':');
    if (colon != std::string::npos) {
        host = stripped.substr(0, colon);
        port = std::stoi(stripped.substr(colon + 1));
    } else {
        host = stripped;
        port = 80;  // 默认端口
    }
}
```

示例：
- `"http://localhost:8001"` → host=`"localhost"`, port=`8001`
- `"http://10.0.0.1"` → host=`"10.0.0.1"`, port=`80`

### 2.3 构造函数与析构函数

```cpp
HTTPPool::HTTPPool(const std::string& self)
    : self_(self), base_path_(kDefaultBasePath) {}

HTTPPool::~HTTPPool() {
    Stop();  // 析构时自动停止服务端
}
```

### 2.4 HandleRequest - 处理 HTTP 请求（核心）

```cpp
void HTTPPool::HandleRequest(const httplib::Request& req, httplib::Response& res) {
    // Expected path: /_geecache/<groupname>/<key>
    std::string path = req.path;

    // 1. 检查 URL 前缀
    if (path.find(base_path_) != 0) {
        res.status = 400;
        res.set_content("bad request", "text/plain");
        return;
    }

    // 2. 解析 groupname 和 key
    std::string remainder = path.substr(base_path_.size());
    auto slash = remainder.find('/');
    if (slash == std::string::npos) {
        res.status = 400;
        res.set_content("bad request: missing key", "text/plain");
        return;
    }

    std::string group_name = remainder.substr(0, slash);
    std::string key = remainder.substr(slash + 1);

    // 3. 查找 Group
    auto group = Group::GetGroup(group_name);
    if (!group) {
        res.status = 404;
        res.set_content("no such group: " + group_name, "text/plain");
        return;
    }

    // 4. 获取缓存值
    auto [view, err] = group->Get(key);
    if (!err.empty()) {
        res.status = 500;
        res.set_content(err, "text/plain");
        return;
    }

    // 5. 返回结果（Day 3 先用纯文本，Day 7 会改为 protobuf）
    res.set_content(view.ByteSlice(), "application/octet-stream");
}
```

**请求处理流程图：**

```
请求: GET /_geecache/scores/Tom
         |
         v
    检查 URL 前缀是否为 /_geecache/
         |
         v
    解析出 group_name="scores", key="Tom"
         |
         v
    Group::GetGroup("scores") 查找 Group
         |
    [未找到] -> 404 Not Found
         |
    [找到] -> group->Get("Tom")
         |
    [出错] -> 500 Internal Server Error
         |
    [成功] -> 200 OK, body = 缓存值
```

### 2.5 Start 和 Stop - 服务端启停

```cpp
void HTTPPool::Start() {
    // 注册路由：匹配 /_geecache/ 开头的所有请求
    std::string pattern = std::string(base_path_) + "(.+)";
    server_.Get(pattern, [this](const httplib::Request& req, httplib::Response& res) {
        HandleRequest(req, res);
    });

    // 解析本节点地址
    std::string host;
    int port;
    ParseURL(self_, host, port);

    std::cerr << "[HTTPPool] serving at " << self_ << std::endl;

    // 在后台线程中启动 HTTP 服务
    server_thread_ = std::thread([this, host, port]() {
        server_.listen(host.c_str(), port);
    });

    // 等待服务端就绪
    while (!server_.is_running()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

void HTTPPool::Stop() {
    if (server_.is_running()) {
        server_.stop();
    }
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
}
```

**为什么在后台线程中启动？**

`server_.listen()` 是阻塞调用，会一直监听直到服务被停止。如果不放在后台线程，主线程就会被阻塞住。

**启动流程：**
1. 注册路由：把匹配 `/_geecache/(.+)` 的 GET 请求交给 `HandleRequest` 处理
2. 解析本节点地址，得到 host 和 port
3. 在后台线程中调用 `server_.listen()` 开始监听
4. 主线程循环等待 `server_.is_running()` 返回 true，确保服务就绪后再继续

**停止流程：**
1. 调用 `server_.stop()` 通知服务端停止
2. 调用 `server_thread_.join()` 等待后台线程退出

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

### 测试：启动 HTTP 服务并通过 HTTP 客户端访问

```cpp
TEST_F(HTTPTest, PeerCommunication) {
    // 模拟数据源
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

    // 创建 HTTPPool 并启动
    auto pool = std::make_shared<geecache::HTTPPool>("http://localhost:9999");
    pool->Start();

    // 等待服务就绪
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 通过 HTTP 客户端直接请求服务端
    httplib::Client client("localhost", 9999);

    // 请求存在的 key
    auto result = client.Get("/_geecache/scores/Sam");
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->status, 200);
    EXPECT_EQ(result->body, "567");

    // 请求不存在的 group -> 404
    auto result2 = client.Get("/_geecache/unknown_group/key");
    ASSERT_NE(result2, nullptr);
    EXPECT_EQ(result2->status, 404);

    pool->Stop();
}
```

> **注意**：上面的测试代码展示的是 Day 3 阶段的实现，此时响应体为纯文本字节。
> Day 7 引入 Protobuf 后，响应体变为序列化的二进制数据，`EXPECT_EQ(result->body, "567")` 会失败。
> 实际项目中的 `tests/http_test.cpp` 已更新为：
> ```cpp
> geecachepb::Response resp;
> ASSERT_TRUE(resp.ParseFromString(result->body));
> EXPECT_EQ(resp.value(), "567");
> ```

**测试验证了：**
1. HTTP 服务端能正常启动和停止
2. 通过 HTTP 请求 `/_geecache/scores/Sam` 能正确返回缓存值 `"567"`
3. 请求不存在的 group 返回 404

## 4. 小结

Day 3 我们为 GeeCache 添加了 HTTP 服务端能力：

- **cpp-httplib**：使用单头文件的 HTTP 库，简化了 HTTP 服务端和客户端的实现
- **URL 设计**：`/_geecache/<groupname>/<key>` 格式，清晰区分缓存请求
- **HandleRequest**：解析 URL → 查找 Group → 获取缓存值 → 返回响应
- **后台线程启动**：`server_.listen()` 在后台线程中运行，避免阻塞主线程
- **优雅停止**：析构函数中自动调用 `Stop()`，确保资源释放

目前的架构：

```
         HTTP Client
              |
              v
    HTTPPool (HTTP Server)
              |
              v
    Group::GetGroup(name)
              |
              v
         Group::Get(key)
              |
         [命中] -> 返回缓存
         [未命中] -> Getter 回源 -> 写缓存 -> 返回
```

Day 4 预告：我们将实现**一致性哈希**算法，这是分布式缓存的关键——它决定了每个 key 应该被分配到哪个节点上。
