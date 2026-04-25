# Day 7: 使用 Protobuf 通信

## 1. 背景知识

### 1.1 为什么需要 Protobuf？

在之前的实现中，节点间的 HTTP 通信存在一些问题：

| 问题 | 说明 |
|------|------|
| 没有统一的通信格式 | 请求靠 URL 路径传参，响应直接返回原始字节 |
| 缺乏类型安全 | 没有明确的"请求"和"响应"结构定义 |
| 扩展性差 | 如果未来要新增字段（如版本号、时间戳），改动量大 |

**Protocol Buffers（简称 Protobuf）** 是 Google 开发的一种数据序列化格式，可以解决这些问题：

- **高效**：二进制编码，比 JSON 小 3-10 倍，解析速度快 20-100 倍
- **类型安全**：通过 `.proto` 文件定义结构，自动生成 C++ 代码
- **向后兼容**：新增字段不会破坏旧版本的解析

### 1.2 Protobuf 工作流程

```
1. 编写 .proto 文件（定义消息结构）
        |
        v
2. protoc 编译器生成 C++ 代码（.pb.h + .pb.cc）
        |
        v
3. 在代码中使用生成的类进行序列化/反序列化
```

在我们的项目中，CMake 会自动调用 protoc 编译器，所以你只需要写 `.proto` 文件即可。

### 1.3 文件结构

```
geecache_cpp/
├── proto/
│   └── geecachepb.proto       # [新增] Protobuf 消息定义
├── CMakeLists.txt             # [修改] 添加 protobuf 依赖
├── src/
│   ├── CMakeLists.txt         # [修改] 编译 protobuf 生成的代码
│   ├── peers.h                # [修改] 接口参数改为 protobuf 类型
│   ├── http.cpp               # [修改] 序列化/反序列化
│   └── geecache.cpp           # [修改] GetFromPeer 使用 protobuf
└── build/
    ├── geecachepb.pb.h        # [自动生成] protobuf C++ 头文件
    └── geecachepb.pb.cc       # [自动生成] protobuf C++ 实现
```

## 2. 代码实现

### 2.1 定义 Protobuf 消息

创建文件 `proto/geecachepb.proto`：

```protobuf
syntax = "proto3";

package geecachepb;

message Request {
    string group = 1;
    string key   = 2;
}

message Response {
    bytes value = 1;
}

service GroupCache {
    rpc Get(Request) returns (Response);
}
```

**逐行解析：**

**`syntax = "proto3";`**
使用 proto3 语法（最新版本，更简洁）。

**`package geecachepb;`**
定义包名，生成的 C++ 类会放在 `geecachepb` 命名空间中。

**`message Request`**
请求消息，包含两个字段：
- `group`（字段编号 1）：缓存组名，如 `"scores"`
- `key`（字段编号 2）：要查询的 key，如 `"Tom"`

**`message Response`**
响应消息，包含一个字段：
- `value`（字段编号 1）：缓存的值，类型为 `bytes`（原始字节），对应 `ByteView` 的内容

**`service GroupCache`**
定义 RPC 服务接口。我们的项目**不使用 gRPC**，而是手动通过 HTTP 传输 protobuf 消息。这个 service 定义起到文档的作用，说明有一个 `Get` 方法，接收 `Request` 返回 `Response`。

**生成的 C++ 代码会提供：**

```cpp
// geecachepb::Request
class Request {
public:
    void set_group(const std::string& value);
    const std::string& group() const;
    void set_key(const std::string& value);
    const std::string& key() const;
    bool SerializeToString(std::string* output) const;
    bool ParseFromString(const std::string& data);
};

// geecachepb::Response
class Response {
public:
    void set_value(const std::string& value);
    const std::string& value() const;
    bool SerializeToString(std::string* output) const;
    bool ParseFromString(const std::string& data);
};
```

### 2.2 CMake 构建配置

**根目录 `CMakeLists.txt`：**

```cmake
cmake_minimum_required(VERSION 3.14)
project(geecache VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# --- Dependencies ---
find_package(Protobuf REQUIRED)    # 查找 protobuf 库
find_package(ZLIB REQUIRED)        # httplib 需要 zlib
find_package(Threads REQUIRED)     # 多线程支持

# cpp-httplib (header-only, vendored)
add_library(httplib INTERFACE)
target_include_directories(httplib INTERFACE ${CMAKE_SOURCE_DIR}/third_party/cpp-httplib)

# --- Library ---
add_subdirectory(src)

# --- Tests ---
enable_testing()
add_subdirectory(tests)
```

**`src/CMakeLists.txt`：**

```cmake
# 自动调用 protoc 编译 .proto 文件，生成 .pb.h 和 .pb.cc
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${CMAKE_SOURCE_DIR}/proto/geecachepb.proto)

add_library(geecache_lib STATIC
    byteview.cpp
    cache.cpp
    geecache.cpp
    http.cpp
    consistenthash/consistenthash.cpp
    singleflight/singleflight.cpp
    ${PROTO_SRCS}         # 包含生成的 .pb.cc
    ${PROTO_HDRS}         # 包含生成的 .pb.h
)

target_include_directories(geecache_lib
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_BINARY_DIR}   # 生成的 .pb.h 在 build 目录中
        ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(geecache_lib
    PUBLIC
        ${Protobuf_LIBRARIES}
        ZLIB::ZLIB
        Threads::Threads
        httplib
)
```

**关键点：**
- `protobuf_generate_cpp` — CMake 的 protobuf 宏，自动调用 `protoc` 编译 `.proto` 文件
- `${CMAKE_CURRENT_BINARY_DIR}` — 生成的 `.pb.h` 文件在这个目录中，需要加入 include 路径
- `${Protobuf_LIBRARIES}` — 链接 protobuf 运行时库

### 2.3 Protobuf 集成到 peers 接口

修改 `src/peers.h`，将接口参数从简单的字符串改为 protobuf 类型：

```cpp
#include "geecachepb.pb.h"    // 引入生成的 protobuf 头文件

class PeerGetter {
public:
    virtual ~PeerGetter() = default;
    // 参数改为 protobuf 类型
    virtual std::string Get(const geecachepb::Request& req,
                            geecachepb::Response* resp) = 0;
};
```

这样 `PeerGetter` 的调用方和实现方都使用 protobuf 消息，通信格式统一。

### 2.4 Protobuf 集成到 HTTP 服务端（HandleRequest）

修改 `src/http.cpp` 中的 `HandleRequest` 方法，将响应序列化为 protobuf：

```cpp
void HTTPPool::HandleRequest(const httplib::Request& req, httplib::Response& res) {
    // ... URL 解析、Group 查找、获取缓存值的代码不变 ...

    auto [view, err] = group->Get(key);
    if (!err.empty()) {
        res.status = 500;
        res.set_content(err, "text/plain");
        return;
    }

    // 使用 protobuf 序列化响应
    geecachepb::Response resp;
    resp.set_value(view.ByteSlice());       // 将缓存值放入 protobuf Response
    std::string body;
    if (!resp.SerializeToString(&body)) {   // 序列化为二进制字符串
        res.status = 500;
        res.set_content("failed to serialize response", "text/plain");
        return;
    }

    res.set_content(body, "application/octet-stream");  // 二进制格式返回
}
```

**改动点：** 原来直接返回 `view.ByteSlice()`，现在先用 protobuf 包装再序列化。

### 2.5 Protobuf 集成到 HTTP 客户端（HttpGetter::Get）

```cpp
std::string HTTPPool::HttpGetter::Get(const geecachepb::Request& req,
                                       geecachepb::Response* resp) {
    // 用 protobuf Request 的字段构建 URL
    std::string url = path_prefix_ + UrlEncode(req.group()) + "/" + UrlEncode(req.key());

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

    // 将 HTTP 响应体反序列化为 protobuf Response
    if (!resp->ParseFromString(result->body)) {
        return "failed to parse response body";
    }

    return "";
}
```

**客户端的 protobuf 使用：**
- 从 `req.group()` 和 `req.key()` 取字段构建 URL
- 将收到的 HTTP 响应体用 `resp->ParseFromString()` 反序列化

### 2.6 Protobuf 集成到 Group::GetFromPeer

```cpp
std::pair<ByteView, std::string> Group::GetFromPeer(
    std::shared_ptr<PeerGetter> peer, const std::string& key) {
    // 构建 protobuf Request
    geecachepb::Request req;
    req.set_group(name_);    // 设置 group 名
    req.set_key(key);        // 设置 key

    // 发送请求，获取 protobuf Response
    geecachepb::Response resp;
    std::string err = peer->Get(req, &resp);
    if (!err.empty()) {
        return {ByteView{}, err};
    }

    // 从 Response 中取出 value
    return {ByteView(resp.value()), ""};
}
```

**整个 protobuf 通信链路：**

```
Group::GetFromPeer
  |
  +-- 构建 geecachepb::Request {group="scores", key="Tom"}
  |
  +-- peer->Get(req, &resp)  [HttpGetter::Get]
        |
        +-- 用 req.group() 和 req.key() 拼 URL
        +-- HTTP GET /_geecache/scores/Tom
        +-- 收到响应 body（二进制 protobuf）
        +-- resp.ParseFromString(body) 反序列化
        |
  +-- 远程节点的 HandleRequest
        |
        +-- group->Get(key) 获取缓存值
        +-- geecachepb::Response resp; resp.set_value(...)
        +-- resp.SerializeToString(&body) 序列化
        +-- 返回 body
```

## 3. 构建与运行

### 3.1 安装依赖

```bash
# Ubuntu/Debian
sudo apt-get install protobuf-compiler libprotobuf-dev zlib1g-dev

# CentOS/RHEL
sudo yum install protobuf-compiler protobuf-devel zlib-devel
```

### 3.2 编译项目

```bash
cd geecache_cpp
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

### 3.3 运行测试

```bash
cd build
ctest --output-on-failure
```

所有 5 个测试套件都应该通过：
- `lru_test` — LRU 缓存测试
- `consistenthash_test` — 一致性哈希测试
- `singleflight_test` — singleflight 测试
- `geecache_test` — Group 缓存测试
- `http_test` — HTTP 通信测试

## 4. 项目总结

恭喜！经过 7 天的学习，你已经从零实现了一个完整的分布式缓存系统 **GeeCache**。

### 4.1 七天回顾

| Day | 模块 | 核心内容 |
|-----|------|----------|
| Day 1 | LRU 缓存淘汰 | `unordered_map` + `list`，O(1) 查找和淘汰 |
| Day 2 | 单机并发缓存 | `ByteView` 不可变值 + `mutex` 并发安全 + `Group` 回源机制 |
| Day 3 | HTTP 服务端 | cpp-httplib 提供 HTTP 服务，URL 路由到 Group |
| Day 4 | 一致性哈希 | 虚拟节点 + `std::map::lower_bound` 环形查找 |
| Day 5 | 分布式节点 | `PeerPicker`/`PeerGetter` 接口 + HTTPPool 集成 |
| Day 6 | 防止缓存击穿 | singleflight 去重，`condition_variable` 等待/通知 |
| Day 7 | Protobuf 通信 | `.proto` 定义 + 序列化/反序列化 + CMake 集成 |

### 4.2 完整架构图

```
                         ┌─────────────────────────────┐
                         │        客户端请求             │
                         └──────────────┬──────────────┘
                                        │
                                        v
                              ┌─────────────────┐
                              │   Group::Get()   │
                              └────────┬────────┘
                                       │
                          ┌────────────┴────────────┐
                          │                         │
                     [缓存命中]                 [缓存未命中]
                          │                         │
                     返回 ByteView          singleflight::Do()
                                                    │
                                          ┌─────────┴─────────┐
                                          │                   │
                                   PickPeer(key)        GetLocally()
                                   [一致性哈希]          [Getter 回源]
                                          │                   │
                                    [远程节点]           [本地数据源]
                                          │
                                  HttpGetter::Get()
                                          │
                                  HTTP + Protobuf
                                          │
                                          v
                                   远程节点的
                                 HandleRequest()
                                          │
                                  Group::Get(key)
                                          │
                                  Protobuf Response
```

### 4.3 项目文件一览

```
geecache_cpp/
├── CMakeLists.txt                          # 根构建文件
├── proto/
│   └── geecachepb.proto                    # Protobuf 消息定义
├── src/
│   ├── CMakeLists.txt                      # 库构建文件
│   ├── lru/
│   │   └── lru.h                           # LRU 缓存（header-only 模板）
│   ├── byteview.h / byteview.cpp           # 不可变字节视图
│   ├── cache.h / cache.cpp                 # 并发安全缓存
│   ├── consistenthash/
│   │   ├── consistenthash.h                # 一致性哈希
│   │   └── consistenthash.cpp
│   ├── singleflight/
│   │   ├── singleflight.h                  # 防缓存击穿
│   │   └── singleflight.cpp
│   ├── peers.h                             # PeerGetter/PeerPicker 接口
│   ├── geecache.h / geecache.cpp           # Group 核心逻辑
│   └── http.h / http.cpp                   # HTTPPool（服务端+客户端）
├── tests/
│   ├── CMakeLists.txt                      # 测试构建文件
│   ├── lru_test.cpp
│   ├── consistenthash_test.cpp
│   ├── singleflight_test.cpp
│   ├── geecache_test.cpp
│   └── http_test.cpp
└── third_party/
    └── cpp-httplib/
        └── httplib.h                       # 第三方 HTTP 库
```
