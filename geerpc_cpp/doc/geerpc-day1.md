# Day 1: 服务端与消息编码

## 1. 背景知识

### 1.1 什么是 RPC？

RPC（Remote Procedure Call，远程过程调用）是一种让程序像调用本地函数一样调用远程服务的技术。客户端发起调用，框架负责将参数序列化后通过网络发送给服务端，服务端执行方法并将结果返回。

一个典型的 RPC 调用如下：

```cpp
client.Call("Arith.Multiply", args_bytes, reply_bytes);
```

这个调用包含三部分信息：

| 部分 | 说明 | 示例 |
|------|------|------|
| 服务名 | 要调用的服务 | `Arith` |
| 方法名 | 要调用的方法 | `Multiply` |
| 参数 | 序列化后的请求数据 | `args_bytes` |

服务端的响应则包含两部分：

| 部分 | 说明 |
|------|------|
| 返回值 | 序列化后的响应数据（`reply_bytes`） |
| 错误信息 | 为空表示成功，非空表示调用出错 |

我们将请求和响应中的参数/返回值抽象为 **body**，其余的元信息（服务名、方法名、序号、错误）放在 **header** 中。

### 1.2 序列化方案选型

RPC 框架需要将数据序列化后通过网络传输。常见的序列化方案有：

| 方案 | 特点 |
|------|------|
| JSON | 人类可读，但性能较低，体积大 |
| Protobuf | 二进制编码，高性能，强类型，跨语言 |
| MessagePack | 类 JSON 的二进制格式，轻量 |
| Go gob | Go 语言专用，不跨语言 |

我们选择 **Protobuf** 作为序列化方案，因为它具备跨语言、高性能、强类型的优势，并且在工业界被广泛使用（gRPC 就是基于 Protobuf 构建的）。

### 1.3 通信协议设计

客户端与服务端建立连接后，首先需要协商编解码方式。我们定义了一个简单的协议：

**第一步：协商**

客户端发送一个 JSON 编码的 `Option` 对象（以 `\n` 结尾），包含 magic number 和编解码类型：

```
| Option{magic_number: 0x3bef5c, codec_type: "application/protobuf"} \n |
| <-------              固定 JSON 编码                         ------> |
```

**第二步：通信**

协商完成后，后续的 header 和 body 按照协商好的编码方式传输。一次连接中可以有多个请求-响应对：

```
| Option | Header1 | Body1 | Header2 | Body2 | ...
```

每个 Header+Body 对的**线格式（wire format）**为：

```
| 4字节 header_len (大端) | header_len 字节 Header proto | 4字节 body_len (大端) | body_len 字节 body |
```

### 1.4 文件结构

Day 1 结束后，项目结构如下：

```
geerpc_cpp/
├── CMakeLists.txt
├── proto/
│   └── geerpc.proto              # Protobuf 消息定义
├── include/geerpc/
│   ├── codec/
│   │   ├── codec.h               # Codec 抽象接口 + CodecFactory
│   │   └── protobuf_codec.h      # ProtobufCodec 声明
│   └── server/
│       ├── service.h             # HandlerFunc + Service
│       └── server.h              # Server + ServerOption
├── src/
│   ├── codec/
│   │   └── protobuf_codec.cpp    # ProtobufCodec 实现
│   └── server/
│       ├── service.cpp           # Service 实现
│       └── server.cpp            # Server 实现
└── examples/
    └── main.cpp                  # 示例程序
```

依赖安装（Ubuntu/Debian）：

```bash
apt-get install -y libprotobuf-dev protobuf-compiler nlohmann-json3-dev cmake
```

## 2. 代码实现

### 2.1 Protobuf 消息定义

首先定义 RPC 框架的两个核心消息：`Option`（协议协商）和 `Header`（请求/响应头）。

创建文件 `proto/geerpc.proto`：

```protobuf
syntax = "proto3";

package geerpc;

// Option 用于客户端与服务端之间的协议协商。
// 在每次连接开始时以 JSON 格式发送。
message Option {
    int32  magic_number     = 1;  // 0x3bef5c 标识这是一个 geerpc 请求
    string codec_type       = 2;  // 编解码类型，如 "application/protobuf"
    int64  connect_timeout  = 3;  // 连接超时（纳秒），0 表示无限制
    int64  handle_timeout   = 4;  // 处理超时（纳秒），0 表示无限制
}

// Header 是 RPC 请求/响应的头部。
message Header {
    string service_method = 1;  // 格式 "Service.Method"
    uint64 seq            = 2;  // 由客户端分配的请求序号
    string error          = 3;  // 错误信息，为空表示成功
}
```

**字段说明：**

| 字段 | 用途 |
|------|------|
| `service_method` | 服务名和方法名，如 `"Foo.Sum"`，用于服务端路由到对应的处理函数 |
| `seq` | 请求序号，用于客户端在并发场景下区分不同请求的响应 |
| `error` | 客户端发送时置为空，服务端处理出错时填入错误信息 |

### 2.2 CMakeLists.txt

构建系统使用 CMake，需要处理 Protobuf 代码生成、头文件搜索路径、线程库链接等：

```cmake
cmake_minimum_required(VERSION 3.14)
project(geerpc_cpp VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ── 依赖 ─────────────────────────────────────────────────────────────────────
find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)

# ── 生成 protobuf 代码 ────────────────────────────────────────────────────────
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/geerpc.proto
)

# ── 头文件搜索路径 ─────────────────────────────────────────────────────────────
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}              # 生成的 proto 头文件
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party  # cpp-httplib 等第三方库
    ${Protobuf_INCLUDE_DIRS}
)

# ── 库源文件 ──────────────────────────────────────────────────────────────────
set(GEERPC_SOURCES
    ${PROTO_SRCS}
    src/codec/protobuf_codec.cpp
    src/server/service.cpp
    src/server/server.cpp
    src/client/client.cpp
    src/client/xclient.cpp
    src/registry/registry.cpp
)

add_library(geerpc ${GEERPC_SOURCES})
target_link_libraries(geerpc PUBLIC ${Protobuf_LIBRARIES} Threads::Threads)
target_include_directories(geerpc PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party
    ${Protobuf_INCLUDE_DIRS}
)

# ── 示例可执行文件 ─────────────────────────────────────────────────────────────
add_executable(geerpc_example examples/main.cpp)
target_link_libraries(geerpc_example PRIVATE geerpc)
```

**设计要点：**

- `protobuf_generate_cpp` 自动将 `.proto` 文件编译为 C++ 源码（`geerpc.pb.h` / `geerpc.pb.cc`），生成到 `CMAKE_CURRENT_BINARY_DIR` 中
- 头文件搜索路径包含 `${CMAKE_CURRENT_BINARY_DIR}`，使得 `#include "geerpc.pb.h"` 能找到生成的文件
- 所有源文件编译为 `geerpc` 静态库，示例程序链接该库即可

### 2.3 Codec 抽象接口

消息的编解码是 RPC 框架的核心能力。我们定义一个 `Codec` 纯虚基类作为接口，这样可以实现不同的编解码器（Protobuf、JSON 等），然后通过 `CodecFactory` 工厂类按类型字符串获取对应的构造函数。

创建文件 `include/geerpc/codec/codec.h`：

```cpp
#pragma once

#include <string>
#include <memory>
#include <functional>
#include "geerpc.pb.h"  // generated from proto/geerpc.proto

namespace geerpc {
namespace codec {

// Codec 抽象了连接上的消息编解码。
class Codec {
public:
    virtual ~Codec() = default;

    // 从连接中读取下一条消息的 header。
    virtual bool ReadHeader(geerpc::Header& header) = 0;

    // 从连接中读取下一条消息的 body（原始字节）。
    virtual bool ReadBody(std::string& body) = 0;

    // 原子地写入一条 header + body。
    virtual bool Write(const geerpc::Header& header, const std::string& body) = 0;

    // 关闭底层连接。
    virtual void Close() = 0;
};

using CodecPtr = std::shared_ptr<Codec>;

// 编解码器类型标识。
constexpr const char* ProtobufType = "application/protobuf";

// 工厂函数类型：接收一个已连接的 fd，返回一个 Codec。
using NewCodecFunc = std::function<CodecPtr(int fd)>;

// CodecFactory 是编解码器的注册表（单例模式）。
class CodecFactory {
public:
    static CodecFactory& instance();

    void Register(const std::string& type, NewCodecFunc fn);
    NewCodecFunc Get(const std::string& type) const;

private:
    std::unordered_map<std::string, NewCodecFunc> map_;
};

} // namespace codec
} // namespace geerpc
```

**设计要点：**

| 组件 | 职责 |
|------|------|
| `Codec` | 纯虚基类，定义 `ReadHeader`、`ReadBody`、`Write`、`Close` 四个接口 |
| `CodecPtr` | `std::shared_ptr<Codec>`，统一管理 Codec 对象的生命周期 |
| `CodecFactory` | 单例工厂，通过 `Register` 注册编解码器，通过 `Get` 按类型字符串获取构造函数 |
| `NewCodecFunc` | 工厂函数类型，接收文件描述符 fd，返回对应的 Codec 实例 |

这种**接口 + 工厂**的设计使得框架可以轻松扩展新的编解码器，只需实现 `Codec` 接口并注册到工厂中即可。

### 2.4 ProtobufCodec 实现

`ProtobufCodec` 是 `Codec` 接口的具体实现，直接基于文件描述符（fd）进行读写。

创建文件 `include/geerpc/codec/protobuf_codec.h`：

```cpp
#pragma once

#include "geerpc/codec/codec.h"
#include <cstdint>

namespace geerpc {
namespace codec {

// ProtobufCodec 通过 TCP socket 编解码消息。
//
// 每条消息的线格式：
//   [4 bytes 大端 header_len][header_len bytes Header proto]
//   [4 bytes 大端 body_len  ][body_len   bytes body 原始字节]
class ProtobufCodec : public Codec {
public:
    explicit ProtobufCodec(int fd);
    ~ProtobufCodec() override;

    bool ReadHeader(geerpc::Header& header) override;
    bool ReadBody(std::string& body) override;
    bool Write(const geerpc::Header& header, const std::string& body) override;
    void Close() override;

private:
    bool readFull(void* buf, size_t n);   // 精确读取 n 字节
    bool writeFull(const void* buf, size_t n); // 精确写入 n 字节

    int fd_;
    uint32_t pending_body_len_{0};  // ReadHeader 中缓存的 body 长度
};

// 将 ProtobufCodec 注册到 CodecFactory 中。
void RegisterProtobufCodec();

} // namespace codec
} // namespace geerpc
```

创建文件 `src/codec/protobuf_codec.cpp`：

```cpp
#include "geerpc/codec/protobuf_codec.h"
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <unordered_map>

namespace geerpc {
namespace codec {

// ── CodecFactory（单例实现）──────────────────────────────────────────────────

CodecFactory& CodecFactory::instance() {
    static CodecFactory inst;
    return inst;
}

void CodecFactory::Register(const std::string& type, NewCodecFunc fn) {
    map_[type] = std::move(fn);
}

NewCodecFunc CodecFactory::Get(const std::string& type) const {
    auto it = map_.find(type);
    if (it == map_.end()) return nullptr;
    return it->second;
}

// ── ProtobufCodec ─────────────────────────────────────────────────────────────

ProtobufCodec::ProtobufCodec(int fd) : fd_(fd) {}

ProtobufCodec::~ProtobufCodec() { Close(); }

void ProtobufCodec::Close() {
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
}

bool ProtobufCodec::readFull(void* buf, size_t n) {
    char* p = static_cast<char*>(buf);
    size_t remaining = n;
    while (remaining > 0) {
        ssize_t r = ::read(fd_, p, remaining);
        if (r <= 0) return false;
        p += r;
        remaining -= static_cast<size_t>(r);
    }
    return true;
}

bool ProtobufCodec::writeFull(const void* buf, size_t n) {
    const char* p = static_cast<const char*>(buf);
    size_t remaining = n;
    while (remaining > 0) {
        ssize_t w = ::write(fd_, p, remaining);
        if (w <= 0) return false;
        p += w;
        remaining -= static_cast<size_t>(w);
    }
    return true;
}

bool ProtobufCodec::ReadHeader(geerpc::Header& header) {
    uint32_t hlen_net = 0;
    if (!readFull(&hlen_net, 4)) return false;
    uint32_t hlen = ntohl(hlen_net);

    std::string hbuf(hlen, '\0');
    if (!readFull(hbuf.data(), hlen)) return false;
    if (!header.ParseFromString(hbuf)) return false;

    uint32_t blen_net = 0;
    if (!readFull(&blen_net, 4)) return false;
    pending_body_len_ = ntohl(blen_net);
    return true;
}

bool ProtobufCodec::ReadBody(std::string& body) {
    body.resize(pending_body_len_);
    if (pending_body_len_ == 0) return true;
    return readFull(body.data(), pending_body_len_);
}

bool ProtobufCodec::Write(const geerpc::Header& header, const std::string& body) {
    std::string hbuf;
    if (!header.SerializeToString(&hbuf)) return false;

    uint32_t hlen_net = htonl(static_cast<uint32_t>(hbuf.size()));
    uint32_t blen_net = htonl(static_cast<uint32_t>(body.size()));

    // 拼入一个连续缓冲区，减少系统调用次数
    std::string out;
    out.reserve(4 + hbuf.size() + 4 + body.size());
    out.append(reinterpret_cast<const char*>(&hlen_net), 4);
    out.append(hbuf);
    out.append(reinterpret_cast<const char*>(&blen_net), 4);
    out.append(body);

    return writeFull(out.data(), out.size());
}

// ── 注册 ──────────────────────────────────────────────────────────────────────

void RegisterProtobufCodec() {
    CodecFactory::instance().Register(
        ProtobufType,
        [](int fd) -> CodecPtr {
            return std::make_shared<ProtobufCodec>(fd);
        }
    );
}

} // namespace codec
} // namespace geerpc
```

**核心操作解析：**

**1. `readFull` / `writeFull` 辅助方法**

系统调用 `read()` / `write()` 不保证一次性读写全部字节（特别是在网络 I/O 场景下），因此需要循环调用直到精确完成 n 个字节的读写：

```cpp
while (remaining > 0) {
    ssize_t r = ::read(fd_, p, remaining);
    if (r <= 0) return false;  // EOF 或错误
    p += r;
    remaining -= r;
}
```

**2. `ReadHeader` 的三步流程**

```
步骤 1: 读 4 字节 → ntohl → 得到 header_len
步骤 2: 读 header_len 字节 → ParseFromString → 解析为 Header proto
步骤 3: 读 4 字节 → ntohl → 得到 body_len → 缓存到 pending_body_len_
```

`ReadHeader` 和 `ReadBody` 是配对调用的：`ReadHeader` 读取 header 并缓存 body 的长度，紧接着 `ReadBody` 根据缓存的长度读取 body 内容。

**3. `Write` 的合并写入**

将 header 和 body 拼接到一个连续缓冲区后再一次性写入，减少系统调用次数。`htonl` 将主机字节序转为网络字节序（大端）。

### 2.5 Service 服务注册

C++ 没有运行时反射，无法像 Go 那样自动发现类的方法。因此我们采用 `std::function` 手动注册的方式，用户显式注册每个 RPC 方法的处理函数。

创建文件 `include/geerpc/server/service.h`：

```cpp
#pragma once

#include <string>
#include <functional>
#include <unordered_map>
#include <memory>
#include <atomic>

namespace geerpc {

// HandlerFunc 是注册 RPC 方法时需要提供的处理函数签名。
//   args_bytes  - 序列化的请求参数
//   reply_bytes - 输出：序列化的响应数据
//   返回空字符串表示成功，非空表示错误信息。
using HandlerFunc = std::function<std::string(
    const std::string& args_bytes,
    std::string&       reply_bytes
)>;

// MethodInfo 跟踪一个已注册的方法。
struct MethodInfo {
    HandlerFunc handler;
    std::atomic<uint64_t> num_calls{0};  // 调用计数

    explicit MethodInfo(HandlerFunc h) : handler(std::move(h)) {}
    // std::atomic 不可拷贝/移动，需手动实现移动构造
    MethodInfo(MethodInfo&& o) noexcept
        : handler(std::move(o.handler)),
          num_calls(o.num_calls.load()) {}
};

// Service 持有一组方法，对应一个服务名。
class Service {
public:
    explicit Service(std::string name);

    void RegisterMethod(const std::string& method_name, HandlerFunc handler);
    MethodInfo* FindMethod(const std::string& method_name);

    const std::string& name() const { return name_; }

private:
    std::string name_;
    std::unordered_map<std::string, MethodInfo> methods_;
};

using ServicePtr = std::shared_ptr<Service>;

} // namespace geerpc
```

创建文件 `src/server/service.cpp`：

```cpp
#include "geerpc/server/service.h"

namespace geerpc {

Service::Service(std::string name) : name_(std::move(name)) {}

void Service::RegisterMethod(const std::string& method_name, HandlerFunc handler) {
    methods_.emplace(method_name, MethodInfo(std::move(handler)));
}

MethodInfo* Service::FindMethod(const std::string& method_name) {
    auto it = methods_.find(method_name);
    if (it == methods_.end()) return nullptr;
    return &it->second;
}

} // namespace geerpc
```

**设计要点：**

| 组件 | 职责 |
|------|------|
| `HandlerFunc` | 统一的方法处理函数签名，接收序列化的 args，填充 reply，返回错误信息 |
| `MethodInfo` | 包装一个 handler + 原子调用计数器 `num_calls` |
| `Service` | 持有一个服务名和一组方法，通过 `RegisterMethod` 注册，通过 `FindMethod` 查找 |

**使用示例：**

```cpp
auto svc = std::make_shared<geerpc::Service>("Foo");
svc->RegisterMethod("Sum", [](const std::string& args, std::string& reply) -> std::string {
    // 解析 args，执行业务逻辑，填充 reply
    reply = "result";
    return "";  // 空字符串 = 成功
});
```

### 2.6 Server 服务端实现

Server 是 RPC 框架的核心，负责监听连接、协商协议、读取请求、调用处理函数、回复响应。

创建文件 `include/geerpc/server/server.h`：

```cpp
#pragma once

#include "geerpc/codec/codec.h"
#include "geerpc/server/service.h"
#include "geerpc.pb.h"

#include <string>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <chrono>

namespace geerpc {

constexpr int MagicNumber = 0x3bef5c;

struct ServerOption {
    int    magic_number    = MagicNumber;
    std::string codec_type = codec::ProtobufType;
    std::chrono::milliseconds connect_timeout{10000};  // 10s
    std::chrono::milliseconds handle_timeout{0};        // 无限制
};

class Server {
public:
    explicit Server(ServerOption opt = {});
    ~Server();

    bool RegisterService(ServicePtr svc);  // 注册服务（线程安全）
    void Accept(int port);                 // 监听端口，阻塞
    void ServeConn(int fd);                // 处理单个连接

private:
    bool readOption(int fd, geerpc::Option& opt);
    void serveCodec(codec::CodecPtr cc, std::chrono::milliseconds handle_timeout);
    bool findService(const std::string& service_method,
                     ServicePtr& svc, MethodInfo*& minfo);
    void sendResponse(codec::CodecPtr cc, const geerpc::Header& h,
                      const std::string& body, std::mutex& sending);

    ServerOption opt_;
    std::mutex service_mu_;
    std::unordered_map<std::string, ServicePtr> services_;
};

Server& DefaultServer();
bool Register(ServicePtr svc);
void Accept(int port);

} // namespace geerpc
```

创建文件 `src/server/server.cpp`（关键部分）：

**Accept — 创建 TCP 监听，每个连接一个线程：**

```cpp
void Server::Accept(int port) {
    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    int yes = 1;
    ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(static_cast<uint16_t>(port));
    addr.sin_addr.s_addr = INADDR_ANY;

    ::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(listen_fd, 128);
    std::cout << "rpc server: listening on port " << port << "\n";

    for (;;) {
        int conn_fd = ::accept(listen_fd, nullptr, nullptr);
        if (conn_fd < 0) break;
        std::thread([this, conn_fd]() { ServeConn(conn_fd); }).detach();
    }
    ::close(listen_fd);
}
```

**ServeConn — 协商协议后进入主循环：**

```cpp
void Server::ServeConn(int fd) {
    geerpc::Option opt;
    if (!readOption(fd, opt)) { ::close(fd); return; }
    if (opt.magic_number() != MagicNumber) { ::close(fd); return; }

    auto factory_fn = codec::CodecFactory::instance().Get(opt.codec_type());
    if (!factory_fn) { ::close(fd); return; }

    auto handle_ms = std::chrono::milliseconds(opt.handle_timeout() / 1000000);
    serveCodec(factory_fn(fd), handle_ms);
}
```

**serveCodec — 读取-处理-回复循环：**

```cpp
void Server::serveCodec(codec::CodecPtr cc,
                        std::chrono::milliseconds handle_timeout) {
    std::mutex sending;

    for (;;) {
        geerpc::Header h;
        if (!cc->ReadHeader(h)) break;  // EOF 或错误

        std::string body;
        if (!cc->ReadBody(body)) break;

        ServicePtr svc;
        MethodInfo* minfo = nullptr;
        if (!findService(h.service_method(), svc, minfo)) {
            geerpc::Header eh = h;
            eh.set_error("rpc server: can't find service " + h.service_method());
            sendResponse(cc, eh, "", sending);
            continue;
        }

        // 调用 handler
        std::string reply_bytes;
        minfo->num_calls.fetch_add(1, std::memory_order_relaxed);
        std::string err = minfo->handler(body, reply_bytes);

        geerpc::Header rh = h;
        rh.set_error(err);
        sendResponse(cc, rh, err.empty() ? reply_bytes : "", sending);
    }
    cc->Close();
}
```

**核心操作解析：**

`serveCodec` 是服务端的主循环，包含三个阶段：

```
读取请求:  cc->ReadHeader(h) + cc->ReadBody(body)
     ↓
处理请求:  findService → minfo->handler(body, reply)
     ↓
回复请求:  sendResponse(cc, rh, reply, sending)
```

**需要注意的三个点：**

1. **回复必须串行发送**：使用 `std::mutex sending` 保护 `sendResponse`，防止多个请求的响应报文在网络上交错
2. **一次连接多个请求**：使用 `for(;;)` 无限循环等待请求到来，只有在 `ReadHeader` 失败（EOF 或错误）时才终止
3. **findService 的分割逻辑**：`"Foo.Sum"` 按 `.` 分割为服务名 `"Foo"` 和方法名 `"Sum"`，分别在 `services_` 和 `Service::methods_` 中查找

## 3. 小结

Day 1 我们实现了 GeeRPC 的两个基础模块——消息编解码器和服务端。核心要点：

| 模块 | 关键实现 | 说明 |
|------|----------|------|
| Protobuf 定义 | `Option` + `Header` | 协议协商和请求/响应头的数据结构 |
| Codec 接口 | `Codec` 纯虚基类 + `CodecFactory` | 接口 + 工厂模式，支持扩展不同编解码器 |
| ProtobufCodec | `readFull` / `writeFull` + 线格式 | 基于 fd 的二进制编解码，`[4B len][data]` 格式 |
| Service | `HandlerFunc` + `MethodInfo` | `std::function` 手动注册，替代 Go 的反射方案 |
| Server | `Accept` + `ServeConn` + `serveCodec` | TCP 监听 → 协议协商 → 读取-处理-回复循环 |

- **线格式**：`[4B header_len][Header proto][4B body_len][body bytes]`，大端字节序
- **协议协商**：连接建立后先发送 JSON 编码的 `Option`（以 `\n` 结尾），再切换到协商好的编码方式
- **并发模型**：每个连接一个 `std::thread`，回复使用 `std::mutex` 保证串行发送
- **服务注册**：用户通过 `RegisterMethod` 显式注册处理函数，编译期类型安全

Day 2 预告：我们将实现一个支持异步和并发的高性能客户端，包括 `RpcCall` 结构体、`Client::Dial` 连接创建、`Go` 异步接口和 `Call` 同步接口。
