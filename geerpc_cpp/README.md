# GeeRPC C++

基于 C++17 实现的轻量级 RPC 框架，是对 [geektutu/geerpc](https://github.com/geektutu/7days-golang) Go 版本的 C++ 重构。

## 功能特性

| 特性 | 说明 |
|------|------|
| **自定义通信协议** | 二进制线格式 `[4B len][Protobuf Header][4B len][body]`，高效紧凑 |
| **Protobuf 序列化** | 使用 Protocol Buffers 序列化消息头，跨语言、高性能 |
| **服务注册** | 通过 `std::function` 手动注册 RPC 方法，编译期类型安全 |
| **并发异步客户端** | 单连接多路复用，`Go`（异步）/ `Call`（同步）两种调用接口 |
| **超时控制** | 连接超时、客户端调用超时、服务端处理超时三层保护 |
| **HTTP 协议支持** | 通过 HTTP CONNECT 隧道升级，RPC 流量可穿越 HTTP 代理/防火墙 |
| **负载均衡** | `XClient` 支持多实例，内置轮询（Round Robin）和随机两种策略 |
| **服务发现** | `MultiServersDiscovery`（静态列表）+ `GeeRegistryDiscovery`（动态注册中心） |
| **注册中心** | `GeeRegistry` 基于 HTTP 实现，自动剔除超时服务，支持心跳保活 |
| **自动心跳** | `HeartbeatAuto` 一行调用，后台线程定时向注册中心保活 |
| **Debug 页面** | `ServeDebugHTTP` 提供 `/debug/geerpc` 页面，实时查看服务调用次数 |

## 技术栈

| 项目 | 选择 |
|------|------|
| 语言标准 | C++17 |
| 构建系统 | CMake 3.14+ |
| 序列化 | Protocol Buffers 3 |
| HTTP 库 | cpp-httplib（header-only，内置于 `third_party/`） |
| JSON 解析 | nlohmann/json |
| 并发模型 | `std::thread` + `std::mutex` + `std::future` |

---

## 快速开始

### 依赖安装（Ubuntu/Debian）

```bash
apt-get install -y \
    cmake \
    libprotobuf-dev \
    protobuf-compiler \
    nlohmann-json3-dev
```

### 构建

```bash
git clone <repo_url> geerpc_cpp
cd geerpc_cpp
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

### 运行示例

```bash
./build/geerpc_example
```

示例程序会依次演示：
1. **单服务端直接调用**：客户端通过 TCP 直连服务端，调用 `Foo.Sum` 方法
2. **多服务端负载均衡**：两个服务端实例 + `XClient` 轮询调度

---

## 使用示例

### 服务端

```cpp
#include "geerpc/codec/protobuf_codec.h"
#include "geerpc/server/service.h"
#include "geerpc/server/server.h"

int main() {
    // 1. 注册 Codec
    geerpc::codec::RegisterProtobufCodec();

    // 2. 创建服务并注册方法
    auto svc = std::make_shared<geerpc::Service>("Foo");
    svc->RegisterMethod("Sum", [](const std::string& args, std::string& reply) -> std::string {
        // 解析 args，执行业务逻辑，填充 reply
        reply = "result";
        return "";  // 空字符串 = 成功
    });

    // 3. 启动服务端
    geerpc::Server server;
    server.RegisterService(svc);

    // 4. 可选：启动 Debug 页面（访问 http://localhost:9001/debug/geerpc）
    server.ServeDebugHTTP(9001);

    // 5. 开始监听（阻塞）
    server.Accept(9999);
}
```

### 客户端（直连 TCP）

```cpp
#include "geerpc/client/client.h"
#include "geerpc/codec/protobuf_codec.h"

int main() {
    geerpc::codec::RegisterProtobufCodec();

    auto client = geerpc::Client::Dial("127.0.0.1", 9999);
    if (!client) return 1;

    std::string reply;
    std::string err = client->Call("Foo.Sum", "1,2", reply);
    if (err.empty()) {
        std::cout << "result: " << reply << "\n";
    }
    client->Close();
}
```

### 客户端（XClient 负载均衡）

```cpp
#include "geerpc/client/xclient.h"
#include "geerpc/codec/protobuf_codec.h"

int main() {
    geerpc::codec::RegisterProtobufCodec();

    // 静态服务列表
    auto discovery = std::make_shared<geerpc::xclient::MultiServersDiscovery>(
        std::vector<std::string>{"tcp@127.0.0.1:9991", "tcp@127.0.0.1:9992"});

    geerpc::xclient::XClient xc(discovery, geerpc::xclient::SelectMode::RoundRobin);

    std::string reply;
    std::string err = xc.Call("Foo.Sum", "3,4", reply);
}
```

### 注册中心

```cpp
// 启动注册中心
#include "geerpc/registry/registry.h"

geerpc::registry::GeeRegistry reg;
reg.Start(9000);  // 阻塞，监听 /_geerpc_/registry
```

```cpp
// 服务端向注册中心注册并自动保活
geerpc::registry::GeeRegistry::HeartbeatAuto(
    "http://localhost:9000/_geerpc_/registry",
    "tcp@localhost:9999"
);
server.Accept(9999);
```

```cpp
// 客户端通过注册中心发现服务
auto discovery = std::make_shared<geerpc::xclient::GeeRegistryDiscovery>(
    "http://localhost:9000/_geerpc_/registry");

geerpc::xclient::XClient xc(discovery, geerpc::xclient::SelectMode::RoundRobin);
```

---

## 项目结构

```
geerpc_cpp/
├── CMakeLists.txt
├── proto/
│   └── geerpc.proto                 # Option + Header Protobuf 定义
├── third_party/
│   └── httplib.h                    # cpp-httplib（header-only）
├── include/geerpc/
│   ├── codec/
│   │   ├── codec.h                  # Codec 抽象接口 + CodecFactory
│   │   └── protobuf_codec.h         # ProtobufCodec 声明
│   ├── server/
│   │   ├── service.h                # HandlerFunc + Service
│   │   └── server.h                 # Server + ServerOption
│   ├── client/
│   │   ├── client.h                 # RpcCall + Client
│   │   └── xclient.h                # Discovery + XClient
│   └── registry/
│       └── registry.h               # GeeRegistry
├── src/                             # 对应实现文件
├── examples/
│   └── main.cpp                     # 示例程序
└── doc/
    ├── geerpc-day1.md               # 服务端与消息编码
    ├── geerpc-day2.md               # 并发异步客户端
    ├── geerpc-day3.md               # 服务注册
    ├── geerpc-day4.md               # 超时处理
    ├── geerpc-day5.md               # HTTP 协议支持
    ├── geerpc-day6.md               # 负载均衡
    └── geerpc-day7.md               # 注册中心
```

---

## 通信协议

### 连接建立

```
客户端                          服务端
  |                               |
  |-- Option JSON（\n 结尾）---->|   协商编解码方式
  |                               |
  |-- [4B hlen][Header][4B blen][body] -->|   RPC 请求
  |<-- [4B hlen][Header][4B blen][body] --|   RPC 响应
  |             ...（多路复用）            |
```

### 线格式

```
| 4字节 header_len（大端）| header_len 字节 Protobuf Header | 4字节 body_len（大端）| body_len 字节 body |
```

### HTTP CONNECT 模式

```
客户端                          服务端
  |-- CONNECT /_geerpc_ HTTP/1.0 -->|
  |<-- HTTP/1.0 200 Connected -----|
  |   （此后切换为 RPC 协议）       |
```

---

## 开发文档

逐天实现文档，适合从 0 学习整个框架的构建过程：

| 文档 | 内容 |
|------|------|
| [Day 1](doc/geerpc-day1.md) | 通信协议、Protobuf 定义、Codec 接口、服务端实现 |
| [Day 2](doc/geerpc-day2.md) | RpcCall 设计、异步客户端、Go/Call 接口 |
| [Day 3](doc/geerpc-day3.md) | 服务注册机制、findService、serveCodec 请求分发 |
| [Day 4](doc/geerpc-day4.md) | 三层超时控制（连接/调用/处理） |
| [Day 5](doc/geerpc-day5.md) | HTTP CONNECT 隧道、ServeDebugHTTP Debug 页面 |
| [Day 6](doc/geerpc-day6.md) | Discovery 接口、XClient 负载均衡（轮询/随机） |
| [Day 7](doc/geerpc-day7.md) | GeeRegistry 注册中心、HeartbeatAuto 自动心跳 |

---

## 面试问答

> 以下问题均围绕本项目的实际实现展开，适合在面试中结合代码讲解。

### 一、协议设计

**Q1：你的 RPC 框架用了什么通信协议？为什么这样设计？**

> 自定义二进制协议，分为两个阶段：
>
> **第一阶段（协商）**：连接建立后，客户端先发送一行 JSON 编码的 `Option`（以 `\n` 结尾），包含 magic number 和编解码类型。服务端据此选择对应的 Codec。用 JSON 做协商是因为它可读性强、易于调试，而且协商只发生一次，性能影响可以忽略。
>
> **第二阶段（通信）**：后续所有请求/响应使用协商好的 Codec 编码，线格式为：
> ```
> [4B header_len][Protobuf Header][4B body_len][body]
> ```
> 长度字段用大端序，Header 用 Protobuf 序列化（包含服务名、方法名、请求序号、错误信息），body 是业务数据的原始字节。这种设计把元信息和业务数据分离，框架只需理解 Header，body 对框架透明。

---

**Q2：为什么选择 Protobuf 而不是 JSON 来序列化 Header？**

> 主要是性能和类型安全两个原因。Protobuf 是二进制编码，序列化/反序列化速度比 JSON 快 5-10 倍，且编码后体积更小。此外 Protobuf 有强类型的 `.proto` 定义，字段变更时有向前/向后兼容性保障，不容易因字段名拼写错误导致静默错误。
>
> body 部分没有强制用 Protobuf，框架只传递原始字节，业务层自己决定序列化方式，保持了灵活性。

---

**Q3：Header 中为什么要有 `seq` 序号字段？**

> 为了支持单连接多路复用。客户端可以在同一个 TCP 连接上并发发送多个请求，服务端处理完毕后返回的响应顺序不一定与发送顺序一致（特别是 handler 处理时间不同时）。`seq` 由客户端分配，服务端在响应中原样返回，客户端的接收线程通过 `seq` 找到对应的等待中请求，完成结果派发。

---

### 二、并发与异步

**Q4：客户端如何实现单连接并发调用？**

> 核心是 `pending_` 表 + 后台接收线程的设计：
>
> 1. 每个调用（`RpcCall`）被分配一个唯一 `seq`，注册进 `pending_` 哈希表后立即返回一个 `std::future<void>`
> 2. 发送线程通过 `sending_` 互斥锁串行写入连接，保证帧不交错
> 3. 一个独立的 `receiveLoop` 线程持续读取响应，根据 Header 中的 `seq` 从 `pending_` 取出对应调用，填写结果后调用 `promise.set_value()` 唤醒等待方
>
> 读和写完全分离：`receiveLoop` 是唯一读 `cc_` 的线程，无需加锁；写操作由 `sending_` 锁串行化。

---

**Q5：`Go` 和 `Call` 接口有什么区别，分别适合什么场景？**

> `Go` 是异步接口，发送请求后立即返回 `std::future<void>` 和 `CallPtr`，调用方可以继续做其他事情，稍后再 `wait()` 等待结果。适合需要同时发起多个 RPC 再统一等待的场景（如 `Broadcast`）。
>
> `Call` 是同步接口，内部调用 `Go` 后立即 `wait()`（或 `wait_for(timeout)`），阻塞直到结果返回。接口简单，适合大多数顺序调用场景。
>
> 两者共用同一套发送和接收逻辑，`Call` 只是对 `Go` 的薄封装。

---

**Q6：项目中用了哪些锁？如何避免死锁？**

> 客户端有两把锁：
> - `sending_`：保护向连接写数据，防止多个并发调用的帧在网络上交错
> - `mu_`：保护 `pending_` 表、`closing_`、`shutdown_` 等共享状态
>
> 为避免死锁，所有需要同时持有两把锁的地方（如 `terminateCalls`）都固定按 `sending_` → `mu_` 的顺序加锁，与 `Go()` 的加锁顺序一致。
>
> 服务端有 `service_mu_`（保护服务注册表）和每连接的 `sending` 局部锁（保护响应写入），两者无交叉持有，不存在死锁风险。

---

### 三、超时控制

**Q7：框架在哪几个地方做了超时保护？**

> 共三层：
>
> | 层次 | 位置 | 机制 |
> |------|------|------|
> | 连接超时 | `Client::Dial` 阶段 | 依赖 OS TCP 超时，`connect_timeout` 通过 Option 协商传递给服务端 |
> | 调用超时 | `Client::Call` | `future.wait_for(timeout)`，超时后从 `pending_` 移除该调用防止泄漏 |
> | 处理超时 | `Server::serveCodec` | `std::async` 异步执行 handler，`wait_for(handle_timeout)` 超时则返回错误响应 |
>
> `handle_timeout` 由客户端在 Option JSON 中协商，服务端从中解析并应用，实现了客户端对服务端处理时间的约束。

---

**Q8：服务端 handler 超时后，后台线程还会继续跑吗？为什么？**

> 会继续跑。C++ 标准没有提供强制终止线程的机制，`std::async` 启动的线程在超时后只是不再等待它的返回值，线程本身仍会自然执行完毕。
>
> 这意味着超时只是「服务端不再等待并提前返回错误响应」，而非真正中断 handler 的执行。因此 handler 本身应避免无限阻塞，建议内部也加入超时或取消检查。这是 C++ 相比有 `context` 机制的语言在超时控制上的局限。

---

### 四、HTTP 协议支持

**Q9：为什么 RPC 框架需要支持 HTTP 协议？是怎么实现的？**

> 在某些网络环境下（如只开放了 80/443 端口的防火墙、HTTP 反向代理），纯 TCP 的 RPC 流量无法通过。支持 HTTP 后，RPC 流量可以伪装成 HTTP 请求穿越这些限制。
>
> 实现上借用了 HTTP 的 **CONNECT 方法**（隧道机制）：
>
> 1. 客户端先发送 `CONNECT /_geerpc_ HTTP/1.0\r\n\r\n`
> 2. 服务端验证后回复 `HTTP/1.0 200 Connected to Gee RPC\r\n\r\n`
> 3. 握手完成，此后双方在同一个 TCP 连接上直接传输 RPC 协议数据，HTTP 层完全退出
>
> 这样核心的 `serveCodec`、`Go`、`Call` 等逻辑完全不需要修改，只是在连接建立阶段多了一次握手。

---

**Q10：`XDial` 的设计意图是什么？**

> `XDial` 是客户端的统一连接入口，通过地址前缀自动路由到不同的连接方式：
> ```
> "tcp@127.0.0.1:9999"  →  直接 TCP 连接（Dial）
> "http@127.0.0.1:9998" →  HTTP CONNECT 隧道（DialHTTP）
> ```
> 好处是调用方无需关心底层连接方式，`XClient` 在负载均衡时统一使用 `XDial`，服务地址的协议前缀就决定了如何建立连接，切换协议只需改地址字符串。

---

### 五、负载均衡与服务发现

**Q11：`XClient` 是如何实现负载均衡的？**

> `XClient` 内部持有一个 `Discovery` 接口和一个客户端连接池（`clients_` map）。每次发起调用时：
>
> 1. 调用 `Discovery::Get(mode)` 按策略选出一个服务地址（`SelectMode::Random` 或 `RoundRobin`）
> 2. 在连接池中查找该地址的已有连接，若连接仍可用则复用，否则重新建立
> 3. 通过该连接发起 RPC 调用
>
> 连接池避免了每次调用都重新建立 TCP 连接的开销，同时在连接失效时自动重连。

---

**Q12：`Discovery` 为什么要设计成接口（抽象基类）？**

> 为了解耦服务发现策略与调用逻辑。`XClient` 依赖 `Discovery` 接口而非具体实现，目前有两种实现：
>
> - `MultiServersDiscovery`：静态服务列表，适合开发测试或服务地址固定的场景
> - `GeeRegistryDiscovery`：从注册中心动态拉取，带本地缓存（`timeout` 内不重复请求），适合生产环境
>
> 用户也可以自行实现 `Discovery`（如基于 etcd、Consul），`XClient` 不需要改动，符合开闭原则。

---

### 六、注册中心

**Q13：注册中心是如何检测服务端是否存活的？**

> 采用**主动心跳**机制，而非被动探活：
>
> - 服务端启动后调用 `HeartbeatAuto(registry_url, addr)`，立即发送第一次心跳，并 detach 一个后台线程每隔 4 分钟（默认 timeout 5 分钟 - 1 分钟余量）定时发送心跳
> - 每次心跳是向注册中心发送一个 HTTP POST，携带 `X-Geerpc-Server: addr` 请求头，注册中心更新该服务的 `last_beat` 时间戳
> - 客户端拉取服务列表时，注册中心遍历所有已注册服务，剔除 `now - last_beat > timeout` 的条目，只返回存活的服务
>
> 这样注册中心无需主动探活，逻辑简单，服务端网络断开后最多 5 分钟内会被自动剔除。

---

**Q14：注册中心的 HTTP API 是如何设计的？**

> 使用自定义 HTTP 响应头传递数据，而非 body，实现简单、解析方便：
>
> | 操作 | 方法 | 路径 | 数据位置 |
> |------|------|------|----------|
> | 获取服务列表 | GET | `/_geerpc_/registry` | 响应头 `X-Geerpc-Servers: addr1,addr2,...` |
> | 注册/心跳 | POST | `/_geerpc_/registry` | 请求头 `X-Geerpc-Server: addr` |
>
> 用响应头而非 body 传递数据的好处是：无需解析 JSON/XML，直接读取一个头字段即可，对注册中心和客户端都更简洁。
