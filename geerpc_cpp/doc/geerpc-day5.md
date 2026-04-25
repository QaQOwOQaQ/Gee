# Day 5: 支持 HTTP 协议

## 1. 背景知识

### 1.1 Day 4 回顾

Day 4 为框架的三个关键位置加入了超时保护。至此框架已经完整支持基于 TCP 的 RPC 调用。

Day 5 的目标是让 RPC 服务端同时支持 **HTTP 协议**，使 RPC 流量可以穿越只允许 HTTP 的网络环境（如反向代理、防火墙）。

### 1.2 为什么需要 HTTP 支持

RPC 消息格式与标准 HTTP 并不兼容，直接通过 HTTP GET/POST 传输 RPC 帧并不现实。但 HTTP 协议提供了 **CONNECT 方法**，可以将 HTTP 连接升级为任意的 TCP 隧道——这正是我们需要的。

HTTP CONNECT 的典型用途是 HTTPS 代理：

```
客户端                    代理服务器
  |                           |
  |-- CONNECT example.com:443 HTTP/1.0 -->|
  |<-- HTTP/1.0 200 Connection Established --|
  |  （之后直接传输加密数据，代理不解析）  |
```

GeeRPC 借用同样的机制，将 HTTP CONNECT 请求升级为 RPC 隧道：

```
客户端                    RPC 服务端
  |                           |
  |-- CONNECT /_geerpc_ HTTP/1.0 -->|
  |<-- HTTP/1.0 200 Connected to Gee RPC --|
  |-- Option JSON -->         |
  |-- RPC 请求帧 -->          |
  |<-- RPC 响应帧 --          |
```

### 1.3 实现思路

| 角色 | 需要做的事 |
|------|------------|
| **服务端** | 监听 HTTP 端口；收到 CONNECT 请求后返回 200，将裸 TCP 连接交给 `ServeConn` 继续处理 |
| **客户端** | 先发送 HTTP CONNECT 请求；收到 200 后，在同一个 fd 上发送 Option JSON 和 RPC 帧 |

### 1.4 文件结构

Day 5 未新增文件，在已有文件中扩展 HTTP 支持：

```
geerpc_cpp/
├── include/geerpc/
│   └── server/server.h      # 声明 ServeConnFromHTTP
└── src/
    ├── server/server.cpp    # [修改] 实现 HTTP CONNECT 升级逻辑
    └── client/client.cpp    # [已有] DialHTTP + XDial
```

---

## 2. 代码实现

### 2.1 服务端：处理 HTTP CONNECT 请求

服务端需要在接受连接后，判断客户端是否发来了 HTTP CONNECT 请求，如果是则完成协议升级，再交给 `ServeConn` 走正常的 RPC 流程。

`ServeConnFromHTTP` 承担这个职责：

```cpp
// src/server/server.cpp
void Server::ServeConnFromHTTP(int fd) {
    // 读取请求行，如 "CONNECT /_geerpc_ HTTP/1.0"
    std::string request_line = readLineFromFd(fd);
    if (request_line.find("CONNECT") == std::string::npos) {
        writeAllToFd(fd, "HTTP/1.0 405 Method Not Allowed\r\n\r\n");
        ::close(fd);
        return;
    }
    // 排空剩余请求头
    while (true) {
        std::string line = readLineFromFd(fd);
        if (line.empty()) break;
    }
    // 返回 200，完成 HTTP CONNECT 握手
    if (!writeAllToFd(fd, "HTTP/1.0 200 Connected to Gee RPC\r\n\r\n")) {
        ::close(fd);
        return;
    }
    // 此后将连接当作普通 RPC 连接处理
    ServeConn(fd);
}
```

协议升级完成后，fd 上后续的数据流与普通 TCP 连接完全相同：客户端先发 Option JSON，再发 RPC 请求帧，服务端正常处理并响应。

**服务端需要单独监听一个 HTTP 端口**（通常与 RPC 端口不同），并在接受连接后调用 `ServeConnFromHTTP` 而非 `ServeConn`：

```cpp
// 示例：同时监听 RPC 端口 9999 和 HTTP 端口 9998
std::thread([&server]() { server.Accept(9999); }).detach();  // 纯 RPC
// HTTP 端口的 accept 循环调用 ServeConnFromHTTP
```

### 2.2 客户端：DialHTTP

客户端通过 `DialHTTP` 建立 HTTP 隧道，实现在 `src/client/client.cpp` 中：

```cpp
ClientPtr Client::DialHTTP(const std::string& host, int port, ClientOption opt) {
    int fd = connectTCP(host, port, opt.connect_timeout);
    if (fd < 0) return nullptr;

    // 1. 发送 HTTP CONNECT 请求
    std::string req = "CONNECT /_geerpc_ HTTP/1.0\r\n\r\n";
    if (!writeStr(fd, req)) { ::close(fd); return nullptr; }

    // 2. 读取响应行，确认包含 "200"
    std::string status = readLine(fd);
    if (status.find("200") == std::string::npos) {
        std::cerr << "rpc client: unexpected HTTP response: " << status << "\n";
        ::close(fd); return nullptr;
    }

    // 3. 排空剩余响应头
    while (true) {
        std::string line = readLine(fd);
        if (line.empty()) break;
    }

    // 4. 隧道建立完成，发送 Option JSON，进入正常 RPC 流程
    if (!sendOption(fd, opt)) { ::close(fd); return nullptr; }
    auto factory_fn = codec::CodecFactory::instance().Get(opt.codec_type);
    if (!factory_fn) { ::close(fd); return nullptr; }
    return std::shared_ptr<Client>(new Client(factory_fn(fd), std::move(opt)));
}
```

`DialHTTP` 与 `Dial` 的差异只在步骤 1-3：多了一次 HTTP CONNECT 握手。握手完成后，后续的 Option 协商和 RPC 帧收发与 `Dial` 完全一致。

### 2.3 客户端：XDial 统一入口

`XDial` 提供统一的连接入口，根据地址前缀自动选择连接方式：

```cpp
ClientPtr Client::XDial(const std::string& rpc_addr, ClientOption opt) {
    // 地址格式："protocol@host:port"
    // 例如："tcp@localhost:9999" 或 "http@localhost:9998"
    auto at = rpc_addr.find('@');
    if (at == std::string::npos) {
        std::cerr << "rpc client: wrong format '" << rpc_addr
                  << "', expect protocol@host:port\n";
        return nullptr;
    }
    std::string protocol = rpc_addr.substr(0, at);
    std::string addr     = rpc_addr.substr(at + 1);

    auto colon = addr.rfind(':');
    if (colon == std::string::npos) return nullptr;
    std::string host = addr.substr(0, colon);
    int port = std::stoi(addr.substr(colon + 1));

    if (protocol == "http") return DialHTTP(host, port, std::move(opt));
    return Dial(host, port, std::move(opt));  // tcp 或默认
}
```

使用示例：

```cpp
// 直连 RPC
auto c1 = Client::XDial("tcp@localhost:9999");

// 通过 HTTP 隧道
auto c2 = Client::XDial("http@localhost:9998");
```

两种方式对调用方完全透明，`Call` / `Go` 接口无需任何修改。

### 2.4 完整通信流程

```
客户端（DialHTTP）              服务端（ServeConnFromHTTP）
        |                               |
        |-- CONNECT /_geerpc_ HTTP/1.0 -->|
        |                               |（读取请求行，排空请求头）
        |<-- HTTP/1.0 200 Connected --  |
        |                               |
        |-- Option JSON（换行结尾）-->  |
        |                               |（解析 Option，选择 Codec）
        |-- [Header][Body] RPC 请求 --> |
        |                               |（findService → handler → sendResponse）
        |<-- [Header][Body] RPC 响应 -- |
        |          ...（多路复用）      |
```

### 2.5 Debug 页面：ServeDebugHTTP

框架还提供了一个可选的调试 HTTP 服务，访问 `/debug/geerpc` 可以实时查看所有已注册服务的方法名和调用次数。

在 `server.h` 中声明：

```cpp
// 在指定端口启动 Debug HTTP 服务（后台线程，立即返回）
void ServeDebugHTTP(int port);
```

在 `server.cpp` 中实现（依赖 `cpp-httplib`）：

```cpp
void Server::ServeDebugHTTP(int port) {
    std::thread([this, port]() {
        httplib::Server svr;
        svr.Get("/debug/geerpc", [this](const httplib::Request&, httplib::Response& res) {
            std::string html;
            html += "<html><body><title>GeeRPC Services</title>\n";
            std::lock_guard<std::mutex> lk(service_mu_);
            for (auto& [svc_name, svc] : services_) {
                html += "<hr><h2>Service: " + svc_name + "</h2>\n";
                html += "<table border=1><tr><th>Method</th><th>Calls</th></tr>\n";
                for (auto& [method_name, minfo] : svc->methods_) {
                    html += "<tr><td>" + method_name + "</td>";
                    html += "<td>" + std::to_string(minfo.num_calls.load()) + "</td></tr>\n";
                }
                html += "</table>\n";
            }
            html += "</body></html>\n";
            res.set_content(html, "text/html");
        });
        std::cout << "rpc debug server: listening on port " << port
                  << " at /debug/geerpc\n";
        svr.listen("0.0.0.0", port);
    }).detach();
}
```

由于 `methods_` 是 `Service` 的私有成员，需要将 `Server` 声明为 `Service` 的友元：

```cpp
// include/geerpc/server/service.h
class Service {
    // ...
private:
    friend class Server;  // 允许 Server::ServeDebugHTTP 访问 methods_
    std::unordered_map<std::string, MethodInfo> methods_;
};
```

使用方式非常简单，服务端启动后调用一行即可：

```cpp
server.ServeDebugHTTP(9001);  // 浏览器访问 http://localhost:9001/debug/geerpc
server.Accept(9999);
```

页面输出效果示例：

```
Service: Foo
┌────────┬───────┐
│ Method │ Calls │
├────────┼───────┤
│ Sum    │  42   │
└────────┴───────┘
```

---

## 3. 小结

Day 5 为框架加入了 HTTP 协议支持，核心要点如下：

| 要点 | 说明 |
|------|------|
| **HTTP CONNECT** | 利用 HTTP CONNECT 方法将 HTTP 连接升级为 RPC 隧道，无需修改 RPC 核心逻辑 |
| **ServeConnFromHTTP** | 服务端读取并验证 CONNECT 请求，返回 200 后将 fd 交给 `ServeConn` |
| **DialHTTP** | 客户端先发 CONNECT 握手，收到 200 后在同一 fd 上进行正常 RPC 协商 |
| **XDial** | 统一连接入口，`tcp@` 走 `Dial`，`http@` 走 `DialHTTP`，对上层调用透明 |
| **零修改** | HTTP 支持完全在连接建立阶段处理，`serveCodec`、`Go`、`Call` 等核心逻辑无需改动 |

### Day 6 预告

Day 6 将实现**负载均衡（Load Balance）**：当同一个 RPC 服务有多个实例时，客户端需要决定将请求发送给哪个实例。核心内容包括：

- `Discovery` 接口：服务发现的抽象，支持静态列表和动态注册两种模式
- `XClient`：支持多实例的客户端封装，内置轮询（Round Robin）和随机（Random）两种负载均衡策略
