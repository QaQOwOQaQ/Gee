# Day 4: 超时处理

## 1. 背景知识

### 1.1 Day 3 回顾

Day 3 实现了服务注册机制，服务端现在可以根据请求中的 `service_method` 字段查找并调用对应的处理函数。

但框架还缺少一个重要的健壮性保障：**超时处理**。

### 1.2 为什么需要超时处理

在真实网络环境中，以下情况都可能导致调用方长时间挂起：

- 网络拥塞，连接建立缓慢
- 服务端负载过高，处理函数迟迟不返回
- 服务端已崩溃，客户端一直等待响应

没有超时保护的 RPC 调用会耗尽调用方的线程/资源，最终导致整个服务不可用。

### 1.3 三处超时控制点

GeeRPC C++ 在以下三个关键位置加入了超时控制：

| 位置 | 超时参数 | 说明 |
|------|----------|------|
| **客户端建立连接**（`Dial`） | `connect_timeout` | 限制 TCP 连接建立的最长时间 |
| **客户端等待响应**（`Call`） | `Call` 的 `timeout` 参数 | 限制整个调用（发送+等待+接收）的最长时间 |
| **服务端执行处理函数**（`serveCodec`） | `handle_timeout` | 限制 handler 执行的最长时间，由客户端在 Option 中协商 |

超时配置通过两个 Option 结构体传递：

```cpp
// 客户端侧
struct ClientOption {
    std::chrono::milliseconds connect_timeout{10000}; // 默认 10s
    std::chrono::milliseconds handle_timeout{0};       // 默认不限
    ...
};

// 服务端侧（从 Option JSON 解析而来）
struct ServerOption {
    std::chrono::milliseconds connect_timeout{10000};
    std::chrono::milliseconds handle_timeout{0};
    ...
};
```

客户端在建立连接时将 `connect_timeout` 和 `handle_timeout` 编码进 Option JSON 发送给服务端，服务端据此决定 handler 的最大执行时间。

### 1.4 文件结构

Day 4 未新增文件，只在已有文件中增强超时逻辑：

```
geerpc_cpp/
├── include/geerpc/
│   ├── client/client.h   # connect_timeout / handle_timeout 字段
│   └── server/server.h   # handle_timeout 字段
└── src/
    ├── client/client.cpp # connectTCP 超时 + Call 超时
    └── server/server.cpp # serveCodec handler 超时
```

---

## 2. 代码实现

### 2.1 连接超时（connectTCP）

连接超时在 `src/client/client.cpp` 的 `connectTCP` 函数中处理。当前实现使用阻塞式 `connect()`，超时依赖操作系统默认的 TCP 连接超时行为：

```cpp
static int connectTCP(const std::string& host, int port,
                      std::chrono::milliseconds timeout) {
    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    std::string port_str = std::to_string(port);
    if (::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res) != 0)
        return -1;

    int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) { ::freeaddrinfo(res); return -1; }

    if (::connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
        ::close(fd);
        ::freeaddrinfo(res);
        return -1;
    }
    ::freeaddrinfo(res);
    return fd;
}
```

`connect_timeout` 参数目前传入但未强制生效，实际超时由操作系统 TCP 栈控制（通常为几十秒）。若需要精确的连接超时，可将 socket 设置为非阻塞模式，再用 `poll`/`select` 监听可写事件，但为保持代码简洁，当前实现足以满足大部分场景。

`connect_timeout` 的主要作用是将超时意图编码进 Option JSON 传递给服务端，供服务端参考。

### 2.2 客户端调用超时（Call）

`Call` 方法通过 `std::future::wait_for` 实现整个 RPC 调用的超时控制，覆盖「发送请求 + 等待处理 + 接收响应」全流程：

```cpp
std::string Client::Call(const std::string& service_method,
                         const std::string& args_bytes,
                         std::string& reply_bytes,
                         std::chrono::milliseconds timeout) {
    CallPtr call;
    auto fut = Go(service_method, args_bytes, call);  // 异步发送请求

    if (timeout.count() == 0) {
        fut.wait();  // 无超时，阻塞等待
    } else {
        if (fut.wait_for(timeout) == std::future_status::timeout) {
            removeCall(call->seq);  // 从 pending_ 中移除，防止内存泄漏
            return "rpc client: call timed out";
        }
    }
    reply_bytes = call->reply_bytes;
    return call->error;
}
```

超时后调用 `removeCall(call->seq)` 将该调用从 `pending_` 中移除。即使后续服务端仍然返回了响应，`receiveLoop` 找不到对应的 `seq` 也会直接丢弃，不会造成资源泄漏。

### 2.3 服务端处理超时（serveCodec）

服务端的 handler 超时通过 `std::async` + `future::wait_for` 实现，在 `serveCodec` 的请求分发循环中控制：

```cpp
// 封装 handler 调用为 lambda
auto do_call = [&]() -> std::pair<std::string, std::string> {
    std::string reply_bytes;
    minfo->num_calls.fetch_add(1, std::memory_order_relaxed);
    std::string err = minfo->handler(body, reply_bytes);
    return {reply_bytes, err};
};

if (handle_timeout.count() == 0) {
    // 无超时限制，在当前线程直接调用
    auto [reply, err] = do_call();
    geerpc::Header rh = h;
    rh.set_error(err);
    sendResponse(cc, rh, err.empty() ? reply : "", sending);
} else {
    // 有超时限制，在异步线程中执行
    auto fut = std::async(std::launch::async, do_call);
    if (fut.wait_for(handle_timeout) == std::future_status::timeout) {
        geerpc::Header eh = h;
        eh.set_error("rpc server: request handle timeout");
        sendResponse(cc, eh, "", sending);
    } else {
        auto [reply, err] = fut.get();
        geerpc::Header rh = h;
        rh.set_error(err);
        sendResponse(cc, rh, err.empty() ? reply : "", sending);
    }
}
```

`handle_timeout` 来自客户端 Option 中的 `handle_timeout` 字段（单位毫秒），通过 Option JSON 协商传递给服务端。

**注意**：超时后服务端返回错误响应，但 `std::async` 启动的后台线程仍会继续执行直到 handler 自然返回。C++ 标准不提供强制终止线程的机制，因此 handler 自身应避免无限阻塞。

三处超时对比：

| 超时点 | 实现机制 | 超时后行为 |
|--------|----------|------------|
| 连接建立 | OS TCP 超时（阻塞 `connect`） | `connect` 返回 -1，`Dial` 返回 `nullptr` |
| 客户端调用 | `future::wait_for(timeout)` | 移除 pending call，返回超时错误给调用方 |
| 服务端 handler | `std::async` + `future::wait_for` | 返回超时错误响应，后台线程继续执行至完成 |

---

## 3. 小结

Day 4 为框架的三个关键位置加入了超时保护，核心要点如下：

| 要点 | 说明 |
|------|------|
| **超时配置** | `ClientOption` 的 `connect_timeout` / `handle_timeout` 统一管理，通过 Option JSON 协商传递给服务端 |
| **连接超时** | 依赖 OS TCP 超时，`connect_timeout` 编码进 Option 传递给服务端参考 |
| **调用超时** | `Call` 用 `future::wait_for` 覆盖整个调用链路；超时后移除 pending call 防止泄漏 |
| **handler 超时** | `serveCodec` 用 `std::async` 异步执行 handler，`wait_for` 检测超时；超时后返回错误响应 |
| **超时不等于终止** | C++ 无法强制终止线程，超时仅代表服务端不再等待结果并返回错误，后台线程仍自然执行完毕 |

### Day 5 预告

Day 5 将实现**支持 HTTP 协议**：让 RPC 服务端同时兼容普通 RPC 客户端和 HTTP 客户端的访问。核心内容包括：

- 服务端处理 HTTP CONNECT 请求，将连接升级为 RPC 隧道
- 客户端通过 `DialHTTP` 先建立 HTTP 隧道，再发送 Option 协商消息
- `XDial` 统一入口，根据地址前缀自动选择连接方式
