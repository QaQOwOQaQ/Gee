# Day 2: 支持并发与异步的客户端

## 1. 背景知识

### 1.1 Day 1 回顾

在 Day 1 中，我们实现了 RPC 框架的服务端——消息编解码器（ProtobufCodec）和服务端（Server）。但目前只有服务端，还缺少客户端来发起 RPC 调用。

Day 2 的目标是实现一个**支持并发和异步**的高性能客户端。

### 1.2 客户端需要解决的问题

一个 RPC 客户端需要具备以下能力：

| 能力 | 说明 |
|------|------|
| **连接管理** | 建立 TCP 连接、发送 Option 协商消息 |
| **请求发送** | 将 RPC 调用序列化后通过网络发送给服务端 |
| **响应接收** | 从网络中读取服务端的响应，匹配到对应的请求 |
| **并发支持** | 多个 RPC 调用可以在同一个连接上同时进行 |
| **异步支持** | 发送请求后不必立即等待，可以稍后获取结果 |

### 1.3 并发与异步的实现思路

为了支持并发，客户端需要：

1. 为每个请求分配一个**唯一序号**（`seq`），服务端在响应中携带相同的序号
2. 维护一个 `pending` 表，记录所有未完成的调用（`seq → Call`）
3. 启动一个**后台接收线程**，不断从连接中读取响应，根据序号匹配到对应的调用

为了支持异步，使用 `std::promise<void>` / `std::future<void>` 作为通知机制：

- 调用方发送请求后，拿到一个 `future`
- 后台接收线程收到响应后，调用 `promise.set_value()` 通知调用方
- 调用方可以选择 `future.wait()`（同步等待）或 `future.wait_for(timeout)`（带超时等待）

### 1.4 文件结构

Day 2 新增以下文件：

```
geerpc_cpp/
├── include/geerpc/
│   └── client/
│       └── client.h       # [新增] RpcCall + Client 声明
└── src/
    └── client/
        └── client.cpp     # [新增] Client 实现
```

---

## 2. 代码实现

### 2.1 RpcCall 设计

`RpcCall` 是客户端对一次 RPC 调用的抽象，定义在 `include/geerpc/client/client.h`：

```cpp
struct RpcCall {
    uint64_t    seq            = 0;
    std::string service_method;
    std::string args_bytes;   // 序列化后的请求体（protobuf bytes）
    std::string reply_bytes;  // 成功时由接收线程填入
    std::string error;        // 非空表示调用出错

    std::promise<void> promise; // 用于异步通知
};

using CallPtr = std::shared_ptr<RpcCall>;
```

各字段说明：

| 字段 | 类型 | 说明 |
|------|------|------|
| `seq` | `uint64_t` | 唯一序号，由 Client 在注册时分配；服务端响应 Header 中原样返回，用于匹配 |
| `service_method` | `string` | 调用目标，格式为 `"ServiceName.MethodName"` |
| `args_bytes` | `string` | protobuf 序列化后的请求参数，作为消息体发送 |
| `reply_bytes` | `string` | 接收线程收到响应后将 body 写入此字段 |
| `error` | `string` | 空字符串表示成功；出错时存放错误描述 |
| `promise` | `std::promise<void>` | 调用完成后由接收线程调用 `set_value()` 唤醒等待方 |

**设计要点**：`promise` 不传递具体值，只传递「完成」信号。调用方通过检查 `call->error` 判断成功与否，通过 `call->reply_bytes` 获取返回值。这样一个 `future<void>` 就能同时支持同步等待和带超时等待两种用法。

### 2.2 Client 类定义与字段

`Client` 类定义在 `include/geerpc/client/client.h`，核心字段如下：

```cpp
class Client {
public:
    static ClientPtr Dial(const std::string& host, int port,
                          ClientOption opt = {});
    static ClientPtr DialHTTP(const std::string& host, int port,
                              ClientOption opt = {});
    static ClientPtr XDial(const std::string& rpc_addr,
                           ClientOption opt = {});

    bool IsAvailable() const;

    std::future<void> Go(const std::string& service_method,
                         std::string args_bytes,
                         CallPtr& out_call);

    std::string Call(const std::string& service_method,
                     const std::string& args_bytes,
                     std::string& reply_bytes,
                     std::chrono::milliseconds timeout = {});

private:
    codec::CodecPtr  cc_;       // 编解码器（持有底层 fd）
    ClientOption     opt_;      // 连接选项

    std::mutex  sending_;       // 保护 Write，防止多线程同时写乱序
    std::mutex  mu_;            // 保护 pending_ / closing_ / shutdown_
    uint64_t    seq_{1};        // 下一个请求序号（0 保留为非法值）
    std::unordered_map<uint64_t, CallPtr> pending_; // 未完成调用表
    bool        closing_{false};   // 用户主动关闭
    bool        shutdown_{false};  // 发生错误，连接不可用

    geerpc::Header send_header_;   // 复用的 Header 对象，减少内存分配
};
```

字段设计要点：

| 字段 | 说明 |
|------|------|
| `cc_` | 持有 `ProtobufCodec`，封装了底层 socket fd 的读写；Client 销毁时 Codec 析构自动关闭 fd |
| `sending_` | 独立的写锁；`Go()` 发送请求时加锁，确保多个并发调用的帧不会交错 |
| `mu_` | 保护 `pending_`、`closing_`、`shutdown_` 三个共享状态 |
| `seq_` | 单调递增，初始为 1（0 为非法序号）；在 `mu_` 保护下自增分配 |
| `pending_` | `seq → CallPtr` 的哈希表；发送时插入，收到响应或出错时移除 |
| `closing_` / `shutdown_` | 区分「用户主动关闭」与「连接异常」两种不可用状态 |
| `send_header_` | 复用同一个 `geerpc::Header` protobuf 对象，避免每次调用都重新分配 |

### 2.3 Call 管理三方法

客户端用三个私有方法管理 `pending_` 表，实现对并发调用的注册、移除和批量终止：

```cpp
// 注册调用：分配序号并插入 pending_
void Client::registerCall(CallPtr call) {
    std::lock_guard<std::mutex> lk(mu_);
    call->seq = seq_++;          // 原子分配序号
    pending_[call->seq] = call;  // 插入等待表
}

// 移除调用：从 pending_ 取出并返回
CallPtr Client::removeCall(uint64_t seq) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = pending_.find(seq);
    if (it == pending_.end()) return nullptr;
    auto call = it->second;
    pending_.erase(it);
    return call;
}

// 终止所有调用：连接异常时批量唤醒所有等待方
void Client::terminateCalls(const std::string& err) {
    std::lock_guard<std::mutex> lk1(sending_);  // 先锁写，防止新请求进入
    std::lock_guard<std::mutex> lk2(mu_);
    shutdown_ = true;
    for (auto& [seq, call] : pending_) {
        call->error = err;
        call->promise.set_value();  // 唤醒所有阻塞的 future
    }
    pending_.clear();
}
```

三个方法的职责：

| 方法 | 调用时机 | 核心操作 |
|------|----------|----------|
| `registerCall` | `Go()` 发送请求前 | 在 `mu_` 保护下分配 `seq` 并写入 `pending_` |
| `removeCall` | 接收线程收到响应，或发送失败时 | 从 `pending_` 移除并返回 `CallPtr`，供调用方填写结果 |
| `terminateCalls` | `receiveLoop` 退出（连接断开）时 | 同时持有 `sending_` 和 `mu_`，将所有未完成调用标记为错误并唤醒 |

**加锁顺序**：`terminateCalls` 先锁 `sending_` 再锁 `mu_`，与 `Go()` 的加锁顺序一致（`Go()` 同样先持有 `sending_` 再操作 `pending_`），从而避免死锁。

### 2.4 receiveLoop 接收循环

`receiveLoop` 是客户端的核心后台线程，在 `Client` 构造时以 `detach` 方式启动，负责持续从连接中读取响应并派发给等待中的调用方：

```cpp
void Client::receiveLoop() {
    std::string err;
    for (;;) {
        geerpc::Header h;
        if (!cc_->ReadHeader(h)) {       // 读取响应 Header
            err = "rpc client: connection closed";
            break;
        }
        auto call = removeCall(h.seq()); // 根据 seq 从 pending_ 中取出对应调用
        if (!call) {
            // 找不到对应调用（已超时被移除），丢弃 body
            std::string dummy;
            cc_->ReadBody(dummy);
            continue;
        }
        if (!h.error().empty()) {
            // 服务端在 Header 中报错，填写错误并唤醒
            call->error = h.error();
            std::string dummy;
            cc_->ReadBody(dummy);
            call->promise.set_value();
            continue;
        }
        if (!cc_->ReadBody(call->reply_bytes)) {
            call->error = "rpc client: error reading body";
        }
        call->promise.set_value();       // 唤醒等待方
    }
    terminateCalls(err);                 // 连接断开，终止所有未完成调用
}
```

循环内的四种分支：

| 场景 | 处理方式 |
|------|----------|
| `ReadHeader` 失败 | 退出循环，调用 `terminateCalls` 批量终止所有未完成调用 |
| `seq` 在 `pending_` 中找不到 | 调用已超时被移除，读取并丢弃 body，继续循环 |
| Header 中 `error` 非空 | 服务端处理出错，填写 `call->error`，丢弃 body，唤醒等待方 |
| 正常响应 | 读取 body 到 `call->reply_bytes`，唤醒等待方 |

**设计要点**：`receiveLoop` 是唯一读取 `cc_` 的线程，因此读取操作无需加锁。写入操作（`Go()` 发送请求）由 `sending_` 锁保护，读写完全分离，无竞争。

### 2.5 创建客户端（Dial / DialHTTP / XDial）

`Client` 的构造函数是私有的，外部通过三个静态工厂方法创建实例：

#### Dial —— 普通 TCP 连接

```cpp
ClientPtr Client::Dial(const std::string& host, int port, ClientOption opt) {
    int fd = connectTCP(host, port, opt.connect_timeout);
    if (fd < 0) return nullptr;

    if (!sendOption(fd, opt)) { ::close(fd); return nullptr; }

    auto factory_fn = codec::CodecFactory::instance().Get(opt.codec_type);
    if (!factory_fn) { ::close(fd); return nullptr; }

    return std::shared_ptr<Client>(new Client(factory_fn(fd), std::move(opt)));
}
```

`sendOption` 将 `ClientOption` 序列化为 JSON 行（以 `\n` 结尾）发送给服务端，服务端据此选择对应的 Codec：

```cpp
bool Client::sendOption(int fd, const ClientOption& opt) {
    json j;
    j["magic_number"]    = opt.magic_number;
    j["codec_type"]      = opt.codec_type;
    j["connect_timeout"] = opt.connect_timeout.count() * 1000000LL; // ms → ns
    j["handle_timeout"]  = opt.handle_timeout.count()  * 1000000LL;
    return writeStr(fd, j.dump() + "\n");
}
```

#### DialHTTP —— 通过 HTTP CONNECT 隧道

```cpp
ClientPtr Client::DialHTTP(const std::string& host, int port, ClientOption opt) {
    int fd = connectTCP(host, port, opt.connect_timeout);
    if (fd < 0) return nullptr;

    // 发送 HTTP CONNECT 请求，建立隧道
    std::string req = "CONNECT /_geerpc_ HTTP/1.0\r\n\r\n";
    if (!writeStr(fd, req)) { ::close(fd); return nullptr; }

    // 读取响应行，确认 200
    std::string status = readLine(fd);
    if (status.find("200") == std::string::npos) {
        ::close(fd); return nullptr;
    }
    // 排空剩余响应头
    while (true) {
        std::string line = readLine(fd);
        if (line.empty()) break;
    }

    if (!sendOption(fd, opt)) { ::close(fd); return nullptr; }
    auto factory_fn = codec::CodecFactory::instance().Get(opt.codec_type);
    if (!factory_fn) { ::close(fd); return nullptr; }
    return std::shared_ptr<Client>(new Client(factory_fn(fd), std::move(opt)));
}
```

#### XDial —— 协议路由

`XDial` 根据地址前缀自动选择连接方式，地址格式为 `protocol@host:port`：

```cpp
ClientPtr Client::XDial(const std::string& rpc_addr, ClientOption opt) {
    // 解析 "tcp@localhost:9999" 或 "http@localhost:9999"
    auto at       = rpc_addr.find('@');
    std::string protocol = rpc_addr.substr(0, at);
    std::string addr     = rpc_addr.substr(at + 1);
    // 拆分 host:port ...
    if (protocol == "http") return DialHTTP(host, port, opt);
    else                    return Dial(host, port, opt);     // tcp / 默认
}
```

三个工厂方法对比：

| 方法 | 适用场景 | 连接方式 |
|------|----------|----------|
| `Dial` | 直连 RPC 服务端 | 普通 TCP，发送 Option JSON |
| `DialHTTP` | 服务端监听在 HTTP 端口 | TCP + HTTP CONNECT 隧道，再发送 Option JSON |
| `XDial` | 统一入口，由地址前缀决定 | 转发到 `Dial` 或 `DialHTTP` |

### 2.6 Go / Call 接口

`Go` 和 `Call` 是客户端对外暴露的两个核心调用接口，分别对应异步和同步两种使用方式。

#### Go —— 异步调用

```cpp
std::future<void> Client::Go(const std::string& service_method,
                              std::string args_bytes,
                              CallPtr& out_call) {
    auto call = std::make_shared<RpcCall>();
    call->service_method = service_method;
    call->args_bytes     = std::move(args_bytes);
    auto fut = call->promise.get_future();
    out_call = call;  // 将 CallPtr 返回给调用方，供后续取结果

    {
        std::lock_guard<std::mutex> lk(mu_);
        if (closing_ || shutdown_) {
            call->error = "rpc client: connection is shut down";
            call->promise.set_value();
            return fut;
        }
    }

    registerCall(call);  // 分配 seq，插入 pending_

    std::lock_guard<std::mutex> lk(sending_);  // 保护写操作
    geerpc::Header h;
    h.set_service_method(service_method);
    h.set_seq(call->seq);
    h.set_error("");

    if (!cc_->Write(h, call->args_bytes)) {
        // 发送失败，立即移除并通知
        auto c = removeCall(call->seq);
        if (c) {
            c->error = "rpc client: write error";
            c->promise.set_value();
        }
    }
    return fut;
}
```

`Go` 返回一个 `std::future<void>`，调用方可以：
- 调用 `fut.wait()` 阻塞等待完成
- 调用 `fut.wait_for(timeout)` 带超时等待
- 完全不等待，继续执行其他逻辑，稍后再检查结果

#### Call —— 同步调用

```cpp
std::string Client::Call(const std::string& service_method,
                         const std::string& args_bytes,
                         std::string& reply_bytes,
                         std::chrono::milliseconds timeout) {
    CallPtr call;
    auto fut = Go(service_method, args_bytes, call);

    if (timeout.count() == 0) {
        fut.wait();  // 无超时，阻塞等待
    } else {
        if (fut.wait_for(timeout) == std::future_status::timeout) {
            removeCall(call->seq);  // 超时，从 pending_ 中移除
            return "rpc client: call timed out";
        }
    }
    reply_bytes = call->reply_bytes;
    return call->error;  // 空字符串表示成功
}
```

`Call` 是对 `Go` 的封装，隐藏了 `future` 的细节，提供更简洁的同步调用接口。两者对比：

| 接口 | 返回值 | 阻塞行为 | 适用场景 |
|------|--------|----------|----------|
| `Go` | `std::future<void>` + `CallPtr` | 不阻塞，立即返回 | 需要并发发起多个 RPC 后统一等待 |
| `Call` | `std::string`（错误描述） | 阻塞直到完成或超时 | 简单的顺序调用，逻辑清晰 |

---

## 3. 小结

Day 2 实现了支持并发与异步的 RPC 客户端，核心要点如下：

| 要点 | 说明 |
|------|------|
| **RpcCall** | 封装一次 RPC 调用的全部状态，用 `promise/future` 实现异步通知 |
| **pending_ 表** | 以 `seq` 为键记录所有未完成调用，接收线程按序号派发响应 |
| **读写分离** | `receiveLoop` 独占读，`Go()` 通过 `sending_` 锁串行写，无竞争 |
| **双锁设计** | `sending_` 保护写操作，`mu_` 保护共享状态，加锁顺序固定避免死锁 |
| **工厂方法** | `Dial` / `DialHTTP` / `XDial` 覆盖直连、HTTP 隧道、协议路由三种场景 |
| **Go / Call** | `Go` 异步返回 future，`Call` 同步封装，两者共用同一发送逻辑 |

### Day 3 预告

Day 3 将实现**服务注册（Service Register）**：服务端通过 `std::function` 手动注册 RPC 方法，并在收到请求时根据 `service_method` 字段查找并调用对应的处理函数。核心内容包括：

- `HandlerFunc` / `MethodInfo` / `Service` 的设计
- 服务端如何在收到请求时查找并调用对应的服务方法
- 错误处理与方法不存在时的返回策略
