# Day 3: 服务注册

## 1. 背景知识

### 1.1 Day 2 回顾

Day 2 实现了支持并发与异步的 RPC 客户端。至此，框架已经具备了基本的收发能力：

- 客户端能通过 TCP 连接向服务端发送请求
- 服务端能接收请求、读取 Header 和 Body

但服务端还缺少一个关键能力：**如何根据请求中的 `service_method` 字段，找到并调用对应的处理函数？** 这正是 Day 3 要解决的问题。

### 1.2 服务注册要解决的问题

收到一条 RPC 请求时，服务端需要完成以下步骤：

1. 从 Header 中解析出 `service_method`（格式：`"ServiceName.MethodName"`）
2. 在已注册的服务表中查找对应的服务和方法
3. 调用处理函数，传入序列化的请求参数，获取序列化的响应
4. 将响应写回客户端

这需要框架维护一张**服务注册表**，将字符串名称映射到实际的处理函数。

### 1.3 C++ 的服务注册策略

Go 版本利用 `reflect` 包，在运行时自动扫描结构体的方法并注册，用户只需一行 `Register(&foo)`。

C++ 没有运行时反射，因此采用**手动注册 `std::function`** 的策略：

```cpp
// 用户注册方式
auto svc = std::make_shared<geerpc::Service>("Foo");
svc->RegisterMethod("Sum", [](const std::string& args, std::string& reply) {
    // 解析 args，计算结果，填写 reply
    return "";  // 空字符串表示成功
});
geerpc::Register(svc);
```

虽然比 Go 多写几行注册代码，但换来了：

| 优势 | 说明 |
|------|------|
| **类型安全** | 编译期即可发现签名不匹配 |
| **灵活性** | 可注册 lambda、函数指针、`std::bind` 绑定的成员函数等任意 callable |
| **无魔法** | 不依赖宏或代码生成，逻辑清晰透明 |

### 1.4 文件结构

Day 3 涉及以下文件：

```
geerpc_cpp/
├── include/geerpc/
│   ├── server/
│   │   ├── service.h   # [新增] HandlerFunc + MethodInfo + Service
│   │   └── server.h    # [修改] 添加 RegisterService / findService
└── src/
    └── server/
        ├── service.cpp  # [新增] Service 实现
        └── server.cpp   # [修改] serveCodec 调用服务注册表
```

---

## 2. 代码实现

### 2.1 HandlerFunc 与 MethodInfo

`HandlerFunc` 是用户注册 RPC 方法时需要提供的处理函数类型，定义在 `include/geerpc/server/service.h`：

```cpp
using HandlerFunc = std::function<std::string(
    const std::string& args_bytes,
    std::string&       reply_bytes
)>;
```

| 参数 | 说明 |
|------|------|
| `args_bytes` | 客户端传来的请求参数（protobuf 序列化后的字节串） |
| `reply_bytes` | 输出参数，处理函数将响应序列化后写入此字段 |
| 返回值 `string` | 空字符串表示成功；非空表示错误信息，将被填入响应 Header 的 `error` 字段 |

`MethodInfo` 包装一个已注册的方法，附带调用次数统计：

```cpp
struct MethodInfo {
    HandlerFunc handler;
    std::atomic<uint64_t> num_calls{0};

    explicit MethodInfo(HandlerFunc h) : handler(std::move(h)) {}
    // std::atomic 不可拷贝/移动，手动提供移动构造
    MethodInfo(MethodInfo&& o) noexcept
        : handler(std::move(o.handler)),
          num_calls(o.num_calls.load()) {}
};
```

`num_calls` 使用 `std::atomic<uint64_t>`，多线程并发调用同一方法时无需额外加锁即可安全统计。由于 `std::atomic` 默认不可移动，需要手动实现移动构造函数，以便 `MethodInfo` 能存入 `std::unordered_map`。

### 2.2 Service 类

`Service` 是一组同名 RPC 方法的容器，对应 Go 版本的 `service` 结构体：

```cpp
// include/geerpc/server/service.h
class Service {
public:
    explicit Service(std::string name);

    // 注册一个方法（method_name 为裸方法名，如 "Sum"）
    void RegisterMethod(const std::string& method_name, HandlerFunc handler);

    // 按名称查找方法，找不到返回 nullptr
    MethodInfo* FindMethod(const std::string& method_name);

    const std::string& name() const { return name_; }

private:
    std::string name_;
    std::unordered_map<std::string, MethodInfo> methods_;
};

using ServicePtr = std::shared_ptr<Service>;
```

实现非常简洁（`src/server/service.cpp`）：

```cpp
Service::Service(std::string name) : name_(std::move(name)) {}

void Service::RegisterMethod(const std::string& method_name, HandlerFunc handler) {
    methods_.emplace(method_name, MethodInfo(std::move(handler)));
}

MethodInfo* Service::FindMethod(const std::string& method_name) {
    auto it = methods_.find(method_name);
    if (it == methods_.end()) return nullptr;
    return &it->second;
}
```

设计要点：

| 要点 | 说明 |
|------|------|
| `methods_` 用 `unordered_map` | O(1) 平均查找，适合高频调用场景 |
| `FindMethod` 返回裸指针 | 方法的生命周期由 `Service` 管理，调用方不需要持有所有权 |
| `ServicePtr` 用 `shared_ptr` | 服务可同时被 `Server` 的注册表和 `findService` 返回值共享持有 |

### 2.3 Server 的服务注册表

`Server` 内部维护一张服务注册表，并提供注册和查找两个操作：

```cpp
// include/geerpc/server/server.h（相关字段）
class Server {
    ...
    bool RegisterService(ServicePtr svc);

private:
    bool findService(const std::string& service_method,
                     ServicePtr& svc, MethodInfo*& minfo);

    std::mutex service_mu_;
    std::unordered_map<std::string, ServicePtr> services_;
};
```

#### RegisterService —— 注册服务

```cpp
bool Server::RegisterService(ServicePtr svc) {
    std::lock_guard<std::mutex> lk(service_mu_);
    auto [it, ok] = services_.emplace(svc->name(), svc);
    return ok;  // false 表示同名服务已存在
}
```

用 `emplace` 的返回值判断是否插入成功，避免覆盖已有服务。`service_mu_` 保证多线程同时注册时的安全性。

#### findService —— 查找服务与方法

```cpp
bool Server::findService(const std::string& service_method,
                         ServicePtr& svc, MethodInfo*& minfo) {
    // 按最后一个 '.' 分割 "ServiceName.MethodName"
    auto dot = service_method.rfind('.');
    if (dot == std::string::npos) return false;
    std::string svc_name    = service_method.substr(0, dot);
    std::string method_name = service_method.substr(dot + 1);

    std::lock_guard<std::mutex> lk(service_mu_);
    auto it = services_.find(svc_name);
    if (it == services_.end()) return false;
    svc   = it->second;
    minfo = svc->FindMethod(method_name);
    return minfo != nullptr;
}
```

`findService` 将 `"Foo.Sum"` 拆分为服务名 `"Foo"` 和方法名 `"Sum"`，先在 `services_` 中找到服务，再调用 `Service::FindMethod` 找到方法。两级查找，逻辑清晰。

框架还提供全局默认服务器的便捷函数，无需手动创建 `Server` 实例：

```cpp
Server& DefaultServer();          // 进程级单例
bool Register(ServicePtr svc);    // 等价于 DefaultServer().RegisterService(svc)
void Accept(int port);            // 等价于 DefaultServer().Accept(port)
```

### 2.4 serveCodec —— 请求分发

`serveCodec` 是服务端处理单个连接的核心循环，集成了服务查找、方法调用和超时控制：

```cpp
void Server::serveCodec(codec::CodecPtr cc,
                        std::chrono::milliseconds handle_timeout) {
    std::mutex sending;  // 保护当前连接的写操作

    for (;;) {
        geerpc::Header h;
        if (!cc->ReadHeader(h)) break;  // EOF 或读取错误，退出循环

        std::string body;
        if (!cc->ReadBody(body)) break;

        ServicePtr  svc;
        MethodInfo* minfo = nullptr;
        if (!findService(h.service_method(), svc, minfo)) {
            // 找不到服务/方法，返回错误响应，继续处理下一条请求
            geerpc::Header eh = h;
            eh.set_error("rpc server: can't find service " + h.service_method());
            sendResponse(cc, eh, "", sending);
            continue;
        }

        // 封装调用逻辑为 lambda，便于超时控制
        auto do_call = [&]() -> std::pair<std::string, std::string> {
            std::string reply_bytes;
            minfo->num_calls.fetch_add(1, std::memory_order_relaxed);
            std::string err = minfo->handler(body, reply_bytes);
            return {reply_bytes, err};
        };

        if (handle_timeout.count() == 0) {
            // 无超时限制，直接调用
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
    }
    cc->Close();
}
```

循环内的处理流程：

| 步骤 | 操作 |
|------|------|
| 1. 读取 Header | 失败则退出循环（连接断开） |
| 2. 读取 Body | 失败则退出循环 |
| 3. 查找服务方法 | 找不到则返回错误响应，**继续**处理下一条请求 |
| 4. 执行处理函数 | 无超时直接调用；有超时则用 `std::async` 异步执行并 `wait_for` |
| 5. 发送响应 | 成功时携带 reply body；失败时 body 为空，错误信息放在 Header.error |

**超时控制**：`handle_timeout` 来自客户端在 Option 中协商的 `handle_timeout` 字段。超时后服务端返回错误响应，但异步线程仍会继续执行直到完成（不强制终止），因此处理函数本身应尽量响应快。

---

## 3. 小结

Day 3 实现了服务注册机制，使服务端具备了根据请求名称查找并调用处理函数的能力。核心要点如下：

| 要点 | 说明 |
|------|------|
| **HandlerFunc** | 统一的处理函数签名，输入序列化请求、输出序列化响应，返回错误描述 |
| **MethodInfo** | 包装 `HandlerFunc`，附带原子计数器统计调用次数 |
| **Service** | 一组同名方法的容器，提供 `RegisterMethod` 和 `FindMethod` 两个操作 |
| **services_ 注册表** | `Server` 内部以服务名为键的哈希表，`service_mu_` 保护并发访问 |
| **findService** | 按 `.` 分割 `"Svc.Method"`，两级查找定位处理函数 |
| **serveCodec** | 读取请求 → 查找方法 → 调用处理函数 → 发送响应，可选超时控制 |
| **DefaultServer** | 进程级单例，`Register` / `Accept` 便捷函数简化使用 |

### Day 4 预告

Day 4 将实现**超时处理（Timeout）**：对连接建立、服务端处理、客户端等待三个阶段分别加入超时控制，防止因网络抖动或慢处理函数导致调用方长时间挂起。核心内容包括：

- 连接超时：`Dial` 阶段限制建立连接的最长时间
- 处理超时：`serveCodec` 中用 `std::async` + `wait_for` 限制处理函数执行时间
- 调用超时：`Call` 中用 `future.wait_for` 限制客户端等待时间
