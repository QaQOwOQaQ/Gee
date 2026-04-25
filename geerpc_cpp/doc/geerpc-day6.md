# Day 6: 负载均衡

## 1. 背景知识

### 1.1 Day 5 回顾

Day 5 为框架加入了 HTTP 协议支持，通过 HTTP CONNECT 隧道使 RPC 流量能够穿越 HTTP 网关。

Day 6 的目标是实现**负载均衡**：当同一个 RPC 服务有多个实例运行时，客户端需要决定将每次请求发送给哪个实例。

### 1.2 为什么需要负载均衡

单个服务实例的处理能力有上限。将同一服务部署在多台机器上，可以：

- **提升吞吐量**：多个实例并行处理请求
- **提高可用性**：某个实例故障时，流量自动转移到其他实例
- **水平扩展**：根据负载动态增减实例数量

### 1.3 负载均衡策略

GeeRPC 实现了两种策略，足以覆盖大多数场景：

| 策略 | 说明 | 适用场景 |
|------|------|----------|
| **Random（随机）** | 从服务列表中随机选择一个实例 | 各实例性能相近，请求无状态 |
| **RoundRobin（轮询）** | 依次循环选择实例，`i = (i + 1) % n` | 各实例性能相近，希望均匀分配 |

### 1.4 文件结构

Day 6 新增 `xclient` 模块：

```
geerpc_cpp/
├── include/geerpc/
│   └── client/
│       └── xclient.h    # [新增] Discovery 接口 + MultiServersDiscovery + XClient
└── src/
    └── client/
        └── xclient.cpp  # [新增] 负载均衡实现
```

---

## 2. 代码实现

### 2.1 Discovery 接口

`Discovery` 是服务发现的抽象接口，定义在 `include/geerpc/client/xclient.h`：

```cpp
enum class SelectMode {
    Random,      // 随机选择
    RoundRobin,  // 轮询选择
};

class Discovery {
public:
    virtual ~Discovery() = default;

    virtual bool Refresh() = 0;                              // 从注册中心刷新服务列表
    virtual void Update(std::vector<std::string> servers) = 0; // 手动更新服务列表
    virtual std::string Get(SelectMode mode) = 0;           // 按策略选择一个实例
    virtual std::vector<std::string> GetAll() = 0;          // 返回所有实例地址
};

using DiscoveryPtr = std::shared_ptr<Discovery>;
```

将服务发现抽象为接口，使 `XClient` 与具体的服务注册机制解耦：可以使用静态地址列表，也可以对接动态注册中心，`XClient` 无需任何修改。

### 2.2 MultiServersDiscovery

`MultiServersDiscovery` 是基于静态地址列表的服务发现实现，无需注册中心：

```cpp
class MultiServersDiscovery : public Discovery {
public:
    explicit MultiServersDiscovery(std::vector<std::string> servers);

    bool Refresh() override { return true; }  // 静态列表，无需刷新
    void Update(std::vector<std::string> servers) override;
    std::string Get(SelectMode mode) override;
    std::vector<std::string> GetAll() override;

protected:
    mutable std::mutex mu_;
    std::vector<std::string> servers_;
    int index_{0};  // 轮询游标
};
```

`Get` 的核心实现：

```cpp
std::string MultiServersDiscovery::Get(SelectMode mode) {
    std::lock_guard<std::mutex> lk(mu_);
    int n = static_cast<int>(servers_.size());
    if (n == 0) throw std::runtime_error("rpc discovery: no available servers");

    switch (mode) {
    case SelectMode::Random: {
        std::mt19937 rng(std::random_device{}());
        return servers_[rng() % n];
    }
    case SelectMode::RoundRobin: {
        std::string s = servers_[index_ % n];
        index_ = (index_ + 1) % n;  // 游标前进
        return s;
    }
    default:
        throw std::runtime_error("rpc discovery: unsupported select mode");
    }
}
```

构造时将轮询游标初始化为随机位置，避免多个 `XClient` 实例总是从同一个服务器开始：

```cpp
MultiServersDiscovery::MultiServersDiscovery(std::vector<std::string> servers)
    : servers_(std::move(servers)) {
    std::mt19937 rng(std::random_device{}());
    if (!servers_.empty())
        index_ = static_cast<int>(rng() % servers_.size());
}
```

### 2.3 XClient

`XClient` 是支持负载均衡的客户端封装，内部维护一个连接池，避免重复建立连接：

```cpp
class XClient {
public:
    XClient(DiscoveryPtr d, SelectMode mode, ClientOption opt = {});

    std::string Call(const std::string& service_method,
                     const std::string& args_bytes,
                     std::string& reply_bytes,
                     std::chrono::milliseconds timeout = {});

    std::string Broadcast(const std::string& service_method,
                          const std::string& args_bytes,
                          std::string& reply_bytes,
                          std::chrono::milliseconds timeout = {});
    void Close();

private:
    ClientPtr dial(const std::string& rpc_addr);  // 从连接池获取或新建连接

    DiscoveryPtr d_;
    SelectMode   mode_;
    ClientOption opt_;

    std::mutex mu_;
    std::unordered_map<std::string, ClientPtr> clients_;  // 连接池
};
```

#### dial —— 连接池管理

```cpp
ClientPtr XClient::dial(const std::string& rpc_addr) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = clients_.find(rpc_addr);
    if (it != clients_.end()) {
        if (it->second->IsAvailable()) return it->second;  // 复用已有连接
        it->second->Close();
        clients_.erase(it);  // 连接已断开，移除
    }
    auto c = Client::XDial(rpc_addr, opt_);  // 建立新连接
    if (!c) return nullptr;
    clients_[rpc_addr] = c;
    return c;
}
```

`dial` 对同一地址只维护一个连接。若连接已不可用则关闭并重建，实现简单的连接复用。

#### Call —— 单实例调用

```cpp
std::string XClient::Call(const std::string& service_method,
                           const std::string& args_bytes,
                           std::string& reply_bytes,
                           std::chrono::milliseconds timeout) {
    std::string addr;
    try { addr = d_->Get(mode_); }  // 按策略选择一个实例
    catch (const std::exception& e) { return e.what(); }

    auto c = dial(addr);
    if (!c) return "rpc xclient: dial failed for " + addr;
    return c->Call(service_method, args_bytes, reply_bytes, timeout);
}
```

#### Broadcast —— 广播调用

`Broadcast` 将同一请求并发发送给所有已知实例，返回第一个成功结果（或第一个错误）：

```cpp
std::string XClient::Broadcast(const std::string& service_method,
                                const std::string& args_bytes,
                                std::string& reply_bytes,
                                std::chrono::milliseconds timeout) {
    auto servers = d_->GetAll();
    if (servers.empty()) return "rpc xclient: no available servers";

    std::mutex mu;
    std::string first_err;
    std::string first_reply;
    bool reply_done = false;
    std::atomic<bool> cancelled{false};

    std::vector<std::future<void>> futs;
    for (auto& addr : servers) {
        futs.push_back(std::async(std::launch::async, [&, addr]() {
            if (cancelled.load()) return;  // 已有错误，跳过
            std::string rep;
            auto c = dial(addr);
            std::string err = c ? c->Call(service_method, args_bytes, rep, timeout)
                                : "rpc xclient: dial failed for " + addr;
            std::lock_guard<std::mutex> lk(mu);
            if (!err.empty() && first_err.empty()) {
                first_err = err;
                cancelled.store(true);   // 通知其他线程停止
            }
            if (err.empty() && !reply_done) {
                first_reply = rep;
                reply_done  = true;
            }
        }));
    }
    for (auto& f : futs) f.wait();

    reply_bytes = first_reply;
    return first_err;
}
```

`Broadcast` 的使用场景：需要确保所有实例都执行了某个操作（如缓存失效），或者希望取最快返回的结果。

---

## 3. 小结

Day 6 实现了负载均衡模块，核心要点如下：

| 要点 | 说明 |
|------|------|
| **Discovery 接口** | 服务发现抽象，解耦服务列表来源与负载均衡逻辑 |
| **MultiServersDiscovery** | 静态地址列表，轮询游标随机初始化，支持 Random 和 RoundRobin 两种策略 |
| **连接池** | `XClient` 对每个地址缓存一个 `ClientPtr`，不可用时自动重建 |
| **Call** | 按策略选择一个实例，转发给底层 `Client::Call` |
| **Broadcast** | 并发向所有实例发请求，`std::async` + `atomic<bool>` 实现早停 |

### Day 7 预告

Day 7 将实现**服务注册中心（Registry）**：服务端启动时主动向注册中心注册自己，客户端从注册中心动态获取可用服务列表，取代 `MultiServersDiscovery` 的静态配置方式。核心内容包括：

- `GeeRegistry`：基于 HTTP 的轻量级注册中心，服务端定期发送心跳保持注册
- `GeeRegistryDiscovery`：从注册中心拉取服务列表，带本地缓存和超时刷新
