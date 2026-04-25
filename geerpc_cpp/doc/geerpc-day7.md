# Day 7: 服务发现与注册中心

## 1. 背景知识

### 1.1 Day 6 回顾

Day 6 实现了负载均衡模块 `XClient`，支持随机和轮询两种策略在多个服务实例间分发请求。但服务列表是**静态硬编码**的——客户端必须提前知道所有服务实例的地址，且无法感知实例的上线/下线。

Day 7 引入**注册中心**来解决这个问题。

### 1.2 注册中心的作用

注册中心是服务端与客户端之间的中间层：

```
服务端实例 1 ──┐
服务端实例 2 ──┼──► 注册中心 ◄── 客户端
服务端实例 3 ──┘
```

- **服务端**：启动时向注册中心注册自己的地址；定期发送**心跳**证明自己仍然存活
- **注册中心**：维护所有已注册服务的列表，自动剔除超时未心跳的实例
- **客户端**：从注册中心拉取可用服务列表，不再需要硬编码地址

客户端和服务端都只需感知注册中心的存在，彼此解耦。

### 1.3 GeeRegistry 设计目标

GeeRPC 实现的是一个**轻量级 HTTP 注册中心**，核心功能：

| 功能 | 说明 |
|------|------|
| 服务注册 | 服务端 POST 请求携带地址，注册中心记录并更新心跳时间 |
| 心跳保活 | 服务端定期重复 POST，注册中心以此判断实例是否存活 |
| 超时剔除 | 超过 `timeout`（默认 5 分钟）未收到心跳的实例自动从列表中移除 |
| 服务查询 | 客户端 GET 请求获取所有存活实例的地址列表 |

### 1.4 文件结构

Day 7 新增 `registry` 模块，并在 `xclient` 中扩展 `GeeRegistryDiscovery`：

```
geerpc_cpp/
├── include/geerpc/
│   ├── registry/
│   │   └── registry.h        # [新增] GeeRegistry
│   └── client/
│       └── xclient.h         # [扩展] GeeRegistryDiscovery
└── src/
    ├── registry/
    │   └── registry.cpp      # [新增] GeeRegistry 实现
    └── client/
        └── xclient.cpp       # [扩展] GeeRegistryDiscovery 实现
```

---

## 2. 代码实现

### 2.1 ServerItem 与 GeeRegistry 类定义

`ServerItem` 记录一个已注册的服务实例：

```cpp
struct ServerItem {
    std::string addr;
    std::chrono::steady_clock::time_point last_beat;  // 上次心跳时间
};
```

`GeeRegistry` 是注册中心主体，定义在 `include/geerpc/registry/registry.h`：

```cpp
class GeeRegistry {
public:
    explicit GeeRegistry(
        std::chrono::seconds timeout = std::chrono::seconds(5 * 60)); // 默认 5 分钟
    ~GeeRegistry();

    void Start(int port);  // 启动 HTTP 服务，阻塞
    void Stop();

    // 服务端调用此静态方法向注册中心发送心跳
    static bool Heartbeat(const std::string& registry_url,
                          const std::string& addr);

private:
    void putServer(const std::string& addr);        // 注册或刷新心跳
    std::vector<std::string> aliveServers();        // 返回存活实例列表，清理超时实例

    std::chrono::seconds timeout_;
    std::mutex mu_;
    std::unordered_map<std::string, ServerItem> servers_;
};
```

字段说明：

| 字段 | 说明 |
|------|------|
| `timeout_` | 实例超过此时长未发心跳则视为下线，0 表示不过期 |
| `mu_` | 保护 `servers_` 的并发访问 |
| `servers_` | 以地址为键的实例表，注册和心跳共用同一个入口 `putServer` |

### 2.2 putServer 与 aliveServers

这两个私有方法是注册中心的核心逻辑：

```cpp
// 注册或刷新心跳——地址已存在则更新 last_beat，不存在则新建
void GeeRegistry::putServer(const std::string& addr) {
    std::lock_guard<std::mutex> lk(mu_);
    auto& item     = servers_[addr];  // 不存在时自动创建
    item.addr      = addr;
    item.last_beat = std::chrono::steady_clock::now();
}

// 返回所有存活实例，同时清理超时实例
std::vector<std::string> GeeRegistry::aliveServers() {
    std::lock_guard<std::mutex> lk(mu_);
    auto now = std::chrono::steady_clock::now();
    std::vector<std::string> alive;
    for (auto it = servers_.begin(); it != servers_.end(); ) {
        bool expired = (timeout_.count() > 0) &&
                       (now - it->second.last_beat > timeout_);
        if (expired) {
            it = servers_.erase(it);  // 边遍历边删除
        } else {
            alive.push_back(it->second.addr);
            ++it;
        }
    }
    std::sort(alive.begin(), alive.end());  // 排序保证输出稳定
    return alive;
}
```

设计要点：

| 要点 | 说明 |
|------|------|
| `servers_[addr]` 自动插入 | 注册与心跳复用同一入口，不需要区分「新注册」和「心跳刷新」 |
| `last_beat` 用 `steady_clock` | 单调时钟，不受系统时间调整影响，适合计算时间差 |
| 查询时顺带清理 | `aliveServers` 每次调用都清理超时实例，无需单独的后台定时清理线程 |
| 排序后返回 | 输出顺序稳定，便于调试和测试 |

### 2.3 HTTP 接口（Start）

`GeeRegistry` 通过 `cpp-httplib` 对外提供两个 HTTP 接口：

```cpp
constexpr const char* kRegistryPath = "/_geerpc_/registry";

void GeeRegistry::Start(int port) {
    httplib::Server svr;

    // GET /_geerpc_/registry — 查询所有存活实例
    svr.Get(kRegistryPath, [this](const httplib::Request&, httplib::Response& res) {
        auto servers = aliveServers();
        std::string val;
        for (size_t i = 0; i < servers.size(); ++i) {
            if (i) val += ',';
            val += servers[i];
        }
        res.set_header("X-Geerpc-Servers", val);  // 地址列表放在响应头
        res.status = 200;
    });

    // POST /_geerpc_/registry — 注册/心跳
    svr.Post(kRegistryPath, [this](const httplib::Request& req, httplib::Response& res) {
        std::string addr = req.get_header_value("X-Geerpc-Server");
        if (addr.empty()) {
            res.status = 500;
            return;
        }
        putServer(addr);  // 注册或刷新心跳
        res.status = 200;
    });

    std::cout << "rpc registry: listening on port " << port
              << " at " << kRegistryPath << "\n";
    svr.listen("0.0.0.0", port);  // 阻塞直到 Stop() 被调用
}
```

两个接口的设计：

| 方法 | 路径 | 请求头 | 响应头 | 说明 |
|------|------|--------|--------|------|
| GET | `/_geerpc_/registry` | 无 | `X-Geerpc-Servers: addr1,addr2,...` | 返回所有存活实例，逗号分隔 |
| POST | `/_geerpc_/registry` | `X-Geerpc-Server: addr` | 无 | 注册实例或刷新心跳 |

信息通过**自定义 HTTP 响应头**传递而非 body，实现简单，客户端解析方便。

### 2.4 Heartbeat 与 HeartbeatAuto（服务端心跳）

框架提供两个静态方法发送心跳：

```cpp
// 单次心跳：发送一次 POST 到注册中心
bool GeeRegistry::Heartbeat(const std::string& registry_url,
                             const std::string& addr) {
    std::cout << addr << " send heartbeat to registry " << registry_url << "\n";
    return rawHttpPost(registry_url, "X-Geerpc-Server", addr);
}

// 自动心跳：立即发送第一次，然后 detach 后台线程定时循环
void GeeRegistry::HeartbeatAuto(const std::string& registry_url,
                                const std::string& addr,
                                std::chrono::seconds interval) {
    // interval 默认为 timeout - 1 分钟（与 Go 版本逻辑一致）
    if (interval.count() == 0)
        interval = std::chrono::seconds(5 * 60 - 60);  // 4 分钟

    Heartbeat(registry_url, addr);  // 立即发送第一次

    std::thread([registry_url, addr, interval]() {
        while (true) {
            std::this_thread::sleep_for(interval);
            Heartbeat(registry_url, addr);
        }
    }).detach();
}
```

服务端典型用法——启动后调用一行 `HeartbeatAuto` 即可：

```cpp
// 服务端启动后，自动向注册中心保活
geerpc::registry::GeeRegistry::HeartbeatAuto(
    "http://localhost:9999/_geerpc_/registry",
    "tcp@localhost:8080"
);
```

两个方法对比：

| 方法 | 说明 | 适用场景 |
|------|------|----------|
| `Heartbeat` | 单次发送，立即返回 bool | 需要自己控制重试/循环逻辑 |
| `HeartbeatAuto` | 立即发送 + 后台定时循环，detach | 生产用法，一行搞定保活 |

### 2.5 GeeRegistryDiscovery（客户端侧）

`GeeRegistryDiscovery` 继承自 `MultiServersDiscovery`，在其基础上增加了从注册中心自动拉取服务列表的能力：

```cpp
class GeeRegistryDiscovery : public MultiServersDiscovery {
public:
    GeeRegistryDiscovery(std::string registry_url,
                         std::chrono::seconds timeout = std::chrono::seconds(10));

    bool Refresh() override;                              // 从注册中心拉取最新列表
    void Update(std::vector<std::string> servers) override;
    std::string Get(SelectMode mode) override;
    std::vector<std::string> GetAll() override;

private:
    std::string registry_url_;
    std::chrono::seconds timeout_;        // 本地缓存有效期
    std::chrono::steady_clock::time_point last_update_;
};
```

#### Refresh —— 拉取服务列表

```cpp
bool GeeRegistryDiscovery::Refresh() {
    std::lock_guard<std::mutex> lk(mu_);
    auto now = std::chrono::steady_clock::now();
    // 缓存未过期，直接返回
    if (last_update_ != std::chrono::steady_clock::time_point::min() &&
        now - last_update_ < timeout_) {
        return true;
    }
    // 向注册中心发 GET 请求，从响应头 X-Geerpc-Servers 解析地址列表
    std::string raw = httpGetHeader(registry_url_, "X-Geerpc-Servers");
    if (raw.empty()) return false;

    servers_.clear();
    std::istringstream ss(raw);
    std::string token;
    while (std::getline(ss, token, ',')) {
        // 去除首尾空白
        auto b = token.find_first_not_of(" \t\r\n");
        auto e = token.find_last_not_of(" \t\r\n");
        if (b != std::string::npos)
            servers_.push_back(token.substr(b, e - b + 1));
    }
    last_update_ = now;
    return true;
}
```

`Get` 和 `GetAll` 都会先调用 `Refresh` 检查缓存，过期则重新拉取：

```cpp
std::string GeeRegistryDiscovery::Get(SelectMode mode) {
    Refresh();
    return MultiServersDiscovery::Get(mode);
}

std::vector<std::string> GeeRegistryDiscovery::GetAll() {
    Refresh();
    return MultiServersDiscovery::GetAll();
}
```

缓存机制对比：

| 实现 | 服务列表来源 | 更新方式 |
|------|------------|----------|
| `MultiServersDiscovery` | 构造时手动传入 | 只能调用 `Update` 手动更新 |
| `GeeRegistryDiscovery` | 从注册中心 HTTP GET 拉取 | 每次 `Get`/`GetAll` 时检查缓存，过期自动重新拉取 |

---

## 3. 小结

Day 7 实现了轻量级服务注册与发现中心，完成了 GeeRPC 框架的最后一块拼图。核心要点如下：

| 要点 | 说明 |
|------|------|
| **HTTP 注册中心** | 用 `cpp-httplib` 实现，GET 查询存活实例，POST 注册/心跳，信息通过自定义响应头传递 |
| **心跳保活** | 服务端定期调用 `Heartbeat` 发送 POST，注册中心更新 `last_beat` 时间戳 |
| **超时剔除** | `aliveServers` 查询时顺带清理超时实例，无需独立定时器线程 |
| **GeeRegistryDiscovery** | 客户端侧带本地缓存的服务发现，过期后自动从注册中心重新拉取 |
| **继承复用** | `GeeRegistryDiscovery` 继承 `MultiServersDiscovery`，只重写 `Refresh`/`Get`/`GetAll` |

### 总结

至此，《7天用C++从零实现RPC框架GeeRPC》全部完成。7天内我们从零搭建了一个功能完整的 RPC 框架：

| Day | 主题 | 核心成果 |
|-----|------|----------|
| Day 1 | 编解码与服务端 | Protobuf 编解码、Server 连接处理 |
| Day 2 | 并发异步客户端 | RpcCall、pending 表、receiveLoop |
| Day 3 | 服务注册 | HandlerFunc、Service、findService |
| Day 4 | 超时处理 | 连接超时、调用超时、handler 超时 |
| Day 5 | HTTP 协议支持 | HTTP CONNECT 隧道、DialHTTP、XDial |
| Day 6 | 负载均衡 | Discovery 接口、随机/轮询、Broadcast |
| Day 7 | 注册中心 | GeeRegistry、心跳、GeeRegistryDiscovery |
