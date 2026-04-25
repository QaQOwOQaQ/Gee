// GeeRPC C++ 示例程序
// 演示：服务注册 -> 启动服务端 -> 客户端调用 -> 负载均衡
//
// 运行步骤：
//   mkdir build && cd build && cmake .. && make
//   ./geerpc_example

#include "geerpc/codec/protobuf_codec.h"
#include "geerpc/server/service.h"
#include "geerpc/server/server.h"
#include "geerpc/client/client.h"
#include "geerpc/client/xclient.h"

#include <iostream>
#include <thread>
#include <chrono>
#include <string>
#include <atomic>
#include <condition_variable>
#include <mutex>

// ── 简单的 Args/Reply 结构（演示用，直接用字符串序列化）────────────────────────

// 真实项目中 args 和 reply 应该是 protobuf Message，这里简化为
// 直接传递字符串，以便专注于框架用法。

static std::string makeArgs(const std::string& a, const std::string& b) {
    return a + "," + b;
}

static std::pair<std::string, std::string> parseArgs(const std::string& s) {
    auto comma = s.find(',');
    if (comma == std::string::npos) return {s, ""};
    return {s.substr(0, comma), s.substr(comma + 1)};
}

// ── 服务端：注册 Foo.Sum 方法 ─────────────────────────────────────────────────

static void startServer(int port, std::atomic<bool>& ready,
                         std::mutex& mu, std::condition_variable& cv) {
    // 初始化 Protobuf Codec
    geerpc::codec::RegisterProtobufCodec();

    // 创建 Service 并注册方法
    auto svc = std::make_shared<geerpc::Service>("Foo");
    svc->RegisterMethod("Sum", [](const std::string& args, std::string& reply) -> std::string {
        auto [a, b] = parseArgs(args);
        int sum = std::stoi(a) + std::stoi(b);
        reply = std::to_string(sum);
        return "";  // 空字符串 = 成功
    });

    geerpc::Server server;
    server.RegisterService(svc);

    std::cout << "[Server] Starting on port " << port << "\n";
    // Bug8 fix: signal ready only after listen() succeeds, not before.
    server.Accept(port, [&]() {
        std::lock_guard<std::mutex> lk(mu);
        ready = true;
        cv.notify_all();
    });
}

// ── 客户端：直接调用 ───────────────────────────────────────────────────────────

static void runDirectClient(int port) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    geerpc::codec::RegisterProtobufCodec();

    auto client = geerpc::Client::Dial("127.0.0.1", port);
    if (!client) {
        std::cerr << "[Client] Connect failed\n";
        return;
    }

    for (int i = 0; i < 5; ++i) {
        std::string args = makeArgs(std::to_string(i), std::to_string(i * 2));
        std::string reply;
        std::string err = client->Call("Foo.Sum", args, reply,
                                       std::chrono::milliseconds(3000));
        if (!err.empty()) {
            std::cerr << "[Client] Error: " << err << "\n";
        } else {
            std::cout << "[Client] Foo.Sum(" << i << ", " << i*2
                      << ") = " << reply << "\n";
        }
    }
    client->Close();
}

// ── XClient：负载均衡调用（两个服务端）────────────────────────────────────────

static void startServerDetached(int port) {
    geerpc::codec::RegisterProtobufCodec();
    auto svc = std::make_shared<geerpc::Service>("Foo");
    svc->RegisterMethod("Sum", [port](const std::string& args, std::string& reply) -> std::string {
        auto [a, b] = parseArgs(args);
        int sum = std::stoi(a) + std::stoi(b);
        reply = "[port=" + std::to_string(port) + "] " + std::to_string(sum);
        return "";
    });
    static geerpc::Server s1, s2;
    // 根据端口选择不同的 Server 实例（演示用，真实场景各自独立进程）
    if (port == 9991) { s1.RegisterService(svc); s1.Accept(port); }
    else               { s2.RegisterService(svc); s2.Accept(port); }
}

static void runXClient() {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    geerpc::codec::RegisterProtobufCodec();

    // 静态服务列表，不使用注册中心
    auto discovery = std::make_shared<geerpc::xclient::MultiServersDiscovery>(
        std::vector<std::string>{"tcp@127.0.0.1:9991", "tcp@127.0.0.1:9992"});

    geerpc::xclient::XClient xc(discovery, geerpc::xclient::SelectMode::RoundRobin);

    for (int i = 0; i < 6; ++i) {
        std::string args = makeArgs(std::to_string(i), "10");
        std::string reply;
        std::string err = xc.Call("Foo.Sum", args, reply);
        if (!err.empty()) {
            std::cerr << "[XClient] Error: " << err << "\n";
        } else {
            std::cout << "[XClient] Foo.Sum(" << i << ", 10) = " << reply << "\n";
        }
    }
}

// ── main ───────────────────────────────────────────────────────────────────────

int main() {
    std::cout << "=== GeeRPC C++ Demo ===\n\n";

    // ── Part 1: 单服务端直接调用 ──
    std::cout << "--- Part 1: Direct Client ---\n";
    {
        std::atomic<bool> ready{false};
        std::mutex mu;
        std::condition_variable cv;

        std::thread srv([&]() { startServer(9999, ready, mu, cv); });
        srv.detach();

        // 等待服务端就绪
        std::unique_lock<std::mutex> lk(mu);
        cv.wait_for(lk, std::chrono::seconds(3), [&]{ return ready.load(); });

        runDirectClient(9999);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // ── Part 2: 多服务端负载均衡 ──
    std::cout << "\n--- Part 2: XClient Round-Robin ---\n";
    {
        std::thread t1([]() { startServerDetached(9991); });
        std::thread t2([]() { startServerDetached(9992); });
        t1.detach();
        t2.detach();

        runXClient();
    }

    std::cout << "\n=== Done ===\n";
    return 0;
}
