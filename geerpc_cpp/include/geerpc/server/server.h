#pragma once

#include "geerpc/codec/codec.h"
#include "geerpc/server/service.h"
#include "geerpc.pb.h"

#include <functional>
#include <string>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <atomic>
#include <chrono>

namespace geerpc {

constexpr int MagicNumber = 0x3bef5c;

// Default option values.
struct ServerOption {
    int    magic_number    = MagicNumber;
    std::string codec_type = codec::ProtobufType;
    std::chrono::milliseconds connect_timeout{10000};  // 10s
    std::chrono::milliseconds handle_timeout{0};        // no limit
};

// Server is the RPC server.
// Mirrors Go's geerpc.Server.
class Server {
public:
    explicit Server(ServerOption opt = {});
    ~Server();

    // Register a service by name. Thread-safe.
    bool RegisterService(ServicePtr svc);

    // Accept connections on the given TCP port indefinitely.
    // on_listening is called (if set) immediately after listen() succeeds,
    // before blocking on accept(). Use it for test/demo ready-signalling.
    void Accept(int port, std::function<void()> on_listening = nullptr);

    // Serve a single already-accepted connection (fd). Blocks until done.
    void ServeConn(int fd);

    // Handle HTTP CONNECT tunnel (for HTTP transport mode).
    // Used by the HTTP handler to hand off an upgraded connection.
    void ServeConnFromHTTP(int fd);

    // Start a debug HTTP server on the given port.
    // Serves GET /debug/geerpc — shows all registered services and call counts.
    // Runs in a background thread; returns immediately.
    void ServeDebugHTTP(int port);

private:
    // Parse Option from the connection (JSON encoded, as in Go version).
    bool readOption(int fd, geerpc::Option& opt);

    // Main request-response loop for a codec.
    void serveCodec(codec::CodecPtr cc,
                    std::chrono::milliseconds handle_timeout);

    // Find service+method by "Service.Method" string.
    bool findService(const std::string& service_method,
                     ServicePtr& svc, MethodInfo*& minfo);

    // Send one response (locked).
    void sendResponse(codec::CodecPtr cc,
                      const geerpc::Header& h,
                      const std::string& body,
                      std::mutex& sending);

    ServerOption opt_;
    std::mutex service_mu_;
    std::unordered_map<std::string, ServicePtr> services_;
};

// DefaultServer is a process-wide default Server instance.
Server& DefaultServer();

// Convenience: register a service on the default server.
bool Register(ServicePtr svc);

// Convenience: start accepting on the default server.
void Accept(int port);

} // namespace geerpc
