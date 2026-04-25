#pragma once

#include "geerpc/codec/codec.h"
#include "geerpc.pb.h"

#include <string>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <future>
#include <chrono>
#include <functional>
#include <atomic>

namespace geerpc {

// RpcCall represents one active RPC invocation.
// Mirrors Go's geerpc.Call.
struct RpcCall {
    uint64_t    seq            = 0;
    std::string service_method;
    std::string args_bytes;   // serialised request (protobuf bytes)
    std::string reply_bytes;  // filled in on success
    std::string error;        // non-empty on failure

    // Promise/future pair used to signal completion.
    std::promise<void> promise;
};

using CallPtr = std::shared_ptr<RpcCall>;

// ClientOption mirrors Go's Option (client-side view).
struct ClientOption {
    int         magic_number    = 0x3bef5c;
    std::string codec_type      = "application/protobuf";
    std::chrono::milliseconds connect_timeout{10000};
    std::chrono::milliseconds handle_timeout{0};
};

// Client is a concurrent, async RPC client.
// Mirrors Go's geerpc.Client.
class Client {
public:
    // Connect to host:port and negotiate the option.
    // Returns nullptr on failure.
    static std::shared_ptr<Client> Dial(const std::string& host, int port,
                                        ClientOption opt = {});

    // Connect via HTTP CONNECT tunnel.
    static std::shared_ptr<Client> DialHTTP(const std::string& host, int port,
                                            ClientOption opt = {});

    // Connect using "protocol@host:port" syntax
    // (e.g. "tcp@127.0.0.1:9999", "http@127.0.0.1:9999").
    static std::shared_ptr<Client> XDial(const std::string& rpc_addr,
                                         ClientOption opt = {});

    ~Client();

    // IsAvailable returns true if the client is still usable.
    bool IsAvailable() const;

    // Close shuts down the connection.
    void Close();

    // Go sends an RPC asynchronously. Returns a future that resolves
    // when the reply arrives (or an error occurs).
    // args_bytes / reply_bytes are raw protobuf-serialised bytes.
    std::future<void> Go(const std::string& service_method,
                         std::string args_bytes,
                         CallPtr& out_call);

    // Call sends an RPC and blocks until it completes or the timeout fires.
    // Returns error string (empty = success).
    std::string Call(const std::string& service_method,
                     const std::string& args_bytes,
                     std::string& reply_bytes,
                     std::chrono::milliseconds timeout = std::chrono::milliseconds(0));

private:
    explicit Client(codec::CodecPtr cc, ClientOption opt);

    // Send the option JSON line to the server.
    static bool sendOption(int fd, const ClientOption& opt);

    // Background receive loop.
    void receiveLoop();

    void registerCall(CallPtr call);
    CallPtr removeCall(uint64_t seq);
    void terminateCalls(const std::string& err);

    codec::CodecPtr cc_;
    ClientOption    opt_;

    std::mutex  sending_;   // protect Write
    std::mutex  mu_;        // protect pending_ / closing_ / shutdown_
    uint64_t    seq_{1};    // next sequence number (0 = invalid)
    std::unordered_map<uint64_t, CallPtr> pending_;
    bool        closing_{false};
    bool        shutdown_{false};
};

using ClientPtr = std::shared_ptr<Client>;

} // namespace geerpc
