#pragma once

#include <string>
#include <functional>
#include <unordered_map>
#include <memory>
#include <atomic>

namespace geerpc {

// HandlerFunc is the type of a registered RPC method handler.
//
// Parameters:
//   args_bytes  - serialised request argument (protobuf bytes)
//   reply_bytes - output: serialised reply (protobuf bytes)
//
// Returns an empty string on success, or an error message on failure.
using HandlerFunc = std::function<std::string(
    const std::string& args_bytes,
    std::string&       reply_bytes
)>;

// MethodInfo tracks a single registered method.
struct MethodInfo {
    HandlerFunc handler;
    std::atomic<uint64_t> num_calls{0};

    explicit MethodInfo(HandlerFunc h) : handler(std::move(h)) {}
    // atomic is not copyable/movable by default; provide move ctor manually.
    MethodInfo(MethodInfo&& o) noexcept
        : handler(std::move(o.handler)),
          num_calls(o.num_calls.load()) {}
};

// Service holds a named group of methods.
// Mirrors Go's `service` struct, but without reflection.
class Service {
public:
    explicit Service(std::string name);

    // Register a method under this service.
    // method_name should be the bare method name (e.g. "Sum").
    void RegisterMethod(const std::string& method_name, HandlerFunc handler);

    // Look up a method by name. Returns nullptr if not found.
    MethodInfo* FindMethod(const std::string& method_name);

    const std::string& name() const { return name_; }

private:
    friend class Server;  // allow Server::ServeDebugHTTP to read methods_
    std::string name_;
    std::unordered_map<std::string, MethodInfo> methods_;
};

using ServicePtr = std::shared_ptr<Service>;

} // namespace geerpc
