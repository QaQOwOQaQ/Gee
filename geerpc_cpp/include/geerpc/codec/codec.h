#pragma once

#include <string>
#include <memory>
#include <functional>
#include "geerpc.pb.h"  // generated from proto/geerpc.proto

namespace geerpc {
namespace codec {

// Codec abstracts message encoding and decoding over a connection.
// Mirrors Go's codec.Codec interface.
class Codec {
public:
    virtual ~Codec() = default;

    // Read the next message header from the stream.
    virtual bool ReadHeader(geerpc::Header& header) = 0;

    // Read the next message body (raw bytes) from the stream.
    virtual bool ReadBody(std::string& body) = 0;

    // Write a header + body (raw bytes) atomically.
    virtual bool Write(const geerpc::Header& header, const std::string& body) = 0;

    // Close the underlying connection.
    virtual void Close() = 0;
};

using CodecPtr = std::shared_ptr<Codec>;

// CodecType identifies a codec variant.
constexpr const char* ProtobufType = "application/protobuf";

// Factory function type: takes a connected fd and returns a Codec.
using NewCodecFunc = std::function<CodecPtr(int fd)>;

// Registry of codec factories.  Populated at startup.
class CodecFactory {
public:
    static CodecFactory& instance();

    void Register(const std::string& type, NewCodecFunc fn);
    NewCodecFunc Get(const std::string& type) const;

private:
    std::unordered_map<std::string, NewCodecFunc> map_;
};

} // namespace codec
} // namespace geerpc
