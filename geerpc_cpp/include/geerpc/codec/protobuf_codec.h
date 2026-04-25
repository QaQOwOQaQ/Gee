#pragma once

#include "geerpc/codec/codec.h"
#include <cstdint>

namespace geerpc {
namespace codec {

// ProtobufCodec encodes/decodes messages over a TCP socket.
//
// Wire format per message:
//   [4 bytes big-endian header_len][header_len bytes Header proto]
//   [4 bytes big-endian body_len  ][body_len   bytes raw body bytes]
class ProtobufCodec : public Codec {
public:
    explicit ProtobufCodec(int fd);
    ~ProtobufCodec() override;

    bool ReadHeader(geerpc::Header& header) override;
    bool ReadBody(std::string& body) override;
    bool Write(const geerpc::Header& header, const std::string& body) override;
    void Close() override;

private:
    // Read exactly `n` bytes into `buf`. Returns false on error/EOF.
    bool readFull(void* buf, size_t n);
    // Write exactly `n` bytes from `buf`. Returns false on error.
    bool writeFull(const void* buf, size_t n);

    int fd_;
    uint32_t pending_body_len_{0};  // cached from the most recent ReadHeader
};

// Factory helper: register ProtobufCodec into CodecFactory.
void RegisterProtobufCodec();

} // namespace codec
} // namespace geerpc
