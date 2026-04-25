#include "geerpc/codec/protobuf_codec.h"
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <stdexcept>
#include <unordered_map>

namespace geerpc {
namespace codec {

// ── CodecFactory ─────────────────────────────────────────────────────────────

CodecFactory& CodecFactory::instance() {
    static CodecFactory inst;
    return inst;
}

void CodecFactory::Register(const std::string& type, NewCodecFunc fn) {
    map_[type] = std::move(fn);
}

NewCodecFunc CodecFactory::Get(const std::string& type) const {
    auto it = map_.find(type);
    if (it == map_.end()) return nullptr;
    return it->second;
}

// ── ProtobufCodec ─────────────────────────────────────────────────────────────

ProtobufCodec::ProtobufCodec(int fd) : fd_(fd) {}

ProtobufCodec::~ProtobufCodec() {
    Close();
}

void ProtobufCodec::Close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

bool ProtobufCodec::readFull(void* buf, size_t n) {
    char* p = static_cast<char*>(buf);
    size_t remaining = n;
    while (remaining > 0) {
        ssize_t r = ::read(fd_, p, remaining);
        if (r <= 0) return false;
        p += r;
        remaining -= static_cast<size_t>(r);
    }
    return true;
}

bool ProtobufCodec::writeFull(const void* buf, size_t n) {
    const char* p = static_cast<const char*>(buf);
    size_t remaining = n;
    while (remaining > 0) {
        ssize_t w = ::write(fd_, p, remaining);
        if (w <= 0) return false;
        p += w;
        remaining -= static_cast<size_t>(w);
    }
    return true;
}

// Wire format:
//   [4B header_len][header_len B Header proto][4B body_len]
// ReadHeader reads the header part and caches body_len for ReadBody.
bool ProtobufCodec::ReadHeader(geerpc::Header& header) {
    uint32_t hlen_net = 0;
    if (!readFull(&hlen_net, 4)) return false;
    uint32_t hlen = ntohl(hlen_net);

    std::string hbuf(hlen, '\0');
    if (!readFull(hbuf.data(), hlen)) return false;
    if (!header.ParseFromString(hbuf)) return false;

    uint32_t blen_net = 0;
    if (!readFull(&blen_net, 4)) return false;
    pending_body_len_ = ntohl(blen_net);
    return true;
}

// ReadBody reads exactly pending_body_len_ bytes into body.
bool ProtobufCodec::ReadBody(std::string& body) {
    body.resize(pending_body_len_);
    if (pending_body_len_ == 0) return true;
    return readFull(body.data(), pending_body_len_);
}

// Write serialises header+body atomically.
bool ProtobufCodec::Write(const geerpc::Header& header, const std::string& body) {
    std::string hbuf;
    if (!header.SerializeToString(&hbuf)) return false;

    uint32_t hlen_net = htonl(static_cast<uint32_t>(hbuf.size()));
    uint32_t blen_net = htonl(static_cast<uint32_t>(body.size()));

    // Build one contiguous buffer to minimise syscalls.
    std::string out;
    out.reserve(4 + hbuf.size() + 4 + body.size());
    out.append(reinterpret_cast<const char*>(&hlen_net), 4);
    out.append(hbuf);
    out.append(reinterpret_cast<const char*>(&blen_net), 4);
    out.append(body);

    return writeFull(out.data(), out.size());
}

// ── Registration ──────────────────────────────────────────────────────────────

void RegisterProtobufCodec() {
    CodecFactory::instance().Register(
        ProtobufType,
        [](int fd) -> CodecPtr {
            return std::make_shared<ProtobufCodec>(fd);
        }
    );
}

} // namespace codec
} // namespace geerpc
