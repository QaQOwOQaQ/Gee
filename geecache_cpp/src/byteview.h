#pragma once

#include <string>

namespace geecache {

// ByteView holds an immutable view of bytes.
class ByteView {
public:
    ByteView() = default;
    explicit ByteView(std::string bytes) : b_(std::move(bytes)) {}
    ByteView(const char* data, size_t len) : b_(data, len) {}

    // Len returns the byte length. Required by lru::Cache.
    int Len() const { return static_cast<int>(b_.size()); }

    // ByteSlice returns a copy of the data as a byte slice (string).
    std::string ByteSlice() const { return b_; }

    // String returns the data as a string.
    std::string String() const { return b_; }

private:
    std::string b_;
};

}  // namespace geecache
