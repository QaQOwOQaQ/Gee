#pragma once

#include <any>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

namespace geecache {
namespace singleflight {

class Group {
public:
    // Do executes fn() for the given key, deduplicating concurrent calls.
    // Returns {value, error_string}. Empty error_string means success.
    std::pair<std::any, std::string> Do(
        const std::string& key,
        std::function<std::pair<std::any, std::string>()> fn);

private:
    struct Call {
        std::mutex mu;
        std::condition_variable cv;
        bool done = false;
        std::any val;
        std::string err;
    };

    std::mutex mu_;
    std::unordered_map<std::string, std::shared_ptr<Call>> calls_;
};

}  // namespace singleflight
}  // namespace geecache
