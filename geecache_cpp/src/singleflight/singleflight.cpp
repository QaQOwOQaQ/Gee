#include "singleflight.h"

namespace geecache {
namespace singleflight {

std::pair<std::any, std::string> Group::Do(
    const std::string& key,
    std::function<std::pair<std::any, std::string>()> fn) {

    std::unique_lock<std::mutex> lock(mu_);

    auto it = calls_.find(key);
    if (it != calls_.end()) {
        // A call is already in flight for this key; wait for it.
        auto c = it->second;
        lock.unlock();

        std::unique_lock<std::mutex> call_lock(c->mu);
        c->cv.wait(call_lock, [&c] { return c->done; });
        return {c->val, c->err};
    }

    // First call for this key.
    auto c = std::make_shared<Call>();
    calls_[key] = c;
    lock.unlock();

    // Execute the function outside the lock.
    auto [val, err] = fn();

    // Store result and signal waiters.
    {
        std::lock_guard<std::mutex> call_lock(c->mu);
        c->val = val;
        c->err = err;
        c->done = true;
    }
    c->cv.notify_all();

    // Cleanup.
    {
        std::lock_guard<std::mutex> guard(mu_);
        calls_.erase(key);
    }

    return {val, err};
}

}  // namespace singleflight
}  // namespace geecache
