#include <gtest/gtest.h>

#include <any>
#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "singleflight/singleflight.h"

TEST(SingleflightTest, Do) {
    geecache::singleflight::Group g;

    auto [val, err] = g.Do("key", []() -> std::pair<std::any, std::string> {
        return {std::any(std::string("bar")), ""};
    });

    ASSERT_TRUE(err.empty());
    EXPECT_EQ(std::any_cast<std::string>(val), "bar");
}

TEST(SingleflightTest, ConcurrentDo) {
    geecache::singleflight::Group g;
    std::atomic<int> call_count{0};

    constexpr int num_threads = 10;
    std::vector<std::thread> threads;
    std::vector<std::string> results(num_threads);
    std::vector<std::string> errors(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            auto [val, err] = g.Do("key", [&call_count]() -> std::pair<std::any, std::string> {
                call_count++;
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                return {std::any(std::string("result")), ""};
            });
            errors[i] = err;
            if (err.empty()) {
                results[i] = std::any_cast<std::string>(val);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Only one call should have been made.
    EXPECT_EQ(call_count.load(), 1);

    // All threads should get the same result.
    for (int i = 0; i < num_threads; ++i) {
        EXPECT_TRUE(errors[i].empty()) << "Thread " << i << " got error: " << errors[i];
        EXPECT_EQ(results[i], "result") << "Thread " << i << " got wrong result";
    }
}
