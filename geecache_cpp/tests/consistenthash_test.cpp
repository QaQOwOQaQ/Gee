#include <gtest/gtest.h>

#include <string>

#include "consistenthash/consistenthash.h"

TEST(ConsistentHashTest, Hashing) {
    // Use a custom hash function for deterministic testing.
    // Hash function: just returns the numeric value of the string.
    auto hash_fn = [](const std::string& key) -> uint32_t {
        return static_cast<uint32_t>(std::stoi(key));
    };

    geecache::consistenthash::Map m(3, hash_fn);
    // Adds nodes "6", "4", "2"
    // With 3 replicas, virtual nodes will be:
    // "06"->6, "16"->16, "26"->26
    // "04"->4, "14"->14, "24"->24
    // "02"->2, "12"->12, "22"->22
    m.Add({"6", "4", "2"});

    struct TestCase {
        std::string key;
        std::string expected;
    };

    std::vector<TestCase> cases = {
        {"2", "2"},
        {"11", "2"},
        {"23", "4"},
        {"27", "2"},
    };

    for (const auto& tc : cases) {
        std::string got = m.Get(tc.key);
        EXPECT_EQ(got, tc.expected)
            << "Asking for " << tc.key << ", should have yielded " << tc.expected;
    }

    // Add "8": virtual nodes "08"->8, "18"->18, "28"->28
    m.Add({"8"});

    // "27" should now map to "8" instead of "2"
    EXPECT_EQ(m.Get("27"), "8");
}

TEST(ConsistentHashTest, Empty) {
    geecache::consistenthash::Map m(3);
    EXPECT_TRUE(m.IsEmpty());
    EXPECT_EQ(m.Get("key"), "");
}
