#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "lru/lru.h"

// A simple value type satisfying the Len() requirement.
struct StringValue {
    std::string s;
    int Len() const { return static_cast<int>(s.size()); }
};

TEST(LruTest, Get) {
    geecache::lru::Cache<StringValue> cache(0);  // unlimited
    cache.Add("key1", StringValue{"1234"});

    auto [val, ok] = cache.Get("key1");
    ASSERT_TRUE(ok);
    EXPECT_EQ(val.s, "1234");

    auto [val2, ok2] = cache.Get("key2");
    EXPECT_FALSE(ok2);
}

TEST(LruTest, RemoveOldest) {
    // maxBytes = len("key1") + 4 + len("key2") + 4 = 16
    // Adding "key3" should evict "key1" (the oldest)
    geecache::lru::Cache<StringValue> cache(16);
    cache.Add("key1", StringValue{"1234"});
    cache.Add("key2", StringValue{"1234"});
    // Now at 16 bytes, adding key3 should evict key1
    cache.Add("key3", StringValue{"1234"});

    auto [val, ok] = cache.Get("key1");
    EXPECT_FALSE(ok);  // key1 should have been evicted
}

TEST(LruTest, OnEvicted) {
    std::vector<std::string> evicted_keys;

    geecache::lru::Cache<StringValue> cache(16,
        [&evicted_keys](const std::string& key, const StringValue&) {
            evicted_keys.push_back(key);
        });

    cache.Add("key1", StringValue{"1234"});
    cache.Add("key2", StringValue{"1234"});
    cache.Add("key3", StringValue{"1234"});  // should evict key1
    cache.Add("key4", StringValue{"1234"});  // should evict key2

    ASSERT_EQ(evicted_keys.size(), 2u);
    EXPECT_EQ(evicted_keys[0], "key1");
    EXPECT_EQ(evicted_keys[1], "key2");
}

TEST(LruTest, Add) {
    geecache::lru::Cache<StringValue> cache(0);
    cache.Add("key", StringValue{"1"});
    cache.Add("key", StringValue{"111"});

    auto [val, ok] = cache.Get("key");
    ASSERT_TRUE(ok);
    EXPECT_EQ(val.s, "111");
    EXPECT_EQ(cache.Len(), 1);
}
