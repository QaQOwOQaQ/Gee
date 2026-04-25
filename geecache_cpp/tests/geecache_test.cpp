#include <gtest/gtest.h>

#include <chrono>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "geecache.h"
#include "peers.h"

class GeeCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        geecache::Group::DestroyAllGroups();
    }
    void TearDown() override {
        geecache::Group::DestroyAllGroups();
    }
};

static std::unordered_map<std::string, std::string> db = {
    {"Tom", "630"},
    {"Jack", "589"},
    {"Sam", "567"},
};

namespace {

class RecordingPeerGetter : public geecache::PeerGetter {
public:
    explicit RecordingPeerGetter(std::string invalidate_err = "")
        : invalidate_err_(std::move(invalidate_err)) {}

    std::string Get(const geecachepb::Request&,
                    geecachepb::Response*) override {
        return "not implemented";
    }

    std::string Invalidate(const geecachepb::Request&) override {
        ++invalidate_calls_;
        return invalidate_err_;
    }

    int invalidate_calls() const { return invalidate_calls_.load(); }

private:
    std::string invalidate_err_;
    std::atomic<int> invalidate_calls_{0};
};

class StaticPeerSet : public geecache::PeerPicker, public geecache::PeerManager {
public:
    explicit StaticPeerSet(std::vector<std::shared_ptr<geecache::PeerGetter>> peers)
        : peers_(std::move(peers)) {}

    std::pair<std::shared_ptr<geecache::PeerGetter>, bool> PickPeer(
        const std::string&) override {
        return {nullptr, false};
    }

    std::vector<std::shared_ptr<geecache::PeerGetter>> GetAllPeers() override {
        return peers_;
    }

private:
    std::vector<std::shared_ptr<geecache::PeerGetter>> peers_;
};

}  // namespace

TEST_F(GeeCacheTest, Getter) {
    geecache::GetterFunc getter([](const std::string& key) -> std::pair<std::string, std::string> {
        return {"value_for_" + key, ""};
    });
    auto [val, err] = getter.Get("test");
    EXPECT_TRUE(err.empty());
    EXPECT_EQ(val, "value_for_test");
}

TEST_F(GeeCacheTest, Get) {
    std::atomic<int> load_counts{0};

    auto getter = std::make_shared<geecache::GetterFunc>(
        [&load_counts](const std::string& key) -> std::pair<std::string, std::string> {
            auto it = db.find(key);
            if (it != db.end()) {
                load_counts++;
                return {it->second, ""};
            }
            return {"", "key not found: " + key};
        });

    auto group = geecache::Group::NewGroup("scores", 2 << 10, getter);

    // First access: should trigger getter
    for (const auto& [key, expected] : db) {
        auto [view, err] = group->Get(key);
        ASSERT_TRUE(err.empty()) << "Failed to get key " << key << ": " << err;
        EXPECT_EQ(view.String(), expected);
    }

    // Second access: should hit cache (no additional getter calls)
    int prev_count = load_counts.load();
    for (const auto& [key, expected] : db) {
        auto [view, err] = group->Get(key);
        ASSERT_TRUE(err.empty());
        EXPECT_EQ(view.String(), expected);
    }
    EXPECT_EQ(load_counts.load(), prev_count);

    // Unknown key
    auto [view, err] = group->Get("unknown");
    EXPECT_FALSE(err.empty());
}

TEST_F(GeeCacheTest, ConcurrentCacheTtlExpiresEntries) {
    geecache::ConcurrentCache cache(0, std::chrono::milliseconds(30));
    cache.Add("Tom", geecache::ByteView("630"));

    auto [first, ok1] = cache.Get("Tom");
    ASSERT_TRUE(ok1);
    EXPECT_EQ(first.String(), "630");

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto [expired, ok2] = cache.Get("Tom");
    EXPECT_FALSE(ok2);
    EXPECT_TRUE(expired.String().empty());

    auto [missing, ok3] = cache.Get("Tom");
    EXPECT_FALSE(ok3);
    EXPECT_TRUE(missing.String().empty());
}

TEST_F(GeeCacheTest, GetGroup) {
    auto getter = std::make_shared<geecache::GetterFunc>(
        [](const std::string& key) -> std::pair<std::string, std::string> {
            return {key, ""};
        });

    auto group = geecache::Group::NewGroup("test_group", 1024, getter);
    EXPECT_EQ(group->Name(), "test_group");

    auto found = geecache::Group::GetGroup("test_group");
    EXPECT_NE(found, nullptr);
    EXPECT_EQ(found->Name(), "test_group");

    auto not_found = geecache::Group::GetGroup("nonexistent");
    EXPECT_EQ(not_found, nullptr);
}

TEST_F(GeeCacheTest, CacheAsideInvalidate) {
    std::unordered_map<std::string, std::string> db = {
        {"Tom", "630"},
    };
    std::atomic<int> load_count{0};

    auto getter = std::make_shared<geecache::GetterFunc>(
        [&db, &load_count](const std::string& key) -> std::pair<std::string, std::string> {
            auto it = db.find(key);
            if (it == db.end()) {
                return {"", "key not found: " + key};
            }
            load_count++;
            return {it->second, ""};
        });

    auto group = geecache::Group::NewGroup("scores", 2 << 10, getter);

    auto [first, err1] = group->Get("Tom");
    ASSERT_TRUE(err1.empty());
    EXPECT_EQ(first.String(), "630");

    db["Tom"] = "631";
    ASSERT_TRUE(group->Invalidate("Tom").empty());

    auto [second, err2] = group->Get("Tom");
    ASSERT_TRUE(err2.empty());
    EXPECT_EQ(second.String(), "631");
    EXPECT_EQ(load_count.load(), 2);
}

TEST_F(GeeCacheTest, CacheAsideSetWritesThenInvalidates) {
    std::unordered_map<std::string, std::string> db = {
        {"Tom", "630"},
    };
    std::atomic<int> load_count{0};

    auto getter = std::make_shared<geecache::GetterFunc>(
        [&db, &load_count](const std::string& key) -> std::pair<std::string, std::string> {
            auto it = db.find(key);
            if (it == db.end()) {
                return {"", "key not found: " + key};
            }
            load_count++;
            return {it->second, ""};
        });
    auto setter = std::make_shared<geecache::SetterFunc>(
        [&db](const std::string& key, const std::string& value) -> std::string {
            db[key] = value;
            return "";
        });

    geecache::GroupOptions options;
    options.setter = setter;

    auto group = geecache::Group::NewGroup("scores", 2 << 10, getter, options);

    auto [first, err1] = group->Get("Tom");
    ASSERT_TRUE(err1.empty());
    EXPECT_EQ(first.String(), "630");

    ASSERT_TRUE(group->Set("Tom", "632").empty());

    auto [second, err2] = group->Get("Tom");
    ASSERT_TRUE(err2.empty());
    EXPECT_EQ(second.String(), "632");
    EXPECT_EQ(load_count.load(), 2);
}

TEST_F(GeeCacheTest, CacheTtlExpiresAndReloadsFromSource) {
    std::unordered_map<std::string, std::string> db = {
        {"Tom", "630"},
    };
    std::atomic<int> load_count{0};

    auto getter = std::make_shared<geecache::GetterFunc>(
        [&db, &load_count](const std::string& key) -> std::pair<std::string, std::string> {
            auto it = db.find(key);
            if (it == db.end()) {
                return {"", "key not found: " + key};
            }
            load_count++;
            return {it->second, ""};
        });

    geecache::GroupOptions options;
    options.cache_ttl = std::chrono::milliseconds(30);

    auto group = geecache::Group::NewGroup("scores", 2 << 10, getter, options);

    auto [first, err1] = group->Get("Tom");
    ASSERT_TRUE(err1.empty());
    EXPECT_EQ(first.String(), "630");

    auto [second, err2] = group->Get("Tom");
    ASSERT_TRUE(err2.empty());
    EXPECT_EQ(second.String(), "630");
    EXPECT_EQ(load_count.load(), 1);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    db["Tom"] = "631";

    auto [third, err3] = group->Get("Tom");
    ASSERT_TRUE(err3.empty());
    EXPECT_EQ(third.String(), "631");
    EXPECT_EQ(load_count.load(), 2);
}

TEST_F(GeeCacheTest, VersionGuardPreventsStaleBackfill) {
    std::mutex mu;
    std::condition_variable cv;
    bool first_started = false;
    bool release_first = false;
    std::string db_value = "old";
    std::atomic<int> load_count{0};

    auto getter = std::make_shared<geecache::GetterFunc>(
        [&](const std::string& key) -> std::pair<std::string, std::string> {
            if (key != "Tom") {
                return {"", "key not found: " + key};
            }

            int call = ++load_count;
            if (call == 1) {
                std::string snapshot = db_value;
                {
                    std::lock_guard<std::mutex> lock(mu);
                    first_started = true;
                }
                cv.notify_all();

                std::unique_lock<std::mutex> lock(mu);
                cv.wait(lock, [&] { return release_first; });
                return {snapshot, ""};
            }
            return {db_value, ""};
        });

    auto group = geecache::Group::NewGroup("scores", 2 << 10, getter);

    std::string result;
    std::string err;
    std::thread reader([&] {
        auto [view, get_err] = group->Get("Tom");
        result = view.String();
        err = get_err;
    });

    {
        std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock, [&] { return first_started; });
    }

    db_value = "new";
    ASSERT_TRUE(group->Invalidate("Tom").empty());

    {
        std::lock_guard<std::mutex> lock(mu);
        release_first = true;
    }
    cv.notify_all();
    reader.join();

    ASSERT_TRUE(err.empty()) << err;
    EXPECT_EQ(result, "new");
    EXPECT_EQ(load_count.load(), 2);

    auto [view, final_err] = group->Get("Tom");
    ASSERT_TRUE(final_err.empty());
    EXPECT_EQ(view.String(), "new");
    EXPECT_EQ(load_count.load(), 2);
}

TEST_F(GeeCacheTest, InvalidateBroadcastIsBestEffortAcrossPeers) {
    auto getter = std::make_shared<geecache::GetterFunc>(
        [](const std::string&) -> std::pair<std::string, std::string> {
            return {"value", ""};
        });
    auto group = geecache::Group::NewGroup("scores", 2 << 10, getter);

    auto failing_peer =
        std::make_shared<RecordingPeerGetter>("peer unavailable");
    auto succeeding_peer = std::make_shared<RecordingPeerGetter>();
    auto peers = std::make_shared<StaticPeerSet>(
        std::vector<std::shared_ptr<geecache::PeerGetter>>{
            failing_peer,
            succeeding_peer,
        });
    group->RegisterPeers(peers);

    std::string err = group->Invalidate("Tom");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("peer unavailable"), std::string::npos);
    EXPECT_EQ(failing_peer->invalidate_calls(), 1);
    EXPECT_EQ(succeeding_peer->invalidate_calls(), 1);
}
