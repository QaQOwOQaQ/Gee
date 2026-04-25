#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "consistenthash/consistenthash.h"
#include "geecache.h"
#include "http.h"

class HTTPTest : public ::testing::Test {
protected:
    void SetUp() override {
        geecache::Group::DestroyAllGroups();
    }
    void TearDown() override {
        geecache::Group::DestroyAllGroups();
    }
};

namespace {

std::string FindKeyOwnedBy(const std::vector<std::string>& peers, const std::string& owner) {
    geecache::consistenthash::Map ring(geecache::kDefaultReplicas);
    ring.Add(peers);
    for (int i = 0; i < 5000; ++i) {
        std::string key = "key_" + std::to_string(i);
        if (ring.Get(key) == owner) {
            return key;
        }
    }
    return "";
}

bool WaitForCondition(const std::function<bool()>& condition,
                      std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (condition()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return condition();
}

}  // namespace

TEST_F(HTTPTest, PeerCommunication) {
    // Create a data source
    std::unordered_map<std::string, std::string> db = {
        {"Tom", "630"},
        {"Jack", "589"},
        {"Sam", "567"},
    };

    auto getter = std::make_shared<geecache::GetterFunc>(
        [&db](const std::string& key) -> std::pair<std::string, std::string> {
            auto it = db.find(key);
            if (it != db.end()) {
                return {it->second, ""};
            }
            return {"", "key not found: " + key};
        });

    auto group = geecache::Group::NewGroup("scores", 2 << 10, getter);

    // Create HTTP pool on a single node (self-serving)
    auto pool = std::make_shared<geecache::HTTPPool>("http://127.0.0.1:9999");
    pool->Set({"http://127.0.0.1:9999"});
    pool->RegisterGroup(group);
    group->RegisterPeers(pool);
    pool->Start();

    // Give server a moment to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Test: Get values through the group (local, since only one node = self)
    auto [view1, err1] = group->Get("Tom");
    ASSERT_TRUE(err1.empty()) << err1;
    EXPECT_EQ(view1.String(), "630");

    auto [view2, err2] = group->Get("Jack");
    ASSERT_TRUE(err2.empty()) << err2;
    EXPECT_EQ(view2.String(), "589");

    // Test: direct HTTP request to the server
    httplib::Client client("127.0.0.1", 9999);
    auto result = client.Get("/_geecache/scores/Sam");
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->status, 200);

    // Parse the protobuf response
    geecachepb::Response resp;
    ASSERT_TRUE(resp.ParseFromString(result->body));
    EXPECT_EQ(resp.value(), "567");

    // Test: 404 for unknown group
    auto result2 = client.Get("/_geecache/unknown_group/key");
    ASSERT_NE(result2, nullptr);
    EXPECT_EQ(result2->status, 404);

    pool->Stop();
}

TEST_F(HTTPTest, MultiNodePeerCommunication) {
    std::vector<std::string> peers = {
        "http://127.0.0.1:9001",
        "http://127.0.0.1:9002",
    };
    std::string key_owned_by_node1 = FindKeyOwnedBy(peers, peers[0]);
    std::string key_owned_by_node2 = FindKeyOwnedBy(peers, peers[1]);

    ASSERT_FALSE(key_owned_by_node1.empty());
    ASSERT_FALSE(key_owned_by_node2.empty());
    ASSERT_NE(key_owned_by_node1, key_owned_by_node2);

    std::unordered_map<std::string, std::string> db1 = {
        {key_owned_by_node1, "node1-value"},
    };
    std::unordered_map<std::string, std::string> db2 = {
        {key_owned_by_node2, "node2-value"},
    };

    auto getter1 = std::make_shared<geecache::GetterFunc>(
        [&db1](const std::string& key) -> std::pair<std::string, std::string> {
            auto it = db1.find(key);
            if (it == db1.end()) {
                return {"", "key not found: " + key};
            }
            return {it->second, ""};
        });
    auto getter2 = std::make_shared<geecache::GetterFunc>(
        [&db2](const std::string& key) -> std::pair<std::string, std::string> {
            auto it = db2.find(key);
            if (it == db2.end()) {
                return {"", "key not found: " + key};
            }
            return {it->second, ""};
        });

    geecache::GroupOptions options;
    options.hot_cache_bytes = 1 << 10;
    options.hot_cache_denominator = 1;

    auto group1 = geecache::Group::NewStandaloneGroup("scores", 2 << 10, getter1, options);
    auto group2 = geecache::Group::NewStandaloneGroup("scores", 2 << 10, getter2, options);

    auto pool1 = std::make_shared<geecache::HTTPPool>(peers[0]);
    auto pool2 = std::make_shared<geecache::HTTPPool>(peers[1]);

    pool1->Set(peers);
    pool2->Set(peers);
    pool1->RegisterGroup(group1);
    pool2->RegisterGroup(group2);
    group1->RegisterPeers(pool1);
    group2->RegisterPeers(pool2);

    pool1->Start();
    pool2->Start();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto [local_view, local_err] = group1->Get(key_owned_by_node1);
    ASSERT_TRUE(local_err.empty()) << local_err;
    EXPECT_EQ(local_view.String(), "node1-value");

    auto [remote_view, remote_err] = group1->Get(key_owned_by_node2);
    ASSERT_TRUE(remote_err.empty()) << remote_err;
    EXPECT_EQ(remote_view.String(), "node2-value");

    pool2->Stop();

    auto [hot_view, hot_err] = group1->Get(key_owned_by_node2);
    ASSERT_TRUE(hot_err.empty()) << hot_err;
    EXPECT_EQ(hot_view.String(), "node2-value");

    pool1->Stop();
}

TEST_F(HTTPTest, InvalidateBroadcastClearsOwnerAndHotCache) {
    std::vector<std::string> peers = {
        "http://127.0.0.1:9101",
        "http://127.0.0.1:9102",
    };
    std::string key_owned_by_node2 = FindKeyOwnedBy(peers, peers[1]);
    ASSERT_FALSE(key_owned_by_node2.empty());

    std::unordered_map<std::string, std::string> db1;
    std::unordered_map<std::string, std::string> db2 = {
        {key_owned_by_node2, "v1"},
    };

    auto getter1 = std::make_shared<geecache::GetterFunc>(
        [&db1](const std::string& key) -> std::pair<std::string, std::string> {
            auto it = db1.find(key);
            if (it == db1.end()) {
                return {"", "key not found: " + key};
            }
            return {it->second, ""};
        });
    auto getter2 = std::make_shared<geecache::GetterFunc>(
        [&db2](const std::string& key) -> std::pair<std::string, std::string> {
            auto it = db2.find(key);
            if (it == db2.end()) {
                return {"", "key not found: " + key};
            }
            return {it->second, ""};
        });

    geecache::GroupOptions options;
    options.hot_cache_bytes = 1 << 10;
    options.hot_cache_denominator = 1;

    auto group1 = geecache::Group::NewStandaloneGroup("scores", 2 << 10, getter1, options);
    auto group2 = geecache::Group::NewStandaloneGroup("scores", 2 << 10, getter2, options);

    auto pool1 = std::make_shared<geecache::HTTPPool>(peers[0]);
    auto pool2 = std::make_shared<geecache::HTTPPool>(peers[1]);

    pool1->Set(peers);
    pool2->Set(peers);
    pool1->RegisterGroup(group1);
    pool2->RegisterGroup(group2);
    group1->RegisterPeers(pool1);
    group2->RegisterPeers(pool2);

    pool1->Start();
    pool2->Start();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto [view1, err1] = group1->Get(key_owned_by_node2);
    ASSERT_TRUE(err1.empty()) << err1;
    EXPECT_EQ(view1.String(), "v1");

    db2[key_owned_by_node2] = "v2";
    ASSERT_TRUE(group1->Invalidate(key_owned_by_node2).empty());

    auto [view2, err2] = group1->Get(key_owned_by_node2);
    ASSERT_TRUE(err2.empty()) << err2;
    EXPECT_EQ(view2.String(), "v2");

    pool2->Stop();
    auto [view3, err3] = group1->Get(key_owned_by_node2);
    ASSERT_TRUE(err3.empty()) << err3;
    EXPECT_EQ(view3.String(), "v2");

    pool1->Stop();
}

TEST_F(HTTPTest, HealthChecksTrackPeerAvailability) {
    std::vector<std::string> peers = {
        "http://127.0.0.1:9201",
        "http://127.0.0.1:9202",
    };
    std::string key_owned_by_node2 = FindKeyOwnedBy(peers, peers[1]);

    ASSERT_FALSE(key_owned_by_node2.empty());

    std::unordered_map<std::string, std::string> db1;
    std::unordered_map<std::string, std::string> db2 = {
        {key_owned_by_node2, "node2-value"},
    };

    auto getter1 = std::make_shared<geecache::GetterFunc>(
        [&db1](const std::string& key) -> std::pair<std::string, std::string> {
            auto it = db1.find(key);
            if (it == db1.end()) {
                return {"", "key not found: " + key};
            }
            return {it->second, ""};
        });
    auto getter2 = std::make_shared<geecache::GetterFunc>(
        [&db2](const std::string& key) -> std::pair<std::string, std::string> {
            auto it = db2.find(key);
            if (it == db2.end()) {
                return {"", "key not found: " + key};
            }
            return {it->second, ""};
        });

    auto group1 = geecache::Group::NewStandaloneGroup("scores", 2 << 10, getter1);
    auto group2 = geecache::Group::NewStandaloneGroup("scores", 2 << 10, getter2);

    auto pool1 = std::make_shared<geecache::HTTPPool>(peers[0]);
    auto pool2 = std::make_shared<geecache::HTTPPool>(peers[1]);

    pool1->Set(peers);
    pool2->Set(peers);
    pool1->SetHealthCheckInterval(std::chrono::milliseconds(50));
    pool2->SetHealthCheckInterval(std::chrono::milliseconds(50));
    pool1->RegisterGroup(group1);
    pool2->RegisterGroup(group2);
    group1->RegisterPeers(pool1);
    group2->RegisterPeers(pool2);

    pool1->Start();
    pool2->Start();

    ASSERT_TRUE(WaitForCondition([&]() { return pool1->IsPeerHealthy(peers[1]); },
                                 std::chrono::milliseconds(1000)));

    auto [peer, ok] = pool1->PickPeer(key_owned_by_node2);
    EXPECT_TRUE(ok);
    EXPECT_NE(peer, nullptr);

    auto [view, err] = group1->Get(key_owned_by_node2);
    ASSERT_TRUE(err.empty()) << err;
    EXPECT_EQ(view.String(), "node2-value");

    pool2->Stop();

    ASSERT_TRUE(WaitForCondition([&]() { return !pool1->IsPeerHealthy(peers[1]); },
                                 std::chrono::milliseconds(1500)));

    auto [dead_peer, dead_ok] = pool1->PickPeer(key_owned_by_node2);
    EXPECT_FALSE(dead_ok);
    EXPECT_EQ(dead_peer, nullptr);

    pool1->Stop();
}

TEST_F(HTTPTest, PeerApplicationErrorsDoNotMarkPeerUnhealthy) {
    std::vector<std::string> peers = {
        "http://127.0.0.1:9401",
        "http://127.0.0.1:9402",
    };
    std::string key_owned_by_node2 = FindKeyOwnedBy(peers, peers[1]);

    ASSERT_FALSE(key_owned_by_node2.empty());

    auto getter1 = std::make_shared<geecache::GetterFunc>(
        [](const std::string& key) -> std::pair<std::string, std::string> {
            return {"", "local miss: " + key};
        });
    auto getter2 = std::make_shared<geecache::GetterFunc>(
        [](const std::string& key) -> std::pair<std::string, std::string> {
            return {"", "key not found: " + key};
        });

    auto group1 = geecache::Group::NewStandaloneGroup("scores", 2 << 10, getter1);
    auto group2 = geecache::Group::NewStandaloneGroup("scores", 2 << 10, getter2);

    auto pool1 = std::make_shared<geecache::HTTPPool>(peers[0]);
    auto pool2 = std::make_shared<geecache::HTTPPool>(peers[1]);

    pool1->Set(peers);
    pool2->Set(peers);
    pool1->SetHealthCheckInterval(std::chrono::milliseconds(50));
    pool2->SetHealthCheckInterval(std::chrono::milliseconds(50));
    pool1->RegisterGroup(group1);
    pool2->RegisterGroup(group2);
    group1->RegisterPeers(pool1);
    group2->RegisterPeers(pool2);

    pool1->Start();
    pool2->Start();

    ASSERT_TRUE(WaitForCondition([&]() { return pool1->IsPeerHealthy(peers[1]); },
                                 std::chrono::milliseconds(1000)));

    auto [view, err] = group1->Get(key_owned_by_node2);
    EXPECT_TRUE(view.String().empty());
    EXPECT_FALSE(err.empty());
    EXPECT_TRUE(pool1->IsPeerHealthy(peers[1]));

    pool2->Stop();
    pool1->Stop();
}

TEST_F(HTTPTest, PeerListCanBeUpdatedAtRuntime) {
    std::vector<std::string> initial_peers = {
        "http://127.0.0.1:9301",
        "http://127.0.0.1:9302",
    };
    std::vector<std::string> updated_peers = {
        "http://127.0.0.1:9301",
        "http://127.0.0.1:9302",
        "http://127.0.0.1:9303",
    };
    std::string key_owned_by_node3 = FindKeyOwnedBy(updated_peers, updated_peers[2]);

    ASSERT_FALSE(key_owned_by_node3.empty());

    std::unordered_map<std::string, std::string> db1;
    std::unordered_map<std::string, std::string> db2;
    std::unordered_map<std::string, std::string> db3 = {
        {key_owned_by_node3, "node3-value"},
    };

    auto getter1 = std::make_shared<geecache::GetterFunc>(
        [&db1](const std::string& key) -> std::pair<std::string, std::string> {
            auto it = db1.find(key);
            if (it == db1.end()) {
                return {"", "key not found: " + key};
            }
            return {it->second, ""};
        });
    auto getter2 = std::make_shared<geecache::GetterFunc>(
        [&db2](const std::string& key) -> std::pair<std::string, std::string> {
            auto it = db2.find(key);
            if (it == db2.end()) {
                return {"", "key not found: " + key};
            }
            return {it->second, ""};
        });
    auto getter3 = std::make_shared<geecache::GetterFunc>(
        [&db3](const std::string& key) -> std::pair<std::string, std::string> {
            auto it = db3.find(key);
            if (it == db3.end()) {
                return {"", "key not found: " + key};
            }
            return {it->second, ""};
        });

    auto group1 = geecache::Group::NewStandaloneGroup("scores", 2 << 10, getter1);
    auto group2 = geecache::Group::NewStandaloneGroup("scores", 2 << 10, getter2);
    auto group3 = geecache::Group::NewStandaloneGroup("scores", 2 << 10, getter3);

    auto pool1 = std::make_shared<geecache::HTTPPool>(updated_peers[0]);
    auto pool2 = std::make_shared<geecache::HTTPPool>(updated_peers[1]);
    auto pool3 = std::make_shared<geecache::HTTPPool>(updated_peers[2]);

    pool1->Set(initial_peers);
    pool2->Set(initial_peers);
    pool1->RegisterGroup(group1);
    pool2->RegisterGroup(group2);
    group1->RegisterPeers(pool1);
    group2->RegisterPeers(pool2);

    pool1->Start();
    pool2->Start();

    auto [missing_view, missing_err] = group1->Get(key_owned_by_node3);
    EXPECT_TRUE(missing_view.String().empty());
    EXPECT_FALSE(missing_err.empty());

    pool3->Set(updated_peers);
    pool3->RegisterGroup(group3);
    group3->RegisterPeers(pool3);
    pool3->Start();

    pool1->Set(updated_peers);
    pool2->Set(updated_peers);
    pool1->SetHealthCheckInterval(std::chrono::milliseconds(50));
    pool2->SetHealthCheckInterval(std::chrono::milliseconds(50));
    pool3->SetHealthCheckInterval(std::chrono::milliseconds(50));

    ASSERT_TRUE(WaitForCondition([&]() { return pool1->IsPeerHealthy(updated_peers[2]); },
                                 std::chrono::milliseconds(1000)));

    auto [view, err] = group1->Get(key_owned_by_node3);
    ASSERT_TRUE(err.empty()) << err;
    EXPECT_EQ(view.String(), "node3-value");

    pool3->Stop();
    pool2->Stop();
    pool1->Stop();
}
