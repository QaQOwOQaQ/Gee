#include <benchmark/benchmark.h>

#include <any>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <thread>
#include <utility>

#include "cache.h"
#include "geecache.h"
#include "singleflight/singleflight.h"

namespace {

std::string MakeValue(std::size_t bytes) {
    return std::string(bytes, 'x');
}

void BM_ConcurrentCacheGetHit(benchmark::State& state) {
    const auto value_size = static_cast<std::size_t>(state.range(0));
    geecache::ConcurrentCache cache(1 << 20);
    const std::string key = "hot-key";
    cache.Add(key, geecache::ByteView(MakeValue(value_size)));

    for (auto _ : state) {
        auto [view, ok] = cache.Get(key);
        if (!ok) {
            state.SkipWithError("cache miss");
            break;
        }
        benchmark::DoNotOptimize(view);
    }

    state.SetBytesProcessed(
        static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(value_size));
}

void BM_GroupGetCacheHit(benchmark::State& state) {
    const auto value_size = static_cast<std::size_t>(state.range(0));
    std::atomic<int64_t> backend_loads{0};

    auto getter = std::make_shared<geecache::GetterFunc>(
        [&backend_loads, value_size](const std::string&) -> std::pair<std::string, std::string> {
            backend_loads.fetch_add(1, std::memory_order_relaxed);
            return {MakeValue(value_size), ""};
        });

    auto group = geecache::Group::NewStandaloneGroup("bench-hit", 1 << 20, getter);
    auto [warm_value, warm_err] = group->Get("hot-key");
    if (!warm_err.empty()) {
        state.SkipWithError(warm_err.c_str());
        return;
    }
    benchmark::DoNotOptimize(warm_value);
    backend_loads.store(0, std::memory_order_relaxed);

    for (auto _ : state) {
        auto [view, err] = group->Get("hot-key");
        if (!err.empty()) {
            state.SkipWithError(err.c_str());
            break;
        }
        benchmark::DoNotOptimize(view);
    }

    state.counters["backend_loads"] = static_cast<double>(backend_loads.load());
    state.SetBytesProcessed(
        static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(value_size));
}

void BM_GroupGetCacheMiss(benchmark::State& state) {
    const auto value_size = static_cast<std::size_t>(state.range(0));
    std::atomic<int64_t> backend_loads{0};

    auto getter = std::make_shared<geecache::GetterFunc>(
        [&backend_loads, value_size](const std::string&) -> std::pair<std::string, std::string> {
            backend_loads.fetch_add(1, std::memory_order_relaxed);
            return {MakeValue(value_size), ""};
        });

    auto group = geecache::Group::NewStandaloneGroup("bench-miss", 1 << 20, getter);
    const std::string key = "cold-key";

    for (auto _ : state) {
        state.PauseTiming();
        auto invalidate_err = group->Invalidate(key);
        if (!invalidate_err.empty()) {
            state.SkipWithError(invalidate_err.c_str());
            return;
        }
        state.ResumeTiming();

        auto [view, err] = group->Get(key);
        if (!err.empty()) {
            state.SkipWithError(err.c_str());
            break;
        }
        benchmark::DoNotOptimize(view);
    }

    state.counters["backend_loads"] = static_cast<double>(backend_loads.load());
    state.SetBytesProcessed(
        static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(value_size));
}

void RunSingleflightFanIn(benchmark::State& state, bool same_key) {
    const int concurrency = static_cast<int>(state.range(0));
    int64_t total_backend_calls = 0;

    for (auto _ : state) {
        geecache::singleflight::Group group;
        std::atomic<int64_t> backend_calls{0};
        std::vector<std::thread> threads;
        threads.reserve(static_cast<std::size_t>(concurrency));
        std::mutex start_mu;
        std::condition_variable start_cv;
        int ready = 0;
        bool go = false;

        for (int i = 0; i < concurrency; ++i) {
            threads.emplace_back([&, i]() {
                {
                    std::unique_lock<std::mutex> lock(start_mu);
                    ++ready;
                    if (ready == concurrency) {
                        go = true;
                        start_cv.notify_all();
                    } else {
                        start_cv.wait(lock, [&go] { return go; });
                    }
                }

                const std::string key = same_key ? "shared-key" : "key-" + std::to_string(i);
                auto [value, err] = group.Do(
                    key,
                    [&backend_calls]() -> std::pair<std::any, std::string> {
                        backend_calls.fetch_add(1, std::memory_order_relaxed);
                        std::this_thread::sleep_for(std::chrono::microseconds(50));
                        return {std::any(std::string("value")), ""};
                    });
                if (!err.empty()) {
                    state.SkipWithError(err.c_str());
                    return;
                }
                benchmark::DoNotOptimize(value);
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }
        total_backend_calls += backend_calls.load(std::memory_order_relaxed);
    }

    state.counters["backend_calls_per_round"] =
        static_cast<double>(total_backend_calls) / static_cast<double>(state.iterations());
    state.counters["fan_in"] = static_cast<double>(concurrency);
}

void BM_SingleflightFanInDistinctKeys(benchmark::State& state) {
    RunSingleflightFanIn(state, false);
}

void BM_SingleflightFanInSameKey(benchmark::State& state) {
    RunSingleflightFanIn(state, true);
}

BENCHMARK(BM_ConcurrentCacheGetHit)->Arg(64)->Arg(512)->Arg(4096);
BENCHMARK(BM_GroupGetCacheHit)->Arg(128);
BENCHMARK(BM_GroupGetCacheMiss)->Arg(128);
BENCHMARK(BM_SingleflightFanInDistinctKeys)->Arg(1)->Arg(4)->Arg(8)->UseRealTime();
BENCHMARK(BM_SingleflightFanInSameKey)->Arg(1)->Arg(4)->Arg(8)->UseRealTime();

}  // namespace

BENCHMARK_MAIN();
