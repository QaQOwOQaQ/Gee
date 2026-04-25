cmake_minimum_required(VERSION 3.14)
project(geecache VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# --- Dependencies ---
find_package(Protobuf REQUIRED)
find_package(ZLIB REQUIRED)
find_package(Threads REQUIRED)

# cpp-httplib (header-only, vendored)
add_library(httplib INTERFACE)
target_include_directories(httplib INTERFACE ${CMAKE_SOURCE_DIR}/third_party/cpp-httplib)

# --- Library ---
add_subdirectory(src)

# --- Tests ---
enable_testing()
add_subdirectory(tests)

option(GEECACHE_BUILD_BENCHMARKS "Build benchmark targets" ON)
if(GEECACHE_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()
