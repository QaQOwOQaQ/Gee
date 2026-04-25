cmake_minimum_required(VERSION 3.14)
project(geerpc_cpp VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ── 依赖：Protobuf ──────────────────────────────────────────────────────────
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

# ── 生成 protobuf 代码 ────────────────────────────────────────────────────────
set(PROTO_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(PROTO_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto_gen)
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS
    ${PROTO_SRC_DIR}/geerpc.proto
)

# ── 依赖：Threads ─────────────────────────────────────────────────────────────
find_package(Threads REQUIRED)

# ── 头文件搜索路径 ─────────────────────────────────────────────────────────────
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}      # for generated proto headers
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party  # for cpp-httplib
)

# ── 库源文件 ──────────────────────────────────────────────────────────────────
set(GEERPC_SOURCES
    ${PROTO_SRCS}
    src/codec/protobuf_codec.cpp
    src/server/service.cpp
    src/server/server.cpp
    src/client/client.cpp
    src/client/xclient.cpp
    src/registry/registry.cpp
)

add_library(geerpc ${GEERPC_SOURCES})
target_link_libraries(geerpc
    PUBLIC
        ${Protobuf_LIBRARIES}
        Threads::Threads
)
target_include_directories(geerpc PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party
    ${Protobuf_INCLUDE_DIRS}
)

# ── 示例可执行文件 ─────────────────────────────────────────────────────────────
add_executable(geerpc_example examples/main.cpp)
target_link_libraries(geerpc_example PRIVATE geerpc)

# ── 测试（可选，后续添加）────────────────────────────────────────────────────────
option(BUILD_TESTS "Build unit tests" OFF)
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
