# Generate protobuf sources
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${CMAKE_SOURCE_DIR}/proto/geecachepb.proto)

add_library(geecache_lib STATIC
    byteview.cpp
    cache.cpp
    geecache.cpp
    http.cpp
    consistenthash/consistenthash.cpp
    singleflight/singleflight.cpp
    ${PROTO_SRCS}
    ${PROTO_HDRS}
)

target_include_directories(geecache_lib
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_BINARY_DIR}
        ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(geecache_lib
    PUBLIC
        ${Protobuf_LIBRARIES}
        ZLIB::ZLIB
        Threads::Threads
        httplib
)
