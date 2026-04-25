include(FetchContent)

set(GEECACHE_LOCAL_GTEST_DIR "${CMAKE_SOURCE_DIR}/build/_deps/googletest-src")

if(EXISTS "${GEECACHE_LOCAL_GTEST_DIR}/CMakeLists.txt")
    add_subdirectory(
        "${GEECACHE_LOCAL_GTEST_DIR}"
        "${CMAKE_BINARY_DIR}/_deps/googletest-build"
        EXCLUDE_FROM_ALL
    )
else()
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        v1.14.0
    )
    FetchContent_MakeAvailable(googletest)
endif()

function(add_geecache_test TEST_NAME TEST_SRC)
    add_executable(${TEST_NAME} ${TEST_SRC})
    target_link_libraries(${TEST_NAME} geecache_lib GTest::gtest_main)
    add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endfunction()

add_geecache_test(lru_test lru_test.cpp)
add_geecache_test(consistenthash_test consistenthash_test.cpp)
add_geecache_test(singleflight_test singleflight_test.cpp)
add_geecache_test(geecache_test geecache_test.cpp)
add_geecache_test(http_test http_test.cpp)
