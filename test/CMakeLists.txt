cmake_minimum_required(VERSION 3.5.1)
project(test_avx)

set(CMAKE_CXX_STANDARD 14)

SET (CMAKE_CXX_FLAGS "-std=c++11 -lcurl -msse2 -march=haswell -O3 -mcx16 -Wno-invalid-offsetof -Wno-deprecated-register -Wno-null-dereference" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_DEBUG "-O0 -g" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_RELEASE "-O3" CACHE INTERNAL "compiler options" FORCE)

#IF (NOT CMAKE_BUILD_TYPE)
    SET (CMAKE_BUILD_TYPE "Release")
#ENDIF ()
MESSAGE (STATUS "Build type of test_avx: ${CMAKE_BUILD_TYPE}")

add_executable(test_avx
        main.cpp
        avx.h
)
