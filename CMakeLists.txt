cmake_minimum_required(VERSION 3.5.1)
project(bigdata_race)

set(CMAKE_CXX_STANDARD 14)

SET (CMAKE_CXX_FLAGS "-std=c++11 -lcurl -msse2 -mcx16 -Wno-invalid-offsetof -Wno-deprecated-register -Wno-null-dereference" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_DEBUG "-O0 -g" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_RELEASE "-O3" CACHE INTERNAL "compiler options" FORCE)

IF (NOT CMAKE_BUILD_TYPE)
  SET (CMAKE_BUILD_TYPE "Release")
ENDIF ()
MESSAGE (STATUS "Build type: ${CMAKE_BUILD_TYPE}")

add_executable(
  bigdata_race main.cpp
  src/Constants.h
  src/Executor.h
  src/Utils.h
  src/Tables.cc
)

include_directories(src/sparsepp)

find_package(Threads REQUIRED)
target_link_libraries(bigdata_race Threads::Threads)
