cmake_minimum_required(VERSION 3.16)
set(TPCCBENCH_DESCRIPTION "tpcc-bench is a portable tpcc benchmark framework for various transaction engines")
project(
  tpcc-bench
  VERSION 1.0.0.0
  DESCRIPTION "${TPCCBENCH_DESCRIPTION}"
  )

message(STATUS "Welcome to tpcc-bench, Version: ${PROJECT_VERSION}")

add_definitions("-Wall -std=c++17")

add_executable(main
  test/main.cpp
  test/db_wrapper.cpp
  test/initializer.cpp
  src/record_generator.cpp
  )

target_include_directories(main PUBLIC
  ${PROJECT_SOURCE_DIR}/include
  ${PROJECT_SOURCE_DIR}/test
  )
