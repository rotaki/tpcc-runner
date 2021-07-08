cmake_minimum_required(VERSION 3.16)
set(MASSTREE_DESCRIPTION "trying to use masstree-beta")
project(
  masstree-beta
  VERSION 1.0
  DESCRIPTION "${MASSTREE_DESCRIPTION}"
  )

# Set a default build type if none was specified
set(DEFAULT_BUILD_TYPE "Release")
set(CMAKE_CXX_FLAGS_RELEASE        "-Ofast -DNDEBUG")
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_EXTENSIONS OFF)
list(APPEND COMMON_COMPILE_FLAGS "")

set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # For language server.

# configure
execute_process(COMMAND sh ./bootstrap.sh WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})
execute_process(COMMAND sh ./configure WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})

add_library(masstree_obj1 OBJECT
"${PROJECT_SOURCE_DIR}/json.cc"
"${PROJECT_SOURCE_DIR}/string.cc"
"${PROJECT_SOURCE_DIR}/straccum.cc"
"${PROJECT_SOURCE_DIR}/str.cc"
"${PROJECT_SOURCE_DIR}/msgpack.cc"
"${PROJECT_SOURCE_DIR}/kvrandom.cc"
"${PROJECT_SOURCE_DIR}/compiler.cc"
"${PROJECT_SOURCE_DIR}/memdebug.cc"
"${PROJECT_SOURCE_DIR}/kvthread.cc")

add_library(masstree_obj2 OBJECT
"${PROJECT_SOURCE_DIR}/clp.c"
)

target_include_directories(masstree_obj1 PUBLIC ${PROJECT_SOURCE_DIR})
target_compile_options(masstree_obj1 PRIVATE -include ${PROJECT_SOURCE_DIR}/config.h)
target_compile_options(masstree_obj1 PRIVATE "${COMMON_COMPILE_FLAGS}")

target_include_directories(masstree_obj2 PUBLIC ${PROJECT_SOURCE_DIR})
target_compile_options(masstree_obj2 PRIVATE "${COMMON_COMPILE_FLAGS}")

add_library(masstree STATIC $<TARGET_OBJECTS:masstree_obj1> $<TARGET_OBJECTS:masstree_obj2>)