cmake_minimum_required(VERSION 3.16)
set(TPCCBENCH_DESCRIPTION "tpcc-bench is a portable tpcc benchmark framework for various transaction engines")
project(
  tpcc-bench
  VERSION 1.0.0.0
  DESCRIPTION "${TPCCBENCH_DESCRIPTION}"
  )

message(STATUS "Welcome to tpcc-bench, Version: ${PROJECT_VERSION}")


###############################################################################
#                            Static library of tpcc                           #
###############################################################################

set(TPCCBENCH_COMPILE_DEFINITIONS "")
set(TPCCBENCH_COMPILE_OPTIONS "")
set(TPCCBENCH_COMPILE_FEATURES "cxx_std_17")
set(TPCCBENCH_LINK_OPTIONS "")
set(TPCCBENCH_INCLUDE_DIRECTORIES "${PROJECT_SOURCE_DIR}/include")

if (APPLE)
  list(APPEND TPCCBENCH_COMPILE_OPTIONS "-fvisibility=hidden")
  list(APPEND TPCCBENCH_LINK_OPTIONS "-fvisibility=hidden")
endif ()

# Create an objectlib, which can be used for creating both static and dynamic library
file(GLOB_RECURSE
  TPCCBENCH_SRCS
  ${PROJECT_SOURCE_DIR}/src/*.cpp
  ${PROJECT_SORUCE_DIR}/include/*.hpp
  )

add_library(tpccbench_objlib OBJECT ${TPCCBENCH_SRCS})

target_compile_definitions(tpccbench_objlib PUBLIC ${TPCCBENCH_COMPILE_DEFINITIONS})
target_compile_options(tpccbench_objlib PUBLIC ${TPCCBENCH_COMPILE_OPTIONS})
target_compile_features(tpccbench_objlib PUBLIC ${TPCCBENCH_COMPILE_FEATURES})
target_link_options(tpccbench_objlib PUBLIC ${TPCCBENCH_LINK_OPTIONS})
target_include_directories(tpccbench_objlib PUBLIC ${TPCCBENCH_INCLUDE_DIRECTORIES})

target_compile_options(tpccbench_objlib PRIVATE "-fPIC" "-Werror" "-Wall")

# create static library
add_library(tpccbench_static STATIC $<TARGET_OBJECTS:tpccbench_objlib>)

target_link_libraries(tpccbench_static PUBLIC tpccbench_objlib)

set_target_properties(
  tpccbench_static
  PROPERTIES
  ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
  LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
  )

message(STATUS "tpccbench_static created!")

###############################################################################
#                         Static library for glue code                        #
###############################################################################

set(GLUE_COMPILE_DEFINITIONS "")
set(GLUE_COMPILE_OPTIONS "")
set(GLUE_COMPILE_FEATURES "cxx_std_17")
set(GLUE_LINK_OPTIONS "")
set(GLUE_LINK_LIBRARIES "tpccbench_static")
set(GLUE_INCLUDE_DIRECTORIES "${PROJECT_SOURCE_DIR}/app/include")

if (APPLE)
  list(APPEND GLUE_COMPILE_OPTIONS "-fvisibility=hidden")
  list(APPEND GLUE_LINK_OPTIONS "-fvisibility=hidden")
endif ()

file(GLOB_RECURSE
  GLUE_SRCS
  ${PROJECT_SOURCE_DIR}/app/src/*.cpp
  ${PROJECT_SOURCE_DIR}/app/include/*.hpp
  )

add_library(glue_objlib OBJECT ${GLUE_SRCS})

target_compile_definitions(glue_objlib PUBLIC ${GLUE_COMPILE_DEFINITIONS})
target_compile_options(glue_objlib PUBLIC ${GLUE_COMPILE_OPTIONS})
target_compile_features(glue_objlib PUBLIC ${GLUE_COMPILE_FEATURES})
target_link_options(glue_objlib PUBLIC ${GLUE_LINK_OPTIONS})
target_link_libraries(glue_objlib PUBLIC ${GLUE_LINK_LIBRARIES})
target_include_directories(glue_objlib PUBLIC ${GLUE_INCLUDE_DIRECTORIES})

target_compile_options(glue_objlib PRIVATE "-fPIC" "-Werror" "-Wall")

# create static library
add_library(glue_static STATIC $<TARGET_OBJECTS:glue_objlib>)

target_link_libraries(glue_static PUBLIC glue_objlib)

set_target_properties(
  glue_static
  PROPERTIES
  ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
  LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
  )

message(STATUS "glue_static created!")

###############################################################################
#                                 Test binaries                                #
###############################################################################

# Ref: https://google.github.io/googletest/quickstart-cmake.html
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

include(GoogleTest)

# test for tpcc

file(GLOB_RECURSE TPCC_TESTS "${PROJECT_SOURCE_DIR}/test/*.cpp")
foreach (TPCC_TEST ${TPCC_TESTS})
  get_filename_component(FILENAME ${TPCC_TEST} NAME_WE)
  if (${FILENAME} MATCHES "test")
    add_executable(${FILENAME} ${TPCC_TEST})
    gtest_discover_tests(${FILENAME})
    target_compile_options(${FILENAME} PRIVATE "-Werror" "-Wall")
    target_include_directories(${FILENAME} PRIVATE "${PROJECT_SOURCE_DIR}/test")
    target_link_libraries(${FILENAME} tpccbench_static gtest_main)
    set_target_properties(${FILENAME} PROPERTIES CXX_EXTENTIONS OFF ENABLE_EXPORTS ON)
    set_target_properties(
      ${FILENAME}
      PROPERTIES
      RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests/tpcctest"
      )
  endif ()
endforeach (TPCC_TEST ${TPCC_TESTS})

set(STATUS "tpccbench tests created!")

# test for glue code

file(GLOB_RECURSE GLUE_TESTS "${PROJECT_SOURCE_DIR}/app/test/*.cpp")
foreach (GLUE_TEST ${GLUE_TESTS})
  get_filename_component(FILENAME ${GLUE_TEST} NAME_WE)
  if (${FILENAME} MATCHES "test")
    add_executable(${FILENAME} ${GLUE_TEST})
    gtest_discover_tests(${FILENAME})
    target_compile_options(${FILENAME} PRIVATE "-Werror" "-Wall")
    target_include_directories(${FILENAME} PRIVATE "${PROJECT_SOURCE_DIR}/app/test")
    target_link_libraries(${FILENAME} glue_static gtest_main)
    set_target_properties(${FILENAME} PROPERTIES CXX_EXTENTIONS OFF ENABLE_EXPORTS ON)
    set_target_properties(
      ${FILENAME}
      PROPERTIES
      RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests/gluetest"
      )
  endif ()
endforeach (GLUE_TEST ${GLUE_TESTS})

set(STATUS "gluecode tests created!")

###############################################################################
#                                 Clang-format                                #
###############################################################################

set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build_support")

find_program(CLANG_FORMAT_BIN clang-format)
if ("${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND")
  message(STATUS "[MISSING] clang-format not found, no check-format")
else()
  
  # target format is only build when `make format` is issued
  add_custom_target(format
    COMMENT "Running: format"
    COMMAND ${BUILD_SUPPORT_DIR}/run-clang-format.py
    --clang-format-executable ${CLANG_FORMAT_BIN}
    -r ${CMAKE_CURRENT_SOURCE_DIR}/app
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/test
    -i                          # format file instead of printing diff
    -q
    USES_TERMINAL
    )

  add_custom_target(check-format
    COMMENT "Running: check-format"
    COMMAND ${BUILD_SUPPORT_DIR}/run-clang-format.py
    --clang-format-executable ${CLANG_FORMAT_BIN}
    -r ${CMAKE_CURRENT_SOURCE_DIR}/app
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/test
    USES_TERMINAL
    )

  add_custom_command(TARGET format DEPENDS gtest_main)
  add_custom_command(TARGET check-format DEPENDS gtest_main)
  
  message(STATUS "[ADDED] format and check-format (${CLANG_FORMAT_BIN})")
  unset(FORMAT_DIRS)
  
endif()
unset(CLANG_FORMAT_BIN)

###############################################################################
#             Apply +x permissions to all scripts in build_support            #
###############################################################################

file(GLOB_RECURSE
        BUILD_SUPPORT_SCRIPTS
        CONFIGURE_DEPENDS
        ${BUILD_SUPPORT_DIR}/*.py
        )

foreach (_var IN LISTS BUILD_SUPPORT_SCRIPTS)
    execute_process(COMMAND chmod +x "${_var}")
endforeach ()
