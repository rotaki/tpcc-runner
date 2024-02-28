cmake_minimum_required(VERSION 3.16)
set(TPCCRUNNER_DESCRIPTION "tpcc-runner is a portable TPC-C benchmark framework for various in-memory transaction engines")
project(
  tpcc-runner
  VERSION 1.0.0.0
  DESCRIPTION "${TPCCRUNNER_DESCRIPTION}"
  )

message(STATUS "Welcome to tpcc-runner, Version: ${PROJECT_VERSION}")
set(CMAKE_C_COMPILER "gcc" CACHE STRING "C compiler")
set(CMAKE_CXX_COMPILER "g++" CACHE STRING "C++ compiler")
message(STATUS "C Compiler: ${CMAKE_C_COMPILER} ${CMAKE_C_COMPILER_ID} ${CMAKE_C_COMPILER_VERSION}")
message(STATUS "C++ Compiler: ${CMAKE_CXX_COMPILER} ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "System: ${CMAKE_SYSTEM_NAME} ${CMAKE_SYSTEM_VERSION} ${CMAKE_SYSTEM_PROCESSOR}")

function(print_sys_info QUERY_TARGET)
    cmake_host_system_information(RESULT SYS_INFO QUERY ${QUERY_TARGET})
    message(STATUS "  System ${QUERY_TARGET}: ${SYS_INFO}")
endfunction()
print_sys_info("NUMBER_OF_LOGICAL_CORES;NUMBER_OF_PHYSICAL_CORES")
print_sys_info("HOSTNAME;FQDN")
print_sys_info("AVAILABLE_VIRTUAL_MEMORY;TOTAL_VIRTUAL_MEMORY")
print_sys_info("AVAILABLE_PHYSICAL_MEMORY;TOTAL_PHYSICAL_MEMORY")
print_sys_info("IS_64BIT;HAS_IA64")
print_sys_info("HAS_FPU;HAS_MMX;HAS_MMX_PLUS")
print_sys_info("HAS_SSE;HAS_SSE2;HAS_SSE_FP;HAS_SSE_MMX")
print_sys_info("HAS_AMD_3DNOW;HAS_AMD_3DNOW_PLUS")
print_sys_info("HAS_SERIAL_NUMBER;PROCESSOR_SERIAL_NUMBER")
print_sys_info("PROCESSOR_NAME;PROCESSOR_DESCRIPTION")
print_sys_info("OS_NAME;OS_RELEASE;OS_VERSION;OS_PLATFORM")

# Set a default build type if none was specified
set(default_build_type "Release")
if(EXISTS "${CMAKE_SOURCE_DIR}/.git")
  set(default_build_type "Debug")
endif()

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${default_build_type}' as none was specified.")
  set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE
      STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
    "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

set(CMAKE_CXX_FLAGS_DEBUG          "-O0 -g")
set(CMAKE_CXX_FLAGS_MINSIZEREL     "-Os -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE        "-Ofast -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-Ofast -DNDEBUG -g")
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

list(APPEND COMMON_COMPILE_FLAGS "-Werror" "-Wall" "-Wextra" "-fPIC")
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # For language server.

set(LOG_LEVEL "1" CACHE STRING "0(off) 1(error) 2(warn) 3(into) 4(debug) 5(trace)")
set_property(CACHE LOG_LEVEL PROPERTY STRINGS "0" "1" "2" "3" "4" "5")

set(LOG_LEVEL_DEFINITION "-DLOG_LEVEL=${LOG_LEVEL}")
if ("${LOG_LEVEL}" STREQUAL "0")
  message(STATUS "LOG_LEVEL: ${LOG_LEVEL} (NONE)")
elseif ("${LOG_LEVEL}" STREQUAL "1")
  message(STATUS "LOG_LEVEL: ${LOG_LEVEL} (ERROR)")
elseif ("${LOG_LEVEL}" STREQUAL "2")
  message(STATUS "LOG_LEVEL: ${LOG_LEVEL} (WARN)")
elseif ("${LOG_LEVEL}" STREQUAL "3")
  message(STATUS "LOG_LEVEL: ${LOG_LEVEL} (INFO)")
elseif ("${LOG_LEVEL}" STREQUAL "4")
  message(STATUS "LOG_LEVEL: ${LOG_LEVEL} (DEBUG)")
elseif ("${LOG_LEVEL}" STREQUAL "5")
  message(STATUS "LOG_LEVEL: ${LOG_LEVEL} (TRACE)")
else()
  message(STATUS "LOG_LEVEL: ${LOG_LEVEL} (UNKWN)")
endif()

include(FetchContent)
function(add_dep NAME GIT_URL GIT_TAG)
    string(TOLOWER "${NAME}" NAME_LOWER)
    FetchContent_Declare(${NAME})
    if (NOT ${NAME}_POPULATED)
        FetchContent_Populate(${NAME}
                QUIET
                SOURCE_DIR ${CMAKE_BINARY_DIR}/_deps/src/${NAME}
                BINARY_DIR ${CMAKE_BINARY_DIR}/_deps/build/${NAME}
                SUBBUILD_DIR ${CMAKE_BINARY_DIR}/_deps/sub/${NAME}
                GIT_REPOSITORY ${GIT_URL}
                GIT_TAG ${GIT_TAG}
                )
        
        set(DEP_CUSTOM_CMAKELISTS ${PROJECT_SOURCE_DIR}/third_party/deps_override/${NAME}_CMakeLists.txt)
        if (EXISTS ${DEP_CUSTOM_CMAKELISTS})
            file(COPY ${DEP_CUSTOM_CMAKELISTS} DESTINATION ${${NAME_LOWER}_SOURCE_DIR})
            file(RENAME ${${NAME_LOWER}_SOURCE_DIR}/${NAME}_CMakeLists.txt ${${NAME_LOWER}_SOURCE_DIR}/CMakeLists.txt)
            message(STATUS "Copied ${DEP_CUSTOM_CMAKELISTS} -> ${${NAME_LOWER}_SOURCE_DIR}/CMakeLists.txt.")
        endif ()
        unset(DEP_CUSTOM_CMAKELISTS)

        add_subdirectory(${${NAME_LOWER}_SOURCE_DIR} ${${NAME_LOWER}_BINARY_DIR})
    endif ()

    execute_process(COMMAND git log -1 --format=%cd --date=short
            WORKING_DIRECTORY ${${NAME_LOWER}_SOURCE_DIR}
            OUTPUT_VARIABLE GIT_LAST_COMMIT)
    string(STRIP "${GIT_LAST_COMMIT}" GIT_LAST_COMMIT)

    message(STATUS "[FOUND] ${NAME} (${GIT_URL} ${GIT_TAG} ${GIT_LAST_COMMIT})")
endfunction()

###############################################################################
#                          Bench Specific Parameters                          #
###############################################################################

set(BENCHMARK "TPCC" CACHE STRING "TPCC, YCSB")
set_property(CACHE BENCHMARK PROPERTY STRINGS "TPCC" "YCSB")

string(TOLOWER "${BENCHMARK}" BENCH_NAME)
# Benchmark Specific Parameters
file(GLOB_RECURSE BENCH_SRCS
"${PROJECT_SOURCE_DIR}/benchmarks/${BENCH_NAME}/*.hpp"
"${PROJECT_SOURCE_DIR}/benchmarks/${BENCH_NAME}/*.cpp"
)

set(BENCHMARK_DEFINITION "")
if (BENCHMARK STREQUAL "YCSB")
  set(PAYLOAD_SIZE 1024 CACHE STRING "Choose payload size for YCSB")
  list(APPEND BENCHMARK_DEFINITION "-DPAYLOAD_SIZE=${PAYLOAD_SIZE}")
endif()

###############################################################################
#                            CC Specific Parameters                           #
###############################################################################

set(CC_ALG "NAIVE" CACHE STRING "Choose CC Algorithm: NAIVE, SILO, NOWAIT, MVTO, WAITDIE, CARACAL, SERVAL")
set_property(CACHE CC_ALG PROPERTY STRINGS "NAIVE" "SILO" "NOWAIT" "MVTO" "WAITDIE" "CARACAL" "SERVAL")

set(CC_LINK_LIBRARIES "")
set(CC_INCLUDE_DIRECTORIES "")
  
if ("${CC_ALG}" STREQUAL "NAIVE")
  set(CMAKE_CXX_STANDARD 20)
elseif ("${CC_ALG}" STREQUAL "SILO")
  set(CMAKE_CXX_STANDARD 17)
  list(APPEND CC_LINK_LIBRARIES "masstree")
  add_dep(masstree https://github.com/wattlebirdaz/masstree-beta.git master)
  list(APPEND CC_INCLUDE_DIRECTORIES "${CMAKE_BINARY_DIR}/_deps/src/") # masstree
elseif ("${CC_ALG}" STREQUAL "NOWAIT")
  set(CMAKE_CXX_STANDARD 17)
  list(APPEND CC_LINK_LIBRARIES "masstree")
  add_dep(masstree https://github.com/wattlebirdaz/masstree-beta.git master)
  list(APPEND CC_INCLUDE_DIRECTORIES "${CMAKE_BINARY_DIR}/_deps/src/") # masstree
elseif ("${CC_ALG}" STREQUAL "MVTO")
  set(CMAKE_CXX_STANDARD 17)
  list(APPEND CC_LINK_LIBRARIES "masstree")
  add_dep(masstree https://github.com/wattlebirdaz/masstree-beta.git MVTO) # MVTO branch
  list(APPEND CC_INCLUDE_DIRECTORIES "${CMAKE_BINARY_DIR}/_deps/src/") # masstree
elseif ("${CC_ALG}" STREQUAL "WAITDIE")
  set(CMAKE_CXX_STANDARD 17)
  list(APPEND CC_LINK_LIBRARIES "masstree")
  add_dep(masstree https://github.com/wattlebirdaz/masstree-beta.git master)
  list(APPEND CC_INCLUDE_DIRECTORIES "${CMAKE_BINARY_DIR}/_deps/src/") # masstree
elseif ("${CC_ALG}" STREQUAL "CARACAL")
  set(CMAKE_CXX_STANDARD 17)
  list(APPEND CC_LINK_LIBRARIES "masstree")
  add_dep(masstree https://github.com/wattlebirdaz/masstree-beta.git master)
  list(APPEND CC_INCLUDE_DIRECTORIES "${CMAKE_BINARY_DIR}/_deps/src/") # masstree
elseif ("${CC_ALG}" STREQUAL "SERVAL")
  set(CMAKE_CXX_STANDARD 17)
  list(APPEND CC_LINK_LIBRARIES "masstree")
  add_dep(masstree https://github.com/wattlebirdaz/masstree-beta.git MVTO) # MVTO branch
  list(APPEND CC_INCLUDE_DIRECTORIES "${CMAKE_BINARY_DIR}/_deps/src/") # masstree
endif()

string(TOLOWER "${CC_ALG}" CC_NAME)
file(GLOB_RECURSE CC_SRCS 
"${PROJECT_SOURCE_DIR}/protocols/common/*.hpp"
"${PROJECT_SOURCE_DIR}/protocols/${BENCH_NAME}_common/*.hpp"
"${PROJECT_SOURCE_DIR}/protocols/${CC_NAME}/include/*.hpp" 
"${PROJECT_SOURCE_DIR}/protocols/${CC_NAME}/${BENCH_NAME}/*.hpp"
)
set(CC_TEST_DIRECTORY "${PROJECT_SOURCE_DIR}/test/${CC_NAME}")

###############################################################################
#                                   Utils                                     #
###############################################################################

file(GLOB_RECURSE UTILS_SRCS
  "${PROJECT_SOURCE_DIR}/utils/*.hpp"
)

###############################################################################
#                       Static library of main project                        #
###############################################################################

# Add common dependencies
add_dep(mimalloc https://github.com/microsoft/mimalloc v1.7.1)
add_dep(googletest https://github.com/google/googletest.git main)

set(TPCCRUNNER_SRCS "")
set(TPCCRUNNER_COMPILE_DEFINITIONS "")
set(TPCCRUNNER_COMPILE_OPTIONS "")
set(TPCCRUNNER_INCLUDE_DIRECTORIES "")
set(TPCCRUNNER_LINK_LIBRARIES "")
set(TPCCRUNNER_LINK_OPTIONS "")

list(APPEND TPCCRUNNER_SRCS "${BENCH_SRCS}" "${CC_SRCS}" "${UTILS_SRCS}")
list(APPEND TPCCRUNNER_COMPILE_DEFINITIONS "${LOG_LEVEL_DEFINITION}")
list(APPEND TPCCRUNNER_COMPILE_OPTIONS "${COMMON_COMPILE_FLAGS}")
list(APPEND TPCCRUNNER_INCLUDE_DIRECTORIES "${PROJECT_SOURCE_DIR}/")
list(APPEND TPCCRUNNER_INCLUDE_DIRECTORIES "${CMAKE_BINARY_DIR}/_deps/src/mimalloc/include")
list(APPEND TPCCRUNNER_INCLUDE_DIRECTORIES "${CMAKE_BINARY_DIR}/_deps/src/")
list(APPEND TPCCRUNNER_INCLUDE_DIRECTORIES "${CC_INCLUDE_DIRECTORIES}")
list(APPEND TPCCRUNNER_LINK_LIBRARIES "mimalloc")
list(APPEND TPCCRUNNER_LINK_LIBRARIES "${CC_LINK_LIBRARIES}")
if (APPLE)
  list(APPEND TPCCRUNNER_COMPILE_OPTIONS "-fvisibility=hidden")
  list(APPEND TPCCRUNNER_LINK_OPTIONS "-fvisibility=hidden")
endif ()

# Create an objectlib, which can be used for creating both static and dynamic library
add_library(tpccrunner_objlib OBJECT "${TPCCRUNNER_SRCS}")
target_compile_definitions(tpccrunner_objlib PUBLIC ${TPCCRUNNER_COMPILE_DEFINITIONS})
target_compile_options(tpccrunner_objlib PUBLIC ${TPCCRUNNER_COMPILE_OPTIONS})
target_compile_options(tpccrunner_objlib PRIVATE ${COMMON_COMPILE_FLAGS})
target_include_directories(tpccrunner_objlib PUBLIC ${TPCCRUNNER_INCLUDE_DIRECTORIES})
target_link_libraries(tpccrunner_objlib PRIVATE ${TPCCRUNNER_LINK_LIBRARIES})
target_link_options(tpccrunner_objlib PUBLIC ${TPCCRUNNER_LINK_OPTIONS})

# create static library
add_library(tpccrunner_static STATIC $<TARGET_OBJECTS:tpccrunner_objlib>)
target_link_libraries(tpccrunner_static PUBLIC tpccrunner_objlib)
set_target_properties(
  tpccrunner_static
  PROPERTIES
  ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
  LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
  )
message(STATUS "[ADDED] tpccrunner_static")

###############################################################################
#                                Main binaries                                #
###############################################################################

set(EXECUTABLE "${PROJECT_SOURCE_DIR}/executables/${BENCH_NAME}_${CC_NAME}.cpp")
if ("${BENCH_NAME}" STREQUAL "ycsb")
  set(FILENAME "${BENCH_NAME}${PAYLOAD_SIZE}_${CC_NAME}")
else ()
  set(FILENAME "${BENCH_NAME}_${CC_NAME}")
endif ()
add_executable(${FILENAME} ${EXECUTABLE})
target_compile_definitions(${FILENAME} PUBLIC "${BENCHMARK_DEFINITION}") 
target_link_options(${FILENAME} PUBLIC "-pthread")
target_compile_options(${FILENAME} PUBLIC "-pthread")
target_compile_options(${FILENAME} PRIVATE ${COMMON_COMPILE_FLAGS})
target_link_libraries(${FILENAME} tpccrunner_static)
set_target_properties(${FILENAME} PROPERTIES CXX_EXTENTIONS OFF ENABLE_EXPORTS ON)
set_target_properties(
  ${FILENAME}
  PROPERTIES
  ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
  LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

###############################################################################
#                                 Test binaries                                #
###############################################################################

# # Ref: https://google.github.io/googletest/quickstart-cmake.html
# enable_testing()

# include(GoogleTest)

# # test for tpcc

# file(GLOB_RECURSE TPCC_TESTS "${CC_TEST_DIRECTORY}/*.cpp")
# foreach (TPCC_TEST ${TPCC_TESTS})
#   get_filename_component(FILENAME ${TPCC_TEST} NAME_WE)
#   if (${FILENAME} MATCHES "test")
#     add_executable(${FILENAME} ${TPCC_TEST})
#     gtest_discover_tests(${FILENAME})
#     target_compile_options(${FILENAME} PRIVATE ${COMMON_COMPILE_FLAGS})
#     target_include_directories(${FILENAME} PRIVATE "${CC_TEST_DIRECTORY}")
#     target_link_libraries(${FILENAME} tpccrunner_static gtest_main)
#     set_target_properties(${FILENAME} PROPERTIES CXX_EXTENTIONS OFF ENABLE_EXPORTS ON)
#     set_target_properties(
#       ${FILENAME}
#       PROPERTIES
#       RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test/${CC_NAME}"
#       )
#   endif ()
# endforeach (TPCC_TEST ${TPCC_TESTS})

# set(STATUS "[ADDED] tpccrunner tests")

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
    -r
    ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
    ${CMAKE_CURRENT_SOURCE_DIR}/test
    ${CMAKE_CURRENT_SOURCE_DIR}/utils
    ${CMAKE_CURRENT_SOURCE_DIR}/executables
    ${CMAKE_CURRENT_SOURCE_DIR}/protocols
    ${CMAKE_CURRENT_SOURCE_DIR}/indexes
    -i                          # format file instead of printing diff
    -q
    USES_TERMINAL
    )

  add_custom_target(check-format
    COMMENT "Running: check-format"
    COMMAND ${BUILD_SUPPORT_DIR}/run-clang-format.py
    --clang-format-executable ${CLANG_FORMAT_BIN}
    -r
    ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks
    ${CMAKE_CURRENT_SOURCE_DIR}/test
    ${CMAKE_CURRENT_SOURCE_DIR}/utils
    ${CMAKE_CURRENT_SOURCE_DIR}/executables
    ${CMAKE_CURRENT_SOURCE_DIR}/protocols
    ${CMAKE_CURRENT_SOURCE_DIR}/indexes
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
