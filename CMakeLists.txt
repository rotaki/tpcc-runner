cmake_minimum_required(VERSION 3.16)
set(TPCCRUNNER_DESCRIPTION " is a portable tpcc benchmark framework for various transaction engines")
project(
  tpcc-runner
  VERSION 1.0.0.0
  DESCRIPTION "${TPCCRUNNER_DESCRIPTION}"
  )

message(STATUS "Welcome to tpcc-runner, Version: ${PROJECT_VERSION}")
set(CMAKE_C_COMPILER /usr/bin/gcc CACHE PATH "")
set(CMAKE_CXX_COMPILER /usr/bin/g++ CACHE PATH "")
message(STATUS "Compiler: ${CMAKE_CXX_COMPILER} ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
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

include(FetchContent)
function(add_dep NAME GIT_URL GIT_TAG)
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

        add_subdirectory(${${NAME}_SOURCE_DIR} ${${NAME}_BINARY_DIR})
    endif ()

    execute_process(COMMAND git log -1 --format=%cd --date=short                        
            WORKING_DIRECTORY ${${NAME}_SOURCE_DIR}                               
            OUTPUT_VARIABLE GIT_LAST_COMMIT)                                            
    string(STRIP "${GIT_LAST_COMMIT}" GIT_LAST_COMMIT)                                  

    message(STATUS "[FOUND] ${NAME} (${GIT_URL} ${GIT_TAG} ${GIT_LAST_COMMIT})")        
endfunction()

add_dep(mimalloc https://github.com/microsoft/mimalloc v1.7.1)
add_dep(googletest https://github.com/google/googletest.git release-1.10.0)

###############################################################################
#                            Static library of tpcc                           #
###############################################################################

set(TPCCRUNNER_COMPILE_DEFINITIONS "")
set(TPCCRUNNER_COMPILE_OPTIONS "-fconcepts")
set(TPCCRUNNER_COMPILE_FEATURES "cxx_std_20")
set(TPCCRUNNER_LINK_OPTIONS "")
set(TPCCRUNNER_INCLUDE_DIRECTORIES "${PROJECT_SOURCE_DIR}/tpcc/include")
list(APPEND TPCCRUNNER_INCLUDE_DIRECTORIES "${PROJECT_SOURCE_DIR}/tx_engine/include")
list(APPEND TPCCRUNNER_INCLUDE_DIRECTORIES "${CMAKE_BINARY_DIR}/_deps/src/mimalloc/include")

set(LOG_LEVEL
  "1"
  CACHE STRING "0(off) 1(error) 2(warn) 3(into) 4(debug) 5(trace)")

list(APPEND TPCCRUNNER_COMPILE_DEFINITIONS "-DLOG_LEVEL=${LOG_LEVEL}")
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

if (APPLE)
  list(APPEND TPCCRUNNER_COMPILE_OPTIONS "-fvisibility=hidden")
  list(APPEND TPCCRUNNER_LINK_OPTIONS "-fvisibility=hidden")
endif ()

# Create an objectlib, which can be used for creating both static and dynamic library
file(GLOB_RECURSE
  TPCCRUNNER_SRCS
  ${PROJECT_SOURCE_DIR}/tpcc/src/*.cpp
  ${PROJECT_SORUCE_DIR}/tpcc/include/*.hpp
  ${PROJECT_SOURCE_DIR}/tx_engine/src/*.cpp
  ${PROJECT_SOURCE_DIR}/tx_engine/include/*.hpp
  )

add_library(tpccrunner_objlib OBJECT ${TPCCRUNNER_SRCS})
target_link_libraries(tpccrunner_objlib PRIVATE mimalloc)

target_compile_definitions(tpccrunner_objlib PUBLIC ${TPCCRUNNER_COMPILE_DEFINITIONS})
target_compile_options(tpccrunner_objlib PUBLIC ${TPCCRUNNER_COMPILE_OPTIONS})
target_compile_features(tpccrunner_objlib PUBLIC ${TPCCRUNNER_COMPILE_FEATURES})
target_link_options(tpccrunner_objlib PUBLIC ${TPCCRUNNER_LINK_OPTIONS})
target_include_directories(tpccrunner_objlib PUBLIC ${TPCCRUNNER_INCLUDE_DIRECTORIES})

target_compile_options(tpccrunner_objlib PRIVATE "-fPIC" "-Werror" "-Wall")

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

file(GLOB_RECURSE EXECUTABLES "${PROJECT_SOURCE_DIR}/executables/*.cpp")
foreach (EXECUTABLE ${EXECUTABLES})
  get_filename_component(FILENAME ${EXECUTABLE} NAME_WE)
  add_executable(${FILENAME} ${EXECUTABLE})
  target_link_options(${FILENAME} PUBLIC "-pthread")
  target_compile_options(${FILENAME} PUBLIC "-pthread")
  target_compile_options(${FILENAME} PRIVATE "-Werror" "-Wall")
  target_link_libraries(${FILENAME} tpccrunner_static)
  set_target_properties(${FILENAME} PROPERTIES CXX_EXTENTIONS OFF ENABLE_EXPORTS ON)
  set_target_properties(
    ${FILENAME}
    PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
  )
endforeach (EXECUTABLE ${EXECUTABLES})

###############################################################################
#                                 Test binaries                                #
###############################################################################

# Ref: https://google.github.io/googletest/quickstart-cmake.html
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
    target_link_libraries(${FILENAME} tpccrunner_static gtest_main)
    set_target_properties(${FILENAME} PROPERTIES CXX_EXTENTIONS OFF ENABLE_EXPORTS ON)
    set_target_properties(
      ${FILENAME}
      PROPERTIES
      RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test/"
      )
  endif ()
endforeach (TPCC_TEST ${TPCC_TESTS})

set(STATUS "[ADDED] tpccrunner tests")

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
    -r ${CMAKE_CURRENT_SOURCE_DIR}/tx_engine/include
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc/src
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc/include
    ${CMAKE_CURRENT_SOURCE_DIR}/test
    -i                          # format file instead of printing diff
    -q
    USES_TERMINAL
    )

  add_custom_target(check-format
    COMMENT "Running: check-format"
    COMMAND ${BUILD_SUPPORT_DIR}/run-clang-format.py
    --clang-format-executable ${CLANG_FORMAT_BIN}
    -r ${CMAKE_CURRENT_SOURCE_DIR}/tx_engine/include
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc/src
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcc/include
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
