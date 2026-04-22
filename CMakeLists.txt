cmake_minimum_required(VERSION 3.10)
project(CXLBandle)

# Default to Release for single-config generators (e.g., Unix Makefiles/Ninja)
# unless the user explicitly sets CMAKE_BUILD_TYPE.
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo MinSizeRel)
    message(STATUS "CMAKE_BUILD_TYPE not set; defaulting to Release")
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(include)

set(BANDLE_SOURCES
    src/cxl_memory.cpp
    src/kv_store.cpp
    src/bandle.cpp
    src/main.cpp
)

add_executable(bandle_replay ${BANDLE_SOURCES})
target_link_libraries(bandle_replay rt)
target_compile_options(bandle_replay PRIVATE -mavx512f)

option(BANDLE_VM "Build for multi-VM shared-memory runs" OFF)

add_executable(bandle_trace_gen src/bandle_trace_gen.cpp)

add_executable(bandle_aggregate src/bandle_aggregate.cpp)

install(TARGETS bandle_replay DESTINATION bin)
