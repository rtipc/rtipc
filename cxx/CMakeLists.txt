cmake_minimum_required(VERSION 3.20)

project(rtipc_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 26)

set(RTIPC_CXX_SRCS
  src/rtipc.cpp
)

add_library(rtipc_cpp)

target_sources(rtipc_cpp
  PRIVATE
    ${RTIPC_CXX_SRCS}
  PUBLIC
    FILE_SET HEADERS
    FILES
      include/rtipc/rtipc.hpp
)


target_include_directories(rtipc_cpp PUBLIC
  ${RTIPC_INCLUDE_DIR}
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_compile_options(rtipc_cpp PRIVATE -Wall -Wextra  -pedantic  -Wsign-compare)

target_link_libraries(rtipc_cpp PRIVATE rtipc)


install(
  TARGETS rtipc_cpp
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  FILE_SET HEADERS
)


if (RTIPC_BUILD_EXAMPLES)
	add_subdirectory(examples)
endif ()
