cmake_minimum_required(VERSION 4.0)

include(GNUInstallDirs)

project(rtipc LANGUAGES C)

set(CMAKE_C_STANDARD 11)

option(RTIPC_BUILD_EXAMPLES "Build examples" OFF)

set(RTIPC_SRCS
  src/log.c
  src/attr.c
  src/attr.h
  src/shm.c
  src/shm.h
  src/queue.c
  src/queue.h
  src/producer.c
  src/producer.h
  src/consumer.c
  src/consumer.h
  src/mem_utils.c
  src/mem_utils.h
  src/index.h
  src/group.c
  src/group.h
  src/channel.c
  src/channel.h
  src/header.c
  src/header.h
  src/unix.c
  src/unix.h
  src/request.h
  src/request.c
  src/connect.c
  src/client.c
)


set(RTIPC_COMPILER_OPTIONS -Wall -Wextra  -Wsign-compare)

add_library(${PROJECT_NAME})
set_target_properties(${PROJECT_NAME} PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_include_directories(rtipc PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_sources(${PROJECT_NAME}
  PRIVATE
    ${RTIPC_SRCS}
  PUBLIC
    FILE_SET HEADERS
    BASE_DIRS
      include
    FILES
      include/rtipc/rtipc.h
      include/rtipc/log.h
      include/rtipc/connect.h
)


target_compile_options(${PROJECT_NAME} PRIVATE -Wall -Wextra  -Wsign-compare)


install(
  TARGETS ${PROJECT_NAME}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  FILE_SET HEADERS
)


if (RTIPC_BUILD_EXAMPLES)
	add_subdirectory(examples)
endif ()
