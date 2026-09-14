set(RTIPC_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../lib")

add_executable(server server.c messages.c rpc.c)
target_include_directories(server PRIVATE ${RTIPC_INCLUDE_DIR})
target_compile_options(server PRIVATE ${RTIPC_COMPILER_OPTIONS})
target_link_libraries(server PRIVATE ${PROJECT_NAME})


add_executable(client client.c messages.c rpc.c)
target_include_directories(client PRIVATE ${RTIPC_INCLUDE_DIR})
target_compile_options(client PRIVATE ${RTIPC_COMPILER_OPTIONS})
target_link_libraries(client PRIVATE ${PROJECT_NAME})


add_executable(benchmark benchmark.c)
target_include_directories(benchmark PRIVATE ${RTIPC_INCLUDE_DIR})
target_compile_options(benchmark PRIVATE ${RTIPC_COMPILER_OPTIONS})
target_link_libraries(benchmark PRIVATE ${PROJECT_NAME})


