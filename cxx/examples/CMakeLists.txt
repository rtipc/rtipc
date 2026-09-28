
get_target_property(INCLUDE_DIRS rtipc_cpp INTERFACE_INCLUDE_DIRECTORIES)
message("INCLUDE_DIRS ${INCLUDE_DIRS}")

add_executable(client_cpp client.cpp)
target_include_directories(client_cpp PRIVATE ${INCLUDE_DIRS})
#target_compile_options(client_cpp PRIVATE ${RTIPC_COMPILER_OPTIONS})
target_link_libraries(client_cpp PRIVATE rtipc_cpp)


add_executable(server_cpp server.cpp)
target_include_directories(server_cpp PRIVATE ${INCLUDE_DIRS})
#target_compile_options(client_cpp PRIVATE ${RTIPC_COMPILER_OPTIONS})
target_link_libraries(server_cpp PRIVATE rtipc_cpp)
