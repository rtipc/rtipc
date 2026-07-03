#pragma once

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ri_vector ri_vector_t;
typedef struct ri_vector_attr ri_vector_attr_t;


/**
 * @typedef ri_server_t
 * @brief Opaque handle representing a server that owns a listening Unix domain socket.
 */
typedef struct ri_server ri_server_t;



/**
 * @brief Create a server, bind it to a UNIX domain socket, and start listening.
 *
 * @param path    Filesystem path for the UNIX domain socket.
 * @param backlog Maximum length for the queue of pending connections (passed to listen()).
 * @return Pointer to the created server instance, or NULL on failure.
 */
ri_server_t* ri_server_new(const char* path, int backlog);


/**
 * @brief Destroy a server instance and release its resources.
 *
 * Closes the listening socket and removes the associated UNIX domain
 * socket file from the filesystem.
 *
 * @param server Pointer to the server instance to destroy.
 */
void ri_server_delete(ri_server_t* server);


/**
 * @brief Get the server's Unix domain socket file descriptor.
 *
 * Returns the file descriptor of the Unix domain socket associated with
 * the given server instance. The descriptor remains owned by the server
 * and must not be closed by the caller.
 *
 * @param server Pointer to an initialized server instance.
 * @return File descriptor of the Unix domain socket.
 */
int ri_server_socket(const ri_server_t* server);

typedef bool (*ri_filter_fn)(const ri_vector_t* vec, void *user_data);

/**
 * @brief Accept a client connection and constructs a channel vector.
 *
 * Accepts a connection on the given UNIX domain socket, receives the vector
 * configuration along with shared memory file descriptors and eventfds,
 * constructs a vector from those resources, and sends a response back to
 * the client.
 *
 * @param socket    UNIX domain socket file descriptor (created with
 *                  socket(AF_UNIX, SOCK_SEQPACKET, 0)).
 * @param filter    Optional callback to accept (true) or reject (false) a
 *                  connection based on the requested channel vector.
 * @param user_data Opaque pointer passed to @p filter.
 *
 * @return A newly created vector on success, or NULL if the connection
 *         fails or is rejected by @p filter.
 */
ri_vector_t* ri_server_socket_accept(int socket, ri_filter_fn filter, void *user_data);


/**
 * @brief Accept a client connection and construct a channel vector.
 *
 * Accepts a connection on the server's UNIX domain socket, receives the
 * vector configuration along with shared memory file descriptors and
 * eventfds, constructs a vector from those resources, and sends a response
 * back to the client.
 *
 * @param server    Pointer to the server instance that owns the listening socket.
 * @param filter    Optional callback to accept (true) or reject (false) a
 *                  connection based on the requested resource.
 * @param user_data Opaque pointer passed to @p filter.
 *
 * @return A newly created vector on success, or NULL if the connection
 *         fails or is rejected by @p filter.
 */
ri_vector_t* ri_server_accept(const ri_server_t* server, ri_filter_fn filter, void *user_data);


/**
 * @brief Connect to a server and create a channel vector.
 *
 * Connects to a server listening on the specified UNIX domain socket, creates
 * the required resources (shared memory and eventfds), sends the vector
 * configuration along with the resources to the server, and, upon receiving
 * a positive response, constructs and returns a channel vector.
 *
 * @param socket  UNIX domain socket file descriptor
 *                (created with socket(AF_UNIX, SOCK_SEQPACKET, 0)).
 * @param vattr Configuration for the channel vector.
 *
 * @return A newly created and initialized vector on success, or NULL if the
 *         connection fails or is rejected by the server.
 */
ri_vector_t* ri_client_socket_connect(int socket, const ri_vector_attr_t *vattr);


/**
 * @brief Connect to a server and create a channel vector.
 *
 * Connects to a server listening on the specified UNIX domain socket path,
 * allocates the required resources (shared memory and eventfds), sends the
 * vector configuration along with these resources to the server, and, upon
 * receiving a positive response, constructs and returns an initialized
 * channel vector.
 *
 * @param path    Filesystem path of the UNIX domain socket created by the server.
 * @param vattr Configuration for the channel vector.
 *
 * @return Pointer to a newly created and initialized vector on success,
 *         or NULL if the connection fails or is rejected by the server.
 */
ri_vector_t* ri_client_connect(const char *path, const ri_vector_attr_t *vattr);





#ifdef __cplusplus
}
#endif