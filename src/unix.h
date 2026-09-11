#pragma once

#include <stdlib.h>
#include <stdbool.h>


/**
 * @typedef ri_uxmsg_t
 * @brief Opaque handle representing a Unix domain socket message.
 *
 * This type encapsulates a message received from or sent over a Unix
 * domain socket, including both payload data and any associated file
 * descriptors passed via ancillary data.
 */
typedef struct ri_uxmsg ri_uxmsg_t;

/**
 * @brief Create and seal a shared memory file descriptor.
 *
 * Creates an anonymous shared memory file descriptor, resizes it to
 * @p size bytes, and applies seals to prevent further modification.
 *
 * @param size Size of the shared memory region in bytes.
 *
 * @return On success, returns a valid file descriptor.
 *         On failure, returns -errno and no file descriptor is created.
 */
int ri_shmfd_create(size_t size);

/**
 * @brief Create a non-blocking eventfd for semaphore-style signaling.
 *
 * Creates an event file descriptor configured for non-blocking I/O,
 * intended to be used as a lightweight semaphore or event notification
 * mechanism between processes.
 *
 * @return On success, returns a valid file descriptor.
 *         On failure, returns -errno and no file descriptor is created.
 */
int ri_eventfd_create(void);

/**
 * @brief Check whether a file descriptor refers to an anonymous memory file.
 *
 * Verifies that @p fd is a valid file descriptor referring to an anonymous
 * in-memory file (for example, one created with memfd_create).
 *
 * @param fd File descriptor to validate.
 *
 * @return Returns 0 if @p fd is a valid anonymous memory file descriptor.
 *         Returns -1 if the descriptor is invalid or does not refer to a
 *         compatible memory file.
 */
int ri_memfd_verify(int fd);


/**
 * @brief Check whether a file descriptor refers to an eventfd object.
 *
 * Verifies that @p fd is a valid file descriptor referring to an eventfd,
 * such as one created with eventfd().
 *
 * @param fd File descriptor to validate.
 *
 * @return Returns 0 if @p fd is a valid eventfd descriptor.
 *         Returns -1 if the descriptor is invalid or does not refer to
 *         an eventfd.
 */
int ri_eventfd_verify(int fd);


int ri_set_nonblocking(int fd);


ri_uxmsg_t* ri_uxmsg_new(size_t size);


void ri_uxmsg_delete(ri_uxmsg_t *msg);

void* ri_uxmsg_data(const ri_uxmsg_t *msg, size_t *size);

int* ri_uxmsg_fds(ri_uxmsg_t *msg, unsigned *n_fds);

int ri_uxmsg_set_num_fds(ri_uxmsg_t *msg, unsigned n_fds);

int ri_uxmsg_send(const ri_uxmsg_t *msg, int socket);

ri_uxmsg_t* ri_uxmsg_receive(int socket);


int ri_uxsocket_send(int socket, const void *data, size_t size);

void* ri_uxsocket_receive(int socket, size_t *size);
