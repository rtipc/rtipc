#include "rtipc/rtipc.h"
#include "rtipc/log.h"

#include <errno.h>
#include <stdio.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "unix.h"

static int connect_path(const char *path)
{
  int r;

  int sockfd = socket(AF_UNIX, SOCK_SEQPACKET, 0);
  if (sockfd < 0) {
    r = -errno;
    LOG_ERR("socket failed errno=%u", errno);
    goto fail_socket;
  }

  struct sockaddr_un addr;

  addr.sun_family = AF_UNIX;
  snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", path);

  r = connect(sockfd, (struct sockaddr*)&addr, sizeof(addr));
  if (r < 0) {
    r = -errno;
    LOG_ERR("connect failed errno=%u", errno);
    goto fail_connect;
  }

  return sockfd;

fail_connect:
  close(sockfd);
fail_socket:
  return r;
}


static int exchange(int socket, ri_uxmsg_t *req)
{
  int r = ri_uxmsg_send(req, socket);
  if (r < 0) {
    LOG_ERR("ri_request_send failed r=%d", r);
    goto fail_send;
  }

  size_t response_size;
  void *response = ri_uxsocket_receive(socket, &response_size);
  if (!response) {
    r = -1;
    LOG_ERR("ri_uxsocket_receive failed");
    goto fail_receive;
  }

  int32_t result;

  if (response_size != sizeof(result)) {
    LOG_ERR("ri_uxsocket_receive failed");
    goto fail_response;

  }

  memcpy(&result, response, sizeof(result));

  free(response);

  return result;

fail_response:
  free(response);
fail_receive:
fail_send:
  return r;
}


static ri_uxmsg_t* uxmsg_from_vector(const ri_vector_t *vec)
{
  size_t req_size = ri_vector_serialize_size(vec);

  ri_uxmsg_t *req = ri_uxmsg_new(req_size);
  if (!req)
    goto fail_alloc;

  void *req_data = ri_uxmsg_data(req, &req_size);
  unsigned n_fds;
  int *fds = ri_uxmsg_fds(req, &n_fds);

  int r = ri_vector_serialize(vec, req_data, req_size, fds, &n_fds);
  if (r < 0)
    goto fail_construct;

  r = ri_uxmsg_set_num_fds(req, n_fds);
  if (r < 0)
    goto fail_construct;

  return req;

fail_construct:
  ri_uxmsg_delete(req);
fail_alloc:
  return NULL;
}


ri_vector_t* ri_client_socket_connect(int socket, const ri_vector_attr_t *vattr)
{
  ri_vector_t *vec = ri_vector_new(vattr);
  if (!vec) {
    LOG_ERR("ri_vector_new failed");
    goto fail_vec;
  }

  ri_uxmsg_t *req = uxmsg_from_vector(vec);
  if (!req) {
    LOG_ERR("uxmsg_from_resource failed");
    goto fail_req;
  }

  int r = exchange(socket, req);
  if (r < 0) {
    LOG_ERR("exchange failed");
    goto fail_exchange;
  }


  ri_uxmsg_delete(req);

  return vec;

fail_exchange:
  ri_uxmsg_delete(req);
fail_req:
  ri_vector_delete(vec);
fail_vec:
  return NULL;
}


ri_vector_t* ri_client_connect(const char *path, const ri_vector_attr_t *vattr)
{
  int socket = connect_path(path);

  if (socket < 0) {
    return NULL;
  }

  ri_vector_t *vec = ri_client_socket_connect(socket, vattr);

  close(socket);

  return vec;
}
