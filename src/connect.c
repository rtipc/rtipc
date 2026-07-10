#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "rtipc/rtipc.h"
#include "rtipc/connect.h"
#include "rtipc/log.h"
#include "unix.h"

typedef struct ri_server ri_server_t;

struct ri_server {
  int sockfd;
  struct sockaddr_un addr;
};


ri_server_t* ri_server_new(const char* path, int backlog)
{
  ri_server_t *server = malloc(sizeof(ri_server_t));

  if (!server) {
    goto fail_alloc;
  }

  server->sockfd = socket(AF_UNIX, SOCK_SEQPACKET, 0);

  if (server->sockfd < 0) {
    LOG_ERR("socket failed errno=%u", errno);
    goto fail_socket;
  }

  server->addr.sun_family = AF_UNIX;
  snprintf(server->addr.sun_path, sizeof(server->addr.sun_path), "%s", path);

  int r = bind(server->sockfd, (struct sockaddr*)&server->addr, SUN_LEN(&server->addr));

  if (r < 0) {
    LOG_ERR("bind (%s) failed errno=%u", path, errno);
    goto fail_bind;
  }

  r = listen(server->sockfd, backlog);

  if (r < 0) {
    LOG_ERR("listen (%s) failed errno=%u", path, errno);
    goto fail_bind;
  }

  return server;

fail_bind:
  close(server->sockfd);
fail_socket:
  free(server);
fail_alloc:
  return NULL;
}


int ri_server_socket(const ri_server_t* server)
{
  return server->sockfd;
}


static int server_send_response(int socket, int32_t result)
{
  return ri_uxsocket_send(socket, &result, sizeof(result));
}


static ri_group_t* request_to_group(ri_uxmsg_t *req)
{
  size_t size;
  unsigned n_fds;
  const void *data = ri_uxmsg_data(req, &size);
  int *fds = ri_uxmsg_fds(req, &n_fds);

  ri_group_t *grp = ri_group_deserialize(data, size, fds, &n_fds);
  if (!grp) {
    LOG_ERR("ri_group_deserialize failed");
    return NULL;
  }

  return grp;
}


ri_group_t* ri_server_socket_accept(int socket, ri_filter_fn filter, void *user_data)
{
  ri_uxmsg_t *req = ri_uxmsg_receive(socket);
  if (!req) {
    LOG_ERR("ri_uxmsg_receive failed");
    goto fail_receive;
  }

  ri_group_t *grp = request_to_group(req);
  if (!grp)
    goto fail_transfer;

  if (filter) {
    ri_group_attr_t attr = ri_group_get_attr(grp);
    unsigned n_consumers = ri_group_num_consumers(grp);
    unsigned n_producers = ri_group_num_producers(grp);
    if (!filter(&attr, n_consumers, n_producers, user_data)) {
      LOG_INF("server rejected request");
      goto fail_rejected;
    }
  }

  server_send_response(socket, 0);

  ri_uxmsg_delete(req);

  return grp;

fail_rejected:
  ri_group_delete(grp);
fail_transfer:
  ri_uxmsg_delete(req);
fail_receive:
  server_send_response(socket, -1);

  return NULL;
}


ri_group_t* ri_server_accept(const ri_server_t* server, ri_filter_fn filter, void *user_data)
{
  int socket = accept(server->sockfd, NULL, NULL);
  if (socket < 0) {
    LOG_ERR("accept failed errno=%u", errno);
    return NULL;
  }

  ri_group_t *vec = ri_server_socket_accept(socket, filter, user_data);

  close(socket);

  return vec;
}


void ri_server_delete(ri_server_t* server)
{
  close(server->sockfd);
  unlink(server->addr.sun_path);
  free(server);
}



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
    LOG_ERR("connect to %s failed errno=%u", path, errno);
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


static ri_uxmsg_t* uxmsg_from_group(const ri_group_t *grp)
{
  size_t req_size = ri_group_serialize_size(grp);

  ri_uxmsg_t *req = ri_uxmsg_new(req_size);
  if (!req)
    goto fail_alloc;

  void *req_data = ri_uxmsg_data(req, &req_size);
  unsigned n_fds;
  int *fds = ri_uxmsg_fds(req, &n_fds);

  int r = ri_group_serialize(grp, req_data, req_size, fds, &n_fds);
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


ri_group_t* ri_client_socket_connect(int socket, const ri_group_attr_t *grp_attr)
{
  ri_group_t *grp = ri_group_from_attr(grp_attr);
  if (!grp) {
    LOG_ERR("ri_group_new failed");
    goto fail_grp;
  }

  ri_uxmsg_t *req = uxmsg_from_group(grp);
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

  return grp;

fail_exchange:
  ri_uxmsg_delete(req);
fail_req:
  ri_group_delete(grp);
fail_grp:
  return NULL;
}


ri_group_t* ri_client_connect(const char *path, const ri_group_attr_t *grp_attr)
{
  int socket = connect_path(path);

  if (socket < 0) {
    return NULL;
  }

  ri_group_t *grp = ri_client_socket_connect(socket, grp_attr);

  close(socket);

  return grp;
}
