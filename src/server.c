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

  ri_group_t *vec = request_to_group(req);
  if (!vec)
    goto fail_transfer;

  if (filter) {
    if (!filter(vec, user_data)) {
      LOG_INF("server rejected request");
      goto fail_rejected;
    }
  }

  server_send_response(socket, 0);

  ri_uxmsg_delete(req);

  return vec;

fail_rejected:
  ri_group_delete(vec);
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
