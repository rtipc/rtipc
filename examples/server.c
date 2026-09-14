#include <stdlib.h>
#include <unistd.h>

#include "rtipc/rtipc.h"
#include "rtipc/connect.h"
#include "rtipc/log.h"
#include "messages.h"

#define MAX_CYCLES 10000

typedef struct server {
    ri_consumer_t *command;
    ri_producer_t *response;
    ri_producer_t *event;
} server_t;



static void server_delete(server_t* server)
{
  if (server->command)
    ri_consumer_release(server->command);
  if (server->response)
    ri_producer_release(server->response);
  if (server->event)
    ri_producer_release(server->event);
  free(server);
}

static bool server_check(const ri_group_attr_t* attr, unsigned n_consumers, unsigned n_producers, void *userdata)
{

  bool r = ri_group_attr_equal(attr, &server_group_rpc);

  if (!r) {
    LOG_ERR("server_check failed");
  }

  return r;
}

static server_t* server_new(const char *path)
{
  ri_server_t *ri_server = ri_server_new(path, 1);
  if (!ri_server)
    goto fail_server;

  ri_group_t *grp = ri_server_accept(ri_server, server_check, ri_server);

  ri_server_delete(ri_server);

  if (!grp)
    goto fail_server;

  server_t *server = calloc(1, sizeof(server_t));

  if (!server)
    goto fail_alloc;

  server->command = ri_group_acquire_consumer(grp, 0);
  if (!server->command)
    goto fail_channel;

  server->response = ri_group_acquire_producer(grp, 0);
  if (!server->response)
    goto fail_channel;

  server->event = ri_group_acquire_producer(grp, 1);
  if (!server->event)
    goto fail_channel;

  ri_group_delete(grp);

  return server;

fail_channel:
  server_delete(server);
fail_alloc:
   ri_group_delete(grp);
fail_server:
  return NULL;
}



static int32_t server_send_events(ri_producer_t *producer, uint32_t id, unsigned num, bool force)
{
  for (unsigned i = 0; i < num; i++) {
    msg_event_t *event = ri_producer_msg(producer);
    event->id = id;
    event->nr = i;
    if (force) {
      ri_producer_force_push(producer);
    } else {
      if (ri_producer_try_push(producer) == RI_TRY_PUSH_RESULT_FAIL) {
        return i;
      }
    }
  }
  return num;
}


static int32_t server_div(double a, double b, double *res)
{
  if (b == 0) {
    return -1;
  } else {
    *res = a / b;
    return 0;
  }
}

static void server_run(server_t *server)
{

  for (int i = 0; i < MAX_CYCLES; i++) {

    bool run = true;
    ri_pop_result_t r = ri_consumer_pop(server->command);

    if ((r == RI_POP_RESULT_NO_MSG) || (r == RI_POP_RESULT_NO_UPDATE)) {
      usleep(1000);
      continue;
    }

    const msg_command_t *cmd = ri_consumer_msg(server->command);
    LOG_INF("server received:");
    msg_command_print(cmd);

    msg_response_t *rsp = ri_producer_msg(server->response);

    rsp->id = cmd->id;
    switch (cmd->id) {
    case CMDID_HELLO:
      rsp->result = 0;
      break;
    case CMDID_STOP:
      run = false;
      rsp->result = 0;
      break;
    case CMDID_SENDEVENT:
      rsp->result = server_send_events(server->event, cmd->args.send.id, cmd->args.send.num, cmd->args.send.force);
      break;
    case CMDID_DIV:
      rsp->result = server_div(cmd->args.div.divisor, cmd->args.div.divident, &rsp->data.quotient);
      break;
    default:
      rsp->result = -1;
      break;
    }
    ri_producer_force_push(server->response);
    if (!run)
      break;
  }
}


int main(void)
{
  server_t* server = server_new("rtipc.sock");

  if (!server) {
    return -1;
  }

  server_run(server);

  LOG_INF("deleting server");
  server_delete(server);

  return 0;
}
