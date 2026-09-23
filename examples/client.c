#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <stdatomic.h>
#include <threads.h>
#include <poll.h>

#include "rtipc/rtipc.h"
#include "rtipc/connect.h"
#include "rtipc/log.h"

#include "messages.h"



typedef struct client {
    ri_producer_t *command;
    ri_consumer_t *response;
    ri_consumer_t *event;
    thrd_t listener;
    atomic_bool run;
} client_t;


static msg_command_t commands[] = {
  {
      .id = CMDID_HELLO,
  },
  {
      .id = CMDID_SENDEVENT,
      .args.send = {11, 20, false},
  },
  {
      .id = CMDID_SENDEVENT,
      .args.send = {12, 20, true},
  },
  {
      .id = CMDID_DIV,
      .args.div = {100, 7},
  },
  {
      .id = CMDID_DIV,
      .args.div = {100, 0},
  },
  {
      .id = CMDID_STOP,
  },
   {
      .id = CMDID_UNKNOWN,
  },
};


static void client_delete(client_t *client)
{
  if (client->command)
    ri_producer_release(client->command);
  if (client->response)
    ri_consumer_release(client->response);
  if (client->event)
    ri_consumer_release(client->event);
  free(client);
}


static int event_listen(void *arg)
{
  client_t *client = arg;

  int eventfd = ri_consumer_eventfd(client->event);

  if (eventfd < 0)
    return eventfd;

  struct pollfd pollfd = {.fd = eventfd, .events = POLLIN, };

  while (atomic_load(&client->run)) {
    int r = poll(&pollfd, 1, 10);

    if (r < 0) {
      return -errno;
    }

    if (pollfd.revents & POLLIN) {
      ri_pop_result_t r = ri_consumer_pop(client->event);

      if ((r == RI_POP_RESULT_NO_MSG) || (r == RI_POP_RESULT_NO_UPDATE)) {
         LOG_ERR("message queue empty");
      }

      msg_event_print(ri_consumer_msg(client->event));
    }
  }

  return 0;
}

static client_t* client_new(const char *path, const ri_group_attr_t *grp_attr)
{
  ri_group_t *grp = ri_client_connect(path, grp_attr);
  if (!grp)
    goto fail_connect;

  client_t *client = calloc(1, sizeof(client_t));
  if (!client)
    goto fail_alloc;

  client->command = client_rpc_acquire_command(grp);
  if (!client->command)
    goto fail_channel;

  client->response = client_rpc_acquire_response(grp);
  if (!client->response)
    goto fail_channel;

  client->event = client_rpc_acquire_event(grp);
  if (!client->event)
    goto fail_channel;

  atomic_store(&client->run, true);

  int r = thrd_create(&client->listener, event_listen, client);
  if (r != thrd_success) {
    goto fail_thread;
  }

  ri_group_delete(grp);

  return client;

fail_thread:
fail_channel:
  client_delete(client);
fail_alloc:
  ri_group_delete(grp);
fail_connect:
  return NULL;
}



static void client_run(client_t *client, const msg_command_t *cmds)
{
  const msg_command_t *cmd = cmds;

  for (;;) {
    if (cmd->id == CMDID_UNKNOWN)
      return;

    *(msg_command_t*)ri_producer_msg(client->command) = *cmd;
    ri_producer_force_push(client->command);

    for (;;) {
      ri_pop_result_t r = ri_consumer_pop(client->response);

      if (r == RI_POP_RESULT_ERROR) {
        LOG_ERR("ri_consumer_pop receive error");
        return;
      } else if ((r == RI_POP_RESULT_NO_MSG) || (r == RI_POP_RESULT_NO_UPDATE)) {
        usleep(1000);
        continue;
      } else {
        break;
      }
    }

    LOG_INF("client received:");
    msg_response_print(ri_consumer_msg(client->response));
    cmd++;
  }

  usleep(10000);

  atomic_store(&client->run, false);

  thrd_join(client->listener, NULL);
}


int main(void)
{
  //LOG_INF("info: %s", rpc_info.data);

  client_t *client = client_new("rtipc.sock", &client_group_rpc);
  if (!client) {
    return -1;
  }

  client_run(client, commands);

  LOG_INF("deleting client");
  client_delete(client);

  return 0;
}
