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


const ri_channel_attr_t client2server_channels[] = {
    (ri_channel_attr_t) { .add_msgs = 0, .msg_size = sizeof(msg_command_t), .eventfd = 1, .info = { .data = COMMAND_INFO, .size = sizeof(COMMAND_INFO) }},
  { 0 },
};


const ri_channel_attr_t server2client_channels[] = {
  (ri_channel_attr_t) { .add_msgs = 0, .msg_size = sizeof(msg_response_t), .eventfd = 1, .info = { .data = RESPONSE_INFO, .size = sizeof(RESPONSE_INFO) }},
  (ri_channel_attr_t) { .add_msgs = 10, .msg_size = sizeof(msg_event_t), .eventfd = 1, .info = { .data = EVENT_INFO, .size = sizeof(EVENT_INFO) }},
  { 0 },
};


typedef struct client {
    ri_producer_t *command;
    ri_consumer_t *response;
    ri_consumer_t *event;
    thrd_t listener;
    atomic_bool run;
} client_t;


static msg_command_t commands[] = {
  (msg_command_t) {
      .id = CMDID_HELLO,
      .args = {1, 2, 0},
  },
  (msg_command_t) {
      .id = CMDID_SENDEVENT,
      .args = {11, 20, 0},
  },
  (msg_command_t) {
      .id = CMDID_SENDEVENT,
      .args = {12, 20, 1},
  },
  (msg_command_t) {
      .id = CMDID_DIV,
      .args = {100, 7, 0},
  },
  (msg_command_t) {
      .id = CMDID_DIV,
      .args = {100, 0, 0},
  },
  (msg_command_t) {
      .id = CMDID_STOP,
      .args = {0, 0, 0},
  },
  (msg_command_t) {
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


int event_listen(void *arg)
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

static client_t* client_new(const char *path, const ri_vector_attr_t *config)
{
  ri_vector_t *vec = ri_client_connect(path, config);
  if (!vec)
    goto fail_connect;

  client_t *client = calloc(1, sizeof(client_t));
  if (!client)
    goto fail_alloc;

  client->command = ri_vector_acquire_producer(vec, 0);
  if (!client->command)
    goto fail_channel;

  client->response = ri_vector_acquire_consumer(vec, 0);
  if (!client->response)
    goto fail_channel;

  client->event = ri_vector_acquire_consumer(vec, 1);
  if (!client->event)
    goto fail_channel;

  atomic_store(&client->run, true);

  int r = thrd_create(&client->listener, event_listen, client);
  if (r != thrd_success) {
    goto fail_thread;
  }


  ri_vector_delete(vec);

  return client;

fail_thread:
fail_channel:
  client_delete(client);
fail_alloc:
  ri_vector_delete(vec);
fail_connect:
  return NULL;
}



void client_run(client_t *client, const msg_command_t *cmds)
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


int main()
{
  const ri_vector_attr_t vattr = {
    .consumers = server2client_channels,
    .producers = client2server_channels,
  };

  client_t *client = client_new("rtipc.sock", &vattr);
  if (!client) {
    return -1;
  }

  client_run(client, commands);

  LOG_INF("deleting client");
  client_delete(client);

  return 0;
}
