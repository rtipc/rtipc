#include <stdint.h>

#include "rtipc/rtipc.h"
#include "rtipc/log.h"

typedef struct msg {
  uint64_t data;
} msg_t;

static const ri_channel_attr_t c2s_channels[] = {
    { .msg_size = sizeof(msg_t), .add_msgs = 2 },
    {0}
};

ri_group_attr_t group_attr = {
    .producers = c2s_channels,
};


static void print_count(const ri_consumer_t *consumer, const ri_producer_t *producer)
{
  int n = ri_producer_count_msgs(producer);
  LOG_INF("\tproducer count = %d", n);

  n = ri_consumer_count_msgs(consumer);
  LOG_INF("\tconsumer count = %d", n);
}


static void push_and_print(const ri_consumer_t *consumer,  ri_producer_t *producer)
{
  static uint64_t counter = 0;
  msg_t *msg = ri_producer_msg(producer);
  msg->data = counter++;
  ri_force_push_result_t r = ri_producer_force_push(producer);
   LOG_INF("push %x", r);
  print_count(consumer, producer);
}

static void pop_and_print(ri_consumer_t *consumer,  const ri_producer_t *producer)
{

  ri_pop_result_t r =ri_consumer_pop(consumer);
  msg_t *msg = ri_consumer_msg(consumer);
  if (msg) {
    LOG_INF("pop %d msg=%llu", r, msg->data);
  } else {
    LOG_INF("pop %d", r);
  }

  print_count(consumer, producer);
}

int main()
{

  ri_group_t *client_group = ri_group_from_attr(&group_attr);
  if (!client_group) {
    LOG_ERR("ri_group_from_attr failed");
    return -1;
  }

  uint8_t req[1000];
  int fds[10];
  int n_fds = 10;

  int req_size = ri_group_serialize(client_group, req, sizeof(req),fds, &n_fds);
  if (req_size < 0) {
    LOG_ERR("ri_group_serialize failed");
    return -1;
  }


  ri_group_t *server_group = ri_group_deserialize(req, req_size, fds, &n_fds);
  if (!server_group) {
    LOG_ERR("ri_group_deserialize failed");
    return -1;
  }

  ri_producer_t * producer = ri_group_acquire_producer(client_group, 0);
  if (!producer) {
    LOG_ERR("ri_group_acquire_producer failed");
    return -1;
  }

  ri_consumer_t * consumer = ri_group_acquire_consumer(server_group, 0);
  if (!consumer) {
    LOG_ERR("ri_group_acquire_consumer failed");
    return -1;
  }

  ri_group_delete(server_group);
  ri_group_delete(client_group);

  print_count(consumer,  producer);
  LOG_INF("msg=%p", ri_consumer_msg(consumer));
  push_and_print(consumer, producer);
  push_and_print(consumer, producer);
  push_and_print(consumer, producer);
push_and_print(consumer, producer);
push_and_print(consumer, producer);


pop_and_print(consumer, producer);
push_and_print(consumer, producer);
push_and_print(consumer, producer);
push_and_print(consumer, producer);
push_and_print(consumer, producer);
push_and_print(consumer, producer);

pop_and_print(consumer, producer);
push_and_print(consumer, producer);
push_and_print(consumer, producer);
push_and_print(consumer, producer);
push_and_print(consumer, producer);
push_and_print(consumer, producer);

pop_and_print(consumer, producer);
push_and_print(consumer, producer);
push_and_print(consumer, producer);
push_and_print(consumer, producer);
push_and_print(consumer, producer);
push_and_print(consumer, producer);

pop_and_print(consumer, producer);
pop_and_print(consumer, producer);
pop_and_print(consumer, producer);
pop_and_print(consumer, producer);
pop_and_print(consumer, producer);



  return 0;
}
