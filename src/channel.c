#include "channel.h"

#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <stdatomic.h>

#include "rtipc/log.h"
#include "mem_utils.h"
#include "producer.h"
#include "consumer.h"
#include "unix.h"


#define RI_OWNER_GROUP_FLAG 0x1
#define RI_OWNER_USER_FLAG 0x2

struct ri_consumer {
  atomic_uint owners;
  ri_consumer_queue_t *queue;
  size_t shm_offset;
  int eventfd;
};

struct ri_producer {
  atomic_uint owners;
  ri_producer_queue_t *queue;
  size_t shm_offset;
  int eventfd;
  void *cache;
};

static size_t ri_calc_data_size(unsigned n_msgs, size_t msg_size)
{
  return n_msgs * cacheline_aligned(msg_size);
}



\

size_t ri_calc_channel_shm_size(unsigned n_msgs, size_t msg_size)
{
  /* tail + head + queue*/
  return ri_calc_queue_size(n_msgs) + ri_calc_data_size(n_msgs, msg_size);
}


size_t ri_calc_shm_size(const ri_channel_attr_t consumers[], const ri_channel_attr_t producers[])
{
  unsigned n_consumers = ri_count_channels(consumers);
  unsigned n_producers = ri_count_channels(producers);

  size_t size = 0;

  for (unsigned i = 0; i < n_consumers; i++)
    size +=  ri_channel_shm_size(&consumers[i]);

  for (unsigned i = 0; i < n_producers; i++)
    size += ri_channel_shm_size(&producers[i]);

  return size;
}


static void producer_cache_write(const ri_producer_t *producer) {
  size_t msg_size = ri_producer_queue_msg_size(producer->queue);
  void *msg = ri_producer_queue_msg(producer->queue);
  memcpy(msg, producer->cache, msg_size);
}


ri_consumer_t* ri_consumer_map(const ri_channel_attr_t *attr, int eventfd, ri_shm_t *shm, size_t shm_offset)
{
  ri_consumer_t *consumer = malloc(sizeof(ri_consumer_t));
  if (!consumer)
    goto fail_alloc;

  *consumer = (ri_consumer_t) {
      .shm_offset = shm_offset,
      .eventfd = attr->eventfd ? eventfd : -1,
  };

  atomic_init(&consumer->owners, RI_OWNER_GROUP_FLAG);

  consumer->queue = ri_consumer_queue_new(attr, shm, shm_offset);

  if (!consumer->queue)
    goto fail_queue;

  LOG_DBG("consumer created add_msg=%u msg_size=%zu, eventfd=%d shm_offset=%zu", attr->add_msgs, attr->msg_size, attr->eventfd, shm_offset);

  return consumer;

fail_queue:
  free(consumer);
fail_alloc:
  return NULL;
}


ri_consumer_t* ri_consumer_new(const ri_channel_attr_t *attr, ri_shm_t *shm, size_t shm_offset)
{
  int eventfd = -1;

  if (attr->eventfd) {
    eventfd = ri_eventfd_create();
    if (eventfd < 0)
      goto fail_eventfd;
  }

  ri_consumer_t *consumer = ri_consumer_map(attr, eventfd, shm, shm_offset);
  if (!consumer)
    goto fail_consumer;

  ri_consumer_queue_init_shm(consumer->queue);

  return consumer;

fail_consumer:
  if (eventfd >= 0)
    close(eventfd);
fail_eventfd:
  return NULL;
}


ri_producer_t* ri_producer_map(const ri_channel_attr_t *attr, int eventfd, ri_shm_t *shm, size_t shm_offset)
{
  ri_producer_t *producer = malloc(sizeof(ri_producer_t));
  if (!producer)
    goto fail_alloc;

  *producer = (ri_producer_t) {
    .shm_offset = shm_offset,
    .eventfd = attr->eventfd ? eventfd : -1,
  };

  atomic_init(&producer->owners, RI_OWNER_GROUP_FLAG);

  producer->queue = ri_producer_queue_new(attr, shm, shm_offset);

  if (!producer->queue)
    goto fail_queue;

  LOG_DBG("producer created add_msg=%u msg_size=%zu, eventfd=%d shm_offset=%zu", attr->add_msgs, attr->msg_size, attr->eventfd, shm_offset);

  return producer;

fail_queue:
  free(producer);
fail_alloc:
  return NULL;
}


ri_producer_t* ri_producer_new(const ri_channel_attr_t *attr, ri_shm_t *shm, size_t shm_offset)
{
  int eventfd = -1;

  if (attr->eventfd) {
    eventfd = ri_eventfd_create();
    if (eventfd < 0)
      goto fail_eventfd;
  }

  ri_producer_t *producer = ri_producer_map(attr, eventfd, shm, shm_offset);
  if (!producer)
    goto fail_producer;

  ri_producer_queue_init_shm(producer->queue);

  return producer;

fail_producer:
  if (eventfd >= 0)
    close(eventfd);
fail_eventfd:
  return NULL;
}


static void ri_producer_delete(ri_producer_t *producer)
{
    ri_producer_queue_delete(producer->queue);

    if (producer->eventfd >= 0)
        close(producer->eventfd);

    free(producer);
}


static void ri_consumer_delete(ri_consumer_t *consumer)
{
    ri_consumer_queue_delete(consumer->queue);

    if (consumer->eventfd >= 0)
        close(consumer->eventfd);

    free(consumer);
}

int ri_consumer_acquire(ri_consumer_t *consumer)
{
    unsigned owners = atomic_fetch_or(&consumer->owners, RI_OWNER_USER_FLAG);

    return owners & RI_OWNER_USER_FLAG ? -1 : 0;
}


void ri_consumer_release(ri_consumer_t *consumer)
{
    unsigned owners = atomic_fetch_and(&consumer->owners, RI_OWNER_GROUP_FLAG);
    if (!(owners & RI_OWNER_GROUP_FLAG)) {
        ri_consumer_delete(consumer);
    }
}


void ri_group_release_consumer(ri_consumer_t *consumer)
{
    unsigned owners = atomic_fetch_and(&consumer->owners, RI_OWNER_USER_FLAG);
    if (!(owners & RI_OWNER_USER_FLAG))
        ri_consumer_delete(consumer);
}


int ri_producer_acquire(ri_producer_t *producer)
{
    unsigned owners = atomic_fetch_or(&producer->owners, RI_OWNER_USER_FLAG);

    return owners & RI_OWNER_USER_FLAG ? -1 : 0;
}


void ri_producer_release(ri_producer_t *producer)
{
    unsigned owners = atomic_fetch_and(&producer->owners, RI_OWNER_GROUP_FLAG);
    if (!(owners & RI_OWNER_GROUP_FLAG))
        ri_producer_delete(producer);
}


void ri_group_release_producer(ri_producer_t *producer)
{
    unsigned owners = atomic_fetch_and(&producer->owners, RI_OWNER_USER_FLAG);
    if (!(owners & RI_OWNER_USER_FLAG))
        ri_producer_delete(producer);
}


const void* ri_consumer_msg(const ri_consumer_t *consumer)
{
  return ri_consumer_queue_msg(consumer->queue);
}


void* ri_producer_msg(const ri_producer_t *producer)
{
  return producer->cache ? producer->cache : ri_producer_queue_msg(producer->queue);
}


unsigned ri_consumer_len(const ri_consumer_t *consumer)
{
  return ri_consumer_queue_len(consumer->queue);
}


unsigned ri_producer_len(const ri_producer_t *producer)
{
  return ri_producer_queue_len(producer->queue);
}


size_t ri_consumer_msg_size(const ri_consumer_t *consumer)
{
  return ri_consumer_queue_msg_size(consumer->queue);
}


size_t ri_producer_msg_size(const ri_producer_t *producer)
{
  return ri_producer_queue_msg_size(producer->queue);
}


size_t ri_consumer_shm_offset(const ri_consumer_t *consumer)
{
  return consumer->shm_offset;
}


size_t ri_prdoucer_shm_offset(const ri_producer_t *producer)
{
  return producer->shm_offset;
}


int ri_consumer_eventfd(const ri_consumer_t *consumer)
{
  return consumer->eventfd;
}


int ri_producer_eventfd(const ri_producer_t *producer)
{
  return producer->eventfd;
}


int ri_consumer_take_eventfd(ri_consumer_t *consumer)
{
  int fd = consumer->eventfd;
  consumer->eventfd = -1;
  return fd;
}


int ri_producer_take_eventfd(ri_producer_t *producer)
{
  int fd = producer->eventfd;
  producer->eventfd = -1;
  return fd;
}


ri_pop_result_t ri_consumer_pop(ri_consumer_t *consumer)
{
  if (consumer->eventfd >= 0) {
    uint64_t v;
    int r = read(consumer->eventfd, &v, sizeof(v));

    if (r < 0) {
      return ri_consumer_queue_msg(consumer->queue)? RI_POP_RESULT_NO_UPDATE : RI_POP_RESULT_NO_MSG;
    }
  }

  return ri_consumer_queue_pop(consumer->queue);
}


ri_pop_result_t ri_consumer_flush(ri_consumer_t *consumer)
{
  ri_pop_result_t r;
  if (consumer->eventfd >= 0) {
    do {
      r = ri_consumer_pop(consumer);
    } while (r == RI_POP_RESULT_SUCCESS);
  } else {
    r = ri_consumer_queue_flush(consumer->queue);
  }

  return r;
}


ri_force_push_result_t ri_producer_force_push(ri_producer_t *producer)
{
  if (producer->cache) {
    producer_cache_write(producer);
  }

  ri_force_push_result_t r = ri_producer_queue_force_push(producer->queue);

  if ((producer->eventfd >= 0) && (r == RI_FORCE_PUSH_RESULT_SUCCESS)) {
    uint64_t v = 1;
    write(producer->eventfd, &v, sizeof(v));
  }

  return r;
}


ri_try_push_result_t ri_producer_try_push(ri_producer_t *producer)
{
  if (producer->cache) {
    if (ri_producer_queue_full(producer->queue))
      return RI_TRY_PUSH_RESULT_FAIL;

    producer_cache_write(producer);
  }

  ri_try_push_result_t r = ri_producer_queue_try_push(producer->queue);

  if ((producer->eventfd >= 0) && (r == RI_TRY_PUSH_RESULT_SUCCESS)) {
    uint64_t v = 1;
    write(producer->eventfd, &v, sizeof(v));
  }

  return r;
}


int ri_producer_cache_enable(ri_producer_t *producer)
{
  if (producer->cache)
    return 0;

  size_t msg_size = ri_producer_queue_msg_size(producer->queue);

  producer->cache = malloc(msg_size);

  if (!producer->cache)
    return -ENOMEM;

  void *msg = ri_producer_queue_msg(producer->queue);

  memcpy(producer->cache, msg, msg_size);

  return 0;
}


void ri_producer_cache_disable(ri_producer_t *producer)
{
  if (!producer->cache)
    return;

  size_t msg_size = ri_producer_queue_msg_size(producer->queue);
  void *msg = ri_producer_queue_msg(producer->queue);

  memcpy(msg, producer->cache, msg_size);

  free(producer->cache);
  producer->cache = NULL;
}
