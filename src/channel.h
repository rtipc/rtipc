#pragma once

#include "rtipc/rtipc.h"

#include "shm.h"

#define RI_CHANNEL_MIN_MSGS 3

size_t ri_calc_queue_size(unsigned n_msgs);

size_t ri_calc_channel_shm_size(unsigned n_msgs, size_t msg_size);


static inline unsigned ri_count_channels(const ri_channel_attr_t attrs[])
{
  if (!attrs)
    return 0;

  unsigned i;

  for (i = 0; attrs[i].msg_size != 0; i++)
    ;

  return i;
}


static inline unsigned ri_channel_queue_len(const ri_channel_attr_t *attr)
{
  return RI_CHANNEL_MIN_MSGS + attr->add_msgs;
}


static inline size_t ri_channel_queue_size(const ri_channel_attr_t *attr)
{
  return ri_calc_queue_size(ri_channel_queue_len(attr));
}


static inline size_t ri_channel_shm_size(const ri_channel_attr_t *attr)
{
  return ri_calc_channel_shm_size(ri_channel_queue_len(attr), attr->msg_size);
}


size_t ri_calc_shm_size(const ri_channel_attr_t consumers[], const ri_channel_attr_t producers[]);


ri_consumer_t* ri_consumer_new(const ri_channel_attr_t *attr, ri_shm_t *shm, size_t shm_offset);

int ri_consumer_acquire(ri_consumer_t *consumer);

void ri_vector_release_consumer(ri_consumer_t *consumer);

ri_producer_t* ri_producer_new(const ri_channel_attr_t *attr, ri_shm_t *shm, size_t shm_offset);

int ri_producer_acquire(ri_producer_t *producer);

void ri_vector_release_producer(ri_producer_t *producer);

ri_consumer_t* ri_consumer_map(const ri_channel_attr_t *attr, int eventfd, ri_shm_t *shm, size_t shm_offset);

ri_producer_t* ri_producer_map(const ri_channel_attr_t *attr, int eventfd, ri_shm_t *shm, size_t shm_offset);

ri_channel_attr_t ri_consumer_attr(const ri_consumer_t *consumer);

ri_channel_attr_t ri_producer_attr(const ri_producer_t *producer);

size_t ri_consumer_shm_offset(const ri_consumer_t *consumer);

size_t ri_prdoucer_shm_offset(const ri_producer_t *producer);

unsigned ri_consumer_len(const ri_consumer_t *consumer);

unsigned ri_producer_len(const ri_producer_t *producer);






