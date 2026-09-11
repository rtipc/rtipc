#pragma once

#include "rtipc/rtipc.h"
#include "shm.h"

typedef struct ri_consumer_queue ri_consumer_queue_t;

ri_consumer_queue_t* ri_consumer_queue_new(const ri_channel_attr_t *attr, ri_shm_t *shm, size_t shm_offset);

void ri_consumer_queue_init_shm(const ri_consumer_queue_t *consumer);

void ri_consumer_queue_delete(ri_consumer_queue_t *consumer);

unsigned ri_consumer_queue_len(const ri_consumer_queue_t *consumer);

size_t ri_consumer_queue_msg_size(const ri_consumer_queue_t *consumer);

const void* ri_consumer_queue_msg(const ri_consumer_queue_t *consumer);

ri_pop_result_t ri_consumer_queue_pop(ri_consumer_queue_t *consumer);

ri_pop_result_t ri_consumer_queue_flush(ri_consumer_queue_t *consumer);

int ri_consumer_queue_count_msgs(const ri_consumer_queue_t *consumer);