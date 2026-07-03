#pragma once

#include <stddef.h>

#include "rtipc/rtipc.h"

#include "index.h"


typedef struct ri_queue
{
  /**
   * Number of messages in the queue. Must be at least 3.
   */
  unsigned n_msgs;

  /**
   * Size of each message in bytes.
   */
  size_t msg_size;

  /**
   * Size of each message aligned to the cache line size for optimal memory access.
   */
  size_t msg_size_aligned;

  /**
   * Pointer to the contiguous block of message storage.
   */
  void* msgs;

  /**
   * Tail index for the queue (atomic). Both producer and consumer can update it.
   * The most significant bit (MSB) indicates which side last modified the tail.
  */
  ri_atomic_index_t *tail;

  /**
   * Head index for the queue (atomic). Written by the producer,
   * but only used by the consumer during flush operations.
   */
  ri_atomic_index_t *head;

  /**
   * Circular queue used for message ordering.
   * Initially set as queue[i] = (i + 1) % n_msgs.
   * May become scrambled due to overruns.
   * Only the producer modifies this array.
   */
  ri_atomic_index_t *chain;
} ri_queue_t;


void* ri_queue_get_msg(const ri_queue_t *queue, ri_index_t idx);

void ri_queue_init(ri_queue_t *queue, const ri_channel_attr_t *attr, void* shm);

void ri_queue_init_shm(const ri_queue_t *queue);

void ri_queue_dump(ri_queue_t *queue);

static inline ri_index_t ri_queue_tail_load(const ri_queue_t *queue)
{
  return atomic_load(queue->tail);
}


static inline void ri_queue_tail_store(const ri_queue_t *queue, ri_index_t val)
{
  atomic_store(queue->tail, val);
}


static inline ri_index_t ri_queue_tail_fetch_or(const ri_queue_t *queue, ri_index_t val)
{
  return atomic_fetch_or(queue->tail, val);
}


static inline bool ri_queue_tail_compare_exchange(const ri_queue_t *queue,
                                                    ri_index_t expected,
                                                    ri_index_t desired)
{
  return atomic_compare_exchange_strong(queue->tail, &expected, desired);
}


static inline ri_index_t ri_queue_head_load(const ri_queue_t *queue)
{
  return atomic_load(queue->head);
}


static inline void ri_queue_head_store(const ri_queue_t *queue, ri_index_t val)
{
  atomic_store(queue->head, val);
}


static inline ri_index_t ri_queue_chain_load(const ri_queue_t *queue, ri_index_t idx)
{
  return atomic_load(&queue->chain[idx]);
}


static inline void ri_queue_chain_store(const ri_queue_t *queue,
                                          ri_index_t idx,
                                          ri_index_t val)
{
  atomic_store(&queue->chain[idx], val);
}


static inline bool ri_queue_index_valid(const ri_queue_t *queue, ri_index_t idx)
{
  return idx < queue->n_msgs;
}
