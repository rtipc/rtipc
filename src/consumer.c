#include "consumer.h"

#include <stdlib.h>

#include "queue.h"

struct ri_consumer_queue {
  /**
   * Pointer to the shared memory this queue is mapped to.
   * Only used to decrement the shared memory reference counter on deletion.
   */
  ri_shm_t *shm;

  /**
   * The queue structure stored in shared memory.
   */
  ri_queue_t queue;

  /**
   * Index of the message currently being used by the consumer.
   */
  ri_index_t current;
};


unsigned ri_consumer_queue_len(const ri_consumer_queue_t *consumer)
{
  return consumer->queue.n_msgs;
}


size_t ri_consumer_queue_msg_size(const ri_consumer_queue_t *consumer)
{
  return consumer->queue.msg_size;
}


ri_consumer_queue_t* ri_consumer_queue_new(const ri_channel_attr_t *attr, ri_shm_t *shm, size_t shm_offset)
{
  ri_consumer_queue_t *consumer = malloc(sizeof(ri_consumer_queue_t));
  if (!consumer)
    goto fail_alloc;

  *consumer = (ri_consumer_queue_t) {
      .shm = shm,
      .current = RI_INDEX_INVALID,
  };

  void *ptr = ri_shm_ptr(shm, shm_offset);

  if (!ptr)
    goto fail_shm;

  ri_queue_init(&consumer->queue, attr, ptr);

  ri_shm_ref(consumer->shm);

  return consumer;

fail_shm:
  free(consumer);
fail_alloc:
  return NULL;
}


void ri_consumer_queue_delete(ri_consumer_queue_t *consumer)
{
  ri_shm_unref(consumer->shm);
  free(consumer);
}


void ri_consumer_queue_init_shm(const ri_consumer_queue_t *consumer)
{
  ri_queue_init_shm(&consumer->queue);
}


int ri_consumer_queue_count_msgs(const ri_consumer_queue_t *consumer)
{
  const ri_queue_t *queue = &consumer->queue;

  ri_index_t next = ri_queue_tail_load(queue);

  if (next == RI_INDEX_INVALID)
    return 0;

  next &= RI_INDEX_MASK;

  if (!ri_queue_index_valid(queue, next))
    return -1;

  unsigned cnt;
  for (cnt = 0; cnt < queue->n_msgs; cnt++) {
    next = ri_queue_chain_load(queue, next);

    if (next == RI_INDEX_INVALID)
      /* end of queue, no newer message available */
      break;

    if (!ri_queue_index_valid(queue, next))
      return -1;
  }

  /* check again for overrun */
  next = ri_queue_tail_load(queue);

  if (!(next & RI_CONSUMED_FLAG) && (consumer->current != RI_INDEX_INVALID)) {
    return queue->n_msgs - 1;
  }

  return cnt + 1;
}

ri_pop_result_t ri_consumer_queue_flush(ri_consumer_queue_t *consumer)
{
  ri_queue_t *queue = &consumer->queue;

  for (;;) {
    ri_index_t tail = ri_queue_tail_fetch_or(queue, RI_CONSUMED_FLAG);

    if (tail == RI_INDEX_INVALID) {
      /* or CONSUMED_FLAG doesn't change INDEX_END*/
      return RI_POP_RESULT_NO_MSG;
    }

    if (!ri_queue_index_valid(queue, tail & RI_INDEX_MASK)) {
      return RI_POP_RESULT_ERROR;
    }

    ri_index_t head = ri_queue_head_load(queue);

    if (!ri_queue_index_valid(queue, head)) {
      return RI_POP_RESULT_ERROR;
    }

    tail |= RI_CONSUMED_FLAG;

    if (ri_queue_tail_compare_exchange(queue, tail, head | RI_CONSUMED_FLAG)) {
      /* only accept head if producer didn't move tail,
           *  otherwise the producer could fill the whole queue and the head could be the
           *  producers current message  */
      consumer->current = head;
      break;
    }
  }

  return RI_POP_RESULT_DISCARDED;
}

ri_pop_result_t ri_consumer_queue_pop(ri_consumer_queue_t *consumer)
{
  ri_queue_t *queue = &consumer->queue;
  ri_index_t tail = ri_queue_tail_fetch_or(queue, RI_CONSUMED_FLAG);

  if (tail == RI_INDEX_INVALID)
    return RI_POP_RESULT_NO_MSG;

  if (!ri_queue_index_valid(queue, tail & RI_INDEX_MASK))
    return RI_POP_RESULT_ERROR;

  if ((tail & RI_CONSUMED_FLAG) == 0) {
    /* producer moved tail (force_push), so use it; one or more messages were discarded */
    consumer->current = tail & RI_INDEX_MASK;
    return (tail & RI_FIRST_FLAG) ? RI_POP_RESULT_SUCCESS : RI_POP_RESULT_DISCARDED;
  }

  if (consumer->current == RI_INDEX_INVALID) {
    /* consumed flag was set, but we don't have a message yet */
    return RI_POP_RESULT_ERROR;
  }

  /* try to get next message */
  ri_index_t next = ri_queue_chain_load(queue, consumer->current);

  if (next == RI_INDEX_INVALID)
    /* end of queue, no newer message available */
    return RI_POP_RESULT_NO_UPDATE;

  if (!ri_queue_index_valid(queue, next))
    return RI_POP_RESULT_ERROR;

  if (ri_queue_tail_compare_exchange(queue, tail, next | RI_CONSUMED_FLAG)) {
    consumer->current = next;
    return RI_POP_RESULT_SUCCESS;
  } else {
    /* producer just moved tail, use it */
    ri_index_t current = ri_queue_tail_fetch_or(queue, RI_CONSUMED_FLAG);

    if (!ri_queue_index_valid(queue, current))
      return RI_POP_RESULT_ERROR;

    consumer->current = current;

    return RI_POP_RESULT_DISCARDED;
  }
}

const void* ri_consumer_queue_msg(const ri_consumer_queue_t *consumer)
{
  if (consumer->current == RI_INDEX_INVALID)
    return NULL;

  return ri_queue_get_msg(&consumer->queue, consumer->current);
}
