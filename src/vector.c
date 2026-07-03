#include "vector.h"

#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

#include "rtipc/rtipc.h"
#include "channel.h"
#include "unix.h"
#include "request.h"

struct ri_vector {
  ri_shm_t *shm;
  unsigned n_consumers;
  unsigned n_producers;
  ri_consumer_t **consumers;
  ri_producer_t **producers;
  struct {
    size_t size;
    void *data;
  } info;
};


static int take_eventfd(unsigned idx, int fds[], unsigned n_fds)
{
  if (idx >= n_fds)
    return -1;

  int eventfd = fds[idx];
  if (eventfd < 0)
    return eventfd;

  int r = ri_eventfd_verify(eventfd);
  if (r < 0)
    return r;

  r = ri_set_nonblocking(eventfd);
  if (r < 0)
    return r;

  fds[idx] = -1;

  return eventfd;
}


static ri_config_t ri_vector_config(const ri_vector_t *vec, ri_attr_t **attrs)
{
  if (!attrs) {
    goto fail_args;
  }
  ri_attr_t *channels = calloc(vec->n_consumers + vec->n_producers + 2, sizeof(ri_attr_t));
  if (!channels) {
    goto fail_alloc;
  }

  ri_attr_t *consumers = &channels[0];
  ri_attr_t *producers = &channels[vec->n_consumers + 1];

  for (unsigned i = 0; i < vec->n_consumers; i++) {
    if (!vec->consumers[i])
      continue;
    consumers[i] = ri_consumer_attr(vec->consumers[i]);
  }

  for (unsigned i = 0; i < vec->n_producers; i++) {
    if (!vec->producers[i])
      continue;
    producers[i] = ri_producer_attr(vec->producers[i]);
  }

  *attrs = channels;

  return (ri_config_t) {
      .consumers = consumers,
      .producers = producers,
      .info.size = vec->info.size,
      .info.data = vec->info.data,
  };


fail_alloc:
fail_args:
  return (ri_config_t) {.consumers = NULL, .producers = NULL};
}


static int build_request(const ri_vector_t *vec, void* req, size_t size) {
  ri_attr_t *attrs = NULL;

  ri_config_t config = ri_vector_config(vec, &attrs);
  if (!attrs)
    return -1;

  int r = ri_request_write(&config, req, size);

  free(attrs);

  return r;
}


static int collect_fds(const ri_vector_t *vec, int fds[], unsigned n_fds) {
  if (n_fds < 1)
    return -EINVAL;

  unsigned idx = 0;

  fds[idx++] = ri_shm_get_fd(vec->shm);

  for (unsigned i = 0; i < vec->n_producers; i++) {
    if (!vec->producers[i])
      continue;

    int eventfd = ri_producer_eventfd(vec->producers[i]);

    if (eventfd >= 0) {
      if (idx >= n_fds)
        return -ENOMEM;
      fds[idx++] = eventfd;
    }
  }

  for (unsigned i = 0; i < vec->n_consumers; i++) {
    if (!vec->consumers[i])
      continue;

    int eventfd = ri_consumer_eventfd(vec->consumers[i]);

    if (eventfd >= 0) {
      if (idx >= n_fds)
        return -ENOMEM;
      fds[idx++] = eventfd;
    }
  }

  return idx;
}


static ri_vector_t* ri_vector_alloc(unsigned n_consumers, unsigned n_producers, const ri_info_t *info)
{
  ri_vector_t *vec = calloc(1, sizeof(ri_vector_t));

  if (!vec)
    goto fail_alloc;

  if (info->size > 0 && vec->info.data) {
    vec->info.data = malloc(info->size);

    if (!vec->info.data)
      goto fail_info;

    memcpy(vec->info.data, info->data, info->size);

    vec->info.size = info->size;
  }

  if (n_consumers > 0) {
    vec->consumers = calloc(n_consumers, sizeof(ri_consumer_t*));

    if (!vec->consumers)
      goto fail_consumers;
  }

  if (n_producers > 0) {
    vec->producers = calloc(n_producers, sizeof(ri_producer_t*));

    if (!vec->producers)
      goto fail_producers;
  }

  vec->n_consumers = n_consumers;
  vec->n_producers = n_producers;

  return vec;

fail_producers:
  if (n_consumers > 0)
    free(vec->consumers);
fail_consumers:
  if (vec->info.data) {
    free(vec->info.data);
  }
fail_info:
  free(vec);
fail_alloc:
  return NULL;
}

static ri_shm_t* shm_new(size_t shm_size)
{
  int shmfd = ri_shmfd_create(shm_size);
  if (shmfd < 0)
    goto fail_fd;

  ri_shm_t *shm = ri_shm_map(shmfd);

  if (!shm)
    goto fail_shm;

  return shm;

fail_shm:
  close(shmfd);
fail_fd:
  return NULL;
}


ri_vector_t* ri_vector_new(const ri_config_t *config)
{
  unsigned n_producers = ri_count_channels(config->producers);
  unsigned n_consumers = ri_count_channels(config->consumers);

  ri_vector_t *vec = ri_vector_alloc(n_consumers, n_producers, &config->info);
  if (!vec)
    goto fail_alloc;

  size_t shm_size = ri_calc_shm_size(config->consumers, config->producers);

  vec->shm = shm_new(shm_size);
  if (!vec->shm)
    goto fail_shm;

  size_t shm_offset = 0;


  for (unsigned i = 0; i < vec->n_producers; i++) {
    const ri_attr_t *attr = &config->producers[i];

    vec->producers[i] = ri_producer_new(attr, vec->shm, shm_offset);
    if (!vec->producers[i])
      goto fail_channel;

    shm_offset += ri_channel_shm_size(attr);
  }

  for (unsigned i = 0; i < vec->n_consumers; i++) {
    const ri_attr_t *attr = &config->consumers[i];

    vec->consumers[i] = ri_consumer_new(attr, vec->shm, shm_offset);
    if (!vec->consumers[i])
      goto fail_channel;

    shm_offset += ri_channel_shm_size(attr);
  }

  return vec;

fail_channel:
fail_shm:
  ri_vector_delete(vec);
fail_alloc:
  return NULL;
}


void ri_vector_delete(ri_vector_t* vec)
{
  if (vec->consumers) {
    for (unsigned i = 0; i < vec->n_consumers; i++) {
        ri_vector_release_consumer(vec->consumers[i]);
    }
    free(vec->consumers);
  }

  if (vec->producers) {
    for (unsigned i = 0; i < vec->n_producers; i++) {
        ri_vector_release_producer(vec->producers[i]);
    }
    free(vec->producers);
  }

  if (vec->shm)
    ri_shm_unref(vec->shm);

  free(vec);
}


size_t ri_vector_serialize_size(const ri_vector_t *vec)
{
  ri_attr_t *attrs = NULL;

  ri_config_t config = ri_vector_config(vec, &attrs);
  if (!attrs)
    return 0;

  size_t size = ri_request_calc_size(&config);

  free(attrs);

  return size;
}


int ri_vector_serialize(const ri_vector_t *vec, void* req, size_t size, int fds[], unsigned *n_fds)
{
  if (!n_fds || (*n_fds < 1))
    return -EINVAL;

  int r = build_request(vec, req, size);
  if (r < 0)
    return r;

  r = collect_fds(vec, fds, *n_fds);
  if (r < 0)
    return r;

  *n_fds = r;

  return 0;
}


static ri_vector_t* ri_vector_map(const ri_config_t *config, int fds[], unsigned *n_fds)
{
  if (!fds || !n_fds || (*n_fds < 1))
    goto fail_args;

  int eventfd = -1;
  unsigned n_consumers = ri_count_channels(config->consumers);
  unsigned n_producers = ri_count_channels(config->producers);

  ri_vector_t *vec = ri_vector_alloc(n_consumers, n_producers, &config->info);
  if (!vec)
    goto fail_alloc;

  int r = ri_memfd_verify(fds[0]);
  if (r < 0)
    goto fail_shm;

  vec->shm = ri_shm_map(fds[0]);
  if (!vec->shm)
    goto fail_shm;

  /* ownership of shmfd transfered to shm */
  fds[0] = -1;

  unsigned idx = 1;
  size_t shm_offset = 0;

  for (unsigned i = 0; i < vec->n_consumers; i++) {
    const ri_attr_t *attr = &config->consumers[i];

    if (attr->eventfd) {
      eventfd = take_eventfd(idx++, fds, *n_fds);
      if (eventfd < 0)
        goto fail_channel;
    }

    vec->consumers[i] = ri_consumer_map(attr, eventfd, vec->shm, shm_offset);

    if (!vec->consumers[i])
      goto fail_channel;

    /* ownership of eventfd transfered to consumer */
    eventfd = -1;
    shm_offset += ri_channel_shm_size(attr);
  }

  for (unsigned i = 0; i < vec->n_producers; i++) {
    const ri_attr_t *attr = &config->producers[i];

    if (attr->eventfd) {
      eventfd = take_eventfd(idx++, fds, *n_fds);
      if (eventfd < 0)
        goto fail_channel;
    }

    vec->producers[i] = ri_producer_map(attr, eventfd, vec->shm, shm_offset);
    if (!vec->producers[i])
      goto fail_channel;

    /* ownership of eventfd transfered to producer */
    eventfd = -1;
    shm_offset += ri_channel_shm_size(attr);
  }

  return vec;

fail_channel:
  if (eventfd >= 0)
    close(eventfd);
fail_shm:
  ri_vector_delete(vec);
fail_alloc:
fail_args:
  return NULL;
}


ri_vector_t* ri_vector_deserialize(const void* req, size_t size, int fds[], unsigned *n_fds)
{
  if (!n_fds || (*n_fds < 1))
    return NULL;

  ri_attr_t *attrs = NULL;
  ri_config_t config = ri_request_parse(req, size, &attrs);
  if (!attrs)
    return NULL;

  ri_vector_t *vec = ri_vector_map(&config, fds, n_fds);

  free(attrs);

  return vec;
}


unsigned ri_vector_num_producers(const ri_vector_t *vec)
{
  return vec->n_producers;
}


unsigned ri_vector_num_consumers(const ri_vector_t *vec)
{
  return vec->n_consumers;
}


ri_info_t ri_vector_get_info(const ri_vector_t* vec)
{
  return (ri_info_t) {
    .data = vec->info.data,
    .size = vec->info.size,
  };
}


void ri_vector_free_info(ri_vector_t* vec)
{
  if (vec->info.data) {
    free(vec->info.data);
    vec->info.data = NULL;
    vec->info.size = 0;
  }
}


ri_producer_t* ri_vector_acquire_producer(ri_vector_t *vec, unsigned index)
{
  if (index >= vec->n_producers)
    return NULL;

  ri_producer_t* producer = vec->producers[index];

  int r = ri_producer_acquire(producer);

  if (r < 0)
      return NULL;

  return producer;
}


ri_consumer_t* ri_vector_acquire_consumer(ri_vector_t *vec, unsigned index)
{
  if (index >= vec->n_consumers)
    return NULL;

  ri_consumer_t* consumer = vec->consumers[index];

  int r = ri_consumer_acquire(consumer);

  if (r < 0)
      return NULL;

  return consumer;
}
