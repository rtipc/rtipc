#include "vector.h"

#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

#include "rtipc/rtipc.h"
#include "channel.h"
#include "unix.h"
#include "request.h"

struct ri_group {
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


static ri_group_attr_t ri_group_attr(const ri_group_t *grp, ri_channel_attr_t **attrs)
{
  if (!attrs) {
    goto fail_args;
  }
  ri_channel_attr_t *channels = calloc(grp->n_consumers + grp->n_producers + 2, sizeof(ri_channel_attr_t));
  if (!channels) {
    goto fail_alloc;
  }

  ri_channel_attr_t *consumers = &channels[0];
  ri_channel_attr_t *producers = &channels[grp->n_consumers + 1];

  for (unsigned i = 0; i < grp->n_consumers; i++) {
    if (!grp->consumers[i])
      continue;
    consumers[i] = ri_consumer_attr(grp->consumers[i]);
  }

  for (unsigned i = 0; i < grp->n_producers; i++) {
    if (!grp->producers[i])
      continue;
    producers[i] = ri_producer_attr(grp->producers[i]);
  }

  *attrs = channels;

  return (ri_group_attr_t) {
      .consumers = consumers,
      .producers = producers,
      .info.size = grp->info.size,
      .info.data = grp->info.data,
  };


fail_alloc:
fail_args:
  return (ri_group_attr_t) {.consumers = NULL, .producers = NULL};
}


static int build_request(const ri_group_t *grp, void* req, size_t size) {
  ri_channel_attr_t *attrs = NULL;

  ri_group_attr_t vattr = ri_group_attr(grp, &attrs);
  if (!attrs)
    return -1;

  int r = ri_request_write(&vattr, req, size);

  free(attrs);

  return r;
}


static int collect_fds(const ri_group_t *grp, int fds[], unsigned n_fds) {
  if (n_fds < 1)
    return -EINVAL;

  unsigned idx = 0;

  fds[idx++] = ri_shm_get_fd(grp->shm);

  for (unsigned i = 0; i < grp->n_producers; i++) {
    if (!grp->producers[i])
      continue;

    int eventfd = ri_producer_eventfd(grp->producers[i]);

    if (eventfd >= 0) {
      if (idx >= n_fds)
        return -ENOMEM;
      fds[idx++] = eventfd;
    }
  }

  for (unsigned i = 0; i < grp->n_consumers; i++) {
    if (!grp->consumers[i])
      continue;

    int eventfd = ri_consumer_eventfd(grp->consumers[i]);

    if (eventfd >= 0) {
      if (idx >= n_fds)
        return -ENOMEM;
      fds[idx++] = eventfd;
    }
  }

  return idx;
}


static ri_group_t* ri_group_alloc(unsigned n_consumers, unsigned n_producers, const ri_info_t *info)
{
  ri_group_t *grp = calloc(1, sizeof(ri_group_t));

  if (!grp)
    goto fail_alloc;

  if (info->size > 0 && grp->info.data) {
    grp->info.data = malloc(info->size);

    if (!grp->info.data)
      goto fail_info;

    memcpy(grp->info.data, info->data, info->size);

    grp->info.size = info->size;
  }

  if (n_consumers > 0) {
    grp->consumers = calloc(n_consumers, sizeof(ri_consumer_t*));

    if (!grp->consumers)
      goto fail_consumers;
  }

  if (n_producers > 0) {
    grp->producers = calloc(n_producers, sizeof(ri_producer_t*));

    if (!grp->producers)
      goto fail_producers;
  }

  grp->n_consumers = n_consumers;
  grp->n_producers = n_producers;

  return grp;

fail_producers:
  if (n_consumers > 0)
    free(grp->consumers);
fail_consumers:
  if (grp->info.data) {
    free(grp->info.data);
  }
fail_info:
  free(grp);
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


ri_group_t* ri_group_new(const ri_group_attr_t *vattr)
{
  unsigned n_producers = ri_count_channels(vattr->producers);
  unsigned n_consumers = ri_count_channels(vattr->consumers);

  ri_group_t *grp = ri_group_alloc(n_consumers, n_producers, &vattr->info);
  if (!grp)
    goto fail_alloc;

  size_t shm_size = ri_calc_shm_size(vattr->consumers, vattr->producers);

  grp->shm = shm_new(shm_size);
  if (!grp->shm)
    goto fail_shm;

  size_t shm_offset = 0;


  for (unsigned i = 0; i < grp->n_producers; i++) {
    const ri_channel_attr_t *attr = &vattr->producers[i];

    grp->producers[i] = ri_producer_new(attr, grp->shm, shm_offset);
    if (!grp->producers[i])
      goto fail_channel;

    shm_offset += ri_channel_shm_size(attr);
  }

  for (unsigned i = 0; i < grp->n_consumers; i++) {
    const ri_channel_attr_t *attr = &vattr->consumers[i];

    grp->consumers[i] = ri_consumer_new(attr, grp->shm, shm_offset);
    if (!grp->consumers[i])
      goto fail_channel;

    shm_offset += ri_channel_shm_size(attr);
  }

  return grp;

fail_channel:
fail_shm:
  ri_group_delete(grp);
fail_alloc:
  return NULL;
}


void ri_group_delete(ri_group_t* grp)
{
  if (grp->consumers) {
    for (unsigned i = 0; i < grp->n_consumers; i++) {
        ri_group_release_consumer(grp->consumers[i]);
    }
    free(grp->consumers);
  }

  if (grp->producers) {
    for (unsigned i = 0; i < grp->n_producers; i++) {
        ri_group_release_producer(grp->producers[i]);
    }
    free(grp->producers);
  }

  if (grp->shm)
    ri_shm_unref(grp->shm);

  free(grp);
}


size_t ri_group_serialize_size(const ri_group_t *grp)
{
  ri_channel_attr_t *attrs = NULL;

  ri_group_attr_t config = ri_group_attr(grp, &attrs);
  if (!attrs)
    return 0;

  size_t size = ri_request_calc_size(&config);

  free(attrs);

  return size;
}


int ri_group_serialize(const ri_group_t *grp, void* req, size_t size, int fds[], unsigned *n_fds)
{
  if (!n_fds || (*n_fds < 1))
    return -EINVAL;

  int r = build_request(grp, req, size);
  if (r < 0)
    return r;

  r = collect_fds(grp, fds, *n_fds);
  if (r < 0)
    return r;

  *n_fds = r;

  return 0;
}


static ri_group_t* ri_group_map(const ri_group_attr_t *vattr, int fds[], unsigned *n_fds)
{
  if (!fds || !n_fds || (*n_fds < 1))
    goto fail_args;

  int eventfd = -1;
  unsigned n_consumers = ri_count_channels(vattr->consumers);
  unsigned n_producers = ri_count_channels(vattr->producers);

  ri_group_t *grp = ri_group_alloc(n_consumers, n_producers, &vattr->info);
  if (!grp)
    goto fail_alloc;

  int r = ri_memfd_verify(fds[0]);
  if (r < 0)
    goto fail_shm;

  grp->shm = ri_shm_map(fds[0]);
  if (!grp->shm)
    goto fail_shm;

  /* ownership of shmfd transfered to shm */
  fds[0] = -1;

  unsigned idx = 1;
  size_t shm_offset = 0;

  for (unsigned i = 0; i < grp->n_consumers; i++) {
    const ri_channel_attr_t *attr = &vattr->consumers[i];

    if (attr->eventfd) {
      eventfd = take_eventfd(idx++, fds, *n_fds);
      if (eventfd < 0)
        goto fail_channel;
    }

    grp->consumers[i] = ri_consumer_map(attr, eventfd, grp->shm, shm_offset);

    if (!grp->consumers[i])
      goto fail_channel;

    /* ownership of eventfd transfered to consumer */
    eventfd = -1;
    shm_offset += ri_channel_shm_size(attr);
  }

  for (unsigned i = 0; i < grp->n_producers; i++) {
    const ri_channel_attr_t *attr = &vattr->producers[i];

    if (attr->eventfd) {
      eventfd = take_eventfd(idx++, fds, *n_fds);
      if (eventfd < 0)
        goto fail_channel;
    }

    grp->producers[i] = ri_producer_map(attr, eventfd, grp->shm, shm_offset);
    if (!grp->producers[i])
      goto fail_channel;

    /* ownership of eventfd transfered to producer */
    eventfd = -1;
    shm_offset += ri_channel_shm_size(attr);
  }

  return grp;

fail_channel:
  if (eventfd >= 0)
    close(eventfd);
fail_shm:
  ri_group_delete(grp);
fail_alloc:
fail_args:
  return NULL;
}


ri_group_t* ri_group_deserialize(const void* req, size_t size, int fds[], unsigned *n_fds)
{
  if (!n_fds || (*n_fds < 1))
    return NULL;

  ri_channel_attr_t *attrs = NULL;
  ri_group_attr_t vattr = ri_request_parse(req, size, &attrs);
  if (!attrs)
    return NULL;

  ri_group_t *grp = ri_group_map(&vattr, fds, n_fds);

  free(attrs);

  return grp;
}


unsigned ri_group_num_producers(const ri_group_t *grp)
{
  return grp->n_producers;
}


unsigned ri_group_num_consumers(const ri_group_t *grp)
{
  return grp->n_consumers;
}


ri_info_t ri_group_get_info(const ri_group_t* grp)
{
  return (ri_info_t) {
    .data = grp->info.data,
    .size = grp->info.size,
  };
}


void ri_group_free_info(ri_group_t* grp)
{
  if (grp->info.data) {
    free(grp->info.data);
    grp->info.data = NULL;
    grp->info.size = 0;
  }
}


ri_producer_t* ri_group_acquire_producer(ri_group_t *grp, unsigned index)
{
  if (index >= grp->n_producers)
    return NULL;

  ri_producer_t* producer = grp->producers[index];

  int r = ri_producer_acquire(producer);

  if (r < 0)
      return NULL;

  return producer;
}


ri_consumer_t* ri_group_acquire_consumer(ri_group_t *grp, unsigned index)
{
  if (index >= grp->n_consumers)
    return NULL;

  ri_consumer_t* consumer = grp->consumers[index];

  int r = ri_consumer_acquire(consumer);

  if (r < 0)
      return NULL;

  return consumer;
}
