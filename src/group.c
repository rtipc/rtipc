#include "group.h"

#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

#include "rtipc/rtipc.h"
#include "channel.h"
#include "attr.h"
#include "unix.h"
#include "request.h"

struct ri_group {
  ri_group_data_t rsc;
  ri_shm_t *shm;
  ri_consumer_t **consumers;
  ri_producer_t **producers;
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


ri_group_attr_t ri_group_attr(const ri_group_t *grp)
{
  return ri_group_data_attr(&grp->rsc);
}




static int collect_fds(const ri_group_t *grp, int fds[], unsigned n_fds) {
  if (n_fds < 1)
    return -EINVAL;

  unsigned idx = 0;

  fds[idx++] = ri_shm_get_fd(grp->shm);

  for (unsigned i = 0; i < grp->rsc.n_producers; i++) {
    if (!grp->producers[i])
      continue;

    int eventfd = ri_producer_eventfd(grp->producers[i]);

    if (eventfd >= 0) {
      if (idx >= n_fds)
        return -ENOMEM;
      fds[idx++] = eventfd;
    }
  }

  for (unsigned i = 0; i < grp->rsc.n_consumers; i++) {
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



static ri_group_t* ri_group_alloc(const ri_group_attr_t *attr)
{
  ri_group_t *grp = calloc(1, sizeof(ri_group_t));

  if (!grp)
    goto fail_alloc;

  int r = ri_group_data_from_attr(&grp->rsc, attr);
  if (r < 0)
    goto fail_rsc;

  ri_group_data_t *rsc = &grp->rsc;

  if (rsc->n_consumers > 0) {
    grp->consumers = calloc(rsc->n_consumers, sizeof(ri_consumer_t*));

    if (!grp->consumers)
      goto fail_consumers;
  }

  if (rsc->n_producers > 0) {
    grp->producers = calloc(rsc->n_producers, sizeof(ri_producer_t*));

    if (!grp->producers)
      goto fail_producers;
  }

  return grp;

fail_producers:
  if (grp->consumers)
    free(grp->consumers);
fail_consumers:
  ri_group_data_delete(rsc);
fail_rsc:
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


ri_group_t* ri_group_new(const ri_group_attr_t *attr)
{
  ri_group_t *grp = ri_group_alloc(attr);

  if (!grp)
    goto fail_alloc;

  size_t shm_size = ri_calc_shm_size(attr->consumers, attr->producers);

  grp->shm = shm_new(shm_size);
  if (!grp->shm)
    goto fail_shm;

  size_t shm_offset = 0;

  ri_group_data_t *rsc = &grp->rsc;

  for (unsigned i = 0; i < rsc->n_producers; i++) {
    const ri_channel_attr_t *attr = &rsc->producers[i];

    grp->producers[i] = ri_producer_new(attr, grp->shm, shm_offset);
    if (!grp->producers[i])
      goto fail_channel;

    shm_offset += ri_channel_shm_size(attr);
  }

  for (unsigned i = 0; i < rsc->n_consumers; i++) {
    const ri_channel_attr_t *attr = &rsc->consumers[i];

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
    for (unsigned i = 0; i < grp->rsc.n_consumers; i++) {
        if (grp->consumers[i])
          ri_group_release_consumer(grp->consumers[i]);
    }

    free(grp->consumers);
  }

  if (grp->producers) {
    for (unsigned i = 0; i < grp->rsc.n_producers; i++) {
        if (grp->producers[i])
          ri_group_release_producer(grp->producers[i]);
    }

    free(grp->producers);
  }

  if (grp->shm)
    ri_shm_unref(grp->shm);

  ri_group_data_delete(&grp->rsc);

  free(grp);
}


size_t ri_group_serialize_size(const ri_group_t *grp)
{
  ri_group_attr_t attr = ri_group_data_attr(&grp->rsc);
  return ri_request_calc_size(&attr);
}


int ri_group_serialize(const ri_group_t *grp, void* req, size_t size, int fds[], unsigned *n_fds)
{
  if (!n_fds || (*n_fds < 1))
    return -EINVAL;

  ri_group_attr_t attr = ri_group_data_attr(&grp->rsc);
  int r =  ri_request_write(&attr, req, size);
  if (r < 0)
    return r;

  r = collect_fds(grp, fds, *n_fds);
  if (r < 0)
    return r;

  *n_fds = r;

  return 0;
}


static int ri_group_map(ri_group_t *grp, int fds[], unsigned *n_fds)
{
  if (!fds || !n_fds || (*n_fds < 1))
    goto fail_args;

  ri_group_data_t *rsc = &grp->rsc;

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

  int eventfd = -1;

  for (unsigned i = 0; i < rsc->n_consumers; i++) {
    const ri_channel_attr_t *attr = &rsc->consumers[i];

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

  for (unsigned i = 0; i < rsc->n_producers; i++) {
    const ri_channel_attr_t *attr = &rsc->producers[i];

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

  return 0;

fail_channel:
  if (eventfd >= 0)
    close(eventfd);
fail_shm:
fail_alloc:
fail_args:
  return -1;
}


ri_group_t* ri_group_deserialize(const void* req, size_t size, int fds[], unsigned *n_fds)
{
  if (!n_fds || (*n_fds < 1))
    goto fail_args;

  ri_group_t *grp = calloc(1, sizeof(ri_group_t));
  if (!grp)
    goto fail_alloc;

  int r = ri_request_parse(&grp->rsc, req, size);
  if (r < 0)
    goto fail_parse;

  r = ri_group_map(grp, fds, n_fds);
  if (r < 0)
    goto fail_map;

  return grp;

fail_map:
fail_parse:
  ri_group_delete(grp);
fail_alloc:
fail_args:
  return NULL;
}


unsigned ri_group_num_producers(const ri_group_t *grp)
{
  return grp->rsc.n_producers;
}


unsigned ri_group_num_consumers(const ri_group_t *grp)
{
  return grp->rsc.n_consumers;
}


ri_info_t ri_group_get_info(const ri_group_t* grp)
{
    return grp->rsc.info;
}


ri_producer_t* ri_group_acquire_producer(ri_group_t *grp, unsigned index)
{
  if (index >= grp->rsc.n_producers)
    return NULL;

  ri_producer_t* producer = grp->producers[index];

  int r = ri_producer_acquire(producer);

  if (r < 0)
      return NULL;

  return producer;
}


ri_consumer_t* ri_group_acquire_consumer(ri_group_t *grp, unsigned index)
{
  if (index >= grp->rsc.n_consumers)
    return NULL;

  ri_consumer_t* consumer = grp->consumers[index];

  int r = ri_consumer_acquire(consumer);

  if (r < 0)
      return NULL;

  return consumer;
}
