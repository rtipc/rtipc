#include "attr.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "channel.h"
#include "mem_utils.h"
#include "rtipc/rtipc.h"



size_t ri_info_align(size_t size)
{
  return mem_maxalign(size);
}

static size_t calc_channels_info_size(const ri_channel_attr_t attrs[])
{
  if (!attrs)
    return 0;

  size_t size = 0;
  for (const ri_channel_attr_t *attr = attrs; attr->msg_size != 0; attr++) {
    if (attr->info.size > 0) {
      size += ri_info_align(attr->info.size);
    }
  }

  return size;
}


size_t ri_attr_calc_info_size(const ri_group_attr_t *attr)
{
  size_t size = mem_maxalign(attr->info.size);

  size += calc_channels_info_size(attr->consumers);

  size += calc_channels_info_size(attr->producers);

  return size;
}


static void* ri_info_copy(void* mem, ri_info_t *dest, const ri_info_t *src)
{
  if (!src->data || (src->size == 0)) {
    *dest = (ri_info_t) { 0 };
    return dest;
  }

  memcpy(mem, src->data, src->size);

  *dest = (ri_info_t) { .data = mem, .size = src->size };

  return mem_offset(mem, ri_info_align(src->size));
}


static void* copy_attrs(ri_channel_attr_t *dest, const ri_channel_attr_t *src, unsigned n,
      void *infos)
{
  for (unsigned i = 0; i < n; i++) {
    dest[i] = src[i];

    infos = ri_info_copy(infos, &dest[i].info, &src[i].info);
  }

  return infos;
}


int ri_group_data_new(ri_group_data_t *rsc, unsigned n_consumers, unsigned n_producers)
{
  *rsc = (ri_group_data_t){
    .n_consumers = n_consumers,
    .n_producers = n_producers,
  };

  if (rsc->n_consumers > 0) {
    rsc->consumers = calloc(rsc->n_consumers + 1, sizeof(ri_channel_attr_t));

    if (!rsc->consumers) {
      goto fail_consumers;
    }
  }

  if (rsc->n_producers > 0) {
    rsc->producers = calloc(rsc->n_producers + 1, sizeof(ri_channel_attr_t));

    if (!rsc->producers) {
      goto fail_producers;
    }
  }

fail_producers:
  if (rsc->consumers)
    free(rsc->consumers);
fail_consumers:
  return -ENOMEM;
}


int ri_group_data_from_attr(ri_group_data_t *grp_data, const ri_group_attr_t *attr)
{
  unsigned n_consumers = ri_count_channels(attr->consumers);
  unsigned n_producers = ri_count_channels(attr->producers);

  int r = ri_group_data_new(grp_data, n_consumers, n_producers);
  if (r < 0)
    goto fail_alloc;

  size_t info_size = ri_attr_calc_info_size(attr);

  if (info_size > 0) {
    grp_data->mem_infos = malloc(info_size);
    if (!grp_data->mem_infos) {
      goto fail_infos;
    }
  }

  void *infos = grp_data->mem_infos;

  infos = copy_attrs(grp_data->consumers, attr->consumers, grp_data->n_consumers, infos);

  copy_attrs(grp_data->producers, attr->producers, grp_data->n_producers, infos);

  return 0;

fail_infos:
  ri_group_data_delete(grp_data);
fail_alloc:
  return -ENOMEM;
}


void ri_group_data_delete(ri_group_data_t *rsc) {
  if (rsc->mem_infos) {
    free(rsc->mem_infos);
    rsc->mem_infos = NULL;
  }

  if (rsc->consumers) {
    free(rsc->consumers);
    rsc->consumers = NULL;
  }

  if (rsc->producers) {
    free(rsc->producers);
    rsc->producers = NULL;
  }

  rsc->consumers = NULL;
  rsc->producers = NULL;
  rsc->info.data = NULL;

  rsc->n_consumers = 0;
  rsc->n_producers = 0;
  rsc->info.size = 0;
}


ri_group_attr_t ri_group_data_attr(const ri_group_data_t *rsc)
{
  return (ri_group_attr_t){
    .consumers = rsc->consumers,
    .producers = rsc->producers,
    .info = rsc->info,
  };
}