#pragma once

#include "rtipc/rtipc.h"

typedef struct ri_group_data {
  ri_info_t info;

  unsigned n_consumers;

  unsigned n_producers;

  ri_channel_attr_t *consumers;

  ri_channel_attr_t *producers;

  void *mem_infos;
} ri_group_data_t;

/**
 * @typedef ri_attr_rsc_t
 */



int ri_group_data_new(ri_group_data_t *rsc, unsigned n_consumers, unsigned n_producers);

int ri_group_data_from_attr(ri_group_data_t *grp_data, const ri_group_attr_t *attr);

void ri_group_data_delete(ri_group_data_t *grp_data);

size_t ri_info_align(size_t size);

size_t ri_attr_calc_info_size(const ri_group_attr_t *attr);

ri_group_attr_t ri_group_data_attr(const ri_group_data_t *data);