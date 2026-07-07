#pragma once

#include "rtipc/rtipc.h"

struct ri_group_data {
    ri_info_t info;

    unsigned n_consumers;

    unsigned n_producers;

    ri_channel_attr_t *consumers;

    ri_channel_attr_t *producers;

    void *mem_infos;
};

size_t ri_info_align(size_t size);

size_t ri_attr_calc_info_size(const ri_group_attr_t *attr);

ri_group_attr_t ri_group_data_attr(const ri_group_data_t *data);