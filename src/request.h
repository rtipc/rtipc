#pragma once

#include "rtipc/rtipc.h"

#include "attr.h"

size_t ri_request_calc_size(const ri_group_attr_t *vattr);

int ri_request_parse(ri_group_data_t *attr, const void *req_data, size_t size);

int ri_request_write(const ri_group_attr_t* grp_attr, void *req_data, size_t size);
