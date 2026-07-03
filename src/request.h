#pragma once

#include "rtipc/rtipc.h"

size_t ri_request_calc_size(const ri_vector_attr_t *vattr);

ri_vector_attr_t ri_request_parse(const void *req, size_t size, ri_channel_attr_t **attrs);

int ri_request_write(const ri_vector_attr_t* vattr, void *req, size_t size);
