#pragma once

#include "rtipc/rtipc.h"

void ri_group_delete(ri_group_t* vec);

ri_info_t ri_group_get_info(const ri_group_t* vec);

void ri_group_free_info(ri_group_t* vec);
