#include "mem_utils.h"

#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>

#include "rtipc/rtipc.h"
#include "rtipc/log.h"

#ifdef RI_CACHELINE_SIZE

size_t cacheline_size(void)
{
  return RI_CACHELINE_SIZE;
}

#else

#define MAX_SANE_CACHE_LINE_SIZE 0x1000

#define FMT_SYS_CPU_CACHE_ATTR  "/sys/devices/system/cpu/cpu%d/cache/index%d/%s"


typedef enum {
  CACHE_TYPE_UNKNOWN = -1,
  CACHE_TYPE_DATA = 0,
  CACHE_TYPE_INSTRUCTION,
  CACHE_TYPE_UNIFIED,
} cache_type_t;

typedef struct {
  size_t size;
  size_t cls;
  unsigned level;
  cache_type_t type;
} cache_t;

static cache_type_t cache_read_type(int cpu, int index)
{
  char path[64];

  int r = snprintf(path, sizeof(path), FMT_SYS_CPU_CACHE_ATTR, cpu, index, "type");
  if (r < 0) {
    LOG_ERR("snprintf failed");
    return CACHE_TYPE_UNKNOWN;
  }

  FILE* fp = fopen(path, "re");
  if (!fp) {
    LOG_ERR("failed to open file %s", path);
    return CACHE_TYPE_UNKNOWN;
  }

  int c = fgetc(fp);
  fclose(fp);

  switch (c) {
    case 'D':
      return CACHE_TYPE_DATA;
    case 'I':
      return CACHE_TYPE_INSTRUCTION;
    case 'U':
      return CACHE_TYPE_UNIFIED;
    default:
      LOG_ERR("unknown cache type %d", c);
      return CACHE_TYPE_UNKNOWN;
  }
}


static long cache_read_long(int cpu, int index, const char* attr)
{
  char path[64];

  int r = snprintf(path, sizeof(path), FMT_SYS_CPU_CACHE_ATTR, cpu, index, attr);
  if (r < 0) {
    LOG_ERR("snprintf failed");
    return -1;
  }

  FILE* fp = fopen(path, "re");
  if (!fp) {
    LOG_ERR("failed to open file %s", path);
    return -1;
  }

  long result;
  r = fscanf(fp, "%ld", &result);
  fclose(fp);

  if (r < 0) {
    LOG_ERR("fscanf failed errno=%d", errno);
    return -1;
  }
  return result;
}


static int read_cache(int cpu, int index, cache_t *cache)
{
  long size = cache_read_long(cpu, index, "size");
  if (size <= 0)
    return -1;

  long level = cache_read_long(cpu, index, "level");
  if (level <= 0)
    return -1;

  long cls = cache_read_long(cpu, index, "coherency_line_size");
  if ((cls <= 0) || (cls > MAX_SANE_CACHE_LINE_SIZE))
    return -1;

  cache_type_t type = cache_read_type(cpu, index);
  if (type == CACHE_TYPE_UNKNOWN)
    return -1;

  *cache = (cache_t) {
    .size = size,
    .cls = cls,
    .level = level,
    .type = type,
  };

  return 0;
}

static size_t get_cls(size_t min)
{
  size_t cls = min;
  for (int i = 0; i < 4; i++) {
    cache_t cache;
    int r = read_cache(0, i, &cache);
    if (r < 0)
      continue;
    if (cache.type != CACHE_TYPE_DATA)
      continue;

    if ((cache.level >= 1) && (cache.level <= 2)) {
      if (cache.cls > cls)
        cls = cache.cls;
    }
  }
  return cls;
}


size_t cacheline_size(void)
{
  static atomic_ulong s_cls = 0;
  size_t cls = atomic_load_explicit(&s_cls, memory_order_relaxed);

  if (cls != 0)
    return cls;

  cls = get_cls(alignof(max_align_t));
  /* LEVEL 3 Cache is usually shared */

  atomic_store_explicit(&s_cls, cls, memory_order_relaxed);

  LOG_INF("cache_line_size=%zu", cls);

  return cls;
}

#endif /* RI_CACHELINE_SIZE */
