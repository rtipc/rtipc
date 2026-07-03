#define _GNU_SOURCE

#include "shm.h"

#include <errno.h>
#include <stdatomic.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>


#include <sys/stat.h> // fstat
#include <sys/mman.h>

#include "rtipc/rtipc.h"
#include "rtipc/log.h"
#include "mem_utils.h"

struct ri_shm
{
  atomic_int ref_cnt;
  void *mem;
  size_t size;
  int fd;
  bool owner;
};


static void shm_delete(ri_shm_t *shm)
{
  LOG_INF("delete shm 0x%p", shm->mem);
  munmap(shm->mem, shm->size);
  close(shm->fd);
  free(shm);
}


ri_shm_t* ri_shm_map(int fd)
{
  struct stat stat;

  ri_shm_t *shm = malloc(sizeof(ri_shm_t));
  if (!shm)
    return NULL;

  *shm = (ri_shm_t) {
    .ref_cnt = 1,
    .fd = fd,
  };

  int r = fstat(shm->fd, &stat);
  if (r < 0) {
    LOG_ERR("fstat failed: %s", strerror(errno));
    goto fail_stat;
  }

  shm->size = stat.st_size;

  shm->mem = mmap(NULL, shm->size, PROT_READ | PROT_WRITE, MAP_SHARED, shm->fd, 0);
  if (shm->mem == MAP_FAILED) {
    LOG_ERR("mmap with size=%zu failed: %s", shm->size, strerror(errno));
    goto fail_map;
  }

  r = mlock(shm->mem, shm->size);
  if (r < 0) {
    LOG_ERR("mlock failed: %s", strerror(errno));
    goto fail_map;
  }

  LOG_INF("mapped shared memory size=%zu, on %p", shm->size, shm->mem);

  return shm;

fail_map:
fail_stat:
  free(shm);
  return NULL;
}


void ri_shm_ref(ri_shm_t *shm)
{
  atomic_fetch_add(&shm->ref_cnt, 1);
}


void ri_shm_unref(ri_shm_t *shm)
{
  if (atomic_fetch_sub(&shm->ref_cnt, 1) == 1) {
    shm_delete(shm);
  }
}


void *ri_shm_ptr(const ri_shm_t *shm, size_t offset)
{
  if (offset >= shm->size)
    return NULL;

  return mem_offset(shm->mem, offset);
}


size_t ri_shm_size(const ri_shm_t *shm)
{
  return shm->size;
}


int ri_shm_get_fd(const ri_shm_t *shm)
{
  return shm->fd;
}
