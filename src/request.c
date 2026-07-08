#include "request.h"

#include <string.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>


#include "attr.h"
#include "rtipc/log.h"
#include "channel.h"
#include "header.h"
#include "mem_utils.h"

typedef struct entry {
  uint32_t add_msgs;
  uint32_t msg_size;
  int32_t eventfd;
  uint32_t info_size;
} entry_t;


typedef struct request_reader {
  const void *data;
  size_t size;
  size_t offset;
} request_reader_t;


typedef struct request_writer {
  void *data;
  size_t size;
  size_t offset;
  size_t offset_info;
} request_writer_t;


static int request_write(request_writer_t *writer, const void *src, size_t size)
{
  if (writer->offset + size > writer->size)
    return -1;

  void *ptr = mem_offset(writer->data, writer->offset);

  memcpy(ptr, src, size);

  writer->offset += size;

  return 0;
}


static int request_write_info(request_writer_t *writer, const ri_info_t* info)
{
  if (info->size == 0 || !info->data)
    return 0;

  if (writer->offset_info + info->size > writer->size)
    return -1;

  void *ptr = mem_offset(writer->data, writer->offset_info);

  memcpy(ptr, info->data, info->size);

  writer->offset_info += info->size;

  return 0;
}


static int request_read(request_reader_t *reader, void *dst, size_t size)
{
  if (reader->offset + size > reader->size)
    return -1;

  const void *ptr = cmem_offset(reader->data, reader->offset);

  memcpy(dst, ptr, size);

  reader->offset += size;

  return 0;
}



static int request_write_channel(request_writer_t *writer, const ri_channel_attr_t *attr)
{
  entry_t entry = {
      .add_msgs = attr->add_msgs,
      .msg_size = attr->msg_size,
      .info_size = attr->info.size,
      .eventfd = attr->eventfd,
  };

  int r = request_write(writer, &entry, sizeof(entry));

  if (r < 0)
    return r;

  if (attr->info.data) {
    r = request_write_info(writer, &attr->info);

    if (r < 0)
      return r;
  }

  return r;
}


static int request_read_channel(request_reader_t *reader, ri_channel_attr_t *attr)
{
  entry_t entry;
  int r = request_read(reader, &entry, sizeof(entry));

  if (r < 0)
    return -r;

  *attr = (ri_channel_attr_t) {
      .add_msgs = entry.add_msgs,
      .msg_size = entry.msg_size,
      .info.size = entry.info_size,
      .eventfd = entry.eventfd,
  };

  return r;
}


size_t ri_request_calc_size(const ri_group_attr_t *config)
{
  unsigned n_consumers = ri_count_channels(config->consumers);
  unsigned n_producers = ri_count_channels(config->producers);

  const ri_channel_attr_t *consumers = config->consumers;
  const ri_channel_attr_t *producers = config->producers;

  size_t size = sizeof(ri_request_header_t);

  /* vector info size + 2 * number of channels */
  size += 3 * sizeof(uint32_t);

  /* channel table */
  size += (n_consumers + n_producers) * sizeof(entry_t);

  /* vector info */
  size += config->info.size;

  /* channel info */
  for (unsigned i = 0; i < n_consumers; i++)
    size +=  consumers[i].info.size;

  for (unsigned i = 0; i < n_producers; i++)
    size += producers[i].info.size;

  return size;
}


static void* read_info(request_reader_t *reader, void *mem, size_t size)
{
  int r = request_read(reader, mem, size);
  if (r < 0)
    return NULL;

  size = ri_info_align(size);
  return mem_offset(mem, size);
}


static int read_infos(request_reader_t *reader, ri_group_data_t *rsc)
{
  void *mem = rsc->mem_infos;

  if (rsc->info.size > 0) {
    rsc->info.data = mem;
    mem = read_info(reader, mem, rsc->info.size);
    if (!mem)
      return -1;
  }

  for (unsigned i = 0; i < rsc->n_consumers; i++) {
    ri_channel_attr_t *attr = &rsc->consumers[i];
    if (attr->info.size > 0) {
      attr->info.data = mem;
      mem = read_info(reader, mem, attr->info.size);
      if (!mem)
        return -1;
    }
  }

  for (unsigned i = 0; i < rsc->n_producers; i++) {
    ri_channel_attr_t *attr = &rsc->producers[i];
    if (attr->info.size > 0) {
      attr->info.data = mem;
      mem = read_info(reader, mem, attr->info.size);
      if (!mem)
        return -1;
    }
  }
  return 0;
}


int ri_request_parse(ri_group_data_t *grp_data, const void *req, size_t size)
{
  int r = -ENOMEM;
  *grp_data = (ri_group_data_t) { 0 };

  request_reader_t reader = {
    .data = req,
    .size = size,
  };

  ri_request_header_t header;

  r = request_read(&reader, &header, sizeof(header));

  if (r < 0) {
    LOG_ERR("request too small (%zu) for header", size);
    goto fail_header;
  }

  r = ri_request_header_validate(&header);

  if (r < 0) {
    LOG_ERR("ri_request_header_validate failed");
    goto fail_header;
  }

  uint32_t group_info_size;
  r = request_read(&reader, &group_info_size, sizeof(group_info_size));

  if (r < 0) {
    LOG_ERR("request too small (%zu) for vec_info_size", size);
    goto fail_header;
  }


  uint32_t n_consumers;
  r = request_read(&reader, &n_consumers, sizeof(n_consumers));
  if (r < 0) {
    LOG_ERR("request too small (%zu) for num_consumers", size);
    goto fail_header;
  }

  uint32_t n_producers;
  r = request_read(&reader, &n_producers, sizeof(n_producers));
  if (r < 0) {
    LOG_ERR("request too small (%zu) for num_producers", size);
    goto fail_header;
  }

  r = ri_group_data_new(grp_data, n_consumers, n_producers);
  if (r) {
      goto fail_header;
  }

  grp_data->info.size = group_info_size;

  for (unsigned i = 0; i < n_consumers; i++) {
    r = request_read_channel(&reader, &grp_data->consumers[i]);
    if (r < 0)
      goto fail_channels;
  }

  for (unsigned i = 0; i < n_producers; i++) {
    r = request_read_channel(&reader, &grp_data->producers[i]);
    if (r < 0)
      goto fail_channels;
  }

  ri_group_attr_t attr = ri_group_data_attr(grp_data);

  size_t info_size = ri_attr_calc_info_size(&attr);

  grp_data->mem_infos = malloc(info_size);
  if (!grp_data->mem_infos) {
      r = -ENOMEM;
      goto fail_channels;
  }

  r = read_infos(&reader, grp_data);
  if (r < 0)
    goto fail_channels;

  return 0;

fail_channels:
    ri_group_data_delete(grp_data);
fail_header:
  return r;
}


int ri_request_write(const ri_group_attr_t* grp_attr, void *req, size_t size)
{
  if (!size)
    goto fail;

  uint32_t n_producers = ri_count_channels(grp_attr->producers);
  uint32_t n_consumers = ri_count_channels(grp_attr->consumers);

  request_writer_t writer = {
    .size = size,
    .data = req,
  };

  ri_request_header_t header = ri_request_header_init();

  int r = request_write(&writer, &header, sizeof(header));

  if (r < 0)
    goto fail;

  uint32_t grp_info_size = grp_attr->info.size;

  r = request_write(&writer, &grp_info_size, sizeof(grp_info_size));

  if (r < 0)
    goto fail;

  r = request_write(&writer, &n_producers, sizeof(n_producers));

  if (r < 0)
    goto fail;

  request_write(&writer, &n_consumers, sizeof(n_consumers));

  if (r < 0)
    goto fail;

  writer.offset_info = writer.offset + (n_producers + n_consumers) * sizeof(entry_t);

  r = request_write_info(&writer, &grp_attr->info);

  if (r < 0)
    goto fail;

  for (unsigned i = 0 ; i < n_producers; i++) {
    const ri_channel_attr_t *attr = &grp_attr->producers[i];
    r = request_write_channel(&writer, attr);

    if (r < 0)
      goto fail;
  }

  for (unsigned i = 0 ; i < n_consumers; i++) {
    const ri_channel_attr_t *attr = &grp_attr->consumers[i];
    r = request_write_channel(&writer, attr);

    if (r < 0)
      goto fail;
  }

  return 0;

fail:
  return -1;
}


