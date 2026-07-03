#include "request.h"

#include <string.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "rtipc/rtipc.h"
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
  size_t offset_info;
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


static const void* request_get_info(request_reader_t *reader, size_t size)
{
  if (reader->offset_info + size > reader->size)
    return NULL;

  const void* data = cmem_offset(reader->data, reader->offset_info);

  reader->offset_info += size;

  return data;
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

  ri_info_t info = { .size = entry.info_size };

  if (info.size > 0) {
    info.data = request_get_info(reader, info.size);

    if (!info.data)
      return -1;
  }

  *attr = (ri_channel_attr_t) {
      .add_msgs = entry.add_msgs,
      .msg_size = entry.msg_size,
      .info = info,
      .eventfd = entry.eventfd,
  };

  return r;
}


size_t ri_request_calc_size(const ri_vector_attr_t *config)
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


ri_vector_attr_t ri_request_parse(const void *req, size_t size, ri_channel_attr_t **attrs)
{
  if (!attrs) {
    goto fail_args;
  }

  request_reader_t reader = {
    .data = req,
    .size = size,
  };

  ri_request_header_t header;

  int r = request_read(&reader, &header, sizeof(header));

  if (r < 0) {
    LOG_ERR("request too small (%zu) for header", size);
    goto fail_parse;
  }

  r = ri_request_header_validate(&header);

  if (r < 0) {
    LOG_ERR("ri_request_header_validate failed");
    goto fail_parse;
  }


  uint32_t vec_info_size;
  r = request_read(&reader, &vec_info_size, sizeof(vec_info_size));

  if (r < 0) {
    LOG_ERR("request too small (%zu) for vec_info_size", size);
    goto fail_parse;
  }


  uint32_t n_consumers;
  r = request_read(&reader, &n_consumers, sizeof(n_consumers));

  if (r < 0) {
    LOG_ERR("request too small (%zu) for num_consumers", size);
    goto fail_parse;
  }

  uint32_t n_producers;
  r = request_read(&reader, &n_producers, sizeof(n_producers));

  if (r < 0) {
    LOG_ERR("request too small (%zu) for num_producers", size);
    goto fail_parse;
  }

  reader.offset_info = reader.offset + (n_producers + n_consumers) * sizeof(entry_t);

  ri_info_t vec_info = {
    .size = vec_info_size,
  };

  if (vec_info_size > 0) {
      vec_info.data = request_get_info(&reader, vec_info_size);

    if (!vec_info.data) {
      LOG_ERR("messsage too small (%zu) for vector info", size);
      goto fail_parse;
    }
  }

  ri_channel_attr_t *channels = calloc(n_consumers + n_producers + 2, sizeof(ri_channel_attr_t));
  if (!channels) {
    goto fail_alloc;
  }

  ri_channel_attr_t *consumers = &channels[0];
  ri_channel_attr_t *producers = &channels[n_consumers + 1];

  for (unsigned i = 0; i < n_consumers; i++) {
    r = request_read_channel(&reader, &consumers[i]);
    if (r < 0)
      goto fail_channel;
  }

  for (unsigned i = 0; i < n_producers; i++) {
    r = request_read_channel(&reader, &producers[i]);
    if (r < 0)
      goto fail_channel;
  }

  *attrs = channels;

  return (ri_vector_attr_t) {
         .consumers = consumers,
         .producers = producers,
         .info = vec_info,
         };

fail_channel:
  free(channels);
fail_parse:
fail_alloc:
fail_args:
  return (ri_vector_attr_t) {.consumers = NULL, .producers = NULL};
}


int ri_request_write(const ri_vector_attr_t* vattr, void *req, size_t size)
{
  if (!size)
    goto fail;

  uint32_t n_producers = ri_count_channels(vattr->producers);
  uint32_t n_consumers = ri_count_channels(vattr->consumers);

  request_writer_t writer = {
    .size = size,
    .data = req,
  };

  ri_request_header_t header = ri_request_header_init();

  int r = request_write(&writer, &header, sizeof(header));

  if (r < 0)
    goto fail;

  uint32_t vec_info = vattr->info.size;

  r = request_write(&writer, &vec_info, sizeof(vec_info));

  if (r < 0)
    goto fail;

  r = request_write(&writer, &n_producers, sizeof(n_producers));

  if (r < 0)
    goto fail;

  request_write(&writer, &n_consumers, sizeof(n_consumers));

  if (r < 0)
    goto fail;

  writer.offset_info = writer.offset + (n_producers + n_consumers) * sizeof(entry_t);

  r = request_write_info(&writer, &vattr->info);

  if (r < 0)
    goto fail;

  for (unsigned i = 0 ; i < n_producers; i++) {
    const ri_channel_attr_t *attr = &vattr->producers[i];
    r = request_write_channel(&writer, attr);

    if (r < 0)
      goto fail;
  }

  for (unsigned i = 0 ; i < n_consumers; i++) {
    const ri_channel_attr_t *attr = &vattr->consumers[i];
    r = request_write_channel(&writer, attr);

    if (r < 0)
      goto fail;
  }

  return 0;

fail:
  return -1;
}


