#include "rtipc/rtipc.hpp"

#include <rtipc/rtipc.h>

using namespace rtipc;

void consumer_release(ri_consumer* consumer)
{
  if (consumer)
    ri_consumer_release(consumer);
}


void producer_release(ri_producer* producer)
{
  if (producer)
    ri_producer_release(producer);
}


void group_delete(ri_group* group)
{
  if (group)
    ri_group_delete(group);
}


ConsumerPtr ChannelGroup::acquire_consumer_impl(unsigned index)
{
  ri_consumer *consumer = ::ri_group_acquire_consumer(group_.get(), index);

  if (consumer == nullptr)
    throw std::runtime_error("ri_group_acquire_consumer returned NULL");

  return ConsumerPtr(consumer);
}


ProducerPtr ChannelGroup::acquire_producer_impl(unsigned index)
{
  ri_producer *producer = ::ri_group_acquire_producer(group_.get(), index);

  if (producer == nullptr)
    throw std::runtime_error("ri_group_acquire_producer returned NULL");

  return ProducerPtr(producer);
}


size_t ChannelGroup::consumer_message_size(unsigned index) const
{
  const ri_channel_attr_t *attr = ::ri_group_get_consumer_attr(group_.get(), index);
  if (attr == nullptr)
    throw std::runtime_error("ri_group_get_consumer_attr returned NULL");

  return attr->msg_size;
}


size_t ChannelGroup::producer_message_size(unsigned index) const
{
  const ri_channel_attr_t *attr = ::ri_group_get_producer_attr(group_.get(), index);
  if (attr == nullptr)
    throw std::runtime_error("ri_group_get_producer_attr returned NULL");

  return attr->msg_size;
}


