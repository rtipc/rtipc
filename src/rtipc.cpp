#include "rtipc/rtipc.hpp"

#include <rtipc/rtipc.h>

namespace rtipc {

void consumer_release(::ri_consumer *consumer) {
  if (consumer)
    ::ri_consumer_release(consumer);
}

void producer_release(::ri_producer *producer) {
  if (producer)
    ::ri_producer_release(producer);
}

void group_delete(::ri_group *group) {
  if (group)
    ::ri_group_delete(group);
}

static constexpr ::ri_info_t to_c_info(const Info &info) {
  return ::ri_info_t{.size = info.size(), .data = info.data()};
}

static constexpr ::ri_channel_attr_t
to_c_channel_attr(const ChannelAttr &attr) {
  return ::ri_channel_attr_t{.msg_size = attr.message_size,
                             .add_msgs = attr.additional_messages,
                             .eventfd = attr.fd,
                             .info = to_c_info(attr.info)};
}

static const ::ri_channel_attr_t zero_attr{};

const void *ConsumerBase::current_message_ptr() const noexcept {
  return ::ri_consumer_msg(consumer_.get());
}

std::expected<PopResult, QueueError> ConsumerBase::pop() noexcept {
  ::ri_pop_result_t result = ri_consumer_pop(consumer_.get());

  switch (result) {
  case ::RI_POP_RESULT_NO_MSG:
    return PopResult::no_message;
  case ::RI_POP_RESULT_NO_UPDATE:
    return PopResult::no_update;
  case ::RI_POP_RESULT_SUCCESS:
    return PopResult::success;
  case ::RI_POP_RESULT_DISCARDED:
    return PopResult::messages_discarded;
  case ::RI_POP_RESULT_ERROR:
    break;
  }

  return std::unexpected(QueueError::invalid_index);
}

std::expected<unsigned, QueueError>
ConsumerBase::count_messages() const noexcept {
  int cnt = ::ri_consumer_count_msgs(consumer_.get());
  if (cnt < 0)
    return std::unexpected(QueueError::invalid_index);

  return cnt;
}

void *ProducerBase::current_message_ptr() const noexcept {
  return ::ri_producer_msg(producer_.get());
}

std::expected<ForcePushResult, QueueError> ProducerBase::force_push() noexcept {
  ::ri_force_push_result_t result = ::ri_producer_force_push(producer_.get());
  switch (result) {
  case ::RI_FORCE_PUSH_RESULT_SUCCESS:
    return ForcePushResult::success;
  case ::RI_FORCE_PUSH_RESULT_DISCARDED:
    return ForcePushResult::message_discarded;
  case ::RI_FORCE_PUSH_RESULT_ERROR:
    break;
  }
  return std::unexpected(QueueError::invalid_index);
}

std::expected<TryPushResult, QueueError> ProducerBase::try_push() noexcept {
  ::ri_try_push_result_t result = ::ri_producer_try_push(producer_.get());
  switch (result) {
  case ::RI_TRY_PUSH_RESULT_SUCCESS:
    return TryPushResult::success;
  case ::RI_TRY_PUSH_RESULT_FAIL:
    return TryPushResult::fail;
  case ::RI_TRY_PUSH_RESULT_ERROR:
    break;
  }
  return std::unexpected(QueueError::invalid_index);
}

std::expected<unsigned, QueueError>
ProducerBase::count_messages() const noexcept {
  int cnt = ::ri_producer_count_msgs(producer_.get());
  if (cnt < 0) {
    return std::unexpected(QueueError::invalid_index);
  }
  return cnt;
}

ChannelGroup::ChannelGroup(const GroupAttr &group_attr) {
  std::vector<ri_channel_attr_t> c_consumers;
  std::vector<ri_channel_attr_t> c_producers;

  c_consumers.reserve(group_attr.consumers.size() + 1);
  c_producers.reserve(group_attr.producers.size() + 1);

  for (const auto &attr : group_attr.consumers) {
    c_consumers.emplace_back(to_c_channel_attr(attr));
  }

  c_consumers.emplace_back(zero_attr);

  for (const auto &attr : group_attr.producers) {
    c_producers.emplace_back(to_c_channel_attr(attr));
  }

  c_producers.emplace_back(zero_attr);

  ::ri_group_attr_t c_group_attr = {.consumers = c_consumers.data(),
                                    .producers = c_producers.data(),
                                    .info = to_c_info(group_attr.info)};

  ::ri_group_t *group = ::ri_group_from_attr(&c_group_attr);

  if (group == nullptr) {
    throw std::runtime_error("ri_group_from_attr returned NULL");
  }

  group_ = GroupPtr(group);
}

ConsumerPtr ChannelGroup::acquire_consumer_impl(unsigned index) {
  ri_consumer *consumer = ::ri_group_acquire_consumer(group_.get(), index);

  if (consumer == nullptr)
    throw std::runtime_error("ri_group_acquire_consumer returned NULL");

  return ConsumerPtr(consumer);
}

ProducerPtr ChannelGroup::acquire_producer_impl(unsigned index) {
  ri_producer *producer = ::ri_group_acquire_producer(group_.get(), index);

  if (producer == nullptr)
    throw std::runtime_error("ri_group_acquire_producer returned NULL");

  return ProducerPtr(producer);
}

size_t ChannelGroup::consumer_message_size(unsigned index) const {
  const ri_channel_attr_t *attr =
      ::ri_group_get_consumer_attr(group_.get(), index);
  if (attr == nullptr)
    throw std::runtime_error("ri_group_get_consumer_attr returned NULL");

  return attr->msg_size;
}

size_t ChannelGroup::producer_message_size(unsigned index) const {
  const ri_channel_attr_t *attr =
      ::ri_group_get_producer_attr(group_.get(), index);
  if (attr == nullptr)
    throw std::runtime_error("ri_group_get_producer_attr returned NULL");

  return attr->msg_size;
}

} // namespace rtipc
