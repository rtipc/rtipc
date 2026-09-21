#include "rtipc/rtipc.hpp"

#include <unistd.h>

#include <rtipc/rtipc.h>
#include <rtipc/connect.h>

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

void server_delete(::ri_server *server) {
  if (server)
    ::ri_server_delete(server);
}

static constexpr ::ri_info_t to_c_info(const Info &info) {
  return ::ri_info_t{.size = info.size(), .data = info.data()};
}

static constexpr ::ri_channel_attr_t
to_c_channel_attr(const ChannelAttr &attr) {
  return ::ri_channel_attr_t{.msg_size = attr.message_size,
                             .add_msgs = attr.additional_messages,
                             .eventfd = attr.eventfd,
                             .info = to_c_info(attr.info)};
}

ChannelAttr ChannelAttr::from_c_attr(const ::ri_channel_attr *c_attr)
{
  return ChannelAttr{
      c_attr->msg_size,
      c_attr->add_msgs,
      c_attr->eventfd,
      Info((char*)c_attr->info.data, (char*)c_attr->info.data + c_attr->info.size)};
}


GroupAttr GroupAttr::from_c_attr(const ::ri_group_attr *group_attr)
{
  auto consumers = std::vector<ChannelAttr>();

  for (const ::ri_channel_attr_t* a = group_attr->consumers; (a != nullptr) && (a->msg_size > 0); a++)
    consumers.emplace_back(ChannelAttr::from_c_attr(a));

  auto producers = std::vector<ChannelAttr>();

  for (const ::ri_channel_attr_t* a = group_attr->producers; (a != nullptr) && (a->msg_size > 0); a++)
    producers.emplace_back(ChannelAttr::from_c_attr(a));

  auto info = Info((char*)group_attr->info.data, (char*)group_attr->info.data + group_attr->info.size);

  return GroupAttr{consumers, producers, info};
}

static const ::ri_channel_attr_t zero_attr{};

const void *ConsumerBase::current_message_ptr() const noexcept {
  return ::ri_consumer_msg(consumer_.get());
}

std::expected<PopResult, QueueError> ConsumerBase::pop() noexcept {
  ::ri_pop_result_t result = ::ri_consumer_pop(consumer_.get());

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


int ConsumerBase::get_eventfd() const noexcept
{
  return ::ri_consumer_eventfd(consumer_.get());
}


int ConsumerBase::take_eventfd() noexcept
{
  return ::ri_consumer_take_eventfd(consumer_.get());
}


void *ProducerBase::current_message_ptr() const noexcept {
  return ::ri_producer_msg(producer_.get());
}


int ProducerBase::get_eventfd() const noexcept
{
  return ::ri_producer_eventfd(producer_.get());
}

int ProducerBase::take_eventfd() noexcept
{
  return ::ri_producer_take_eventfd(producer_.get());
}

void ProducerBase::cache_enable()
{
  int r = ::ri_producer_cache_enable(producer_.get());
  if (r < 0)
    throw std::runtime_error("ri_producer_cache_enable failed");
}


void ProducerBase::cache_disable() noexcept
{
  ::ri_producer_cache_disable(producer_.get());
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

ChannelGroup::ChannelGroup(const std::span<std::byte> req, std::span<int> fds) {
  const void *c_req = req.data();
  size_t req_size = req.size();
  int *c_fds = fds.data();
  unsigned n_fds = fds.size();

  ::ri_group_t *group = ::ri_group_deserialize(c_req, req_size, c_fds, &n_fds);
  if (group == nullptr) {
    throw std::runtime_error("ri_group_deserialize returned NULL");
  }

  // close remaining fds
  for (int &fd : fds.subspan(n_fds)) {
    if (fd >= 0) {
      ::close(fd);
      fd = -1;
    }
  }

  group_ = GroupPtr(group);
}

std::tuple<std::vector<std::byte>, std::vector<int>>
ChannelGroup::serialize() const {
  unsigned n_fds = 253; // maximum file descriptors that can be sent over a unix
                        // domain socket (kernel/include/net/scm.h)
  size_t req_size = ::ri_group_serialize_size(group_.get());
  auto req = std::vector<std::byte>(req_size);
  auto fds = std::vector<int>(n_fds);
  int r = ::ri_group_serialize(group_.get(), req.data(), req.size(), fds.data(),
                               &n_fds);
  if (r < 0)
    throw std::runtime_error("ri_group_serialize returned error");

  // delete unused fds
  fds.erase(fds.begin() + n_fds, fds.end());

  return {req, fds};
}

ConsumerPtr ChannelGroup::acquire_consumer_impl(unsigned index) {
  ::ri_consumer *consumer = ::ri_group_acquire_consumer(group_.get(), index);

  if (consumer == nullptr)
    throw std::runtime_error("ri_group_acquire_consumer returned NULL");

  return ConsumerPtr(consumer);
}

ProducerPtr ChannelGroup::acquire_producer_impl(unsigned index) {
  ::ri_producer *producer = ::ri_group_acquire_producer(group_.get(), index);

  if (producer == nullptr)
    throw std::runtime_error("ri_group_acquire_producer returned NULL");

  return ProducerPtr(producer);
}

size_t ChannelGroup::consumer_message_size(unsigned index) const {
  const ::ri_channel_attr_t *attr =
      ::ri_group_get_consumer_attr(group_.get(), index);
  if (attr == nullptr)
    throw std::runtime_error("ri_group_get_consumer_attr returned NULL");

  return attr->msg_size;
}

size_t ChannelGroup::producer_message_size(unsigned index) const {
  const ::ri_channel_attr_t *attr =
      ::ri_group_get_producer_attr(group_.get(), index);
  if (attr == nullptr)
    throw std::runtime_error("ri_group_get_producer_attr returned NULL");

  return attr->msg_size;
}


bool filter_callback(const ::ri_group_attr_t* c_attr, unsigned, unsigned, void* user_data)
{
  GroupAttr attr = GroupAttr::from_c_attr(c_attr);
  auto& filter = *static_cast<Server::Filter*>(user_data);
  return filter(attr);
}

ChannelGroup Server::accept(Filter filter)
{
  ::ri_group_t *group = ::ri_server_accept(
      server_.get(),
      filter_callback,
      &filter);

  if (group == nullptr) {
    throw std::runtime_error("ri_server_accept returned NULL");
  }

  return ChannelGroup(GroupPtr(group));
  }

} // namespace rtipc
