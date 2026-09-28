#include "rtipc/rtipc.hpp"

#include <unistd.h>

#include <rtipc/connect.h>
#include <rtipc/rtipc.h>

namespace rtipc {

static const ::ri_channel_attr_t zero_attr{};

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
to_c_channel_attributes(const ChannelAttributes &attr) {
  return ::ri_channel_attr_t{.msg_size = attr.message_size,
                             .add_msgs = attr.additional_messages,
                             .eventfd = attr.eventfd,
                             .info = to_c_info(attr.info)};
}

ChannelAttributes ChannelAttributes::from_c_attributes(const ::ri_channel_attr *c_attr) {
  return ChannelAttributes{c_attr->msg_size, c_attr->add_msgs, c_attr->eventfd,
                     Info((char *)c_attr->info.data,
                          (char *)c_attr->info.data + c_attr->info.size)};
}

GroupAttributes GroupAttributes::from_c_attributes(const ::ri_group_attr *group_attr) {
  auto consumers = std::vector<ChannelAttributes>();

  for (const auto *a = group_attr->consumers;
       (a != nullptr) && (a->msg_size > 0); a++)
    consumers.emplace_back(ChannelAttributes::from_c_attributes(a));

  auto producers = std::vector<ChannelAttributes>();

  for (const auto *a = group_attr->producers;
       (a != nullptr) && (a->msg_size > 0); a++)
    producers.emplace_back(ChannelAttributes::from_c_attributes(a));

  auto info = Info((char *)group_attr->info.data,
                   (char *)group_attr->info.data + group_attr->info.size);

  return GroupAttributes{consumers, producers, info};
}

class CGroupAttributes final {
public:
  explicit CGroupAttributes(const GroupAttributes &group_attr) {
    consumers_.reserve(group_attr.consumers.size() + 1);
    producers_.reserve(group_attr.producers.size() + 1);

    for (const auto &attr : group_attr.consumers) {
      consumers_.emplace_back(to_c_channel_attributes(attr));
    }

    consumers_.emplace_back(zero_attr);

    for (const auto &attr : group_attr.producers) {
      producers_.emplace_back(to_c_channel_attributes(attr));
    }

    producers_.emplace_back(zero_attr);

    info_ = group_attr.info;

    c_attr_ = ::ri_group_attr_t{.consumers = consumers_.data(),
                                .producers = producers_.data(),
                                .info = to_c_info(info_)};
  }

  const ::ri_group_attr_t *get() const { return &c_attr_; }

private:
  std::vector<ri_channel_attr_t> consumers_;
  std::vector<ri_channel_attr_t> producers_;
  Info info_;
  ::ri_group_attr_t c_attr_;
};

const void *ConsumerBase::current_message_ptr() const noexcept {
  return ::ri_consumer_msg(consumer_.get());
}

std::expected<PopResult, QueueError> ConsumerBase::pop() noexcept {
  auto result = ::ri_consumer_pop(consumer_.get());

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

int ConsumerBase::get_eventfd() const noexcept {
  return ::ri_consumer_eventfd(consumer_.get());
}

int ConsumerBase::take_eventfd() noexcept {
  return ::ri_consumer_take_eventfd(consumer_.get());
}

void *ProducerBase::current_message_ptr() const noexcept {
  return ::ri_producer_msg(producer_.get());
}

int ProducerBase::get_eventfd() const noexcept {
  return ::ri_producer_eventfd(producer_.get());
}

int ProducerBase::take_eventfd() noexcept {
  return ::ri_producer_take_eventfd(producer_.get());
}

void ProducerBase::cache_enable() {
  int r = ::ri_producer_cache_enable(producer_.get());
  if (r < 0)
    throw std::bad_alloc();
}

void ProducerBase::cache_disable() noexcept {
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

std::expected<ChannelGroup, Error> ChannelGroup::from_attributes(const GroupAttributes &group_attr) noexcept
{
  std::vector<ri_channel_attr_t> c_consumers;
  std::vector<ri_channel_attr_t> c_producers;

  c_consumers.reserve(group_attr.consumers.size() + 1);
  c_producers.reserve(group_attr.producers.size() + 1);

  for (const auto &attr : group_attr.consumers) {
    c_consumers.emplace_back(to_c_channel_attributes(attr));
  }

  c_consumers.emplace_back(zero_attr);

  for (const auto &attr : group_attr.producers) {
    c_producers.emplace_back(to_c_channel_attributes(attr));
  }

  c_producers.emplace_back(zero_attr);

  ::ri_group_attr_t c_group_attr = {.consumers = c_consumers.data(),
                                    .producers = c_producers.data(),
                                    .info = to_c_info(group_attr.info)};

  ::ri_group_t *group = ::ri_group_from_attr(&c_group_attr);

  if (!group) {
    return std::unexpected(Error::null_pointer);
  }

  return ChannelGroup(GroupPtr(group));
}

std::expected<ChannelGroup, Error> ChannelGroup::deserialize(const std::span<std::byte> req, std::span<int> fds)
{
  const void *c_req = req.data();
  size_t req_size = req.size();
  int *c_fds = fds.data();
  unsigned n_fds = fds.size();

  auto *group = ::ri_group_deserialize(c_req, req_size, c_fds, &n_fds);
  if (!group) {
    return std::unexpected(Error::null_pointer);
  }

  // close remaining fds
  for (int &fd : fds.subspan(n_fds)) {
    if (fd >= 0) {
      ::close(fd);
      fd = -1;
    }
  }

  return ChannelGroup(GroupPtr(group));
}

std::expected<std::tuple<std::vector<std::byte>, std::vector<int>>, int> ChannelGroup::serialize() const noexcept
{
  unsigned n_fds = 253; // maximum file descriptors that can be sent over a unix
                        // domain socket (kernel/include/net/scm.h)
  size_t req_size = ::ri_group_serialize_size(group_.get());
  auto req = std::vector<std::byte>(req_size);
  auto fds = std::vector<int>(n_fds);
  int r = ::ri_group_serialize(group_.get(), req.data(), req.size(), fds.data(),
                               &n_fds);
  if (r < 0)
    return std::unexpected(r);

  // delete unused fds
  fds.erase(fds.begin() + n_fds, fds.end());

  return std::tuple<std::vector<std::byte>, std::vector<int>>{req, fds};
}

std::expected<ChannelAttributes, Error>
ChannelGroup::get_consumer_attributes(unsigned index) const noexcept {
  const ::ri_channel_attr *attr =
      ::ri_group_get_consumer_attr(group_.get(), index);
  if (!attr)
    return std::unexpected(Error::index_out_of_range);

  return ChannelAttributes::from_c_attributes(attr);
}

std::expected<ChannelAttributes, Error>
ChannelGroup::get_producer_attributes(unsigned index) const noexcept {
  const ::ri_channel_attr *attr =
      ::ri_group_get_producer_attr(group_.get(), index);
  if (!attr)
    return std::unexpected(Error::index_out_of_range);

  return ChannelAttributes::from_c_attributes(attr);
}

std::expected<ConsumerPtr, Error>
ChannelGroup::acquire_consumer_impl(unsigned index) noexcept {
  auto *consumer = ::ri_group_acquire_consumer(group_.get(), index);

  if (!consumer)
    return std::unexpected(Error::index_out_of_range);

  return ConsumerPtr(consumer);
}

std::expected<ProducerPtr, Error>
ChannelGroup::acquire_producer_impl(unsigned index) noexcept {
  auto *producer = ::ri_group_acquire_producer(group_.get(), index);

  if (!producer)
    return std::unexpected(Error::index_out_of_range);

  return ProducerPtr(producer);
}

std::expected<size_t, Error>
ChannelGroup::consumer_message_size(unsigned index) const noexcept {
  const auto *attr = ::ri_group_get_consumer_attr(group_.get(), index);
  if (!attr)
    return std::unexpected(Error::index_out_of_range);

  return attr->msg_size;
}

std::expected<size_t, Error>
ChannelGroup::producer_message_size(unsigned index) const noexcept {
  const auto *attr = ::ri_group_get_producer_attr(group_.get(), index);
  if (!attr)
    return std::unexpected(Error::index_out_of_range);

  return attr->msg_size;
}

bool filter_callback(const ::ri_group_attr_t *c_attr, unsigned, unsigned,
                     void *user_data) {
  auto attr = GroupAttributes::from_c_attributes(c_attr);
  auto &filter = *static_cast<Server::Filter *>(user_data);
  return filter(attr);
}

std::expected<Server, Error> Server::listen(const std::string &path, int backlog) noexcept
{
  auto *server = ::ri_server_new(path.c_str(), backlog);
  if (!server)
    return std::unexpected(Error::null_pointer);

  return Server(ServerPtr(server));
}

std::expected<ChannelGroup, Error>
Server::accept(Filter filter) noexcept {
  auto *group = ::ri_server_accept(server_.get(), filter_callback, &filter);
  if (!group)
    return std::unexpected(Error::null_pointer);

  return ChannelGroup(GroupPtr(group));
}

int Server::get_socket() const noexcept {
  return ::ri_server_socket(server_.get());
}

std::expected<ChannelGroup, Error>
client_connect(int socket, const GroupAttributes &attr) noexcept {
  auto c_attr = CGroupAttributes(attr);
  ::ri_group_t *group = ::ri_client_socket_connect(socket, c_attr.get());
  if (!group)
    return std::unexpected(Error::null_pointer);

  return ChannelGroup(GroupPtr(group));
}

std::expected<ChannelGroup, Error>
client_connect(const std::string &path, const GroupAttributes &attr) noexcept {
  auto c_attr = CGroupAttributes(attr);
  ::ri_group_t *group = ::ri_client_connect(path.c_str(), c_attr.get());
  if (!group)
    return std::unexpected(Error::null_pointer);

  return ChannelGroup(GroupPtr(group));
}

} // namespace rtipc
