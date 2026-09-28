#pragma once

#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

extern "C" {
struct ri_consumer;
struct ri_producer;
struct ri_group;
struct ri_server;
struct ri_channel_attr;
struct ri_group_attr;
}

namespace rtipc {

template <typename T>
concept TriviallyCopyable = std::is_trivially_copyable_v<T>;

void consumer_release(::ri_consumer *consumer);
void producer_release(::ri_producer *producer);
void group_delete(::ri_group *group);
void server_delete(::ri_server *server);

struct ConsumerDeleter {
  void operator()(::ri_consumer *consumer) const noexcept {
    if (consumer)
      consumer_release(consumer);
  }
};

struct ProducerDeleter {
  void operator()(::ri_producer *producer) const noexcept {
    if (producer)
      producer_release(producer);
  }
};

struct GroupDeleter {
  void operator()(::ri_group *group) const noexcept {
    if (group)
      group_delete(group);
  }
};

struct ServerDeleter {
  void operator()(::ri_server *server) const noexcept {
    if (server)
      server_delete(server);
  }
};

using Info = std::vector<char>;
using ConsumerPtr = std::unique_ptr<ri_consumer, ConsumerDeleter>;
using ProducerPtr = std::unique_ptr<ri_producer, ProducerDeleter>;
using GroupPtr = std::unique_ptr<ri_group, GroupDeleter>;
using ServerPtr = std::unique_ptr<ri_server, ServerDeleter>;

enum class Error {
  null_pointer,
  index_out_of_range,
  attribute_mismatch,
  message_size_mismatch,
};

enum class QueueError {
  invalid_index,
};

enum class TryPushResult {
  fail,
  success,
};

enum class ForcePushResult {
  success,
  message_discarded,
};

enum class PopResult {
  no_message,
  no_update,
  success,
  messages_discarded,
};

struct ChannelAttributes {
  static ChannelAttributes from_c_attributes(const ::ri_channel_attr *c_attr);

  size_t message_size;
  unsigned additional_messages;
  bool eventfd;
  Info info;
  bool operator==(const ChannelAttributes &) const = default;
};

struct GroupAttributes {
  static GroupAttributes from_c_attributes(const ::ri_group_attr *c_attr);
  std::vector<ChannelAttributes> consumers;
  std::vector<ChannelAttributes> producers;
  Info info;
  bool operator==(const GroupAttributes &) const = default;
};

class ConsumerBase {
  friend class ChannelGroup;

protected:
  explicit ConsumerBase(ConsumerPtr consumer) noexcept
      : consumer_(std::move(consumer)) {};
  virtual ~ConsumerBase() noexcept = default;

  ConsumerBase(const ConsumerBase &) = delete;
  ConsumerBase &operator=(const ConsumerBase &) = delete;

  ConsumerBase(ConsumerBase &&other) noexcept = default;
  ConsumerBase &operator=(ConsumerBase &&other) noexcept = default;

  const void *current_message_ptr() const noexcept;

  std::expected<PopResult, QueueError> pop() noexcept;
  std::expected<unsigned, QueueError> count_messages() const noexcept;

  int get_eventfd() const noexcept;
  int take_eventfd() noexcept;

protected:
  ConsumerPtr consumer_;
};

template <TriviallyCopyable T> class Consumer final : private ConsumerBase {
  friend class ChannelGroup;

private:
  explicit Consumer(ConsumerPtr consumer) noexcept
      : ConsumerBase(std::move(consumer)) {}

public:
  ~Consumer() noexcept override = default;

  // Movable
  Consumer(Consumer &&other) noexcept = default;
  Consumer &operator=(Consumer &&other) noexcept = default;

  std::optional<std::reference_wrapper<const T>> current_message() const noexcept {
    const void *vptr = current_message_ptr();
    if (vptr == nullptr)
      return std::nullopt;

    const T *ptr = static_cast<const T *>(vptr);
    return *ptr;
  }

  using ConsumerBase::count_messages;
  using ConsumerBase::get_eventfd;
  using ConsumerBase::pop;
  using ConsumerBase::take_eventfd;
};

class ProducerBase {
  friend class ChannelGroup;

protected:
  explicit ProducerBase(ProducerPtr producer) noexcept
      : producer_(std::move(producer)) {};
  virtual ~ProducerBase() noexcept = default;

  // Non-copyable
  ProducerBase(const ProducerBase &) = delete;
  ProducerBase &operator=(const ProducerBase &) = delete;

  // Movable
  ProducerBase(ProducerBase &&other) noexcept = default;
  ProducerBase &operator=(ProducerBase &&other) noexcept = default;

  void *current_message_ptr() const noexcept;

  std::expected<TryPushResult, QueueError> try_push() noexcept;
  std::expected<ForcePushResult, QueueError> force_push() noexcept;

  std::expected<unsigned, QueueError> count_messages() const noexcept;

  int get_eventfd() const noexcept;
  int take_eventfd() noexcept;

  void cache_enable();
  void cache_disable() noexcept;

protected:
  ProducerPtr producer_;
};

template <TriviallyCopyable T> class Producer final : private ProducerBase {
  friend class ChannelGroup;

private:
  explicit Producer(ProducerPtr producer) noexcept
      : ProducerBase(std::move(producer)) {}

public:
  ~Producer() noexcept override = default;

  // Movable
  Producer(Producer &&other) noexcept = default;
  Producer &operator=(Producer &&other) noexcept = default;

  using ProducerBase::cache_disable;
  using ProducerBase::cache_enable;
  using ProducerBase::count_messages;
  using ProducerBase::force_push;
  using ProducerBase::get_eventfd;
  using ProducerBase::take_eventfd;
  using ProducerBase::try_push;

  T &current_message() const noexcept {
    void *vptr = current_message_ptr();
    T *ptr = static_cast<T *>(vptr);
    return *ptr;
  }
};

class ChannelGroup final {
  friend class Server;

public:
  explicit ChannelGroup(GroupPtr group) noexcept : group_(std::move(group)) {}
  static std::expected<ChannelGroup, Error>
  from_attributes(const GroupAttributes &group_attr) noexcept;

  // deserialize
  static std::expected<ChannelGroup, Error>
  deserialize(const std::span<std::byte> req, std::span<int> fds) noexcept;

  ~ChannelGroup() noexcept = default;

  // Non-copyable
  ChannelGroup(const ChannelGroup &) = delete;
  ChannelGroup &operator=(const ChannelGroup &) = delete;

  // Movable
  ChannelGroup(ChannelGroup &&other) noexcept = default;
  ChannelGroup &operator=(ChannelGroup &&other) noexcept = default;

  std::expected<std::tuple<std::vector<std::byte>, std::vector<int>>, int>
  serialize() const noexcept;

  std::expected<ChannelAttributes, Error>
  get_consumer_attributes(unsigned index) const noexcept;

  std::expected<ChannelAttributes, Error>
  get_producer_attributes(unsigned index) const noexcept;

  template <TriviallyCopyable T>
  std::expected<Consumer<T>, Error> acquire_consumer(unsigned index) noexcept {
    auto size = consumer_message_size(index);
    if (!size)
      return std::unexpected(Error::index_out_of_range);

    if (*size < sizeof(T))
      return std::unexpected(Error::message_size_mismatch);

    auto consumer = acquire_consumer_impl(index);

    if (!consumer)
      return std::unexpected(consumer.error());

    return Consumer<T>(std::move(*consumer));
  }

  template <TriviallyCopyable T>
  std::expected<Producer<T>, Error> acquire_producer(unsigned index) noexcept {
    auto size = producer_message_size(index);
    if (!size)
      return std::unexpected(Error::index_out_of_range);

    if (*size < sizeof(T))
      return std::unexpected(Error::message_size_mismatch);

    auto producer = acquire_producer_impl(index);

    if (!producer)
      return std::unexpected(producer.error());

    return Producer<T>(std::move(*producer));
  }

private:
  std::expected<ConsumerPtr, Error>
  acquire_consumer_impl(unsigned index) noexcept;
  std::expected<ProducerPtr, Error>
  acquire_producer_impl(unsigned index) noexcept;

  std::expected<size_t, Error>
  consumer_message_size(unsigned index) const noexcept;
  std::expected<size_t, Error>
  producer_message_size(unsigned index) const noexcept;

private:
  GroupPtr group_;
};

class Server final {
public:
  using Filter = std::function<bool(const GroupAttributes &attr)>;
  static std::expected<Server, Error> listen(const std::string &path,
                                             int backlog = 1) noexcept;
  ~Server() noexcept = default;

  // Non-copyable
  Server(const Server &) = delete;
  Server &operator=(const Server &) = delete;

  // Movable
  Server(Server &&other) noexcept = default;
  Server &operator=(Server &&other) noexcept = default;

  std::expected<ChannelGroup, Error> accept(Filter filter) noexcept;
  int get_socket() const noexcept;

private:
  explicit Server(ServerPtr server) noexcept : server_(std::move(server)) {}
  ServerPtr server_;
};

std::expected<ChannelGroup, Error>
client_connect(int socket, const GroupAttributes &attr) noexcept;
std::expected<ChannelGroup, Error>
client_connect(const std::string &path, const GroupAttributes &attr) noexcept;
} // namespace rtipc
