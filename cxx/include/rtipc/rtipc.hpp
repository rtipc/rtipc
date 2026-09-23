#pragma once

#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
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

struct ChannelAttr {
  static ChannelAttr from_c_attr(const ::ri_channel_attr *c_attr);

  size_t message_size;
  unsigned additional_messages;
  bool eventfd;
  Info info;
  bool operator==(const ChannelAttr&) const = default;
};

struct GroupAttr {
  static GroupAttr from_c_attr(const ::ri_group_attr *c_attr);
  std::vector<ChannelAttr> consumers;
  std::vector<ChannelAttr> producers;
  Info info;
  bool operator==(const GroupAttr&) const = default;
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

  std::optional<const T &> current_message() const noexcept {
    const void *vptr = current_message_ptr();
    if (vptr == nullptr)
      return std::nullopt;

    const T *ptr = static_cast<const T *>(vptr);
    return *ptr;
  }

  using ConsumerBase::count_messages;
  using ConsumerBase::pop;
  using ConsumerBase::get_eventfd;
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

  using ProducerBase::count_messages;
  using ProducerBase::force_push;
  using ProducerBase::try_push;
  using ProducerBase::get_eventfd;
  using ProducerBase::take_eventfd;
  using ProducerBase::cache_enable;
  using ProducerBase::cache_disable;

  T &current_message() const noexcept {
    void *vptr = current_message_ptr();
    T *ptr = static_cast<T *>(vptr);
    return *ptr;
  }
};

class ChannelGroup final {
public:
  explicit ChannelGroup(GroupPtr group) : group_(std::move(group)) {}
  explicit ChannelGroup(const GroupAttr &attr);

  // deserialize
  explicit ChannelGroup(const std::span<std::byte> req, std::span<int> fds);

  ~ChannelGroup() noexcept = default;

  // Non-copyable
  ChannelGroup(const ChannelGroup &) = delete;
  ChannelGroup &operator=(const ChannelGroup &) = delete;

  // Movable
  ChannelGroup(ChannelGroup &&other) noexcept = default;
  ChannelGroup &operator=(ChannelGroup &&other) noexcept = default;

  std::tuple<std::vector<std::byte>, std::vector<int>> serialize() const;

  template <TriviallyCopyable T> Consumer<T> acquire_consumer(unsigned index) {

    size_t size = consumer_message_size(index);

    if (size < sizeof(T))
      throw std::length_error("channel message size too small to hold struct");

    return Consumer<T>(acquire_consumer_impl(index));
  }

  template <TriviallyCopyable T> Producer<T> acquire_producer(unsigned index) {
    if (!std::is_trivially_copyable_v<T>)
      throw std::invalid_argument("Message type is not trivially copyable");

    size_t size = producer_message_size(index);
    if (size < sizeof(T))
      throw std::length_error("channel message size too small to hold struct");

    return Producer<T>(acquire_producer_impl(index));
  }

private:
  ConsumerPtr acquire_consumer_impl(unsigned index);
  ProducerPtr acquire_producer_impl(unsigned index);

  size_t consumer_message_size(unsigned index) const;
  size_t producer_message_size(unsigned index) const;

private:
  GroupPtr group_;
};



class Server final
{
  public:
  using Filter = std::function<bool(const GroupAttr& attr)>;
  Server(const std::string &path, int backlog = 1);
  ~Server() noexcept = default;

  // Non-copyable
  Server(const Server &) = delete;
  Server &operator=(const Server &) = delete;

  // Movable
  Server(Server &&other) noexcept = default;
  Server &operator=(Server &&other) noexcept = default;

  ChannelGroup accept(Filter filter);
  int get_socket() const noexcept;

  private:
  ServerPtr server_;
};

ChannelGroup client_connect(int socket, const GroupAttr& attr);
ChannelGroup client_connect(const std::string &path, const GroupAttr& attr);
} // namespace rtipc
