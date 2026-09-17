#pragma once

#include <expected>
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
}

namespace rtipc {

template <typename T>
concept TriviallyCopyable = std::is_trivially_copyable_v<T>;

void consumer_release(::ri_consumer *consumer);
void producer_release(::ri_producer *producer);
void group_delete(::ri_group *group);

struct ConsumerDeleter {
  void operator()(ri_consumer *consumer) const noexcept {
    if (consumer)
      consumer_release(consumer);
  }
};

struct ProducerDeleter {
  void operator()(ri_producer *producer) const noexcept {
    if (producer)
      producer_release(producer);
  }
};

struct GroupDeleter {
  void operator()(ri_group *group) const noexcept {
    if (group)
      group_delete(group);
  }
};

using ConsumerPtr = std::unique_ptr<ri_consumer, ConsumerDeleter>;
using ProducerPtr = std::unique_ptr<ri_producer, ProducerDeleter>;
using GroupPtr = std::unique_ptr<ri_group, GroupDeleter>;
using Info = std::vector<char>;

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
  size_t message_size;
  unsigned additional_messages;
  bool fd;
  Info info;
};

struct GroupAttr {
  std::vector<ChannelAttr> consumers;
  std::vector<ChannelAttr> producers;
  Info info;
};

class ConsumerBase {
  friend class ChannelGroup;

protected:
  explicit ConsumerBase(ConsumerPtr consumer) noexcept;

public:
  virtual ~ConsumerBase() noexcept = default;

  ConsumerBase(const ConsumerBase &) = delete;
  ConsumerBase &operator=(const ConsumerBase &) = delete;

  ConsumerBase(ConsumerBase &&other) noexcept = default;
  ConsumerBase &operator=(ConsumerBase &&other) noexcept = default;

  std::expected<PopResult, QueueError> pop() noexcept;
  std::expected<unsigned, QueueError> count_messages() const noexcept;

protected:
  ConsumerPtr consumer_;
};

template <TriviallyCopyable T> class Consumer final : private ConsumerBase {
  friend class ChannelGroup;

private:
  explicit Consumer(ConsumerPtr consumer) noexcept
      : ConsumerBase(std::move(consumer)) {}

public:
  Consumer();
  ~Consumer() noexcept override = default;

  // Non-copyable
  Consumer(const Consumer &) = delete;
  Consumer &operator=(const Consumer &) = delete;

  // Movable
  Consumer(Consumer &&other) noexcept = default;
  Consumer &operator=(Consumer &&other) noexcept = default;

  std::optional<const T &> current_message() const noexcept;

  using ConsumerBase::count_messages;
  using ConsumerBase::pop;
};

class ProducerBase {
  friend class ChannelGroup;

protected:
  explicit ProducerBase(ProducerPtr producer) noexcept;

public:
  virtual ~ProducerBase() noexcept = default;

  // Non-copyable
  ProducerBase(const ProducerBase &) = delete;
  ProducerBase &operator=(const ProducerBase &) = delete;

  // Movable
  ProducerBase(ProducerBase &&other) noexcept = default;
  ProducerBase &operator=(ProducerBase &&other) noexcept = default;

  std::expected<TryPushResult, QueueError> try_push() noexcept;
  std::expected<ForcePushResult, QueueError> force_push() noexcept;
  std::expected<unsigned, QueueError> count_messages() const noexcept;

  void cache_enable();
  void cache_disable() noexcept;

protected:
  ProducerPtr producer_;
};

template <TriviallyCopyable T> class Producer final : protected ProducerBase {
  friend class ChannelGroup;

private:
  explicit Producer(ProducerPtr producer) noexcept
      : ProducerBase(std::move(producer)) {}

public:
  ~Producer() noexcept override = default;

  // Non-copyable
  Producer(const Producer &) = delete;
  Producer &operator=(const Producer &) = delete;

  // Movable
  Producer(Producer &&other) noexcept = default;
  Producer &operator=(Producer &&other) noexcept = default;

  using ProducerBase::cache_disable;
  using ProducerBase::cache_enable;
  using ProducerBase::count_messages;
  using ProducerBase::force_push;
  using ProducerBase::try_push;

  T &current_message() noexcept;
};

class ChannelGroup final {
public:
  // deserializw
  explicit ChannelGroup(const std::span<std::byte> &req, std::span<int> &fds);

  explicit ChannelGroup(const GroupAttr &attr);

  ~ChannelGroup() noexcept = default;

  // Non-copyable
  ChannelGroup(const ChannelGroup &) = delete;
  ChannelGroup &operator=(const ChannelGroup &) = delete;

  // Movable
  ChannelGroup(ChannelGroup &&other) noexcept = default;
  ChannelGroup &operator=(ChannelGroup &&other) noexcept = default;

  std::tuple<std::vector<std::byte>, std::vector<int>> serialize();

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

} // namespace rtipc
