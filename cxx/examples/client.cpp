#include <thread>
#include <print>

#include "rpc.hpp"
#include "common.hpp"

enum class ClientError {
  error,
  again,
};

class Client {
public:
  static std::expected<Client, rtipc::Error>
  connect(const std::string &path) noexcept {
    auto group = rtipc::client_connect("rtipc.sock", rpc::client_group_rpc);
    if (!group)
      return std::unexpected(group.error());

    auto command = rpc::client_rpc_acquire_command(*group);
    if (!command)
      return std::unexpected(command.error());

    auto response = rpc::client_rpc_acquire_response(*group);
    if (!response)
      return std::unexpected(response.error());

    auto event = rpc::client_rpc_acquire_event(*group);
    if (!event)
      return std::unexpected(event.error());

    return Client(std::move(*command), std::move(*response), std::move(*event));
  }

  Client(Client &&) noexcept = default;
  Client &operator=(Client &&) noexcept = default;

  Client(const Client &) = delete;
  Client &operator=(const Client &) = delete;

  ~Client() = default;

  std::expected<std::monostate, ClientError> send_command(const rpc::MsgCommand &cmd)
  {
    std::print("send id = {}\n", cmd.id);
    auto &msg = command_.current_message();
    msg = cmd;

    auto result = command_.force_push();
    if (!result)
      return std::unexpected(ClientError::error);

    return await_response();
  }

  std::expected<std::monostate, ClientError> await_response() {
    for (int i = 0; i < 30000; i++) {
      auto result = response_.pop();
      if (!result) {
        return std::unexpected(ClientError::error);
      }
      if ((result == rtipc::PopResult::no_message)
       || (result == rtipc::PopResult::no_update)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        continue;
      }
      auto msg = response_.current_message().value();
      std::print("received id = {} result = {}\n", msg.id, msg.result);
      return {};
    }

    return std::unexpected(ClientError::error);
  }

private:
  explicit Client(rtipc::Producer<rpc::MsgCommand> command,
                  rtipc::Consumer<rpc::MsgResponse> response,
                  rtipc::Consumer<rpc::MsgEvent> event)
      : command_(std::move(command)), response_(std::move(response)),
        event_(std::move(event)) {}

  rtipc::Producer<rpc::MsgCommand> command_;
  rtipc::Consumer<rpc::MsgResponse> response_;
  rtipc::Consumer<rpc::MsgEvent> event_;
};

static constexpr std::array<rpc::MsgCommand, 6> commands = {
    rpc::MsgCommand{ .id = static_cast<uint32_t>(CommandId::hello)},
    rpc::MsgCommand{ .id = static_cast<uint32_t>(CommandId::send_event),  .args = {.send = { .id = 11, .force = false, .num = 20}}},
    rpc::MsgCommand{ .id =  static_cast<uint32_t>(CommandId::send_event), .args = {.send = { .id = 12, .force = true, .num = 20}}},
    rpc::MsgCommand{ .id = static_cast<uint32_t>(CommandId::div),         .args = {.div = {100.0, 7.0}}},
    rpc::MsgCommand{ .id = static_cast<uint32_t>(CommandId::div),         .args = {.div = {100.0, 0.0}}},
    rpc::MsgCommand{ .id = static_cast<uint32_t>(CommandId::stop)},
    };


int main(void) {

   auto client = Client::connect("rtipc.sock");
  for (auto cmd : commands) {
     auto result = client->send_command(cmd);
    if (!result)
       return -1;
  }

  return 0;
}
