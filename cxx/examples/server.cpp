#include <thread>


#include "rpc.hpp"
#include "common.hpp"

#define MAX_CYCLES 10000

enum class ServerError {
  error,
  again,
};

class Server {
  static bool filter(const rtipc::GroupAttributes &attr) {
    return attr == rpc::server_group_rpc;
  }

public:
  static std::expected<Server, rtipc::Error>
  connect(const std::string &path) noexcept {
    auto server = rtipc::Server::listen("rtipc.sock");
    if (!server)
      return std::unexpected(server.error());

    auto group = server->accept(filter);

    auto command = rpc::server_rpc_acquire_command(*group);
    if (!command)
      return std::unexpected(command.error());

    auto response = rpc::server_rpc_acquire_response(*group);
    if (!response)
      return std::unexpected(response.error());

    auto event = rpc::server_rpc_acquire_event(*group);
    if (!event)
      return std::unexpected(event.error());

    return Server(std::move(*command), std::move(*response), std::move(*event));
  }

  Server(Server &&) noexcept = default;
  Server &operator=(Server &&) noexcept = default;

  Server(const Server &) = delete;
  Server &operator=(const Server &) = delete;

  ~Server() = default;

  std::expected<CommandId, ServerError> proccess() {
    auto pop_result = command_.pop();

    if (!pop_result)
      return std::unexpected(ServerError::error);

    if ((pop_result == rtipc::PopResult::no_message) ||
        (pop_result == rtipc::PopResult::no_update)) {
      return std::unexpected(ServerError::again);
    }

    const auto &cmd = command_.current_message()->get();

    auto &rsp = response_.current_message();

    rsp.id = cmd.id;

    auto cmdid = static_cast<CommandId>(cmd.id);

    switch (cmdid) {
    case CommandId::hello:
      rsp.result = 0;
      break;
    case CommandId::stop:
      rsp.result = 0;
      break;
    case CommandId::send_event:
      rsp.result = send_events(cmd.args.send.id, cmd.args.send.num,
                               cmd.args.send.force);
      break;
    case CommandId::div:
      rsp.result = server_div(cmd.args.div.divisor, cmd.args.div.divident,
                              &rsp.data.quotient);
      break;
    default:
      rsp.result = -1;
      break;
    }

    auto push_result = response_.force_push();
    if (!push_result)
      return std::unexpected(ServerError::error);
    return cmdid;
  }

private:
  rtipc::Consumer<rpc::MsgCommand> command_;
  rtipc::Producer<rpc::MsgResponse> response_;
  rtipc::Producer<rpc::MsgEvent> event_;
  explicit Server(rtipc::Consumer<rpc::MsgCommand> command,
                  rtipc::Producer<rpc::MsgResponse> response,
                  rtipc::Producer<rpc::MsgEvent> event)
      : command_(std::move(command)), response_(std::move(response)),
        event_(std::move(event)) {}

  int32_t send_events(uint32_t id, unsigned num, bool force) {
    for (unsigned i = 0; i < num; i++) {
      auto &event = event_.current_message();
      event.id = id;
      event.nr = i;
      if (force) {
        auto result = event_.force_push();
        if (!result)
          throw std::runtime_error("queue error");
      } else {
        auto result = event_.try_push();
        if (!result)
          throw std::runtime_error("queue error");

        if (result == rtipc::TryPushResult::fail)
          return i;
      }
    }
    return num;
  }
  int32_t server_div(double a, double b, double *res) const {
    if (b == 0) {
      return -1;
    } else {
      *res = a / b;
      return 0;
    }
  }
};

int main(void) {

  auto server = Server::connect("rticp.sock");

  if (!server)
    return -1;

  for (int i = 0; i < MAX_CYCLES; ++i) {
    auto result = server->proccess();
    if (!result) {
      if (result.error() == ServerError::again) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        continue;
      } else {
        return -1;
      }
    }

    if (result == CommandId::stop)
        break;
  }
    return 0;
}
