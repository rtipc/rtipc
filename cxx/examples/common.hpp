#pragma once


#include <cstdint>


enum class CommandId : std::uint32_t {
  unknown = 0,
  hello,
  stop,
  send_event,
  div,
};
