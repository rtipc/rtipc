#pragma once


#include <vector>

#include <rtipc/rtipc.hpp>


namespace rpc {


struct SendEventArgs {
    std::uint32_t id;
    bool force;
    std::uint32_t num;
};

struct DivArgs {
    double divisor;
    double divident;
};

union CommandArgs {
    DivArgs div;
    SendEventArgs send;
};

struct MsgCommand {
    std::uint32_t id;
    CommandArgs args;
};

union ResponseData {
    double quotient;
};

struct MsgResponse {
    std::uint32_t id;
    std::int32_t result;
    ResponseData data;
};

struct MsgEvent {
    std::uint32_t id;
    std::uint32_t nr;
};


const rtipc::Info c_channel_send_event_args_info{
    0x2,0x3,0x2,0x69,0x64,0x1,0x13,0x5,0x66,0x6f,0x72,0x63,0x65,0x1,0x0,
    0x3,0x6e,0x75,0x6d,0x1,0x13};


const rtipc::Info c_channel_div_args_info{
    0x2,0x2,0x7,0x64,0x69,0x76,0x69,0x73,0x6f,0x72,0x1,0x24,0x8,0x64,0x69,
    0x76,0x69,0x64,0x65,0x6e,0x74,0x1,0x24};


const rtipc::Info c_channel_command_args_info{
    0x3,0x2,0x3,0x64,0x69,0x76,0x2,0x2,0x7,0x64,0x69,0x76,0x69,0x73,0x6f,
    0x72,0x1,0x24,0x8,0x64,0x69,0x76,0x69,0x64,0x65,0x6e,0x74,0x1,0x24,0x4,
    0x73,0x65,0x6e,0x64,0x2,0x3,0x2,0x69,0x64,0x1,0x13,0x5,0x66,0x6f,0x72,
    0x63,0x65,0x1,0x0,0x3,0x6e,0x75,0x6d,0x1,0x13};


const rtipc::Info c_channel_msg_command_info{
    0x2,0x2,0x2,0x69,0x64,0x1,0x13,0x4,0x61,0x72,0x67,0x73,0x3,0x2,0x3,
    0x64,0x69,0x76,0x2,0x2,0x7,0x64,0x69,0x76,0x69,0x73,0x6f,0x72,0x1,0x24,
    0x8,0x64,0x69,0x76,0x69,0x64,0x65,0x6e,0x74,0x1,0x24,0x4,0x73,0x65,
    0x6e,0x64,0x2,0x3,0x2,0x69,0x64,0x1,0x13,0x5,0x66,0x6f,0x72,0x63,0x65,
    0x1,0x0,0x3,0x6e,0x75,0x6d,0x1,0x13};


const rtipc::Info c_channel_response_data_info{
    0x3,0x1,0x8,0x71,0x75,0x6f,0x74,0x69,0x65,0x6e,0x74,0x1,0x24};


const rtipc::Info c_channel_msg_response_info{
    0x2,0x3,0x2,0x69,0x64,0x1,0x13,0x6,0x72,0x65,0x73,0x75,0x6c,0x74,0x1,
    0x3,0x4,0x64,0x61,0x74,0x61,0x3,0x1,0x8,0x71,0x75,0x6f,0x74,0x69,0x65,
    0x6e,0x74,0x1,0x24};


const rtipc::Info c_channel_msg_event_info{
    0x2,0x2,0x2,0x69,0x64,0x1,0x13,0x2,0x6e,0x72,0x1,0x13};


const rtipc::Info c_group_rpc_info{
    0x3,0x52,0x50,0x43,0x7,0x63,0x6f,0x6d,0x6d,0x61,0x6e,0x64,0x8,0x72,
    0x65,0x73,0x70,0x6f,0x6e,0x73,0x65,0x5,0x65,0x76,0x65,0x6e,0x74};


const std::vector<rtipc::ChannelAttributes> c_rpc_c2s_channels{
    rtipc::ChannelAttributes{ .message_size = sizeof(MsgCommand),
     .additional_messages = 0, .eventfd = true,
     .info = c_channel_msg_command_info},
    };

const std::vector<rtipc::ChannelAttributes> c_rpc_s2c_channels{
    rtipc::ChannelAttributes{ .message_size = sizeof(MsgResponse),
     .additional_messages = 0, .eventfd = true,
     .info = c_channel_msg_response_info},
    rtipc::ChannelAttributes{ .message_size = sizeof(MsgEvent),
     .additional_messages = 10, .eventfd = true,
     .info = c_channel_msg_event_info},
    };



const rtipc::GroupAttributes client_group_rpc = {
    .consumers = c_rpc_s2c_channels,
    .producers = c_rpc_c2s_channels,
    .info = c_group_rpc_info
};


std::expected<rtipc::Consumer<MsgResponse>, rtipc::Error> client_rpc_acquire_response(rtipc::ChannelGroup &group)
{
    auto remote_attr = group.get_consumer_attributes(0);

    const auto& expect_attr = client_group_rpc.consumers[0];
    if (!(expect_attr == remote_attr)) {
        return std::unexpected(rtipc::Error::attribute_mismatch);
    }

    return group.acquire_consumer<MsgResponse>(0);
}


std::expected<rtipc::Consumer<MsgEvent>, rtipc::Error> client_rpc_acquire_event(rtipc::ChannelGroup &group)
{
    auto remote_attr = group.get_consumer_attributes(1);

    const auto& expect_attr = client_group_rpc.consumers[1];
    if (!(expect_attr == remote_attr)) {
        return std::unexpected(rtipc::Error::attribute_mismatch);
    }

    return group.acquire_consumer<MsgEvent>(1);
}


std::expected<rtipc::Producer<MsgCommand>, rtipc::Error> client_rpc_acquire_command(rtipc::ChannelGroup &group)
{
    auto remote_attr = group.get_producer_attributes(0);

    const auto& expect_attr = client_group_rpc.producers[0];
    if (!(expect_attr == remote_attr)) {
        return std::unexpected(rtipc::Error::attribute_mismatch);
    }

    return group.acquire_producer<MsgCommand>(0);
}


const rtipc::GroupAttributes server_group_rpc = {
    .consumers = c_rpc_c2s_channels,
    .producers = c_rpc_s2c_channels,
    .info = c_group_rpc_info
};


std::expected<rtipc::Consumer<MsgCommand>, rtipc::Error> server_rpc_acquire_command(rtipc::ChannelGroup &group)
{
    auto remote_attr = group.get_consumer_attributes(0);

    const auto& expect_attr = server_group_rpc.consumers[0];
    if (!(expect_attr == remote_attr)) {
        return std::unexpected(rtipc::Error::attribute_mismatch);
    }

    return group.acquire_consumer<MsgCommand>(0);
}


std::expected<rtipc::Producer<MsgResponse>, rtipc::Error> server_rpc_acquire_response(rtipc::ChannelGroup &group)
{
    auto remote_attr = group.get_producer_attributes(0);

    const auto& expect_attr = server_group_rpc.producers[0];
    if (!(expect_attr == remote_attr)) {
        return std::unexpected(rtipc::Error::attribute_mismatch);
    }

    return group.acquire_producer<MsgResponse>(0);
}


std::expected<rtipc::Producer<MsgEvent>, rtipc::Error> server_rpc_acquire_event(rtipc::ChannelGroup &group)
{
    auto remote_attr = group.get_producer_attributes(1);

    const auto& expect_attr = server_group_rpc.producers[1];
    if (!(expect_attr == remote_attr)) {
        return std::unexpected(rtipc::Error::attribute_mismatch);
    }

    return group.acquire_producer<MsgEvent>(1);
}





}
