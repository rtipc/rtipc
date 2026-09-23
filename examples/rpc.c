#include "rpc.h"


static const uint8_t channel_send_event_args_info_data[] = {0x2,0x3,0x2,
    0x69,0x64,0x1,0x13,0x5,0x66,0x6f,0x72,0x63,0x65,0x1,0x0,0x3,0x6e,0x75,
    0x6d,0x1,0x13};

const ri_info_t channel_send_event_args_info = {
    .data = channel_send_event_args_info_data,
    .size = sizeof(channel_send_event_args_info_data)
};


static const uint8_t channel_div_args_info_data[] = {0x2,0x2,0x7,0x64,0x69,
    0x76,0x69,0x73,0x6f,0x72,0x1,0x24,0x8,0x64,0x69,0x76,0x69,0x64,0x65,
    0x6e,0x74,0x1,0x24};

const ri_info_t channel_div_args_info = {
    .data = channel_div_args_info_data,
    .size = sizeof(channel_div_args_info_data)
};


static const uint8_t channel_command_args_info_data[] = {0x3,0x2,0x3,0x64,
    0x69,0x76,0x2,0x2,0x7,0x64,0x69,0x76,0x69,0x73,0x6f,0x72,0x1,0x24,0x8,
    0x64,0x69,0x76,0x69,0x64,0x65,0x6e,0x74,0x1,0x24,0x4,0x73,0x65,0x6e,
    0x64,0x2,0x3,0x2,0x69,0x64,0x1,0x13,0x5,0x66,0x6f,0x72,0x63,0x65,0x1,
    0x0,0x3,0x6e,0x75,0x6d,0x1,0x13};

const ri_info_t channel_command_args_info = {
    .data = channel_command_args_info_data,
    .size = sizeof(channel_command_args_info_data)
};


static const uint8_t channel_msg_command_info_data[] = {0x2,0x2,0x2,0x69,
    0x64,0x1,0x13,0x4,0x61,0x72,0x67,0x73,0x3,0x2,0x3,0x64,0x69,0x76,0x2,
    0x2,0x7,0x64,0x69,0x76,0x69,0x73,0x6f,0x72,0x1,0x24,0x8,0x64,0x69,0x76,
    0x69,0x64,0x65,0x6e,0x74,0x1,0x24,0x4,0x73,0x65,0x6e,0x64,0x2,0x3,0x2,
    0x69,0x64,0x1,0x13,0x5,0x66,0x6f,0x72,0x63,0x65,0x1,0x0,0x3,0x6e,0x75,
    0x6d,0x1,0x13};

const ri_info_t channel_msg_command_info = {
    .data = channel_msg_command_info_data,
    .size = sizeof(channel_msg_command_info_data)
};


static const uint8_t channel_response_data_info_data[] = {0x3,0x1,0x8,0x71,
    0x75,0x6f,0x74,0x69,0x65,0x6e,0x74,0x1,0x24};

const ri_info_t channel_response_data_info = {
    .data = channel_response_data_info_data,
    .size = sizeof(channel_response_data_info_data)
};


static const uint8_t channel_msg_response_info_data[] = {0x2,0x3,0x2,0x69,
    0x64,0x1,0x13,0x6,0x72,0x65,0x73,0x75,0x6c,0x74,0x1,0x3,0x4,0x64,0x61,
    0x74,0x61,0x3,0x1,0x8,0x71,0x75,0x6f,0x74,0x69,0x65,0x6e,0x74,0x1,0x24
    };

const ri_info_t channel_msg_response_info = {
    .data = channel_msg_response_info_data,
    .size = sizeof(channel_msg_response_info_data)
};


static const uint8_t channel_msg_event_info_data[] = {0x2,0x2,0x2,0x69,
    0x64,0x1,0x13,0x2,0x6e,0x72,0x1,0x13};

const ri_info_t channel_msg_event_info = {
    .data = channel_msg_event_info_data,
    .size = sizeof(channel_msg_event_info_data)
};



static const uint8_t group_rpc_info_data[] = {0x3,0x52,0x50,0x43,0x7,0x63,
    0x6f,0x6d,0x6d,0x61,0x6e,0x64,0x8,0x72,0x65,0x73,0x70,0x6f,0x6e,0x73,
    0x65,0x5,0x65,0x76,0x65,0x6e,0x74};

const ri_info_t group_rpc_info = {
    .data = group_rpc_info_data,
    .size = sizeof(group_rpc_info_data)
};


static const ri_channel_attr_t group_rpc_c2s_channels[] = {
    { .add_msgs = 0, .msg_size = sizeof(channel_msg_command_info),
         .eventfd = 1, .info = channel_msg_command_info},
    { 0 },
};

static const ri_channel_attr_t group_rpc_s2c_channels[] = {
    { .add_msgs = 0, .msg_size = sizeof(channel_msg_response_info),
         .eventfd = 1, .info = channel_msg_response_info},
    { .add_msgs = 10, .msg_size = sizeof(channel_msg_event_info),
         .eventfd = 1, .info = channel_msg_event_info},
    { 0 },
};


const ri_group_attr_t client_group_rpc = {
    .consumers = group_rpc_s2c_channels,
    .producers = group_rpc_c2s_channels,
    .info = group_rpc_info
};

ri_consumer_t* client_rpc_acquire_response(ri_group_t *group)
{
    const ri_channel_attr_t *remote_attr = ri_group_get_consumer_attr(group, 0);
    if (!remote_attr) {
        return NULL;
    }

    const ri_channel_attr_t *ecpected_attr = &client_group_rpc.consumers[0];
    if (!ri_channel_attr_equal(ecpected_attr, remote_attr)) {
        return NULL;
    }

    return ri_group_acquire_consumer(group, 0);
}

ri_consumer_t* client_rpc_acquire_event(ri_group_t *group)
{
    const ri_channel_attr_t *remote_attr = ri_group_get_consumer_attr(group, 1);
    if (!remote_attr) {
        return NULL;
    }

    const ri_channel_attr_t *ecpected_attr = &client_group_rpc.consumers[1];
    if (!ri_channel_attr_equal(ecpected_attr, remote_attr)) {
        return NULL;
    }

    return ri_group_acquire_consumer(group, 1);
}

ri_producer_t* client_rpc_acquire_command(ri_group_t *group)
{
    const ri_channel_attr_t *remote_attr = ri_group_get_producer_attr(group, 0);
    if (!remote_attr) {
        return NULL;
    }

    const ri_channel_attr_t *ecpected_attr = &client_group_rpc.producers[0];
    if (!ri_channel_attr_equal(ecpected_attr, remote_attr)) {
        return NULL;
    }

    return ri_group_acquire_producer(group, 0);
}


const ri_group_attr_t server_group_rpc = {
    .consumers = group_rpc_c2s_channels,
    .producers = group_rpc_s2c_channels,
    .info = group_rpc_info
};

ri_consumer_t* server_rpc_acquire_command(ri_group_t *group)
{
    const ri_channel_attr_t *remote_attr = ri_group_get_consumer_attr(group, 0);
    if (!remote_attr) {
        return NULL;
    }

    const ri_channel_attr_t *ecpected_attr = &server_group_rpc.consumers[0];
    if (!ri_channel_attr_equal(ecpected_attr, remote_attr)) {
        return NULL;
    }

    return ri_group_acquire_consumer(group, 0);
}

ri_producer_t* server_rpc_acquire_response(ri_group_t *group)
{
    const ri_channel_attr_t *remote_attr = ri_group_get_producer_attr(group, 0);
    if (!remote_attr) {
        return NULL;
    }

    const ri_channel_attr_t *ecpected_attr = &server_group_rpc.producers[0];
    if (!ri_channel_attr_equal(ecpected_attr, remote_attr)) {
        return NULL;
    }

    return ri_group_acquire_producer(group, 0);
}

ri_producer_t* server_rpc_acquire_event(ri_group_t *group)
{
    const ri_channel_attr_t *remote_attr = ri_group_get_producer_attr(group, 1);
    if (!remote_attr) {
        return NULL;
    }

    const ri_channel_attr_t *ecpected_attr = &server_group_rpc.producers[1];
    if (!ri_channel_attr_equal(ecpected_attr, remote_attr)) {
        return NULL;
    }

    return ri_group_acquire_producer(group, 1);
}



