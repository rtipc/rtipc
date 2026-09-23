#pragma once


#include <stdint.h>

#include <rtipc/rtipc.h>


#ifdef __cplusplus
extern "C" {
#endif


typedef struct send_event_args {
    uint32_t id;
    bool force;
    uint32_t num;
} send_event_args_t;

typedef struct div_args {
    double divisor;
    double divident;
} div_args_t;

typedef union command_args {
    struct div_args div;
    struct send_event_args send;
} command_args_t;

typedef struct msg_command {
    uint32_t id;
    union command_args args;
} msg_command_t;

typedef union response_data {
    double quotient;
} response_data_t;

typedef struct msg_response {
    uint32_t id;
    int32_t result;
    union response_data data;
} msg_response_t;

typedef struct msg_event {
    uint32_t id;
    uint32_t nr;
} msg_event_t;


extern const ri_info_t channel_send_event_args_info;

extern const ri_info_t channel_div_args_info;

extern const ri_info_t channel_command_args_info;

extern const ri_info_t channel_msg_command_info;

extern const ri_info_t channel_response_data_info;

extern const ri_info_t channel_msg_response_info;

extern const ri_info_t channel_msg_event_info;


extern const ri_info_t group_rpc_info;


extern const ri_group_attr_t client_group_rpc;
ri_consumer_t* client_rpc_acquire_response(ri_group_t *group);
ri_consumer_t* client_rpc_acquire_event(ri_group_t *group);
ri_producer_t* client_rpc_acquire_command(ri_group_t *group);

extern const ri_group_attr_t server_group_rpc;
ri_consumer_t* server_rpc_acquire_command(ri_group_t *group);
ri_producer_t* server_rpc_acquire_response(ri_group_t *group);
ri_producer_t* server_rpc_acquire_event(ri_group_t *group);




#ifdef __cplusplus
}
#endif
