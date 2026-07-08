#pragma once

#include <stdint.h>

#define GROUP_INFO "rpc group"
#define COMMAND_INFO "rpc command"
#define RESPONSE_INFO "rpc response"
#define EVENT_INFO "rpc event"

typedef enum  {
    CMDID_UNKNOWN = 0,
    CMDID_HELLO,
    CMDID_STOP,
    CMDID_SENDEVENT,
    CMDID_DIV,
} command_id_t;

typedef struct msg_command {
    uint32_t id;
    int32_t args[3];
} msg_command_t;


typedef struct msg_response {
    uint32_t id;
    int32_t result;
    int32_t data;
} msg_response_t;

typedef struct msg_event {
    uint32_t id;
    uint32_t nr;
} msg_event_t;



void msg_command_print(const msg_command_t *msg);


void msg_response_print(const msg_response_t *msg);


void msg_event_print(const msg_event_t *msg);
