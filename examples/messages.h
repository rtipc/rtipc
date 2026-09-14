#pragma once

#include <stdint.h>
#include "rpc.h"




typedef enum  {
    CMDID_UNKNOWN = 0,
    CMDID_HELLO,
    CMDID_STOP,
    CMDID_SENDEVENT,
    CMDID_DIV,
} command_id_t;



void msg_command_print(const msg_command_t *msg);


void msg_response_print(const msg_response_t *msg);


void msg_event_print(const msg_event_t *msg);
