#include "messages.h"

#include <rtipc/log.h>
#include <rtipc/rtipc.h>

void msg_command_print(const msg_command_t *msg)
{
  LOG_INF("command: \tid=0x%x", msg->id);
  //LOG_INF("\targ[0]=0x%x", msg->args.[0]);
  //LOG_INF("\targ[1]=0x%x", msg->args[1]);
  //LOG_INF("\targ[2]=0x%x", msg->args[2]);
}


void msg_response_print(const msg_response_t *msg)
{
  LOG_INF("response: \tid=0x%x", msg->id);
  LOG_INF("\tresult=%i", msg->result);
  LOG_INF("\tdata[1]=0x%x", msg->data);
}


void msg_event_print(const msg_event_t *msg)
{
  LOG_INF("event: \tid=%u", msg->id);
  LOG_INF("\tnr=%u", msg->nr);
}

