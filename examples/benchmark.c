#define _GNU_SOURCE

#include <stdint.h>
#include <error.h>
#include <unistd.h>
#include <errno.h>
#include <threads.h>
#include <sched.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>

#include "rtipc/rtipc.h"
#include "rtipc/connect.h"
#include "rtipc/log.h"


#define CLOCKID CLOCK_PROCESS_CPUTIME_ID

#ifndef ADDITIONAL_MSGS
#define ADDITIONAL_MSGS 100
#endif

#ifndef SEND_NUM_MSGS
#define SEND_NUM_MSGS 10000000
#endif

#ifndef CPU_SERVER
#define CPU_SERVER 2
#endif

#ifndef CPU_CLIENT
#define CPU_CLIENT 0
#endif


#define COUNTER_INIT UINT64_MAX

typedef int (*entry_fn)(int);

typedef struct msg {
  uint64_t counter;
  bool stop;
} msg_t;


typedef struct server_stat {
  uint64_t received;
  uint64_t overflowed;
  uint64_t dropped;
} server_stat_t;


typedef struct client_stat {
  uint64_t sent;
  uint64_t dropped;
} client_stat_t;


void print_server_stat(const server_stat_t *stat)
{
  LOG_INF("server:");
  LOG_INF("\tmsgs received %lu", stat->received);
  LOG_INF("\tmsgs dropped %lu", stat->dropped);
  LOG_INF("\tqueue overflowed %lu", stat->overflowed);
}


void print_client_stat(const client_stat_t *stat)
{
  LOG_INF("client:");
  LOG_INF("\tmsgs sent %lu", stat->sent);
  LOG_INF("\tmsgs dropped %lu", stat->dropped);
}


static void set_affinity(int cpu)
{
  if (cpu < 0)
    return;

  int r;
  const int num_cpus = 1;
  cpu_set_t set;

  CPU_ZERO(&set);
  CPU_SET(cpu, &set);

  r = sched_setaffinity(0, CPU_ALLOC_SIZE(num_cpus), &set);

  if (r < 0) {
    error(-1, errno, "set_affinity failed");
  }
}


static uint64_t timespec_diff(const struct timespec *later, const struct timespec *earlier)
{
  uint64_t dt = later->tv_sec - earlier->tv_sec;
  dt *= 1000000000;

  dt += later->tv_nsec;
  dt -= earlier->tv_nsec;

  return dt;
}

// in us
static struct timespec timestamp_now(void)
{
  struct timespec ts;
  int r = clock_gettime(CLOCKID, &ts);
  if (r < 0) {
    error(-1, errno, "clock_gettime failed");
  }

  return ts;
}


static uint64_t timestamp_elapsed(const struct timespec *earlier)
{
  struct timespec now = timestamp_now();

  return timespec_diff(&now, earlier);
}




static int consume(ri_consumer_t *consumer, uint64_t *counter, server_stat_t *stat) {
  int r = 1;
  ri_pop_result_t result = ri_consumer_pop(consumer);

  switch (result) {
    case RI_POP_RESULT_ERROR:
      r = -1;
      LOG_ERR("RI_CONSUME_RESULT_ERROR");
      break;
    case RI_POP_RESULT_NO_MSG:
      if (*counter != COUNTER_INIT) {
        LOG_ERR("RI_CONSUME_RESULT_NO_MSG but message was already received");
        r = -1;
      }
      break;
    case RI_POP_RESULT_NO_UPDATE:
      break;
    case RI_POP_RESULT_SUCCESS: {
        const msg_t *msg = ri_consumer_msg(consumer);
        stat->received++;

        if ((*counter != COUNTER_INIT) && (msg->counter != *counter + 1)) {
          LOG_ERR("RI_CONSUME_RESULT_SUCCESS counter missmatch msg:%lu previous:%lu" , msg->counter, *counter);
          r = -1;
        }

        *counter = msg->counter;
        if (msg->stop) {
          r = 0;
        }
      }
      break;
    case RI_POP_RESULT_DISCARDED: {
        const msg_t *msg = ri_consumer_msg(consumer);

        stat->received++;
        stat->overflowed++;

        if ((*counter != COUNTER_INIT) && (msg->counter <= *counter + 1)) {
          LOG_ERR("RI_CONSUME_RESULT_DISCARDED counter missmatch msg:%lu previous:%lu", msg->counter, *counter);
          r = -1;
          break;
        }

        stat->dropped += *counter == COUNTER_INIT ? msg->counter : msg->counter - *counter - 1;
        *counter = msg->counter;

        if (msg->stop) {
          r = 0;
        }
      }
      break;
  }

  return r;
}


static int produce(ri_producer_t *producer, uint64_t counter, client_stat_t *stat)
{
  int r = 1;

  msg_t *msg = ri_producer_msg(producer);
  msg->counter = counter;
  ri_force_push_result_t result = ri_producer_force_push(producer);

  switch (result) {
    case RI_FORCE_PUSH_RESULT_ERROR:
      r = -1;
      LOG_ERR("RI_PRODUCE_RESULT_ERROR");
      break;
    case RI_FORCE_PUSH_RESULT_SUCCESS:
      stat->sent++;
      break;
    case RI_FORCE_PUSH_RESULT_DISCARDED:
      stat->sent++;
      stat->dropped++;
      break;
  }
  return r;
}

static int client_entry(int socket)
{
  const ri_attr_t producers[] = {
    (ri_attr_t) { .add_msgs = ADDITIONAL_MSGS, .msg_size = sizeof(msg_t)},
    { 0 },
  };

  const ri_config_t config = {
    .producers = producers,
  };


  ri_vector_t *vec = ri_client_socket_connect(socket, &config);

  if (!vec) {
    goto fail_connect;
  }

  ri_producer_t *producer = ri_vector_acquire_producer(vec, 0);

  if (!producer) {
    goto fail_connect;
  }

  /* not needed anymore */
  ri_vector_delete(vec);

  client_stat_t stat = {0};

  struct timespec start = timestamp_now();

  for (uint64_t counter = 0; counter < SEND_NUM_MSGS; counter++) {
    int r = produce(producer, counter, &stat);

    if (r < 0) {
      LOG_ERR("produce failed\n");
      break;
    }
  }

  uint64_t elapsed = timestamp_elapsed(&start);

  /* stop the server by sending stop */
  msg_t *msg = ri_producer_msg(producer);
  msg->counter = SEND_NUM_MSGS;
  msg->stop = true;
  ri_producer_force_push(producer);

  /* not needed anymore */
  ri_producer_release(producer);

  /* let the server print first */
  usleep(100000);

  print_client_stat(&stat);
  LOG_INF("%lu nanoseconds elapsed", elapsed);
  double msg_sec = (double)stat.sent / (double)elapsed * 1000000000.0;
  LOG_INF("send rate: %f msg/s", msg_sec);

  return 0;

fail_connect:
  return -1;
}


static int server_entry(int socket)
{
  ri_vector_t *vec = ri_server_socket_accept(socket, NULL, NULL);

  if (!vec) {
    goto fail_vec;
  }

  ri_consumer_t *consumer = ri_vector_acquire_consumer(vec, 0);

  if (!consumer) {
    goto fail_consumer;
  }


  ri_vector_delete(vec);

  int state = 1;
  const msg_t *msg = NULL;

  server_stat_t stat = {0};

  uint64_t counter = COUNTER_INIT;

  while (state > 0) {
    state = consume(consumer, &counter, &stat);
  }

  ri_consumer_release(consumer);

  print_server_stat(&stat);

  return 0;

fail_consumer:
  ri_vector_delete(vec);
fail_vec:
  return -1;
}

static pid_t fork_on_cpu(int cpu, entry_fn entry, int socket)
{
  pid_t pid = fork();

  if (pid < 0) {
    return -errno;
  }

  if (pid == 0) {
    set_affinity(cpu);

    int r = entry(socket);

    _exit(r);
  }

  return pid;
}



int main()
{
  int sockets[2];

  int r = socketpair(AF_UNIX, SOCK_SEQPACKET, 0, sockets);

  if (r < 0)
    goto fail_sockets;

  pid_t server = fork_on_cpu(CPU_SERVER, server_entry, sockets[0]);

  /* give the server some time */
  usleep(10000);

  pid_t client = fork_on_cpu(CPU_CLIENT, client_entry, sockets[1]);

  close(sockets[0]);
  close(sockets[1]);

  int client_status;
  int server_status;

  waitpid(client, &client_status, 0);
  waitpid(server, &server_status, 0);

  LOG_INF("server (%d) and client (%d) terminated", server_status, client_status);

  usleep(10000);

  return 0;
fail_sockets:
  return r;
}

