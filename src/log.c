#include "rtipc/rtipc.h"
#include "rtipc/log.h"


#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


#define ARG_UNUSED(x) (void)(x)

static void log_std(int priority,
                    const char *file,
                    const char *line,
                    const char *func,
                    const char *format,
                    va_list ap);

static int m_log_min_level = RI_LOG_LEVEL_DBG;

static ri_log_fn ri_log_handler = log_std;

static void log_dummy(int priority,
                      const char *file,
                      const char *line,
                      const char *func,
                      const char *format,
                      va_list ap)
{
  ARG_UNUSED(priority);
  ARG_UNUSED(file);
  ARG_UNUSED(line);
  ARG_UNUSED(func);
  ARG_UNUSED(func);
  ARG_UNUSED(format);
  ARG_UNUSED(ap);
}

static void log_std(int priority,
                    const char *file,
                    const char *line,
                    const char *func,
                    const char *format,
                    va_list ap)
{
  if (priority > m_log_min_level)
    return;

  FILE *out = priority == RI_LOG_LEVEL_ERR ? stderr : stdout;

  fprintf(out, "[%d] %s:%s in %s: ", priority, file, line, func);
  vfprintf(out, format, ap);
}

void ri_log(
    int priority, const char *file, const char *line, const char *func, const char *format, ...)
{
  va_list ap;
  va_start(ap, format);
  ri_log_handler(priority, file, line, func, format, ap);
  va_end(ap);
}

void ri_set_log_handler(ri_log_fn log_handler)
{
  ri_log_handler = log_handler ? log_handler : log_dummy;
}
