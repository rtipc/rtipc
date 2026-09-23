#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#define RI_UNUSED(x) ((void)(x))

#define RI_LOG_LEVEL_NONE 0
#define RI_LOG_LEVEL_ERR 1
#define RI_LOG_LEVEL_WRN 2
#define RI_LOG_LEVEL_INF 3
#define RI_LOG_LEVEL_DBG 4

#if RTIPC_LOG_DISABLE == 1
#define RI_LOG_MAX_LEVEL RI_LOG_LEVEL_NONE
#else
#ifndef RI_LOG_MAX_LEVEL
#define RI_LOG_MAX_LEVEL RI_LOG_LEVEL_INF
#endif
#endif

#define RI_NO_LOG(format, ...) ri_no_log(format, ##__VA_ARGS__)

#define RI_LOG_STRINGIFY(val) RI_LOG_STRINGIFY_ARG(val)
#define RI_LOG_STRINGIFY_ARG(contents) #contents

#define RI_LOG(priority, format, ...)                                          \
  ri_log(priority, __FILE_NAME__, RI_LOG_STRINGIFY(__LINE__), __func__,        \
         format "\n", ##__VA_ARGS__)

#if RI_LOG_MAX_LEVEL == RI_LOG_LEVEL_NONE

#define LOG_ERR(format, ...) RI_NO_LOG(format, ##__VA_ARGS__)
#define LOG_WRN(format, ...) RI_NO_LOG(format, ##__VA_ARGS__)
#define LOG_INF(format, ...) RI_NO_LOG(format, ##__VA_ARGS__)
#define LOG_DBG(format, ...) RI_NO_LOG(format, ##__VA_ARGS__)

#else /* RI_LOG_MAX_LEVEL == RI_LOG_LEVEL_NONE */

#if RI_LOG_MAX_LEVEL == RI_LOG_LEVEL_ERR
#define LOG_ERR(format, ...) RI_LOG(RI_LOG_LEVEL_ERR, format, ##__VA_ARGS__)
#define LOG_WRN(format, ...) RI_NO_LOG(format, ##__VA_ARGS__)
#define LOG_INF(format, ...) RI_NO_LOG(format, ##__VA_ARGS__)
#define LOG_DBG(format, ...) RI_NO_LOG(format, ##__VA_ARGS__)
#elif RI_LOG_MAX_LEVEL == RI_LOG_LEVEL_WRN
#define LOG_ERR(format, ...) RI_LOG(RI_LOG_LEVEL_ERR, format, ##__VA_ARGS__)
#define LOG_WRN(format, ...) RI_LOG(RI_LOG_LEVEL_WRN, format, ##__VA_ARGS__)
#define LOG_INF(format, ...) RI_NO_LOG(format, ##__VA_ARGS__)
#define LOG_DBG(format, ...) RI_NO_LOG(format, ##__VA_ARGS__)
#elif RI_LOG_MAX_LEVEL == RI_LOG_LEVEL_INF
#define LOG_ERR(format, ...) RI_LOG(RI_LOG_LEVEL_ERR, format, ##__VA_ARGS__)
#define LOG_WRN(format, ...) RI_LOG(RI_LOG_LEVEL_WRN, format, ##__VA_ARGS__)
#define LOG_INF(format, ...) RI_LOG(RI_LOG_LEVEL_INF, format, ##__VA_ARGS__)
#define LOG_DBG(format, ...) RI_NO_LOG(format, ##__VA_ARGS__)
#elif RI_LOG_MAX_LEVEL == RI_LOG_LEVEL_DBG
#define LOG_ERR(format, ...) RI_LOG(RI_LOG_LEVEL_ERR, format, ##__VA_ARGS__)
#define LOG_WRN(format, ...) RI_LOG(RI_LOG_LEVEL_WRN, format, ##__VA_ARGS__)
#define LOG_INF(format, ...) RI_LOG(RI_LOG_LEVEL_INF, format, ##__VA_ARGS__)
#define LOG_DBG(format, ...) RI_LOG(RI_LOG_LEVEL_DBG, format, ##__VA_ARGS__)
#endif

#endif /* RI_LOG_MAX_LEVEL == RI_LOG_LEVEL_NONE */

void ri_log(int priority, const char *file, const char *line, const char *func,
            const char *format, ...) __attribute__((format(printf, 5, 6)));

static inline void ri_no_log(const char *format, ...)
    __attribute__((format(printf, 1, 2)));

static inline void ri_no_log(const char *format, ...) { RI_UNUSED(format); }

#ifdef __cplusplus
}
#endif