#pragma once

#include <ctime>
#include <string>
#include <thread>

#define LOG_LOG_TIME_FORMAT "%Y-%m-%d %H:%M:%S"
#define LOG_OUTPUT_STREAM THREAD_LOCAL_FILE()

// Log levels
#define LOG_LEVEL_OFF 0
#define LOG_LEVEL_ERROR 1
#define LOG_LEVEL_WARN 2
#define LOG_LEVEL_INFO 3
#define LOG_LEVEL_DEBUG 4
#define LOG_LEVEL_TRACE 5

inline FILE* THREAD_LOCAL_FILE() {
    thread_local uint64_t id = std::hash<std::thread::id>{}(std::this_thread::get_id());
    thread_local std::string file_name = "log" + std::to_string(id) + ".txt";
    thread_local FILE* pFile = fopen(file_name.c_str(), "w");
    return pFile;
}

// https://blog.galowicz.de/2016/02/20/short_file_macro/
using cstr = const char*;
inline constexpr cstr PastLastSlash(cstr a, cstr b) {
    return *a == '\0' ? b : *b == '/' ? PastLastSlash(a + 1, a + 1) : PastLastSlash(a + 1, b);
}
inline constexpr cstr PastLastSlash(cstr a) {
    return PastLastSlash(a, a);
}
#define __SHORT_FILE__                                \
    ({                                                \
        constexpr cstr sf__{PastLastSlash(__FILE__)}; \
        sf__;                                         \
    })

inline void OutputLogHeader(const char* file_name, int line, const char* func, int level);

#if LOG_LEVEL >= LOG_LEVEL_ERROR
#    define LOG_ERROR(...)                                                        \
        OutputLogHeader(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_ERROR); \
        ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                                \
        ::fprintf(LOG_OUTPUT_STREAM, "\n");                                       \
        ::fflush(LOG_OUTPUT_STREAM)
#else

#    define LOG_ERROR(...) ((void)0)
#endif

#if LOG_LEVEL >= LOG_LEVEL_WARN
#    define LOG_WARN(...)                                                        \
        OutputLogHeader(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_WARN); \
        ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                               \
        ::fprintf(LOG_OUTPUT_STREAM, "\n");                                      \
        ::fflush(LOG_OUTPUT_STREAM)
#else
#    define LOG_WARN(...) ((void)0)
#endif

#if LOG_LEVEL >= LOG_LEVEL_INFO
#    define LOG_INFO(...)                                                        \
        OutputLogHeader(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_INFO); \
        ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                               \
        ::fprintf(LOG_OUTPUT_STREAM, "\n");                                      \
        ::fflush(LOG_OUTPUT_STREAM)
#else
#    define LOG_INFO(...) ((void)0)
#endif

#if LOG_LEVEL >= LOG_LEVEL_DEBUG
#    define LOG_DEBUG(...)                                                        \
        OutputLogHeader(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_DEBUG); \
        ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                                \
        ::fprintf(LOG_OUTPUT_STREAM, "\n");                                       \
        ::fflush(LOG_OUTPUT_STREAM)
#else
#    define LOG_DEBUG(...) ((void)0)
#endif

#if LOG_LEVEL >= LOG_LEVEL_TRACE
#    define LOG_TRACE(...)                                                        \
        OutputLogHeader(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_TRACE); \
        ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                                \
        ::fprintf(LOG_OUTPUT_STREAM, "\n");                                       \
        ::fflush(LOG_OUTPUT_STREAM)
#else
#    define LOG_TRACE(...) ((void)0)
#endif

inline void OutputLogHeader(const char* file_name, int line, const char* func, int level) {
    time_t t = ::time(nullptr);
    tm* curTime = localtime(&t);
    char time_str[32];
    ::strftime(time_str, 32, LOG_LOG_TIME_FORMAT, curTime);
    const char* type;
    switch (level) {
    case LOG_LEVEL_ERROR: type = "ERROR"; break;
    case LOG_LEVEL_WARN: type = "WARN"; break;
    case LOG_LEVEL_INFO: type = "INFO"; break;
    case LOG_LEVEL_DEBUG: type = "DEBUG"; break;
    case LOG_LEVEL_TRACE: type = "TRACE"; break;
    default: type = "UNKWN";
    }
    ::fprintf(LOG_OUTPUT_STREAM, "%s [/%s:%d:%s] %s - ", time_str, file_name, line, func, type);
}
