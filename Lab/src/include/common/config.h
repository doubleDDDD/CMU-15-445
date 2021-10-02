/**
 * config.h
 *
 * Database system configuration
 */

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

#include <sys/syscall.h>
#include <unistd.h>
#define gettid() syscall(__NR_gettid)

namespace cmudb {

// std::chrono 是 c++ 的日期与时间库 类似于 std::time
// duration 表示一段是时间，即数据库的登录超时时间，被设为了1s
extern std::chrono::duration<long long int> LOG_TIMEOUT;

extern std::atomic<bool> ENABLE_LOGGING;  // 保证线程安全的

#define INVALID_PAGE_ID  (-1) // representing an invalid page id
#define INVALID_TXN_ID   (-1) // representing an invalid txn (transaction) id
#define INVALID_LSN      (-1) // representing an invalid lsn (log sequence numbers)
#define HEADER_PAGE_ID   0    // the header page id, per file one
// 数据库引擎指定，ext4文件系统的 system IO 是 4096，能够保证一个 page 在磁盘上是连续的
#define PAGE_SIZE        4096 // size of a data page in byte

#define LOG_BUFFER_SIZE  ((BUFFER_POOL_SIZE + 1) * PAGE_SIZE) // size of a log buffer in byte
#define BUCKET_SIZE      50   // size of extendible hash bucket
#define BUFFER_POOL_SIZE 10   // size of buffer pool

typedef int32_t page_id_t;    // page id type
typedef int32_t txn_id_t;     // transaction id type
typedef int32_t lsn_t;        // log sequence number type

} // namespace cmudb