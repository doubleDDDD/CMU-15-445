/**
 * disk_manager.cpp
 *
 * Disk manager takes care of the allocation and deallocation of pages within a
 * database. It also performs read and write of pages to and from disk, and
 * provides a logical file layer within the context of a database management
 * system.
 */

#include <assert.h>
#include <cstring>
#include <iostream>
#include <sys/stat.h>
#include <thread>

#include "common/logger.h"
#include "disk/disk_manager.h"

namespace cmudb {

static char *buffer_used = nullptr;

/**
 * Constructor: open/create a single database file & log file
 * 所以一个数据库的连接就是打开一个数据库文件，断开连接就是close文件
 * @input db_file: database file name
 */
DiskManager::DiskManager(const std::string &db_file)
    : file_name_(db_file), next_page_id_(0), num_flushes_(0), flush_log_(false), flush_log_f_(nullptr) {

    std::string::size_type n = file_name_.find(".");
    if (n == std::string::npos) {
        LOG_DEBUG("wrong file format");
        return;
    }

    log_name_ = file_name_.substr(0, n) + ".log";

    log_io_.open(log_name_, std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
    // directory or file does not exist
    if (!log_io_.is_open()) {
        log_io_.clear();
        // create a new file
        log_io_.open(log_name_, std::ios::binary | std::ios::trunc | std::ios::app | std::ios::out);
        log_io_.close();
        // reopen with original mode
        log_io_.open(log_name_, std::ios::binary | std::ios::in | std::ios::app | std::ios::out);
    }

    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    // directory or file does not exist
    if (!db_io_.is_open()) {
        db_io_.clear();
        // create a new file
        db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
        db_io_.close();
        // reopen with original mode
        db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    }

    // 根据文件大小，为next_page_id_赋值
    next_page_id_ = GetFileSize(file_name_) / PAGE_SIZE;
}

DiskManager::~DiskManager() {
    db_io_.close();
    log_io_.close();
}

/**
 * Write the contents of the specified page into disk file
 */
void DiskManager::WritePage(page_id_t page_id, const char *page_data) {
    size_t offset = page_id *   PAGE_SIZE;
    // set write cursor to offset
    db_io_.seekp(offset);
    db_io_.write(page_data, PAGE_SIZE);
    // check for I/O error
    if (db_io_.bad()) {
        LOG_DEBUG("I/O error while writing");
        return;
    }
    /**
    * @brief 
    *   needs to flush to keep disk file in sync
    *   写db是需要自己主动刷磁盘的
    *   flsush to disk: flush 主要作用是将 lib 库函数的缓存刷到内核
    *   这个地方绝对没有直接采用 fsync，否则要日志何用，性能将没法看
    */
    db_io_.flush();
}

/**
 * Read the contents of the specified page into the given memory area
 * 读磁盘的一个page到指定的内存区域
 */
void DiskManager::ReadPage(page_id_t page_id, char *page_data) {
    int offset = page_id * PAGE_SIZE;
    // check if read beyond file length
    if (offset > GetFileSize(file_name_)) {
        BackTracePlus();
        std::printf("pageid:%d, offset: %d, file size: %d\n\n", page_id, offset, GetFileSize(file_name_));
        LOG_DEBUG("I/O error while reading");
        // std::cerr << "I/O error while reading" << std::endl;
    } else {
        // set read cursor to offset
        // std::fstream db_io_
        db_io_.seekp(offset);
        db_io_.read(page_data, PAGE_SIZE);
        // if file ends before reading PAGE_SIZE
        int read_count = db_io_.gcount();  /* Get character count */
        if (read_count < PAGE_SIZE) {
            LOG_DEBUG("Read less than a page");
            // std::cerr << "Read less than a page" << std::endl;
            memset(page_data + read_count, 0, PAGE_SIZE - read_count);  /* system app buffer */
        }
    }
}

/**
 * Write the contents of the log into disk file
 * Only return when sync is done, and only perform sequence write
 */
void DiskManager::WriteLog(char *log_data, int size) {
    // enforce swap log buffer
    assert(log_data != buffer_used);
    buffer_used = log_data;

    if (size == 0) // no effect on num_flushes_ if log buffer is empty
        return;

    flush_log_ = true;

    if (flush_log_f_ != nullptr)
        // used for checking non-blocking flushing
        assert(flush_log_f_->wait_for(std::chrono::seconds(10)) == std::future_status::ready);

    num_flushes_ += 1;
    // sequence write
    log_io_.write(log_data, size);

    // check for I/O error
    if (log_io_.bad()) {
        LOG_DEBUG("I/O error while writing log");
        return;
    }

    // needs to flush to keep disk file in sync
    log_io_.flush();
    flush_log_ = false;
}

/**
 * Read the contents of the log into the given memory area
 * Always read from the beginning and perform sequence read
 * @return: false means already reach the end
 */
bool DiskManager::ReadLog(char *log_data, int size, int offset) {
    if (offset >= GetFileSize(log_name_)) {
        LOG_DEBUG("end of log file");
        LOG_DEBUG("file size is %d", GetFileSize(log_name_));
        return false;
    }
    log_io_.seekp(offset);
    log_io_.read(log_data, size);
    // if log file ends before reading "size"
    int read_count = log_io_.gcount();
    if (read_count < size) {
        log_io_.clear();
        memset(log_data + read_count, 0, size - read_count);
    }

    return true;
}

/**
 * Allocate new page (operations like create index/table)
 * For now just keep an increasing counter
 * 本质是向磁盘写信内容
 */
page_id_t DiskManager::AllocatePage() { return next_page_id_++; }

/**
 * Deallocate page (operations like drop index/table)
 * Need bitmap in header page for tracking pages
 */
void DiskManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
    return;
}

/**
 * Returns number of flushes made so far
 */
int DiskManager::GetNumFlushes() const { return num_flushes_; }

/**
 * Returns true if the log is currently being flushed
 */
bool DiskManager::GetFlushState() const { return flush_log_; }

/**
 * Private helper function to get disk file size
 */
int DiskManager::GetFileSize(const std::string &file_name) {
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

} // namespace cmudb
