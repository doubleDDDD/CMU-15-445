/*
 * buffer_pool_manager.h
 *
 * Functionality: The simplified Buffer Manager interface allows a client to
 * new/delete pages on disk, to read a disk page into the buffer pool and pin
 * it, also to unpin a page in the buffer pool.
 */

#pragma once

#include <list>
#include <mutex>

#include "buffer/lru_replacer.h"
#include "disk/disk_manager.h"
#include "hash/extendible_hash.h"
#include "logging/log_manager.h"
#include "page/page.h"

namespace cmudb {

class BufferPoolManager {
public:
	BufferPoolManager(size_t pool_size, DiskManager *disk_manager,
					  LogManager *log_manager = nullptr);
	
	// 不是基类，析构函数无所谓是否是虚函数了
	~BufferPoolManager();

	// disable copy
	BufferPoolManager(BufferPoolManager const &) = delete;
	BufferPoolManager &operator=(BufferPoolManager const &) = delete;

	Page *FetchPage(page_id_t page_id);
	bool UnpinPage(page_id_t page_id, bool is_dirty);
	bool FlushPage(page_id_t page_id);
    void FlushAllDirtyPage();
	Page *NewPage(page_id_t &page_id);
	bool DeletePage(page_id_t page_id);
    HashTable<page_id_t, Page *>* GetPageTable() { return page_table_; }

	// for debug
	bool Check() const
	{
		//std::cerr << "table: " << page_table_->Size() << " replacer: "
		//          << replacer_->Size() << std::endl;
		// +1 for header_page, in the test environment,
		// header_page is out the replacer's control
		return page_table_->Size() == (replacer_->Size() + 1);
	}

private:
	size_t pool_size_;  // buffer pool 中 Page 的数量
	Page *pages_;  // 所 hold 的存储空间是一个 Page 数组，用户态的空间 vm_area
	std::list<Page *> *free_list_;
	HashTable<page_id_t, Page *> *page_table_;  // page id 到 Page 对象的 kv
	Replacer<Page *> *replacer_;
	DiskManager *disk_manager_;
	LogManager *log_manager_;
	std::mutex mutex_;  // 用来加锁的
};
} // namespace cmudb