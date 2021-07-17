/*
 * buffer_pool_manager.cpp
 *
 * Functionality: The simplified Buffer Manager interface allows a client to
 * new/delete pages on disk, to read a disk page into the buffer pool and pin
 * it, also to unpin a page in the buffer pool.
 */

#include "buffer/buffer_pool_manager.h"

namespace cmudb
{

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 * 内存中的 page buff 由一个 list 与一个可扩展的 hash 表来共同管理
 * 当然还有一个 LRU, LRU 有一个头就可以了
 */
BufferPoolManager::BufferPoolManager(
	size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
	: pool_size_(pool_size), disk_manager_(disk_manager),
	  log_manager_(log_manager)
{
	/**
	 * @brief BUFFER_POOL_SIZE is 10 = pool_size_
		a consecutive memory space for buffer pool
		下面的都是类内的对象
		所以内存池是在最开始就分配好的 目前是40KB的bool
	 */ 
	pages_ = new Page[pool_size_];  /* 分配连续的内存空间，每一个page有4K, pool_size 代表了buffer pool 的大小 */
	free_list_ = new std::list<Page *>;

	/**
	 * @brief page 替换算法 LRU
	 LRU 实际就是一个 list，有指针指向 page 对象，然后指针在一直变化在 list 中的位置
	 */
	replacer_ = new LRUReplacer<Page *>;

	/**
	 * @brief 可扩展的hash表, 理解为 page id 与 page 的 kv
	 page id 是可以变的，实际上表示的是文件中的偏移，page id 是多少，就是哪一部分 disk 的 cache
	 */
	page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);  // BUCKET_SIZE is 50

	/**
	 * @brief put all the pages into free list
	 page 由一个 list管理
	 */
	for (size_t i = 0; i < pool_size_; ++i) { free_list_->push_back(&pages_[i]); }
}

/*
 * BufferPoolManager Destructor
 * WARNING: Do Not Edit This Function*
 * free or delete 之后，指针变量本身是不会被释放的，一般需要自行置 null
 * 但是在类的析构函数中，不用 care 这个了
 */
BufferPoolManager::~BufferPoolManager()
{
	delete[] pages_;
	delete page_table_;
	delete replacer_;
	delete free_list_;
}

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an
 * entry for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
Page* BufferPoolManager::FetchPage(page_id_t page_id)
{
	/**
	 * @brief page_table_就是这个 hash 表
	 * fetch 是获取一个已经存在的 page，假设一个数据库文件占 3 个 page, 0-2 的 id 就是可以 fetch 的
	 * 本质是一个读操作	
	 */
	assert(page_id != INVALID_PAGE_ID);
	std::lock_guard<std::mutex> lock(mutex_);  // 上锁，离开 {} 后释放锁

	Page *res = nullptr;
	// search in hash table
	if (page_table_->Find(page_id, res)) {
		++res->pin_count_;  // mark the Page as pinned, 线程引用+1
		replacer_->Erase(res);  // remove its entry from LRUReplacer，用完再搞回去
		return res;
	}
	else {
		/* id没有对应的 page 被使用，需要分配新 page */
		if (!free_list_->empty()) {
			/* list容器中有元素，说明还有空闲的page */
			res = free_list_->front();  /* 第一个元素的ref */
			free_list_->pop_front();  /* 删除容器头部的第一个元素 */
		}
		else {
			/* 没有空页了，需要执行页面替换算法，把一部分数据丢到disk */
			if (!replacer_->Victim(res)) {
				return nullptr;  /* 无page可换 */
			}
		}
	}

	assert(res->pin_count_ == 0);  /* free list中的page一定是没有ref的 */

	/* 脏页写回 */
	if (res->is_dirty_) { disk_manager_->WritePage(res->page_id_, res->GetData()); }

	// 删一个 kv，再 insert 一个 kv
	// delete the entry for old page in hash table
	page_table_->Remove(res->page_id_);
	// insert an entry for the new page in hash table
	page_table_->Insert(page_id, res);

	// initial meta data
	res->page_id_ = page_id;
	res->is_dirty_ = false;
	res->pin_count_ = 1;
	disk_manager_->ReadPage(page_id, res->GetData());  /* disk 数据到内存 page */

	return res;
	/* 释放锁 */
}

/*
 * Implementation of unpin page 用完一个 page 之后要 unpin, 核心目的就是引用计数 -1
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
bool
BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty)
{
	/* 得到锁，才继续向下执行 */
	std::lock_guard<std::mutex> lock(mutex_);

	Page *res = nullptr;
	if (!page_table_->Find(page_id, res)) { return false; }
	else {
		if (res->pin_count_ > 0) {
			if (--res->pin_count_ == 0) { replacer_->Insert(res); }
			// else do nothing
		}
		else { return false; }

		// 用完之后标记一下我用完了，你已经不干净了
		if (is_dirty) { res->is_dirty_ = true; }
		return true;
	}
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool
BufferPoolManager::FlushPage(page_id_t page_id)
{
	/* just like fsync */
	std::lock_guard<std::mutex> lock(mutex_);

	if (page_id == INVALID_PAGE_ID)
		return false;

	Page *res = nullptr;
	if (page_table_->Find(page_id, res))
	{
		disk_manager_->WritePage(page_id, res->GetData());  // 说白了就是把 page 中的数据写到 disk 中，write 系统调用
		return true;
	}
	return false;
}

/**
 * User should call this method for deleting a page. 
 * This routine will call disk manager to deallocate the page. 
 * First, if page is found within page table, buffer pool manager should be responsible for removing this entry out
 * of page table, resetting page metadata and adding back to free list. 
 * 
 * Second, call disk manager's DeallocatePage() method to delete from disk file. 
 * 
 * If the page is found within page table, but pin_count != 0, return false
 */
bool
BufferPoolManager::DeletePage(page_id_t page_id)
{
	/* 实际即使删除磁盘上的部分文件 */
	std::lock_guard<std::mutex> lock(mutex_);

	Page *res = nullptr;
	if(page_table_->Find(page_id, res)) {
		page_table_->Remove(page_id);
		res->page_id_ = INVALID_PAGE_ID;
		res->is_dirty_ = false;

		replacer_->Erase(res);
		disk_manager_->DeallocatePage(page_id);

		free_list_->push_back(res);

		return true;
	}
	return false;
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */
Page* BufferPoolManager::NewPage(page_id_t &page_id)
{
	/* 说白了就是 append in disk file，本质是一个写操作 */
	std::lock_guard<std::mutex> lock(mutex_);

	Page *res = nullptr;
	if(!free_list_->empty()){
		res = free_list_->front();
		free_list_->pop_front();
	}
	else {
		if(!replacer_->Victim(res)) {
			return nullptr;
		}
	}

	page_id = disk_manager_->AllocatePage();
	if(res->is_dirty_){
		disk_manager_->WritePage(res->page_id_, res->GetData());
	}

	page_table_->Remove(res->page_id_);

	page_table_->Insert(page_id, res);

	res->page_id_ = page_id;
	res->is_dirty_ = false;
	res->pin_count_ = 1;
	res->ResetMemory();

	return res;
}

} // namespace cmudb
