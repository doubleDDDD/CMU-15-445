/**
 * page.h
 *
 * Wrapper around actual data page in main memory and also contains bookkeeping
 * information used by buffer pool manager like pin_count/dirty_flag/page_id.
 * Use page as a basic unit within the database system
 */

#pragma once

#include <cstring>
#include <iostream>

#include "common/config.h"
#include "common/rwmutex.h"

namespace cmudb {

class Page {
    friend class BufferPoolManager;

public:
    Page() { ResetMemory(); }
    ~Page() {};

    // disable copy
    Page(Page const &) = delete;
    Page &operator=(Page const &) = delete;

    // get actual data page content
    inline char *GetData() { return data_; }

    // get page id
    inline page_id_t GetPageId() { return page_id_; }

    // get page pin count
    inline int GetPinCount() { return pin_count_; }

    // method use to latch/unlatch page content
    inline void WUnlatch() { rwlatch_.WUnlock(); }
    inline void WLatch() { rwlatch_.WLock(); }
    inline void RUnlatch() { rwlatch_.RUnlock(); }
    inline void RLatch() { rwlatch_.RLock(); }

    inline lsn_t GetLSN() { return *reinterpret_cast<lsn_t *>(GetData() + 4); }
    inline void SetLSN(lsn_t lsn) { memcpy(GetData() + 4, &lsn, 4); }
    inline void SetDirty(){ is_dirty_=true; }
    inline bool GetDirtyFlag(){ return is_dirty_; }

private:
    // method used by buffer pool manager
    inline void ResetMemory() { memset(data_, 0, PAGE_SIZE); }  // 清0

    // members, page的元数据
    char data_[PAGE_SIZE]; // actual data，代表内存中的一个页
    page_id_t page_id_ = INVALID_PAGE_ID;  // id
    int pin_count_ = 0;
    bool is_dirty_ = false;
    RWMutex rwlatch_;  // 读写锁
};

} // namespace cmudb