/**
 * table_heap.cpp
 */

#include <cassert>

#include "common/logger.h"
#include "table/table_heap.h"

namespace cmudb {

// open table
TableHeap::TableHeap(BufferPoolManager *buffer_pool_manager,
                     LockManager *lock_manager, LogManager *log_manager,
                     page_id_t first_page_id)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
      log_manager_(log_manager), first_page_id_(first_page_id) {}

// create table
TableHeap::TableHeap(BufferPoolManager *buffer_pool_manager,
                     LockManager *lock_manager, LogManager *log_manager,
                     Transaction *txn)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
      log_manager_(log_manager) {
  // 在最简单的测试用例中，新分配的pageid=1
  auto first_page = static_cast<TablePage *>(buffer_pool_manager_->NewPage(first_page_id_));
  assert(first_page != nullptr); // todo: abort table creation?
  first_page->WLatch();
  //LOG_DEBUG("new table page created %d", first_page_id_);

  first_page->Init(first_page_id_, PAGE_SIZE, INVALID_PAGE_ID, log_manager_, txn);
  first_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(first_page_id_, true);
}


/**
 * @brief TableHeap对应一个table
 * 记录了一个table中的第一个page的id
 * 事务本身的失败是事务回滚的一个重要原因，事务出问题返回false。不出问题正常返回 true
 */
bool TableHeap::InsertTuple(const Tuple &tuple, RID &rid, Transaction *txn) 
{
    if (tuple.size_ + 32 > PAGE_SIZE) {
        // larger than one page size
        txn->SetState(TransactionState::ABORTED);
        return false;
    }

    /* 获取table对应的第一个page */
    auto cur_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
    if (cur_page == nullptr) {
        txn->SetState(TransactionState::ABORTED);
        return false;
    }

    /**
     * @brief insert一个tuple的时候。是从table的第一个page开始遍历的，性能很低
     * 但是操作每一个page确实是需要锁的 
     */
    cur_page->WLatch();
    while (!cur_page->InsertTuple(tuple, rid, txn, lock_manager_, log_manager_)) {
        /* fail to insert due to not enough space */
        auto next_page_id = cur_page->GetNextPageId();
        if (next_page_id != INVALID_PAGE_ID) { // valid next page
            cur_page->WUnlatch();
            buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
            cur_page = static_cast<TablePage *>(
                buffer_pool_manager_->FetchPage(next_page_id));
            cur_page->WLatch();
        } else { // create new page
            auto new_page =
                static_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
            if (new_page == nullptr) {
                cur_page->WUnlatch();
                buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
                txn->SetState(TransactionState::ABORTED);
                return false;
            }
            new_page->WLatch();
            std::cout << "new table page " << next_page_id << " created" << std::endl;
            /* 利用list将page都连接起来 */
            cur_page->SetNextPageId(next_page_id);
            new_page->Init(next_page_id, PAGE_SIZE, cur_page->GetPageId(), log_manager_, txn);
            cur_page->WUnlatch();
            buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), true);
            cur_page = new_page;
        }
    }

    cur_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), true);
    txn->GetWriteSet()->emplace_back(rid, WType::INSERT, Tuple{}, this);
    return true;
}

bool TableHeap::MarkDelete(const RID &rid, Transaction *txn) {
  // todo: remove empty page
  auto page = reinterpret_cast<TablePage *>(
      buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  txn->GetWriteSet()->emplace_back(rid, WType::DELETE, Tuple{}, this);
  return true;
}

/**
 * @brief 该线程持有page锁，即所update的tuple所在的page
 * 
 * 在这里分析一下 page的读写锁 与事务的锁管理器锁管理的锁（读写锁以及update锁）的交互的关系
 *  事务的锁管理器所管理的锁必须在事务的shrinking阶段释放，但是page锁可以在事务的任何阶段释放并重新get
 *  还是去md里说吧
 * @param  tuple            desc 新构建的tuple 用于替换旧值的
 * @param  rid              desc 新构建的与tuple对应的rid
 * @param  txn              desc
 * @return true @c 
 * @return false @c 
 */
bool TableHeap::UpdateTuple(const Tuple &tuple, const RID &rid,
                            Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(
      buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  Tuple old_tuple;
  // 事务如果想要操作某tuple，首先是要得到page锁的
  // 这个page锁是必须加的
  page->WLatch();
  bool is_updated = page->UpdateTuple(tuple, old_tuple, rid, txn, lock_manager_,
                                      log_manager_);
  page->WUnlatch();
  // 到此为止，事务所持有的 rid tuple 的写锁还未释放
  // 这两个锁的get与release还是比较有趣哦，可以分析一下
  buffer_pool_manager_->UnpinPage(page->GetPageId(), is_updated);  // 减少一个ref
  if (is_updated && txn->GetState() != TransactionState::ABORTED){
    txn->GetWriteSet()->emplace_back(rid, WType::UPDATE, old_tuple, this);
  }
  return is_updated;
}

void TableHeap::ApplyDelete(const RID &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(
      buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  lock_manager_->Unlock(txn, rid);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

void TableHeap::RollbackDelete(const RID &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(
      buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

// called by tuple iterator
bool TableHeap::GetTuple(const RID &rid, Tuple &tuple, Transaction *txn) {
  auto page = static_cast<TablePage *>(
      buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  page->RLatch();
  bool res = page->GetTuple(rid, tuple, txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
  if(!res){
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  return res;
}

bool TableHeap::DeleteTableHeap() {
  // todo: real delete
  return true;
}

TableIterator TableHeap::begin(Transaction *txn) {
  auto page =
      static_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  page->RLatch();
  RID rid;
  // if failed (no tuple), rid will be the result of default
  // constructor, which means eof
  page->GetFirstTupleRid(rid);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(first_page_id_, false);
  return TableIterator(this, rid, txn);
}

TableIterator TableHeap::end() {
  return TableIterator(this, RID(INVALID_PAGE_ID, -1), nullptr);
}

} // namespace cmudb
