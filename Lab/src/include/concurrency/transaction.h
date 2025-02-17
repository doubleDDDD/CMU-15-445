/**
 * transaction.h
 */

#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <thread>
#include <unordered_set>

#include "common/config.h"
#include "common/logger.h"
#include "page/page.h"
#include "table/tuple.h"

namespace cmudb {

/**
 * Transaction states:
 *
 *     _________________________
 *    |                         v
 * GROWING -> SHRINKING -> COMMITTED   ABORTED
 *    |__________|________________________^
 *
 **/
enum class TransactionState { GROWING, SHRINKING, COMMITTED, ABORTED };

enum class WType { INSERT = 0, DELETE, UPDATE };

class TableHeap;

// write set record
class WriteRecord {
public:
    WriteRecord(RID rid, WType wtype, const Tuple &tuple, TableHeap *table)
        : rid_(rid), wtype_(wtype), tuple_(tuple), table_(table) {}

    RID rid_;
    WType wtype_;
    // tuple is only for update operation
    Tuple tuple_;
    // which table
    TableHeap *table_;  // table_heap 就是 table 的指针
};

class Transaction {
public:
  Transaction(Transaction const &) = delete;
  Transaction(txn_id_t txn_id)
      : state_(TransactionState::GROWING),
        thread_id_(std::this_thread::get_id()),
        txn_id_(txn_id), prev_lsn_(INVALID_LSN), shared_lock_set_{new std::unordered_set<RID>},
        exclusive_lock_set_{new std::unordered_set<RID>} 
    {
        // initialize sets
        write_set_.reset(new std::deque<WriteRecord>);
        page_set_.reset(new std::deque<Page *>);
        deleted_page_set_.reset(new std::unordered_set<page_id_t>);
    }

    ~Transaction() {}

    //===--------------------------------------------------------------------===//
    // Mutators and Accessors
    //===--------------------------------------------------------------------===//
    inline std::thread::id GetThreadId() const { return thread_id_; }
    inline txn_id_t GetTransactionId() const { return txn_id_; }
    inline std::shared_ptr<std::deque<WriteRecord>> GetWriteSet() { return write_set_; }
    inline std::shared_ptr<std::deque<Page *>> GetPageSet() { return page_set_; }
    inline void AddIntoPageSet(Page *page) { page_set_->push_back(page); }
    inline std::shared_ptr<std::unordered_set<page_id_t>> GetDeletedPageSet() { return deleted_page_set_; }
    inline void AddIntoDeletedPageSet(page_id_t page_id) { deleted_page_set_->insert(page_id); }
    inline std::shared_ptr<std::unordered_set<RID>> GetSharedLockSet() { return shared_lock_set_; }
    inline std::shared_ptr<std::unordered_set<RID>> GetExclusiveLockSet() { return exclusive_lock_set_; }
    inline TransactionState GetState() { return state_; }
    inline void SetState(TransactionState state) { state_ = state; }
    inline lsn_t GetPrevLSN() { return prev_lsn_; }
    inline void SetPrevLSN(lsn_t prev_lsn) { prev_lsn_ = prev_lsn; }

private:
    TransactionState state_;

    // thread id, single-threaded transactions 一个事务代表的是一个线程的操作
    std::thread::id thread_id_;
    // transaction id 系统维护的自增id，事务manager管理，新来一个事务就开始一个新的id
    txn_id_t txn_id_;

    // Below are used by transaction, undo set, undo 用于回滚操作
    // 虽然没有提交的事务也可以被部分持久化，但是前提是日志需要被持久化
    // undo set old_tuple，如果在一次事务中，有两次update的操作，第一次事务update成功，修改将被写到log并且被原地更新，old value将被保存到 write_set_
    // 如果第二次update的过程中失败了，将导致事务的abort，那么第一个就需要被回滚，回滚依赖的就是write_set_中的entry
    std::shared_ptr<std::deque<WriteRecord>> write_set_;
    // prev lsn
    lsn_t prev_lsn_;

    // Below are used by concurrent index
    // this deque contains page pointer that was latched during index operation
    std::shared_ptr<std::deque<Page *>> page_set_;

    // this set contains page_id that was deleted during index operation
    std::shared_ptr<std::unordered_set<page_id_t>> deleted_page_set_;

    // Below are used by lock manager
    // this set contains rid of shared-locked tuples by this transaction
    // 这个set包含一系列的rids，当前事务拥有这些rids的共享锁
    std::shared_ptr<std::unordered_set<RID>> shared_lock_set_;
    // this set contains rid of exclusive-locked tuples by this transaction
    // 这个set是一系列的rids，当前事务拥有这些rids的排它锁
    std::shared_ptr<std::unordered_set<RID>> exclusive_lock_set_;
};
} // namespace cmudb
