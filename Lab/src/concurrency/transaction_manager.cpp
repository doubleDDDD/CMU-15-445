/**
 * transaction_manager.cpp
 *
 */

#include "concurrency/transaction_manager.h"
#include "table/table_heap.h"

#include <cassert>

namespace cmudb {

/**
 * @brief begin 一个事务
 * @return Transaction* @c 
 */
Transaction *TransactionManager::Begin() {
    Transaction *txn = new Transaction(next_txn_id_++);

    // 可以看到事务的开始会打一个 op log，即 begin log
    if (ENABLE_LOGGING) {
        // TODO: write log and update transaction's prev_lsn here
        LogRecord log(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::BEGIN);
        txn->SetPrevLSN(log_manager_->AppendLogRecord(log));
    }

    return txn;
}

void TransactionManager::Commit(Transaction *txn) {
    txn->SetState(TransactionState::COMMITTED);
    // truly delete before commit,如果没有到提交这些是需要回滚的
    auto write_set = txn->GetWriteSet();
    while (!write_set->empty()) {
        auto &item = write_set->back();
        auto table = item.table_;
        if (item.wtype_ == WType::DELETE) {
            // this also release the lock when holding the page latch
            std::printf("rid=%s, txn=%d\n", item.rid_.ToString().c_str(), static_cast<int>(txn->GetTransactionId()));
            table->ApplyDelete(item.rid_, txn);
        }
        write_set->pop_back();
    }
    write_set->clear();

    if (ENABLE_LOGGING) {
        // TODO: write log and update transaction's prev_lsn here
        LogRecord log(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::COMMIT);
        txn->SetPrevLSN(log_manager_->AppendLogRecord(log));

        // A txnis not considered committed until allits log records have been written to stable storage
        // Make sure that all log records are flushed before it returns an acknowledgement to application
        // 事务的提交确实意味着持久化的完成，这里会卡着不返回
        while(txn->GetPrevLSN() > log_manager_->GetPersistentLSN()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    // release all the lock
    std::unordered_set<RID> lock_set;
    for (auto item : *txn->GetSharedLockSet()) { lock_set.emplace(item); }
    for (auto item : *txn->GetExclusiveLockSet()) { lock_set.emplace(item); }
    // lock_set表示的是该事务所有持有锁的rid
    // release all the lock
    for (auto locked_rid : lock_set) {
        lock_manager_->Unlock(txn, locked_rid);
    }
}

void TransactionManager::Abort(Transaction *txn) {
  txn->SetState(TransactionState::ABORTED);
  // rollback before releasing lock
  // 除去log中的数据，原地更新的数据全部原地回滚
  auto write_set = txn->GetWriteSet();
  while (!write_set->empty()) {
    auto &item = write_set->back();
    auto table = item.table_;
    if (item.wtype_ == WType::DELETE) {
      LOG_DEBUG("rollback delete");
      table->RollbackDelete(item.rid_, txn);
    } else if (item.wtype_ == WType::INSERT) {
      LOG_DEBUG("rollback insert");
      table->ApplyDelete(item.rid_, txn);
    } else if (item.wtype_ == WType::UPDATE) {
      LOG_DEBUG("rollback update");
      table->UpdateTuple(item.tuple_, item.rid_, txn);
    }
    write_set->pop_back();
  }
  write_set->clear();

  if (ENABLE_LOGGING) {
    // TODO: write log and update transaction's prev_lsn here
    LogRecord log(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::ABORT);
    txn->SetPrevLSN(log_manager_->AppendLogRecord(log));
    // abort的返回也意味着持久化的完成
    while(txn->GetPrevLSN() > log_manager_->GetPersistentLSN())
    {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  // release all the lock
  std::unordered_set<RID> lock_set;
  for (auto item : *txn->GetSharedLockSet())
    lock_set.emplace(item);
  for (auto item : *txn->GetExclusiveLockSet())
    lock_set.emplace(item);
  // release all the lock
  for (auto locked_rid : lock_set) {
    lock_manager_->Unlock(txn, locked_rid);
  }
}
} // namespace cmudb
