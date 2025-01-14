/**
 * header_page.cpp
 */

#include <cassert>

#include "page/table_page.h"

namespace cmudb {
/**
 * Header related
 * page id (4字节) | prev page id (4字节) | next page id (4字节)
 */
void
TablePage::Init(
    page_id_t page_id, size_t page_size, page_id_t prev_page_id, 
    LogManager *log_manager, Transaction *txn) 
{
    /**
     * @brief page的前4个字节用于描述page id 
     * page_id_t 是4字节的
     * 4 字节的 unsigned int 4G 左右的大小，一个page 4KB算的话，一个文件最大16GB左右
     *  1 << 32 右边32个0
     * 所有的page是用list来组织的
     * 第二个四字节是page的LSN
     *      The LSNof the most recent update to that page
     */
    memcpy(GetData(), &page_id, 4); // set page_id
    if (ENABLE_LOGGING) {
        // TODO: add your logging logic here
        // 创建一条日志并添加
        LogRecord log(txn->GetTransactionId(), txn->GetPrevLSN(),
                        LogRecordType::NEWPAGE, prev_page_id);
        lsn_t lsn = log_manager->AppendLogRecord(log);
        txn->SetPrevLSN(lsn);
        SetLSN(lsn);
    }
    SetPrevPageId(prev_page_id);
    SetNextPageId(INVALID_PAGE_ID);
    SetFreeSpacePointer(page_size);
    SetTupleCount(0);
}

page_id_t 
TablePage::GetPageId() {
    return *reinterpret_cast<page_id_t *>(GetData());
}

page_id_t 
TablePage::GetPrevPageId() {
    return *reinterpret_cast<page_id_t *>(GetData() + 8);
}

page_id_t 
TablePage::GetNextPageId() {
    return *reinterpret_cast<page_id_t *>(GetData() + 12);
}

void TablePage::SetPrevPageId(page_id_t prev_page_id) {
    memcpy(GetData() + 8, &prev_page_id, 4);
}

void 
TablePage::SetNextPageId(page_id_t next_page_id) {
    memcpy(GetData() + 12, &next_page_id, 4);
}

/**
 * Tuple related
 */
bool TablePage::InsertTuple(
    const Tuple &tuple, RID &rid, Transaction *txn,
    LockManager *lock_manager,
    LogManager *log_manager) 
{
    assert(tuple.size_ > 0);
    if (GetFreeSpaceSize() < tuple.size_) {
        return false; // not enough space
    }

    // try to reuse a free slot first
    int i;
    for (i = 0; i < GetTupleCount(); ++i) {
        rid.Set(GetPageId(), i);
        if (GetTupleSize(i) == 0) { // empty slot
            if (ENABLE_LOGGING) {
                assert(txn->GetSharedLockSet()->find(rid) ==
                    txn->GetSharedLockSet()->end() &&
                    txn->GetExclusiveLockSet()->find(rid) ==
                        txn->GetExclusiveLockSet()->end());
            }
            break;
        }
    }

    // no free slot left
    if (i == GetTupleCount() && GetFreeSpaceSize() < tuple.size_ + 8) {
        return false; // not enough space
    }

    SetFreeSpacePointer(GetFreeSpacePointer() - tuple.size_); // update free space pointer first
    memcpy(GetData() + GetFreeSpacePointer(), tuple.data_, tuple.size_);
    SetTupleOffset(i, GetFreeSpacePointer());
    SetTupleSize(i, tuple.size_);
    if (i == GetTupleCount()) {
        rid.Set(GetPageId(), i);
        SetTupleCount(GetTupleCount() + 1);
    }

    // write the log after set rid，这个是妥妥的 op log
    if (ENABLE_LOGGING) {
        // acquire the exclusive lock
        assert(lock_manager->LockExclusive(txn, rid.Get()));
        // TODO: add your logging logic here
        // 创建一条插入日志并添加
        LogRecord log(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::INSERT, rid, tuple);
        lsn_t lsn = log_manager->AppendLogRecord(log);
        txn->SetPrevLSN(lsn);
        SetLSN(lsn);
    }
    //LOG_DEBUG("Tuple inserted");
    return true;
}

/*
 * MarkDelete method does not truly delete a tuple from table page
 * Instead it set the tuple as 'deleted' by changing the tuple size metadata to
 * negative so that no other transaction can reuse this slot
 *
 */
bool TablePage::MarkDelete(const RID &rid, Transaction *txn,
                           LockManager *lock_manager, LogManager *log_manager) {
  int slot_num = rid.GetSlotNum();
  if (slot_num >= GetTupleCount()) {
    if (ENABLE_LOGGING) {
      txn->SetState(TransactionState::ABORTED);
    }
    return false;
  }

  int32_t tuple_size = GetTupleSize(slot_num);
  if (tuple_size < 0) {
    if (ENABLE_LOGGING) {
      txn->SetState(TransactionState::ABORTED);
    }
    return false;
  }

  if (ENABLE_LOGGING) {
    // acquire exclusive lock
    // if has shared lock
    if (txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end()) {
      if (!lock_manager->LockUpgrade(txn, rid))
        return false;
    } else if (txn->GetExclusiveLockSet()->find(rid) ==
        txn->GetExclusiveLockSet()->end() &&
        !lock_manager->LockExclusive(txn, rid)) { // no shared lock
      return false;
    }

    // TODO: add your logging logic here
    // 首先要先制造tuple
    Tuple tuple;
    tuple.size_ = tuple_size;
    tuple.data_ = new char[tuple_size];
    memcpy(tuple.data_, GetData()+GetTupleOffset(slot_num), tuple_size);
    tuple.rid_ = rid;
    tuple.allocated_ = true;

    // 创建一条删除日志并添加
    LogRecord log(txn->GetTransactionId(), txn->GetPrevLSN(),
                    LogRecordType::MARKDELETE, rid, tuple);
    lsn_t lsn = log_manager->AppendLogRecord(log);
    txn->SetPrevLSN(lsn);
    SetLSN(lsn);
  }

  // set tuple size to negative value
  if (tuple_size > 0)
    SetTupleSize(slot_num, -tuple_size);
  return true;
}

/**
 * @brief 以下过程的执行是 hold tuple 锁的，注意一下事务锁的获取与阻塞等过程
 * @param  new_tuple        desc
 * @param  old_tuple        desc
 * @param  rid              desc
 * @param  txn              desc
 * @param  lock_manager     desc
 * @param  log_manager      desc
 * @return true @c 
 * @return false @c 
 */
bool TablePage::UpdateTuple(const Tuple &new_tuple, Tuple &old_tuple,
                            const RID &rid, Transaction *txn,
                            LockManager *lock_manager,
                            LogManager *log_manager) {
  int slot_num = rid.GetSlotNum();
  if (slot_num >= GetTupleCount()) {
    if (ENABLE_LOGGING) {
      txn->SetState(TransactionState::ABORTED);
    }
    return false;
  }
  int32_t tuple_size = GetTupleSize(slot_num); // old tuple size
  if (tuple_size <= 0) {
    if (ENABLE_LOGGING) {
      txn->SetState(TransactionState::ABORTED);
    }
    return false;
  }
  if (GetFreeSpaceSize() < new_tuple.size_ - tuple_size) {
    // should delete/insert because not enough space
    return false;
  }

  // copy out old value 备份先
  int32_t tuple_offset =
      GetTupleOffset(slot_num); // the tuple offset of the old tuple
  old_tuple.size_ = tuple_size;
  if (old_tuple.allocated_)
    delete[] old_tuple.data_;
  old_tuple.data_ = new char[old_tuple.size_];
  memcpy(old_tuple.data_, GetData() + tuple_offset, old_tuple.size_);
  old_tuple.rid_ = rid;
  old_tuple.allocated_ = true;

  // log一定是op的严格顺序，依赖锁等并发控制机制实现
  if (ENABLE_LOGGING) {
    // acquire exclusive lock
    // 首先当前事务准备执行一个写操作，下面是能够比较好的说明update锁的作用的
    // ...
    // 在尝试获取锁的过程中，都有可能阻塞在条件变量上，那就是连续获取了两个锁啊，即会在持有 page锁 的前提下等待，等待并获取锁后返回true
    // ...
    // if has shared lock
    if (txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end()) {
        // 在集合中，end返回的是最后的迭代器，find一直没有找到，最后就会到迭代器
        // 当前事务对当前rid有加读锁，显然不能继续加写锁，因为读写锁是不兼容的，或者说是互斥的，只能加update锁
        // update锁有共享锁是兼容的，表示想要有一个写操作，而且该锁的add也无需考虑读锁的释放
        // 如果没有update锁，那么在拥有读锁的情况下想要获取写锁就会直接与2pl相违背，所以update锁的出现是必然的也是必要的
      if (!lock_manager->LockUpgrade(txn, rid)){
        return false;
      }
    } else if (txn->GetExclusiveLockSet()->find(rid) ==
        txn->GetExclusiveLockSet()->end() &&
        !lock_manager->LockExclusive(txn, rid)) { 
      // no shared lock
      // 如果没有读锁，那么该事务直接尝试获取写锁以完成写操作，如果后续有读操作，在持有写锁的情况下，读取是不受影响的
      return false;
    }

    // TODO: add your logging logic here
    LogRecord log(txn->GetTransactionId(), txn->GetPrevLSN(),
                    LogRecordType::UPDATE, rid, old_tuple, new_tuple);
    lsn_t lsn = log_manager->AppendLogRecord(log);
    txn->SetPrevLSN(lsn);
    SetLSN(lsn);
  }

  // update 能执行下来即说明已经得到了对应的锁，并且在log已经按照严格的顺序将op写入了log，完成原子的更新
  int32_t free_space_pointer =
      GetFreeSpacePointer(); // old pointer to the free space
  assert(tuple_offset >= free_space_pointer);
  memmove(GetData() + free_space_pointer + tuple_size - new_tuple.size_,
          GetData() + free_space_pointer, tuple_offset - free_space_pointer);
  SetFreeSpacePointer(free_space_pointer + tuple_size - new_tuple.size_);
  memcpy(GetData() + tuple_offset + tuple_size - new_tuple.size_,
         new_tuple.data_,
         new_tuple.size_);                 // copy new tuple
  SetTupleSize(slot_num, new_tuple.size_); // update tuple size in slot
  for (int i = 0; i < GetTupleCount();
       ++i) { // update tuple offsets (including the updated one)
    int32_t tuple_offset_i = GetTupleOffset(i);
    if (GetTupleSize(i) > 0 && tuple_offset_i < tuple_offset + tuple_size) {
      SetTupleOffset(i, tuple_offset_i + tuple_size - new_tuple.size_);
    }
  }
  return true;
}

/*
 * ApplyDelete function truly delete a tuple from table page, and make the slot
 * available for use again.
 * This function is called when a transaction commits or when you undo insert
 */
void TablePage::ApplyDelete(const RID &rid, Transaction *txn,
                            LogManager *log_manager) {
  int slot_num = rid.GetSlotNum();
  assert(slot_num < GetTupleCount());
  // the tuple offset of the deleted tuple
  int32_t tuple_offset = GetTupleOffset(slot_num);
  int32_t tuple_size = GetTupleSize(slot_num);
  if (tuple_size < 0) { // commit delete
    tuple_size = -tuple_size;
  } // else: rollback insert op

  // copy out delete value, for undo purpose
  Tuple delete_tuple;
  delete_tuple.size_ = tuple_size;
  delete_tuple.data_ = new char[delete_tuple.size_];
  memcpy(delete_tuple.data_, GetData() + tuple_offset, delete_tuple.size_);
  delete_tuple.rid_ = rid;
  delete_tuple.allocated_ = true;

  if (ENABLE_LOGGING) {
    // BackTracePlus();
    // std::printf("txn id is %d\n", txn->GetTransactionId());
    // must already grab the exclusive lock
    assert(txn->GetExclusiveLockSet()->find(rid) !=
        txn->GetExclusiveLockSet()->end());

    // TODO: add your logging logic here
    LogRecord log(txn->GetTransactionId(), txn->GetPrevLSN(),
                    LogRecordType::APPLYDELETE, rid, delete_tuple);
    lsn_t lsn = log_manager->AppendLogRecord(log);
    txn->SetPrevLSN(lsn);
    SetLSN(lsn);
  }

  int32_t free_space_pointer =
      GetFreeSpacePointer(); // old pointer to the free space
  assert(tuple_offset >= free_space_pointer);
  memmove(GetData() + free_space_pointer + tuple_size,
          GetData() + free_space_pointer, tuple_offset - free_space_pointer);
  SetFreeSpacePointer(free_space_pointer + tuple_size);
  SetTupleSize(slot_num, 0);
  SetTupleOffset(slot_num, 0); // invalid offset
  for (int i = 0; i < GetTupleCount(); ++i) {
    int32_t tuple_offset_i = GetTupleOffset(i);
    if (GetTupleSize(i) != 0 && tuple_offset_i < tuple_offset) {
      SetTupleOffset(i, tuple_offset_i + tuple_size);
    }
  }
}

/*
 * RollbackDelete is a complementary function wrt MarkDelete function.
 * It flip the tuple size from negative to positive, so that the tuple becomes
 * visible again.
 * This function is called when abort a transaction
 */
void TablePage::RollbackDelete(const RID &rid, Transaction *txn,
                               LogManager *log_manager) {
  int slot_num = rid.GetSlotNum();
  assert(slot_num < GetTupleCount());
  int32_t tuple_size = GetTupleSize(slot_num);

  if (ENABLE_LOGGING) {
    // must have already grab the exclusive lock
    assert(txn->GetExclusiveLockSet()->find(rid) !=
        txn->GetExclusiveLockSet()->end());

    // TODO: add your logging logic here
    // 首先要先制造tuple
    Tuple tuple;
    tuple.size_ = tuple_size;
    tuple.data_ = new char[tuple_size];
    memcpy(tuple.data_, GetData()+GetTupleOffset(slot_num), tuple_size);
    tuple.rid_ = rid;
    tuple.allocated_ = true;

    LogRecord log(txn->GetTransactionId(), txn->GetPrevLSN(),
                    LogRecordType::ROLLBACKDELETE, rid, tuple);
    lsn_t lsn = log_manager->AppendLogRecord(log);
    txn->SetPrevLSN(lsn);
    SetLSN(lsn);
  }

  // set tuple size to positive value
  if (tuple_size < 0)
    SetTupleSize(slot_num, -tuple_size);
}

/**
 * @brief 在事务的锁管理中，需要得到一个读锁，就是PPT中R(A)的过程
 * 不需要追加log
 * @param  rid              desc
 * @param  tuple            desc
 * @param  txn              desc
 * @param  lock_manager     desc
 * @return true @c 
 * @return false @c 
 */
bool TablePage::GetTuple(const RID &rid, Tuple &tuple, Transaction *txn,
                         LockManager *lock_manager) {
  int slot_num = rid.GetSlotNum();
  if (slot_num >= GetTupleCount()) {
    if (ENABLE_LOGGING)
      txn->SetState(TransactionState::ABORTED);
    return false;
  }
  int32_t tuple_size = GetTupleSize(slot_num);
  if (tuple_size <= 0) {
    if (ENABLE_LOGGING)
      txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (ENABLE_LOGGING) {
    // acquire shared lock
    if (txn->GetExclusiveLockSet()->find(rid) ==
        txn->GetExclusiveLockSet()->end() &&
        txn->GetSharedLockSet()->find(rid) == txn->GetSharedLockSet()->end() &&
        !lock_manager->LockShared(txn, rid)) {
      // 当前事务未持有写锁 且 当前事务未持有读锁 且 尝试从锁管理器获取读锁时失败
      // std::printf("get tuple false!\n");
      // BackTracePlus();
      return false;
    }
    // 如果当前事务持有写锁或持有读锁都直接就继续执行了
  }

  int32_t tuple_offset = GetTupleOffset(slot_num);
  tuple.size_ = tuple_size;
  if (tuple.allocated_)
    delete[] tuple.data_;
  tuple.data_ = new char[tuple.size_];
  memcpy(tuple.data_, GetData() + tuple_offset, tuple.size_);
  tuple.rid_ = rid;
  tuple.allocated_ = true;
  return true;
}

/**
 * Tuple iterator
 */
bool TablePage::GetFirstTupleRid(RID &first_rid) {
  for (int i = 0; i < GetTupleCount(); ++i) {
    if (GetTupleSize(i) > 0) { // valid tuple
      first_rid.Set(GetPageId(), i);
      return true;
    }
  }
  // there is no tuple within current page
  first_rid.Set(INVALID_PAGE_ID, -1);
  return false;
}

bool TablePage::GetNextTupleRid(const RID &cur_rid, RID &next_rid) {
  assert(cur_rid.GetPageId() == GetPageId());
  for (auto i = cur_rid.GetSlotNum() + 1; i < GetTupleCount(); ++i) {
    if (GetTupleSize(i) > 0) { // valid tuple
      next_rid.Set(GetPageId(), i);
      return true;
    }
  }
  return false; // End of last tuple
}

/**
 * helper functions
 */

// tuple slots
int32_t TablePage::GetTupleOffset(int slot_num) {
  return *reinterpret_cast<int32_t *>(GetData() + 24 + 8*slot_num);
}

int32_t TablePage::GetTupleSize(int slot_num) {
  return *reinterpret_cast<int32_t *>(GetData() + 28 + 8*slot_num);
}

void TablePage::SetTupleOffset(int slot_num, int32_t offset) {
  memcpy(GetData() + 24 + 8*slot_num, &offset, 4);
}

void TablePage::SetTupleSize(int slot_num, int32_t offset) {
  memcpy(GetData() + 28 + 8*slot_num, &offset, 4);
}

// free space
int32_t TablePage::GetFreeSpacePointer() {
  return *reinterpret_cast<int32_t *>(GetData() + 16);
}

void TablePage::SetFreeSpacePointer(int32_t free_space_pointer) {
  memcpy(GetData() + 16, &free_space_pointer, 4);
}

// tuple count
int32_t TablePage::GetTupleCount() {
  return *reinterpret_cast<int32_t *>(GetData() + 20);
}

void TablePage::SetTupleCount(int32_t tuple_count) {
  memcpy(GetData() + 20, &tuple_count, 4);
}

// for free space calculation
int32_t TablePage::GetFreeSpaceSize() {
  return GetFreeSpacePointer() - 24 - GetTupleCount()*8;
}
} // namespace cmudb
