/**
 * lock_manager.cpp
 */

#include <cassert>
#include "concurrency/lock_manager.h"

namespace cmudb
{

/**
 * @brief 事务尝试向锁管理器请求共享锁
 * @param  txn              desc
 * @param  rid              desc
 * @return true @c 
 * @return false @c 
 */
bool LockManager::LockShared(Transaction *txn, const RID &rid)
{
  std::unique_lock<std::mutex> latch(mutex_);
  if (txn->GetState() == TransactionState::ABORTED)
  {
    return false;
  }

  assert(txn->GetState() == TransactionState::GROWING);

  Request req{txn->GetTransactionId(), LockMode::SHARED, false};
  if (lock_table_.count(rid) == 0)
  {
    // 当前没有其它事务持有该rid的锁，所以下面的在条件变量上的等待是一定可以过的
    lock_table_[rid].exclusive_cnt = 0;
    lock_table_[rid].oldest = txn->GetTransactionId();
    lock_table_[rid].list.push_back(req);
  }
  else
  {
    // 目前有事务等待在rid上（或读或写）
    if (lock_table_[rid].exclusive_cnt != 0 &&
        txn->GetTransactionId() > lock_table_[rid].oldest)
    {
      // wait die预防死锁
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
    if (lock_table_[rid].oldest > txn->GetTransactionId())
    {
      lock_table_[rid].oldest = txn->GetTransactionId();
    }
    lock_table_[rid].list.push_back(req);
  }

  // 等待条件变量，等待直到获取读锁
  // 如果所有阻塞在rid上的事务都是读事务且都获得了授权，则当前线程继续进入读取
  Request *cur = nullptr;
  cond.wait(latch, [&]() -> bool {
    bool all_shared = true, all_granted = true;
    for (auto &r : lock_table_[rid].list)
    {
      if (r.txn_id != txn->GetTransactionId())
      {
        if (r.mode != LockMode::SHARED || !r.granted)
        {
          return false;
        }
      }
      else
      {
        // 本身是被push到最后的，所以最后一个一定是新的
        cur = &r;
        return all_shared && all_granted;
      }
    }
    return false;
  });

  // 得到了读锁
  assert(cur != nullptr && cur->txn_id == txn->GetTransactionId());
  cur->granted = true;
  txn->GetSharedLockSet()->insert(rid);  // 事务要维护一个set, track当前事务所加读锁的rid

  cond.notify_all();
  return true;
}

/**
 * @brief 事务想要对 tuple rid 上写锁
 * @param  txn              desc
 * @param  rid              desc
 * @return true @c 
 * @return false @c 
 */
bool LockManager::LockExclusive(Transaction *txn, const RID &rid)
{
  std::unique_lock<std::mutex> latch(mutex_);
  if (txn->GetState() == TransactionState::ABORTED)
  {
    return false;
  }

  // 2PL锁只能在GROWING阶段加锁
  assert(txn->GetState() == TransactionState::GROWING);

  // 构造请求
  Request req{txn->GetTransactionId(), LockMode::EXCLUSIVE, false};
  if (lock_table_.count(rid) == 0)
  {
    // 还没有事务碰过这个rid
    lock_table_[rid].oldest = txn->GetTransactionId();
    lock_table_[rid].list.push_back(req);
  }
  else
  {
    // wait die 是一种非剥夺策略，老的事务等待新的事务释放资源
    // 即若A比B老，则等待B执行结束，否则A卷回(roll-back)
    // 一段时间后会以原先的时间戳继续申请。老的才有资格等，年轻的全部卷回
    // ...
    // 假设事务 T5、T10、T15 分别具有时间戳 5、10 和 15
    // 如果 T5 请求 T10 持有的数据项，则 T5 将等待
    // 如果 T15 请求 T10 持有的数据项，则 T15 将被杀死(死亡)
    // wait die 为什么能否防止环的出现呢
    // 如果Ti等待Tj释放锁，记录为 Ti->Tj, wait die只允许Ti等待Tj，且i<j(或j<i)。反正一定是单边的，绝对不可能形成环
    if (txn->GetTransactionId() > lock_table_[rid].oldest)
    {
      // 从根源上杜绝了环的出现，但是也abort了一些无需abort的事务，在性能上有损失 
      txn->SetState(TransactionState::ABORTED);
      return false;
    }

    // 否则就wait
    lock_table_[rid].oldest = txn->GetTransactionId();
    lock_table_[rid].list.push_back(req);
  }

  ++lock_table_[rid].exclusive_cnt;  // 目标rid如果是第一次被访问并加锁，那就是从0到1

  // 排它锁只有在等待队列中的第一个才能获得锁
  // 首先下面的语句等价于
  // while(lock_table_[rid].list.front().txn_id == txn->GetTransactionId()){
  //     cond.wait(latch);
  // }
  // 如果当前的rid被其它事务加了排它锁，那么当前事务（线程）只能等待，事务（线程）等待在锁管理器的代码上
  cond.wait(latch, [&]() -> bool {
    return lock_table_[rid].list.front().txn_id == txn->GetTransactionId();
  });

  assert(lock_table_[rid].list.front().txn_id == txn->GetTransactionId());

  // 成功得到锁，被授权，返回true，代表成功获取到锁
  lock_table_[rid].list.front().granted = true;
  txn->GetExclusiveLockSet()->insert(rid);
  return true;
}

/**
 * @brief 更新锁，是为了解决死锁的
 * 先show一个例子
 * ...
 * T1
 * begin Transaction t1
 * select * from table with (holdlock) （holdlock的意思是加共享锁，直到事务结束（提交或回滚）才会释放）
 * update table set column1='hello'
 * ...
 * 先读后写，自己就把自己死锁了，无法满足2pl的话
 * 读锁不释放是无法获取写锁的，但是如果释放了读锁，那就无法再继续加锁了，这样就违反了2pl
 * 
 * 才发现PPT里没怎么说在一个事务中，又读又写的情况，要么一个事务是读一系列值要么是写一系列值
 * 这里又读又写是需要在这里解决的
 * 
 * 否则是必然的死锁
 * 
 * 更新锁的由来
 * 更新锁与共享锁是兼容的
 * 共享锁之间的是兼容的
 * 更新锁之间是不兼容的
 * 更新锁可以转排它锁，但是共享锁无法转排它锁
 * 
 * 这就是共享锁和更新锁的一个区别了
 *      共享锁之间是兼容的，但是更新锁之间互不兼容，因此仅有一个更新锁直接转为排他锁是安全的
 *      而多个共享锁直接转为排他锁，那怎么行呢，排他锁只能有一个的
 *      这就是为什么共享锁需要等待其他共享锁释放才可以升级为排他锁的原因了
 * 
 * 更新锁的引入是因为在一个事务中，如果需要即获得的排它锁又需要获得共享锁，即又读又写
 *      但是显然会违反2pl规则，即先加一个读或写锁，然后在释放再加锁，显然是违反2pl的
 * 
 * 如果先得到排它锁，如果有读需求，则直接读取即可，如果先得到的是读锁，后续有写请求，那么需要获取更新锁，不会违反2pl协议
 * update lock在需要的时候可以转变为写锁，所有的锁都可以在 shrink 阶段得到释放，依然可以保证冲突可串行化，即不会违反2pl协议
 * 
 * 关键是在于共享锁无法升级为排它锁，但是更新锁可以，更新锁与共享锁是兼容的，无需等待（死锁的由来）共享锁释放后即可获取
 * 
 * 更新锁是为了将来潜在的写操作，能在不被死锁的前提下获得写权限
 * 
 * @param  txn              desc
 * @param  rid              desc
 * @return true @c
 * @return false @c
 */
bool LockManager::LockUpgrade(Transaction *txn, const RID &rid)
{
  std::unique_lock<std::mutex> latch(mutex_);
  if (txn->GetState() == TransactionState::ABORTED)
  {
    return false;
  }

  // 必须位于2pl的加锁阶段
  assert(txn->GetState() == TransactionState::GROWING);

  // 1. move cur request to the end of `shared` period
  // 2. change granted to false
  // 3. change lock mode to EXCLUSIVE
  auto src = lock_table_[rid].list.end(), tgt = src;
  for (auto it = lock_table_[rid].list.begin();
       it != lock_table_[rid].list.end(); ++it)
  {
    if (it->txn_id == txn->GetTransactionId())
    {
      src = it;
    }
    if (src != lock_table_[rid].list.end())
    {
      if (it->mode == LockMode::EXCLUSIVE)
      {
        tgt = it;
        break;
      }
    }
  }
  assert(src != lock_table_[rid].list.end());

  // wie-die
  for (auto it = lock_table_[rid].list.begin(); it != tgt; ++it)
  {
    if (it->txn_id < src->txn_id)
    {
      return false;
    }
  }

  Request req = *src;
  req.granted = false;
  req.mode = LockMode::EXCLUSIVE;

  lock_table_[rid].list.insert(tgt, req);
  lock_table_[rid].list.erase(src);

  // 等地啊条件变量
  cond.wait(latch, [&]() -> bool {
    return lock_table_[rid].list.front().txn_id == txn->GetTransactionId();
  });

  assert(lock_table_[rid].list.front().txn_id == txn->GetTransactionId() &&
         lock_table_[rid].list.front().mode == LockMode::EXCLUSIVE);

  lock_table_[rid].list.front().granted = true;

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->insert(rid);
  return true;
}

/**
 * @brief 锁表记录了一个事务是否对rid持有某种锁，即rid上挂了一个list
 * 这个list包含一系列的txn请求，锁表记录了哪些事务对哪些rid拥有哪些锁
 * 
 * 解锁是事务对某rid的解锁
 * @param  txn              desc
 * @param  rid              desc
 * @return true @c
 * @return false @c
 */
bool LockManager::Unlock(Transaction *txn, const RID &rid)
{
  std::unique_lock<std::mutex> latch(mutex_);

  if (strict_2PL_)
  {
    // SS2PL限制了解锁的时机，即必须在事务结束之后才可以解锁
    // 进一步牺牲了并发，会造成脏读，如果被脏读的事务回滚了，那么脏读的事务也需要回滚
    // SS2PL可以防止这个问题
    if (txn->GetState() != TransactionState::COMMITTED &&
        txn->GetState() != TransactionState::ABORTED)
    {
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
  }
  else
  {
    // 开始解锁，即进入shrinking阶段
    if (txn->GetState() == TransactionState::GROWING)
    {
      txn->SetState(TransactionState::SHRINKING);
    }
  }

  assert(lock_table_.count(rid));
  for (auto it = lock_table_[rid].list.begin();
       it != lock_table_[rid].list.end(); ++it)
  {
    // 遍历rid以找到目标rid
    if (it->txn_id == txn->GetTransactionId())
    {
      bool first = it == lock_table_[rid].list.begin();
      bool exclusive = it->mode == LockMode::EXCLUSIVE;

      if (exclusive)
      {
        // 先-1。这个就是一个统计数据
        --lock_table_[rid].exclusive_cnt;
      }
      lock_table_[rid].list.erase(it);  // 相当于delete 1

      if (first || exclusive)
      {
        // 需要唤醒其它等待在该rid上的事务 
        cond.notify_all();
      }
      break;
    }
  }
  return true;
}

} // namespace cmudb
