/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 * 按照 tuple 来锁
 * 事务（线程）对tuple加锁
 * 好在这个地方无需care持久化的问题，纯内存的操作
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/rid.h"
#include "concurrency/transaction.h"

#define LOCK_TIME_OUT 1000

namespace cmudb {

enum class LockMode { SHARED = 0, EXCLUSIVE };

class LockManager {
    struct Request {
        // 某事务需要向某rid加锁（MODE），很形象就是事务向所管理器请求锁
        explicit Request(txn_id_t id, LockMode m, bool g) :
            txn_id(id), mode(m), granted(g) {}
        txn_id_t txn_id;
        LockMode mode = LockMode::SHARED;
        bool granted = false;  // 请求是否被授权
    };
    struct Waiting {
        // how many exclusive requests
        // 排它锁的request是先自增，再尝试获取锁，否则排队
        // 但是只要入队，排它锁请求exclusive_cnt需要自增
        // 在这个设计里，只要rid被访问过，rid就会常驻，oldest也要跟着变化
        // oldest指的是最小的事务id
        size_t exclusive_cnt = 0;
        txn_id_t oldest = -1;      // wait-die: txn older than `oldest`(<) can wait or die
        std::list<Request> list; // 锁表的list
        //std::mutex mutex_;
        //std::condition_variable cond;
    };
public:
    explicit LockManager(bool strict_2PL) : strict_2PL_(strict_2PL) {};

    // disable copy
    LockManager(LockManager const &) = delete;
    LockManager &operator=(LockManager const &) = delete;

    /*** below are APIs need to implement ***/
    // lock:
    // return false if transaction is aborted
    // it should be blocked on waiting and should return true when granted
    // note the behavior of trying to lock locked rids by same txn is undefined
    // it is transaction's job to keep track of its current locks
    bool LockShared(Transaction *txn, const RID &rid);
    bool LockExclusive(Transaction *txn, const RID &rid);
    bool LockUpgrade(Transaction *txn, const RID &rid);

    // unlock:
    // release the lock hold by the txn
    bool Unlock(Transaction *txn, const RID &rid);
    /*** END OF APIs ***/

private:
    bool strict_2PL_;  // 进一步限制的锁可以被释放的时机
    std::mutex mutex_;
    std::condition_variable cond;
    // 系统维护的锁表
    std::unordered_map<RID, Waiting> lock_table_;
};

} // namespace cmudb
