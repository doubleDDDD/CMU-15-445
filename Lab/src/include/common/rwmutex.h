/**
 * rwmutex.h
 *
 * Reader-Writer lock
 * no .c, all defined in .h
 * https://en.cppreference.com/w/cpp/thread/condition_variable
 * 读写锁
 */

#pragma once

#include <climits>
#include <condition_variable>
#include <mutex>  // 线程互斥体

namespace cmudb {
class RWMutex {

    typedef std::mutex mutex_t;
    // The condition_variable class is a synchronization primitive that can be used 
    // to block a thread, or multiple threads at the same time
    // until another thread both modifies a shared variable (the condition), and notifies the condition_variable
    // 即线程阻塞在某个条件变量上
    typedef std::condition_variable cond_t;
    static const uint32_t max_readers_ = UINT_MAX;

public:
    RWMutex() : reader_count_(0), writer_entered_(false) {}

    ~RWMutex() {
        /**
         * @brief 
         *  When a lock_guard object is created
         *  it attempts to take ownership of the mutex it is given
         *  When control leaves the scope in which the lock_guard object was created
         *  the lock_guard is destructed and the mutex is released.
         * @return std::lock_guard<mutex_t> @c 
         */
        std::lock_guard<mutex_t> guard(mutex_);  /* 持有锁 */

        /* 运行结束之后会自动释放锁 */
    }

    // disable copy 构造函数
    RWMutex(const RWMutex &) = delete;
    RWMutex &operator=(const RWMutex &) = delete;

    void WLock() {
        // 某task尝试获取对该对象的写锁
        /* 构造一个 unique_lock 对象 lock,  */
        std::unique_lock<mutex_t> lock(mutex_);  // lock是mutex_的封装
        while (writer_entered_){
            // writer_entered_ 的初始值是false
            // 这句话的意思是如果有写操作，则读等待，读线程全部挂在条件变量上
            // 如果有其它写者的话 先挂在 reader上, wait可以释放锁，并且被唤醒后，可以再次获得锁，以判断while循环
            reader_.wait(lock);
        }
        // writer_entered_之前是false，才能走下来
        writer_entered_ = true;  /* 安全的修改了 writer_entered_ 表明有写者想要入场 */
        /* 如果始终有读者，则写者始终等待 */
        while (reader_count_ > 0){
            writer_.wait(lock);
        }

        // 反正是有写有读都得不到锁，所以有2个wait,没有尝试进入的写操作以及读者才能够获得锁
        /* 写者等待完成，得到了锁，开始执行临界区代码 */
    }

    /**
     * @brief 写完成之后bool值修改为false即可
     */
    void WUnlock() {
        /* 本身该函数的执行有一个原子的语义 */
        std::lock_guard<mutex_t> guard(mutex_);  /* 得到锁，不需要条件变量，所以用了guard */
        writer_entered_ = false;  /* 写完了 */
        reader_.notify_all();  /* 通知所有在 reader 上等待的线程，reader.wait()被唤醒 */
        /* 函数结束释放锁 */
    }

    /**
     * @brief 只要当前没有写操作既可以获得锁
     没有写操作，且读者的数量小于最大值
     */
    void RLock() {
        std::unique_lock<mutex_t> lock(mutex_);  /* 还未持有锁 */
        /* 如果当前 有写者 或 读者数量 达到了最大，则读失败，当前线程被挂到reader上，直到被唤醒 */
        while (writer_entered_ || reader_count_ >= max_readers_){
            reader_.wait(lock);
        }
        // 被唤醒后，当前线程持有持有锁
        // 并且在读线程读的过程中，其它读者依然可以获取到读锁
        reader_count_++;  // 依然是在有锁的情况下修改的
    }

    void RUnlock() {
        std::lock_guard<mutex_t> guard(mutex_);  /* 持有锁，该锁可重入 */
        reader_count_--;  /* 读者数量-1 */
        if (writer_entered_) {
            /* 如果有写者，且无读者，则优先唤醒 waiter 上的写者，写者每次只能有一个入场 */
            if (reader_count_ == 0)
                writer_.notify_one();
            /* 依然有读者，则写者等待 */
        } else {
            /* 如果没有写者，则唤醒一个等待队列上的读者，控制读者的数量 */
            if (reader_count_ == max_readers_ - 1)
                reader_.notify_one();
        }
        /* 离开之后释放锁 */
    }

private:
    mutex_t mutex_;  // 互斥体
    cond_t writer_;  // 线程可以阻塞在条件变量上
    cond_t reader_;
    uint32_t reader_count_;
    bool writer_entered_;
};
} // namespace cmudb
