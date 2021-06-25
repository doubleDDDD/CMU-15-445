/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include <unordered_map>
#include <mutex>
#include <memory>
#include "buffer/replacer.h"

namespace cmudb {

/**
 * @brief 任何一个类都要考虑能不能写成模板
 * @tparam T
 */

template <typename T> 
class LRUReplacer : public Replacer<T> {
    struct node {
        node() = default;
        explicit node(T d, node *p = nullptr) : data(d), pre(p) {}
        T data;  // 在这里，T是一个指向 Page 的指针
        node *pre = nullptr;
        node *next = nullptr;
    };
public:
    // do not change public interface
    LRUReplacer();
    ~LRUReplacer();  // 非基类的虚函数可以不写成 virtual

    // disable copy
    LRUReplacer(const LRUReplacer &) = delete;
    LRUReplacer &operator=(const LRUReplacer &) = delete;

    void Insert(const T &value);
    bool Victim(T &value);
    bool Erase(const T &value);
    size_t Size();

private:
    // 就是一个简单的双向的 list
    mutable std::mutex mutex_;
    size_t size_;
    std::unordered_map<T, node *> table_;  // hash table 是为了加速索引的
    node *head_;
    node *tail_;
};
} // namespace cmudb