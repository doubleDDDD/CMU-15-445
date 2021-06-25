/**
 * LRU implementation
 * 对尾是最近刚访问过的，无论读写
 * 队首是最近一直都没有访问过的，victim返回的是该对象，暂时用不到了，先不缓存了就
 *  保存到 LRU 中是认为它最近可能会频繁的访问，在队首的话就说明最近不怎么需要了
 */
#include <cassert>

#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> 
LRUReplacer<T>::LRUReplacer() : size_(0) 
{
    head_ = new node();  /* LRU初始化一个节点，它会随着程序的运行而增加 */
    tail_ = head_;
}

template <typename T> 
LRUReplacer<T>::~LRUReplacer() = default;

/**
* Insert value into LRU，刚用了一个 page，需要将 page 丢到 list 中
*/
template <typename T> 
void LRUReplacer<T>::Insert(const T &value) 
{
    std::lock_guard<std::mutex> lock(mutex_);  // get lock，函数结束释放锁

    /* table is a map, space for time */
    auto it = table_.find(value);
    if(it == table_.end()) {
        // Page 不在 LRU list 中
        tail_->next = new node(value, tail_);
        tail_ = tail_->next;
        table_.emplace(value, tail_);  // push_back 的作用
        ++size_;
    } else {
        /* page 位于 LRU list 中，需要调整位置 */
        if(it->second != tail_) 
        {
            // 先从原位置移除
            node *pre = it->second->pre;
            node *cur = pre->next;
            // 左值转为右值，确实省了临时变量的生成，cur->next 的指针所指向的对象直接为 pre->next 所有
            pre->next = std::move(cur->next);
            pre->next->pre = pre;
            // 再放到尾部
            cur->pre = tail_;
            tail_->next = std::move(cur);
            tail_ = tail_->next;
        }
    }
}

/** 
* If LRU is non-empty, pop the head member from LRU to argument "value", and
* return true. If LRU is empty, return false
*/
template <typename T> 
bool LRUReplacer<T>::Victim(T &value) 
{
    std::lock_guard<std::mutex> lock(mutex_);
    if(size_ == 0) { return false; }

    value = head_->next->data;
    head_->next = head_->next->next;
    if(head_->next != nullptr) { head_->next->pre = head_; }

    table_.erase(value);
    if(--size_ == 0) { tail_ = head_; }

    return true;
}

/*
* Remove value from LRU. If removal is successful, return true, otherwise
* return false
* 在使用的过程中,对象需要从 LRU 上取下来
*/
template <typename T> 
bool LRUReplacer<T>::Erase(const T &value) 
{
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = table_.find(value);
    if(it != table_.end()) {
        if(it->second != tail_) {
            node *pre = it->second->pre;
            node *cur = pre->next;
            pre->next = std::move(cur->next);
            pre->next->pre = pre;
        } else {
            tail_ = tail_->pre;
            delete tail_->next;
        }

        table_.erase(value);
        if(--size_ == 0) {
            tail_ = head_;
        }
        return true;
    }

    return false;
}

template <typename T> 
size_t LRUReplacer<T>::Size() {
    std::lock_guard<std::mutex> lock(mutex_);
    return size_;
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;
} // namespace cmudb
