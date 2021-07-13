/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace cmudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
IndexIterator<KeyType, ValueType, KeyComparator>::
IndexIterator(
    BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf,
    int index_, BufferPoolManager *buff_pool_manager): 
    leaf_(leaf), index_(index_), buff_pool_manager_(buff_pool_manager) 
    {}

template <typename KeyType, typename ValueType, typename KeyComparator>
IndexIterator<KeyType, ValueType, KeyComparator>::
~IndexIterator() {
    buff_pool_manager_->FetchPage(leaf_->GetPageId())->RUnlatch();
    buff_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
};

template <typename KeyType, typename ValueType, typename KeyComparator>
bool IndexIterator<KeyType, ValueType, KeyComparator>::
isEnd() {
    return (
        leaf_ == nullptr || (index_ == leaf_->GetKeySize() 
        && leaf_->GetNextPageId() == INVALID_PAGE_ID));
}

// 这个就是对指针的 * 运算符的重载
template <typename KeyType, typename ValueType, typename KeyComparator>
const MappingType &IndexIterator<KeyType, ValueType, KeyComparator>::
operator*() {
    if (isEnd()) { throw std::out_of_range("IndexIterator: out of range"); }
    return leaf_->GetItem(index_);
}

// 对指针自增运算符的重载，可以看到，这里没有 -- 的重载，核心目的是 b+tree 的范围查找，就是正向遍历 b+tree 叶子节点即可
template <typename KeyType, typename ValueType, typename KeyComparator>
IndexIterator<KeyType, ValueType, KeyComparator> &IndexIterator<KeyType, ValueType, KeyComparator>::
operator++() {
    // 用来在 b+tree 的成 list 的叶子节点上的自增操作
    ++index_;
    if (index_ == leaf_->GetKeySize() && leaf_->GetNextPageId() != INVALID_PAGE_ID) 
    {
        // first unpin leaf_, then get the next leaf
        page_id_t next_page_id = leaf_->GetNextPageId();
        auto *page = buff_pool_manager_->FetchPage(next_page_id);
        if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while IndexIterator(operator++)"); }

        // first acquire next page, then release previous page
        page->RLatch();

        buff_pool_manager_->FetchPage(leaf_->GetPageId())->RUnlatch();
        buff_pool_manager_->UnpinPage(leaf_->GetPageId(), false);

        auto next_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
        assert(next_leaf->IsLeafPage());
        index_ = 0;
        leaf_ = next_leaf;
    }
    // 返回的是迭代器本身
    return *this;
};

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
