/**
 * b_plus_tree_internal_page.h
 *
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 * 这个组织形式比我想象的要简单的多
 * b+ tree 的 page 上的key与指针应该是错开的，即假设有2个key,就应该有2+1个指针的slot，指针的 slot 会比 key 的数量多一个
 * 如何巧妙的设计这种对应关系呢，这里的实现非常简单，就是在page中pair，然后第一个key无效即可，然后注意一下对应关系就可以了
 */

#pragma once

#include <queue>

#include "page/b_plus_tree_page.h"

namespace cmudb {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE                                         \
  BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>

template <typename KeyType, typename ValueType, typename KeyComparator>
class BPlusTreeInternalPage : public BPlusTreePage 
{
public:
    // must call initialize method after "create" a new node
    void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID);

    KeyType KeyAt(int index) const;
    void SetKeyAt(int index, const KeyType &key);
    int ValueIndex(const ValueType &value) const;
    ValueType ValueAt(int index) const;
    void SetValueAt(int index, const ValueType &value);

    ValueType Lookup(const KeyType &key, const KeyComparator &comparator) const;
    void PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);
    int InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);
    void Remove(int index);
    ValueType RemoveAndReturnOnlyChild();

    void MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager);
    void MoveAllTo(BPlusTreeInternalPage *recipient, int index_in_parent, BufferPoolManager *buffer_pool_manager);
    void MoveFirstToEndOf(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager);
    void MoveLastToFrontOf(BPlusTreeInternalPage *recipient, int parent_index, BufferPoolManager *buffer_pool_manager);
    // DEBUG and PRINT
    std::string ToString(bool verbose) const;
    void QueueUpChildren(std::queue<BPlusTreePage *> *queue, BufferPoolManager *buffer_pool_manager);
private:
    void CopyHalfFrom(MappingType *items, int size,
                        BufferPoolManager *buffer_pool_manager);
    void CopyAllFrom(MappingType *items, int size,
                        BufferPoolManager *buffer_pool_manager);
    void CopyLastFrom(const MappingType &pair,
                        BufferPoolManager *buffer_pool_manager);
    void CopyFirstFrom(const MappingType &pair, int parent_index,
                        BufferPoolManager *buffer_pool_manager);
    MappingType array[0];
};
} // namespace cmudb
