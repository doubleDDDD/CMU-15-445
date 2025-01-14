/**
 * b_plus_tree_internal_page.h
 *
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 * 我丢，这个作者好像没有这样写哦，这里怕是要自己重写一版了
 * 怕是看错了，叶子节点是不怕的，叶子节点可以正常用kv，最后一个v是指向下一个page的指针
 * 但是中间节点确实是需要 kv 错开的
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

    int GetValueSize() const  { return v_size; }
    void SetValueSize(int size) { v_size = size; }
    void IncreaseValueSize(int amount) { v_size += amount; }

    // internal
    // helper function，这个要取决于 b+tree 的秩, 这里是中间节点
    int GetMaxValueSize() const {
        // 秩为 order，则 key 的数量最大是 order-1, 不超过 M/2 的最大整数
        // 即如果 秩=3，则 叶子节点最少 2 个，最多 2 个
        return GetOrder(); 
    }
    int GetMinValueSize() const { return (GetOrder()+1)/2; }

    // DEBUG and PRINT
    std::string ToString(bool verbose = false) const;

    // 将节点的kv入队
    void QueueUpChildren(std::queue<BPlusTreePage *> *queue, BufferPoolManager *buffer_pool_manager);
private:
    void CopyHalfFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager);
    void CopyAllFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager);
    void CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager);
    void CopyFirstFrom(const MappingType &pair, int parent_index, BufferPoolManager *buffer_pool_manager);

    int v_size;  // 节点中 v 的数量，即tree的秩，v-1就是key的数量

    MappingType array[0];
};
} // namespace cmudb