/**
 * b_plus_tree_leaf_page.h
 *
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *  果然，value是pageid+slotid，是能够直接定位到一个tuple的

 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *  map 类的容器还真没法直接用，最朴素的方式就是自定义数组来存放 std::pair
 * 
 *  Header format (size in byte, 24 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) | ParentPageId (4) |
 *  ---------------------------------------------------------------------
 *  ------------------------------
 * | PageId (4) | NextPageId (4)
 *  ------------------------------
 */

#pragma once

#include <utility>
#include <vector>

#include "page/b_plus_tree_page.h"

namespace cmudb {
#define B_PLUS_TREE_LEAF_PAGE_TYPE                                             \
    BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>

template <typename KeyType, typename ValueType, typename KeyComparator>
class BPlusTreeLeafPage : public BPlusTreePage {
public:
    // After creating a new leaf page from buffer pool, must call initialize
    // method to set default values
    void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID);

    // helper methods
    page_id_t GetNextPageId() const;
    void SetNextPageId(page_id_t next_page_id);

    KeyType KeyAt(int index) const;
    int KeyIndex(const KeyType &key, const KeyComparator &comparator) const;

    const MappingType &GetItem(int index);

    // insert and delete methods
    int Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator);
    bool Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const;
    int RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator);

    // Split and Merge utility methods
    void MoveHalfTo(BPlusTreeLeafPage *recipient, BufferPoolManager *buffer_pool_manager /* Unused */);

    void MoveAllTo(BPlusTreeLeafPage *recipient, int /* Unused */, BufferPoolManager * /* Unused */);

    void MoveFirstToEndOf(BPlusTreeLeafPage *recipient, BufferPoolManager *buffer_pool_manager);

    void MoveLastToFrontOf(BPlusTreeLeafPage *recipient, int parentIndex, BufferPoolManager *buffer_pool_manager);

    int GetKeySize() const { return key_size; }
    void SetKeySize(int size) { key_size = size; }
    void IncreaseKeySize(int amount) { key_size += amount; }

    // leaf node
    // helper function，这个要取决于 b+tree 的秩, 这里是叶子节点
    int GetMaxKeySize() const { return GetOrder() - 1; }
    int GetMinKeySize() const { return (GetOrder()+1)/2-1; }

    // Debug
    std::string ToString(bool verbose = false) const;

private:
    void CopyHalfFrom(MappingType *items, int size);
    void CopyAllFrom(MappingType *items, int size);
    void CopyLastFrom(const MappingType &item);
    void CopyFirstFrom(const MappingType &item, int parentIndex, BufferPoolManager *buffer_pool_manager);

    page_id_t next_page_id_;
    // 节点的 容量 与 real_order 都在基类中，这里我想要保存一下 key 的大小，因为能否 insert 一个 k 是取决于 k 的大小的
    int key_size;  // k 的数量，最大是 阶-1，叶子节点能够再 insert 一个值就取决于该值
    /** 
     * b+ tree 叶子节点所在的页 
     * put a variable-sized array at the end of a structure 
     * struct array { size_t size; int a[];}
     * struct array *array = malloc(sizeof (struct array) + size * sizeof (int))
     * 这种结构用的还是蛮多的
     */
    MappingType array[0];
};
} // namespace cmudb
