/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 * b+tree 本身构成了index, 与数据库中的table是对标的
 *  b+tree 本身就是排好序的指针，不含有任何的数据
 * 磁盘中的指针是如何实现的呢？
 *  看到这里就理解了，磁盘中能够表现出来的只有文件
 * 能作为指针的只有文件的偏移，所谓的指针实际上就是一个逻辑上的页号
 *      再写一次，所谓的指针就是一个逻辑上的页号，磁盘文件被视作了一个一维数组（数组元素就是 page 的大小）
 *      页号就能够完全独立的指向一个page
 */

#pragma once

#include <queue>
#include <vector>

#include "concurrency/transaction.h"
#include "index/index_iterator.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"

// 现在看来这个宏是必须要的了
// #define DEBUG_TREE_SHOW

namespace cmudb {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

enum class Operation { READONLY = 0, INSERT, DELETE };

// Main class providing the API for the Interactive B+ Tree.
template <typename KeyType, typename ValueType, typename KeyComparator>
class BPlusTree {
public:
    explicit BPlusTree(const std::string &name,
                    BufferPoolManager *buffer_pool_manager,
                    const KeyComparator &comparator,
                    page_id_t root_page_id = INVALID_PAGE_ID);

    // Returns true if this B+ tree has no keys and values.
    bool IsEmpty() const;

    // Insert a key-value pair into this B+ tree.
    bool Insert(const KeyType &key, const ValueType &value, 
                    Transaction *transaction = nullptr);

    // Remove a key and its value from this B+ tree.
    void Remove(const KeyType &key, Transaction *transaction = nullptr);

    // return the value associated with a given key
    bool GetValue(const KeyType &key, std::vector<ValueType> &result,
                    Transaction *transaction = nullptr);

    // index iterator
    IndexIterator<KeyType, ValueType, KeyComparator> Begin();
    IndexIterator<KeyType, ValueType, KeyComparator> Begin(const KeyType &key);

    // read data from file and insert one by one
    void InsertFromFile(const std::string &file_name,
                        Transaction *transaction = nullptr);

    // read data from file and remove one by one
    void RemoveFromFile(const std::string &file_name,
                        Transaction *transaction = nullptr);

    // expose for test purpose
    BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *
    FindLeafPage(const KeyType &key, bool leftMost = false,
                Operation op = Operation::READONLY,
                Transaction *transaction = nullptr);

    // set为用户指定的阶
    void SetOrder(int _order);

    // b+ tree 叶子节点分配后都会将阶指定为一个节点所能够容纳的最大值，所以这里需要reset一下，以tree的阶为准
    void ReSetPageOrder(BPlusTreePage *);

    // Print this B+ tree to stdout using a simple command-line
    std::string ToString(bool verbose = false);

    // void Show() const;
    // void PrintSingleNode(BPlusTreePage *) const;

private:
    class Checker {
    public:
        explicit Checker(BufferPoolManager *b) : buffer(b) {}
        ~Checker() {
        assert(buffer->Check());
        }
    private:
        BufferPoolManager *buffer;
    };

    void StartNewTree(const KeyType &key, const ValueType &value);

    bool InsertIntoLeaf(const KeyType &key, const ValueType &value,
                        Transaction *transaction = nullptr);

    void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key,
                            BPlusTreePage *new_node,
                            Transaction *transaction = nullptr);

    template <typename N> 
    N *Split(N *node);

    template <typename N>
    bool CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr);

    /*
    template <typename N>
    bool Coalesce(N *&neighbor_node, N *&node,
                    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
                    int index, Transaction *transaction = nullptr);
    */
    template <typename N>
    void Coalesce(N *neighbor_node, N *node,
                    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent,
                    int index, Transaction *transaction = nullptr);

    template <typename N> 
    void Redistribute(N *neighbor_node, N *node, int index);

    bool AdjustRoot(BPlusTreePage *node);

    void UpdateRootPageId(bool insert_record = false);

    // unlock all parents
    void UnlockUnpinPages(Operation op, Transaction *transaction);

    template <typename N>
    bool isSafe(N *node, Operation op);

    inline void lockRoot() { mutex_.lock(); }
    inline void unlockRoot() { mutex_.unlock(); }

    // member variable
    std::string index_name_;  // b+tree是为index服务的，比如说为数据库的哪一个key去建立索引
    std::mutex mutex_;                       // protect `root_page_id_` from concurrent modification
    static thread_local bool root_is_locked; // root is locked?
    page_id_t root_page_id_;
    BufferPoolManager *buffer_pool_manager_;
    KeyComparator comparator_;
    /**
     * @brief 实际上 B+tree 在每一个节点中能够容纳的k的个数，b+tree的阶一般是节点容量的 1/2 或 2/3，这个秩的定义是有的。在节点中，每一个节点的 header 部分有存
     * 有点奇怪，秩应该是 b+ tree 的性质，而不是节点的性质哦
     * 秩限制的是key的数量
     */
    int order = 0;
};

} // namespace cmudb