/**
 * index_iterator.h
 * For range scan of b+ tree
 * 这个迭代器的目的是为了范围查找 b+tree 的叶子节点
 * 所以迭代器对象所指向的对象就是 叶子结点
 * 即指向 叶子节点 的 指针
 * 因为只对符号 ++ 进行了重载，所以该迭代器仅仅支持单向的访问
 *      不支持 --，即不支持双向的访问
 *      不支持 +=i，即不支持随机访问
 * 但是该迭代器仅仅是为了实现 b+tree 叶子节点的范围查找，所以 ++ 的符号重载就足够了
 */

#pragma once

#include "page/b_plus_tree_leaf_page.h"
#include "buffer/buffer_pool_manager.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
    IndexIterator<KeyType, ValueType, KeyComparator>

template <typename KeyType, typename ValueType, typename KeyComparator>
class IndexIterator {
public:
    // you may define your own constructor based on your member variables
    IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *, int, BufferPoolManager *);

    ~IndexIterator();

    bool isEnd();

    const MappingType &operator*();

    IndexIterator &operator++();

private:
    // add your own private member variables here
    BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_;  // 指向 叶子节点
    int index_;  // 单 node 内的下标
    // 这相当于是有了2个指针啊，共同决定当前迭代到哪里了
    BufferPoolManager *buff_pool_manager_;
};

} // namespace cmudb
