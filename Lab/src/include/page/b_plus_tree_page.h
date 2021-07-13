/**
 * b_plus_tree_page.h
 *
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 24 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 * | ParentPageId (4) | PageId(4) |
 * ----------------------------------------------------------------------------
 */

#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "index/generic_key.h"

namespace cmudb {

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS                                               \
    template <typename KeyType, typename ValueType, typename KeyComparator>

// define page type enum
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

/**
 * @brief 均为中间节点与叶子节点的公共方法以及属性
 * 我才发现，BPlusTreePage 是非模板的类
 */
class BPlusTreePage 
{
public:
    bool IsLeafPage() const;  /* 是否叶子节点 */
    bool IsRootPage() const;  /* 是否 root 节点 */
    void SetPageType(IndexPageType page_type);  /* helper */

    /* 有几个函数写成虚函数，可能会造成一些冗余，先写起来再说，叶子节点可能要补一些实现，但是表示的是 未实现 */
    // virtual KeyType KeyAt(int index) const;
    // virtual int KeyIndex(const KeyType &key, const KeyComparator &comparator) const;

    // 彻底修改完后删除
    int GetSize() const { return size_; }
    void SetSize(int size) { size_ = size; }
    void IncreaseSize(int amount) { size_ += amount; }
    int GetMaxSize() const { return max_size_; }
    void SetMaxSize(int max_size) { max_size_ = max_size; }

    int GetMaxCapacity() const { return max_kv_capacity_; }
    void SetMaxCapacity(int capacity_) { max_kv_capacity_ = capacity_; }

    // 主要是计算有些内容的时候，用节点自己的秩方便一些，尽管节点的计算都是在 tree 下做的，但是这个秩我希望只 set 一次
    int GetOrder() const { return real_order_; }
    void SetOrder(int order_) { real_order_ = order_; }

    // int GetMinSize() const;

    page_id_t GetParentPageId() const;
    void SetParentPageId(page_id_t parent_page_id);

    page_id_t GetPageId() const;
    void SetPageId(page_id_t page_id);

    void SetLSN(lsn_t lsn = INVALID_LSN);

    void SetLayerId(int _layer) { layer=_layer; }
    int GetLayerId() const { return layer; }

private:
    // member variable, attributes that both internal and leaf page share
    IndexPageType page_type_;
    lsn_t lsn_;  // log sequence number
    /**
     * @brief size均指的是key的数量
     * kv的第一个k是不用的
     * 这里其实有一个tips，中间节点与叶子节点的一个区别
     *  由于 header的存在，nextpageid 保存在了元数据中，不占叶子节点的kv数量，所以 kv 是实际使用的数量
     *      即 kv 的数量代表的是 M-1，即 秩-1
     *  在中间节点中，kv有一个总量，但是k与v差一个，所以 kv 的这值代表的是 秩，因为最后要弃用的是第一个kv的k，所以这个 size 代表的是 秩
     * 忽然就觉得有点蛋疼了这里
     * 叶子节点的所有kv都能拿来用，v最大是 秩-1。如果就直观的用 kv 对来表示的话，叶子节点中的 size 是 key 的数量
     * 中间节点的第一个kv的key需要失效，v的最大数量就是秩。而中间节点的 size 是 value 的数量
     * 核心就是叶子节点的 key 可以全部使用，最后那个成 list 的 V 被放到了 header 的 meta 数据中
     * size 在叶子节点与中间节点的含义是不同的，所以 size 这个量也放到各自的派生类中定义
     */
    int max_kv_capacity_;  // 存储空间上能够容纳的 kv 的最大数量
    int real_order_;  // 真实的b+tree的秩
    page_id_t parent_page_id_;
    page_id_t page_id_;
    int layer;  // b+ tree 节点所处的层号

    // 彻底修改完后删除
    int size_;
    int max_size_;
};

} // namespace cmudb
