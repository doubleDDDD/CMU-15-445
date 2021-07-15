/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "common/logger.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb
{

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 * b+ tree 的每一个 node 除了 kv 之外，还有一个 header，可以被视作是 meta 数据
 * 不要用那个抽象的 B+ tree 示意图去理解代码，示意图真的是高度抽象的
 * 代码的实现是有区别的，发现作者写的这个也不是一个优美的做法
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void
BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::Init(
    page_id_t page_id, page_id_t parent_id)
{
    // set page type
    SetPageType(IndexPageType::LEAF_PAGE);
    // SetSize(0);
    SetKeySize(0);  // 中间节点是需要 set 1 for 第一个无效的节点
    // set page id
    SetPageId(page_id);
    // set parent id
    SetParentPageId(parent_id);
    // set next page id
    SetNextPageId(INVALID_PAGE_ID);

    // set max capacity
    int size = (PAGE_SIZE - sizeof(BPlusTreeLeafPage)) / (sizeof(KeyType) + sizeof(ValueType));
    SetMaxCapacity(size);
}

/**
 * Helper methods to set/get next page id
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
page_id_t BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::GetNextPageId() const
{
    return next_page_id_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::SetNextPageId(page_id_t next_page_id)
{
    next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * b+tree节点中的 k 是按照升序排列的
 *  如果给定一个key，想要判断其位置，假设这个key是比较大的，那么key在线性比较的过程中
 *  一定是key会大于前面的值，从中间的某一个值开始小
 * 给定key返回index
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
int
BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const
{
    for (int i = 0; i < GetKeySize(); ++i) {
        if (comparator(key, array[i].first) <= 0) { return i; }
    }
    return GetKeySize();
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 * 给定index，返回key
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::KeyAt(int index) const
{
    // replace with your own code
    assert(0 <= index && index < GetKeySize());
    return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 * 给定 index，返回 value, 在本例中就是指向 数据库文件 page 的指针
 * 迭代器遍历的过程中可能会使用到
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
const MappingType &BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::GetItem(int index)
{
    // replace with your own code
    assert(0 <= index && index < GetKeySize());
    return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 * 要注意这个已经是叶子节点的方法了，确定叶子节点 以及叶子节点能否insert 要调用者来确定，但是讲道理在调用栈上都要 check
 * 这里并没有判断叶子节点能够新增element，所以应该是调用者来判断叶子节点能够继续添加的，否则需要split
 * 叶子节点的kv都是有直接对应关系的 (不存在那个错位的问题 )，只是最后一个不用，最后一个kv中，k是无效的，v指向下一个叶子节点
 * 这里就是一个简单的按顺序插入，其它什么都不需要 care
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
int BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::Insert(
    const KeyType &key, const ValueType &value,
    const KeyComparator &comparator)
{
    // 为空或者要插入的值比当前最大的还大
    // 真的是底层写好之后，上面的逻辑确实是在调包
    // 叶子节点 getsize 获得的是 k 的数量，即比 秩小一 的那个值
    if (GetKeySize() == 0 || comparator(key, KeyAt(GetKeySize() - 1)) > 0) {
        // 当前叶子节点为空 或 insert 的大于该节点中的最大 key
        // c++ 11 就可以用花括号来为 std::pair 初始化，类型转换构造函数，std::make_pair 不是必须的
        // std::pair(key, value);
        array[GetKeySize()] = {key, value};
    }

    // 要插入的值当前最小的还小
    else if (comparator(key, array[0].first) < 0) {
        // 整体向后搬一个 slot, 这些均为内存操作
        memmove(
            (void *)(array + 1), 
            (void *)array, 
            static_cast<size_t>(GetKeySize() * sizeof(MappingType)));
        array[0] = {key, value};
    } else {
        // 要插入的位置是一个居中的位置，因为是排序的key，所以采用二分查找
        int low = 0, high = GetKeySize() - 1, mid;
        while (low < high && low + 1 != high)
        {
            mid = low + (high - low) / 2;
            if (comparator(key, array[mid].first) < 0) { high = mid; }
            else if (comparator(key, array[mid].first) > 0) { low = mid; }
            else {
                // 由于只支持不重复的key，所以不应当执行到这
                assert(0);
            }
        }
        // move 剩下的部分，同样的这也都是内存操作，仅仅是使 page dirty
        memmove(
            (void *)(array + high + 1), 
            (void *)(array + high), 
            static_cast<size_t>((GetKeySize() - high) * sizeof(MappingType)));

        array[high] = {key, value};
    }

    IncreaseKeySize(1);  // b+tree 叶子节点中增加了一个元素
    assert(GetKeySize() <= GetMaxCapacity());
    // assert(GetKeySize() < GetOrder()); 这里是有可能超过，我打算先 insert 再 split
    return GetKeySize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::MoveHalfTo(
    BPlusTreeLeafPage *recipient,  __attribute__((unused)) BufferPoolManager *buffer_pool_manager)
{
    /**
     * @brief recipient 是 new node，即 new page
     * 就是b+ tree叶子节点的分裂过程
     * 这里与 旧金山大学数据结构可视化的实现相一致
     * 将多的后半段copy到L2
     * 
     * 函数调用到这里的时候，新的 key 还没有 insert, 所以现在 key 的数量应该是 秩-1
     * 如果秩是3, 则叶子节点中的key的数量达到2，并且要 insert 一个新 key 的时候，会调用到这里来
     */
    assert(GetKeySize() > 0);

    int size = GetKeySize() / 2;  // 这是向下取整的，如果阶是3，则这里的 3/2=1
    // src 是指向 MappingType( std::pair ) 的指针，所以 +1 -1 这种操作都是针对 std::pair 的
    // MappingType *src = array + GetKeySize() - size;

    MappingType *src = array + size;  // 前少半部分保留在原 node 中的kv
    int movesize = GetKeySize() - size;
    recipient->CopyHalfFrom(src, movesize);
    IncreaseKeySize(-1 * movesize);  // 并没有去清理不需要的kv，仅仅是把游标改了
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::CopyHalfFrom(
    MappingType *items, int size)
{
    assert(IsLeafPage() && GetKeySize() == 0);
    for (int i = 0; i < size; ++i)
    {
        array[i] = *items++;  // 这里是值传递，不存在传丢了的情况
    }
    IncreaseKeySize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 * 叶子结点的二分查找，返回的就是最终结果所在的页
 * b+tree 本身就只是一个 index，类似于 LRU，是围绕核心的真实数据结构的辅助数据结构
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::Lookup(
    const KeyType &key, ValueType &value,
    const KeyComparator &comparator) const
{
    if (GetKeySize() == 0 || comparator(key, KeyAt(0)) < 0 || comparator(key, KeyAt(GetKeySize() - 1)) > 0) { 
        return false; 
    }

    int low = 0, high = GetKeySize() - 1, mid;
    while (low <= high)
    {
        mid = low + (high - low) / 2;
        if (comparator(key, KeyAt(mid)) > 0)
        {
            low = mid + 1;
        }
        else if (comparator(key, KeyAt(mid)) < 0)
        {
            high = mid - 1;
        }
        else
        {
            value = array[mid].second;
            return true;
        }
    }
    return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion  被删除 key 之后的内容要整体前移
 * @return   page size after deletion
 * 仅仅是单纯的删除一个 key
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
int BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator)
{
    if (GetKeySize() == 0 || comparator(key, KeyAt(0)) < 0 || 
        comparator(key, KeyAt(GetKeySize() - 1)) > 0) { 
        // 要被删除的 key 不在这个叶子节点中
        return GetKeySize();
    }

    int low = 0, high = GetKeySize() - 1, mid;
    while (low <= high) {
        mid = low + (high - low) / 2;
        if (comparator(key, KeyAt(mid)) > 0) { low = mid + 1; }
        else if (comparator(key, KeyAt(mid)) < 0) { high = mid - 1; }
        else {
            // 删除节点
            memmove(
                (void *)(array + mid), 
                (void *)(array + mid + 1), 
                static_cast<size_t>((GetSize() - mid - 1) * sizeof(MappingType)));
            IncreaseSize(-1);
            break;
        }
    }

    // 返回减少后的节点中的 key 的数量
    return GetKeySize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
    MoveAllTo(BPlusTreeLeafPage *recipient, int, BufferPoolManager *)
{
  recipient->CopyAllFrom(array, GetKeySize());
  recipient->SetNextPageId(GetNextPageId());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
    CopyAllFrom(MappingType *items, int size)
{
  assert(GetKeySize() + size <= GetMaxSize());
  auto start = GetKeySize();
  for (int i = 0; i < size; ++i)
  {
    array[start + i] = *items++;
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relevant key & value pair in its parent page.
 * 
 *  将当前 node 的第一个 kv 给到 recipient
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager)
{
    MappingType pair = GetItem(0);
    IncreaseKeySize(-1);  // 拿走第一个
    // 整体前移
    memmove(array, array + 1, static_cast<size_t>(GetKeySize() * sizeof(MappingType)));
    recipient->CopyLastFrom(pair);

    // update parent's kv
    auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while MoveFirstToEndOf"); }

    // decltype 这个东西的含义在哪里呢，也算是学习到了，编译时推导，泛型编程是一定要在编译时确定类型的
    auto parent =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()), KeyComparator> *>(page->GetData());

    // B+tree 的 key 是大于等于左边，小于右边的
    // recipient 以及 node 共同对应的那个 kv 对的 k 要发生改变
    // 小于目标 k 的 k 位于 recipient 中，而大于等于目标 k 的 k 位于 node 中
    // 由于错位的关系，接收多余kv的是排在前面的node，所以对应需要修改的 k 就是 value 就是当前 pageid 的

    // parent->SetKeyAt(parent->ValueIndex(GetPageId()), pair.first);  这里我觉得作者写的有点问题
    parent->SetKeyAt(parent->ValueIndex(GetPageId()), array[0].first);

    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::CopyLastFrom(const MappingType &item)
{
    assert(GetKeySize() + 1 <= GetMaxKeySize());
    array[GetKeySize()] = item;
    IncreaseKeySize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relevant key & value pair in its parent page.
 * 
 * 被借的 node 排在前面，接收的 recipient 排在后面，要找指向 recipient 的 v 的 k，并且将其减小
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager)
{
    // 父节点的 index 在这里给到了
    MappingType pair = GetItem(GetKeySize() - 1);  // 把最后一个搞走
    IncreaseKeySize(-1);
    recipient->CopyFirstFrom(pair, parentIndex, buffer_pool_manager);
}

/**
 * @brief 叶子节点接收了一个新的 kv 在自己的最前端，指向自己的kv对中的key要做对应的减小
 * @tparam KeyType 
 * @tparam ValueType 
 * @tparam KeyComparator 
 * @param  item             desc
 * @param  parentIndex      desc
 * @param  buffer_pool_managerdesc
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::CopyFirstFrom(
    const MappingType &item,
    int parentIndex,
    BufferPoolManager *buffer_pool_manager)
{
    assert(GetKeySize() + 1 <= GetMaxKeySize());
    memmove((void *)(array + 1), (void *)array, GetKeySize() * sizeof(MappingType));
    IncreaseKeySize(1);

    array[0] = item;
    auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CopyFirstFrom"); }

    auto parent =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()), KeyComparator> *>(page->GetData());
    
    // 指向 node 的 key 的含义应该是所有大于等于 item.first 的 key
    parent->SetKeyAt(parentIndex, item.first);

    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*****************************************************d**********************
 * DEBUG
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
std::string BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::ToString(bool verbose) const
{
    if (GetKeySize() == 0) { return ""; }
    std::ostringstream stream;
    if (verbose) {stream << "[" << GetPageId() << ":" << GetParentPageId() << "] ———— "; }
    int entry = 0;
    int end = GetKeySize();

    // std::cout << "leaf end is " << end  << std::endl;

    bool first = true;

    while (entry < end)
    {
        if (first) { first = false;}
        else { stream << " "; }
        stream << std::dec << " " << array[entry].first;
        if (verbose) { stream << " (" << array[entry].second << ")";}  // 叶子节点的值 是 pageid+slotid 密集索引
        ++entry;
        stream << " ";
    }

    // std::cout << stream.str() << std::endl;
    return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
