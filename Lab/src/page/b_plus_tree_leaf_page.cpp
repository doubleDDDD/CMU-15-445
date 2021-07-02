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
 * 叶子节点相比于中间节点好像要简单一些
 *****************************************************************************/

/**
 * b+ tree 的每一个 node 除了 kv 之外，还有一个 header，可以被视作是meta数据
 *  这个在实际实现过程中与看数据结构示意图的过程中还是有差异的
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::Init(
    page_id_t page_id, page_id_t parent_id)
{
    // set page type
    SetPageType(IndexPageType::LEAF_PAGE);
    // set current size: 1 for the first invalid key
    SetSize(0);
    // set page id
    SetPageId(page_id);
    // set parent id
    SetParentPageId(parent_id);
    // set next page id
    SetNextPageId(INVALID_PAGE_ID);

    // set max page size, header is 28bytes
    int size = (PAGE_SIZE - sizeof(BPlusTreeLeafPage)) /
                (sizeof(KeyType) + sizeof(ValueType));
    SetMaxSize(size);
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
    for (int i = 0; i < GetSize(); ++i)
    {
        // 在i之前的比较中，给定key都应该是比较大的那一个
        if (comparator(key, array[i].first) <= 0) { return i; }
    }
    return GetSize();
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
    assert(0 <= index && index < GetSize());
    return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 * 给定 index，返回 value,在本例中就是指向 数据库文件page 的指针
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
const MappingType &BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::GetItem(int index)
{
    // replace with your own code
    assert(0 <= index && index < GetSize());
    return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 * 要注意这个已经是叶子节点的方法了，确定叶子节点要调用者来搞定
 * 这里并没有判断叶子节点能够新增element，所以应该是调用者来判断叶子节点能够继续添加的，否则需要split
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
int BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::Insert(
    const KeyType &key, const ValueType &value,
    const KeyComparator &comparator)
{
    // 为空或者要插入的值比当前最大的还大
    // 真的是底层写好之后，上面的逻辑确实是在调包
    if (GetSize() == 0 || comparator(key, KeyAt(GetSize() - 1)) > 0)
    {
        array[GetSize()] = {key, value};
    }
    // 要插入的值当前最小的还小
    else if (comparator(key, array[0].first) < 0)
    {
        // 整体向后搬一个 slot
        memmove((void *)(array + 1), (void *)array, static_cast<size_t>(GetSize() * sizeof(MappingType)));
        array[0] = {key, value};
    }
    else
    {
        // 要插入的位置是一个居中的位置，因为是排序的key，所以采用log
        int low = 0, high = GetSize() - 1, mid;
        while (low < high && low + 1 != high)
        {
        mid = low + (high - low) / 2;
        if (comparator(key, array[mid].first) < 0)
        {
            high = mid;
        }
        else if (comparator(key, array[mid].first) > 0)
        {
            low = mid;
        }
        else
        {
            // 由于只支持不重复的key，所以不应当执行到这
            assert(0);
        }
        }
        memmove((void *)(array + high + 1), (void *)(array + high),
                static_cast<size_t>((GetSize() - high) * sizeof(MappingType)));
        array[high] = {key, value};
    }

    IncreaseSize(1);
    assert(GetSize() <= GetMaxSize());
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
    MoveHalfTo(BPlusTreeLeafPage *recipient,
               __attribute__((unused)) BufferPoolManager *buffer_pool_manager)
{
  assert(GetSize() > 0);

  int size = GetSize() / 2;
  MappingType *src = array + GetSize() - size;
  recipient->CopyHalfFrom(src, size);
  IncreaseSize(-1 * size);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
    CopyHalfFrom(MappingType *items, int size)
{
  assert(IsLeafPage() && GetSize() == 0);
  for (int i = 0; i < size; ++i)
  {
    array[i] = *items++;
  }
  IncreaseSize(size);
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
    if (GetSize() == 0 || comparator(key, KeyAt(0)) < 0 || comparator(key, KeyAt(GetSize() - 1)) > 0)
    {
        return false;
    }

    int low = 0, high = GetSize() - 1, mid;
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
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
int BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
    RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator)
{
  if (GetSize() == 0 || comparator(key, KeyAt(0)) < 0 ||
      comparator(key, KeyAt(GetSize() - 1)) > 0)
  {
    return GetSize();
  }

  int low = 0, high = GetSize() - 1, mid;
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
      // 删除节点
      memmove((void *)(array + mid), (void *)(array + mid + 1),
              static_cast<size_t>((GetSize() - mid - 1) * sizeof(MappingType)));
      IncreaseSize(-1);
      break;
    }
  }
  return GetSize();
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
  recipient->CopyAllFrom(array, GetSize());
  recipient->SetNextPageId(GetNextPageId());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
    CopyAllFrom(MappingType *items, int size)
{
  assert(GetSize() + size <= GetMaxSize());
  auto start = GetSize();
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
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
    MoveFirstToEndOf(BPlusTreeLeafPage *recipient,
                     BufferPoolManager *buffer_pool_manager)
{
  MappingType pair = GetItem(0);
  IncreaseSize(-1);
  memmove(array, array + 1, static_cast<size_t>(GetSize() * sizeof(MappingType)));

  recipient->CopyLastFrom(pair);

  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr)
  {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while MoveFirstToEndOf");
  }

  auto parent =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),
                                             KeyComparator> *>(page->GetData());

  
  parent->SetKeyAt(parent->ValueIndex(GetPageId()), pair.first);

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
    CopyLastFrom(const MappingType &item)
{
  assert(GetSize() + 1 <= GetMaxSize());
  array[GetSize()] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relevant key & value pair in its parent page.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
    MoveLastToFrontOf(BPlusTreeLeafPage *recipient, int parentIndex,
                      BufferPoolManager *buffer_pool_manager)
{
  MappingType pair = GetItem(GetSize() - 1);
  IncreaseSize(-1);
  recipient->CopyFirstFrom(pair, parentIndex, buffer_pool_manager);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
    CopyFirstFrom(const MappingType &item, int parentIndex,
                  BufferPoolManager *buffer_pool_manager)
{
  assert(GetSize() + 1 < GetMaxSize());
  memmove((void *)(array + 1), (void *)array, GetSize() * sizeof(MappingType));
  IncreaseSize(1);
  array[0] = item;

  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr)
  {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while CopyFirstFrom");
  }

  auto parent =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),
                                             KeyComparator> *>(page->GetData());

  
  parent->SetKeyAt(parentIndex, item.first);

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*****************************************************d**********************
 * DEBUG
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
std::string BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::
    ToString(bool verbose) const
{
  if (GetSize() == 0)
  {
    return "";
  }
  std::ostringstream stream;
  if (verbose)
  {
    stream << "[" << GetPageId() << "-" << GetParentPageId() << "]";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end)
  {
    if (first)
    {
      first = false;
    }
    else
    {
      stream << " ";
    }
    stream << std::dec << " " << array[entry].first;
    if (verbose)
    {
      stream << " (" << array[entry].second << ")";
    }
    ++entry;
    stream << " ";
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
