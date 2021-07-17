/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb
{
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
// 每次new一个页面后需要自己调用这个函数进行初始化
template <typename KeyType, typename ValueType, typename KeyComparator>
void 
BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::Init(
    page_id_t page_id, page_id_t parent_id)
{
    // set page type
    SetPageType(IndexPageType::INTERNAL_PAGE);
    // set current size: 1 for the first invalid key
    // SetSize(1);
    SetValueSize(1);
    // set page id
    SetPageId(page_id);
    // set parent id
    SetParentPageId(parent_id);
    /**
     * @brief set max page size, header is 24bytes
     * sizeof(BPlusTreeInternalPage) is the header
     */
    int size = (PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / (sizeof(KeyType) + sizeof(ValueType));
    SetMaxCapacity(size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::KeyAt(int index) const
{
    // replace with your own code
    assert(0 <= index && index < GetValueSize());
    return array[index].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::SetKeyAt(int index, const KeyType &key)
{
    assert(0 <= index && index < GetValueSize());
    array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 * 中间节点才有这个方法，叶子节点无
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
int BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::ValueIndex(const ValueType &value) const
{
    for (int i = 0; i < GetValueSize(); ++i) { if (array[i].second == value) { return i; } }
    return GetValueSize();
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::ValueAt(int index) const
{
    assert(0 <= index && index < GetValueSize());
    return array[index].second;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::SetValueAt(int index, const ValueType &value)
{
    assert(0 <= index && index < GetValueSize());
    array[index].second = value;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 *
 * //sequential scan
 * ValueType dir = array[0].second;
 * for (int i = 1; i < GetSize(); ++i) {
 *   if (comparator(key, array[i].first) < 0) {
 *     break;
 *   }
 *   dir = array[i].second;
 * }
 * return dir;
 * 本例中返回 page_id
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::Lookup(
    const KeyType &key, const KeyComparator &comparator) const
{
    /* 要查找的key小于第一个或大于最后一个，就直接返回了，否则在一个 node 内部依然需要一些查找方法 */
    assert(GetValueSize() > 1);
    if (comparator(key, array[1].first) < 0){ return array[0].second; }
    else if (comparator(key, array[GetValueSize() - 1].first) >= 0) { 
        return array[GetValueSize() - 1].second; 
    }

    // 二分查找,节点内部的典型实现方式就是二分查找
    int low = 1, high = GetValueSize() - 1, mid;
    while (low < high && low + 1 != high)
    {
        mid = low + (high - low) / 2;
        if (comparator(key, array[mid].first) < 0) { high = mid; }
        else if (comparator(key, array[mid].first) > 0) { low = mid; }
        else { return array[mid].second; }
    }
    return array[low].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 *  root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
 *  root 是一个新 page
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::PopulateNewRoot(
    const ValueType &old_value, 
    const KeyType &new_key, 
    const ValueType &new_value)
{
    // must be an empty page
    // old是左边的，new是新分裂的，该类的 this 是新的 root
    // 在 init 的时候已经 set 过 1 了 for 无效的一个中间节点
    assert(GetValueSize() == 1);
    array[0].second = old_value;
    array[1] = {new_key, new_value};
    IncreaseValueSize(1);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 * 就是一个非常简单的 insert 操作，有限制保证叶子节点与中间节点的秩是与树保持一致的
 * 叶子节点中的 kv 对，v 表示的是大于等于 K 的那部分
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
int BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::InsertNodeAfter(
    const ValueType &old_value, 
    const KeyType &new_key, 
    const ValueType &new_value)
{
    // 从后向前扫
    for (int i = GetValueSize(); i > 0; --i) {
        if(array[i-1].second == old_value){
            array[i] = {new_key, new_value};
            IncreaseValueSize(1);
            break;
        }
        array[i] = array[i-1];
    }
    assert(GetValueSize() <= GetOrder() + 1);  // 这里采用了先 insert 再 split 的策略
    return GetValueSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * 中间节点的 平均对半分 函数
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager)
{
    // node MoveHalfTo recipient
    // recipient 是扩展出的中间节点，需要把自己匀一半出去的就是当前 node
    // 依然是新节点多一个，完事后会将新节点的第一个 k push 到父节点中
    // 怕是不能直接均的，直接均完之后，新节点的kv都是有效的 但是要上推第一个 key 到上一层，所以依然符合中间节点的 kv 布局
    //      感觉作者做的确实有些麻烦了，写成了我看不懂的样子
    // ...
    // ...
    // 如果是奇数，half 是一个向下取整的数
    // 我想起来之前自己做的充值返利过程了，(a+1)/2 能保证向上取整
    // 如果 a 是奇数，则结果是大的那个，如果 a 是偶数，对结果是没有影响的

    /**
     * @brief 上面关于分配的方法全部都是 bullshit, 理解错了我丢
     * 这么个 kv 分配方法会导致分配不均匀，最终导致 B+tree 的错误
     */
    // auto more_half = (GetValueSize()+1)/2;
    // auto less_half = GetValueSize() - more_half;

    assert(GetValueSize()-1 > 0);
    int keysize = GetValueSize() - 1;  // 当前这个key的size应该是和秩一样的

    // 首先对 key 对半分，如果不平均，则后面这个多取一个。在后面多取一个的情况下，前面的还有一个无效 key，所以要么 前面的数量大 1，要么前后一样大
    // more half 代表的是一个数量，是后半段 kv 对的数量
    // less half 代表的也是一个数量，是前半段 kv 对的数量，已经包含了无效 key
    auto more_half = (keysize+1)/2;
    auto less_half = GetValueSize() - more_half;

    // 将后半部分 copy 到新 node 中，应该是 copy 多的那部分
    // recipient->CopyHalfFrom(array + GetSize() - half, half, buffer_pool_manager);
    recipient->CopyHalfFrom(array + less_half, more_half, buffer_pool_manager);  // 这个就可以写的非常顺

    /**
     * @brief  中间节点完成了分裂，recipient 是一个 new node
     * copy 了 old node 中的孩子来 new node，所以这些 new node 的孩子节点的父节点发生了变化，修改之
     * 更新孩子节点的父节点id
     * 这里依旧在原节点中计算
     * 
     * 前半部可能有节点是没有父节点的，也要处理一下
     * 沃日，这个  more half 这个地方有可能会修改错 page index
     *      蛋疼哦
     *  后半部分确实是 less half 开始的，而非 more half
     */
    for (auto index = less_half; index < GetValueSize(); ++index)
    {
        auto *page = buffer_pool_manager->FetchPage(ValueAt(index));
        if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CopyLastFrom"); }
        auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
        child->SetParentPageId(recipient->GetPageId());  // 这里相当于set了new_node的parent id
        // assert(child->GetParentPageId() == recipient->GetPageId());
        buffer_pool_manager->UnpinPage(child->GetPageId(), true);
    }

    /**
     * @brief
     * 这个是中间节点 
     * 解决一下前半部分的问题
     */
    for(auto _index = 0; _index < less_half; ++_index) 
    {
        auto *page = buffer_pool_manager->FetchPage(ValueAt(_index));
        if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CopyLastFrom"); }
        auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
        // 失联的部分自己补一下, 这个 child 有可能是一个没有父节点的 new_node
        if (child->GetParentPageId()<0) { 
            child->SetParentPageId(GetPageId());
            // std::printf("triggrt\n"); 
        }
        buffer_pool_manager->UnpinPage(child->GetPageId(), true);
    }

    IncreaseValueSize(-1 * more_half);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::CopyHalfFrom(
    MappingType *items, 
    int size,
    BufferPoolManager *buffer_pool_manager)
{
    assert(!IsLeafPage() && GetValueSize() == 1 && size > 0);
    for (int i = 0; i < size; ++i) { array[i] = *items++;}
    IncreaseValueSize(size - 1);  // 中间节点在初始化的时候初始 size=1，这里必须少一个1才是正确的
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 *
 * 就是简单的直接删除的操作
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::Remove(int index)
{
    assert(0 <= index && index < GetValueSize());
    for (int i = index; i < GetValueSize() - 1; ++i) { array[i] = array[i + 1]; }
    IncreaseValueSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::RemoveAndReturnOnlyChild()
{
    IncreaseValueSize(-1);
    assert(GetValueSize() == 1);
    return ValueAt(0);
}

/*****************************************************************************
 * MERGE 与 redistribution U1S1 写的还是蛮清晰的
 *****************************************************************************/

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relevant key & value pair in its parent page.
 *
 * 中间节点的 merge 操作，等一下自己就没了。自己会被删除掉
 *
 * 有过一个 bug，是因为无法确定 当前 node 与 recipient 的关系
 * 原作者这个意思看来，只有右边的向左边 靠拢 这一种选择
 * 但是应该左右 merge 应该都是可以的，但是始终向左好像还要顺畅一些
 * 我该用哪种方式呢
 *
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::MoveAllTo(
    BPlusTreeInternalPage *recipient, 
    int index_in_parent,
    BufferPoolManager *buffer_pool_manager)
{
    // 首先先获得父节点页面
    auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while MoveAllTo"); }
    auto *parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());  // 这个 parent 是中间节点的 parent
   
    // 被合并的 node 的第一个 key 是无效的，被合并之后就不是第一个 key 了，所以需要 update 成一个有效的 key
    // 相当于有一个 key 下沉的动作，之后这个 父节点中的 key 会被删除掉
    SetKeyAt(0, parent->KeyAt(index_in_parent));

    if(parent->ValueAt(index_in_parent) != GetPageId()){
        std::printf(
            "debug info: index_in_parent: %d, first:%d, second:%d\n", 
            index_in_parent, 
            parent->ValueAt(index_in_parent), 
            GetPageId());
    }

    assert(parent->ValueAt(index_in_parent) == GetPageId());
    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
    recipient->CopyAllFrom(array, GetValueSize(), buffer_pool_manager);

    // 更新孩子节点的父节点id
    for (auto index = 0; index < GetValueSize(); ++index)
    {
        auto *page = buffer_pool_manager->FetchPage(ValueAt(index));
        if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CopyLastFrom"); }
        auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
        child->SetParentPageId(recipient->GetPageId());
        assert(child->GetParentPageId() == recipient->GetPageId());
        buffer_pool_manager->UnpinPage(child->GetPageId(), true);
    }
}

/**
 * @brief 中间节点的 copy 操作
 * @tparam KeyType 
 * @tparam ValueType 
 * @tparam KeyComparator 
 * @param  items            desc
 * @param  size             desc
 * @param  buffer_pool_managerdesc
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::CopyAllFrom(
    MappingType *items, 
    int size,
    BufferPoolManager *buffer_pool_manager)
{
    if(GetValueSize() + size > GetMaxValueSize()) {
        std::printf(
            "copy all from error, size:%d, add:%d, max:%d\n", 
            GetValueSize(), size, GetMaxValueSize()); 
        BackTracePlus();
    }
    assert(GetValueSize() + size <= GetMaxValueSize());
    int start = GetValueSize();
    for (int i = 0; i < size; ++i) { array[start + i] = *items++; }
    IncreaseValueSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relevant key & value pair in its parent page.
 * 
 * 叶子节点 merge 之后才会触发中间节点的 delete，进而引发重新分配等问题
 * 
 * 需要将自己第一个有效的 kv 拿出来
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager)
{
    assert(GetValueSize() > 1);

    MappingType pair{KeyAt(1), ValueAt(0)};  // 这个 key 要用来替换父节点那个拿下来的值，要重新退一个意思就是

    page_id_t child_page_id = ValueAt(0);  // 等下 child_page_id  的爸爸就要变了
    SetValueAt(0, ValueAt(1));
    Remove(1);  // remove操作会整体前移

    // 其实就是把第一个全部都备份下来了么
    recipient->CopyLastFrom(pair, buffer_pool_manager);

    // 更新孩子节点的父节点id
    auto *page = buffer_pool_manager->FetchPage(child_page_id);
    if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CopyLastFrom"); }
    auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
    child->SetParentPageId(recipient->GetPageId());

    assert(child->GetParentPageId() == recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
}

/**
 * @brief 对于接受者来说应该是一个追加的过程
 * @tparam KeyType 
 * @tparam ValueType 
 * @tparam KeyComparator 
 * @param  pair             desc
 * @param  buffer_pool_managerdesc
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::CopyLastFrom(
    const MappingType &pair, 
    BufferPoolManager *buffer_pool_manager)
{
    assert(GetValueSize() + 1 <= GetMaxValueSize());

    auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CopyLastFrom"); }
    auto parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());

    auto index = parent->ValueIndex(GetPageId());
    auto key = parent->KeyAt(index + 1);  // 这个key是之前split的时候推上去的key

    array[GetValueSize()] = {key, pair.second};
    IncreaseValueSize(1);

    parent->SetKeyAt(index + 1, pair.first);

    // 这个关系确实有点复杂哦
    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relevant key & value pair in its parent page.
 * 要把最后一个贡献出来
 * parent_index 是接收者在父节点中的 index
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, 
    int parent_index,
    BufferPoolManager *buffer_pool_manager)
{
    assert(GetValueSize() > 1);
    IncreaseValueSize(-1);

    MappingType pair = array[GetValueSize()];
    page_id_t child_page_id = pair.second;  // 这个孩子的爸爸要变了

    recipient->CopyFirstFrom(pair, parent_index, buffer_pool_manager);

    // 更新孩子节点的父节点id
    auto *page = buffer_pool_manager->FetchPage(child_page_id);
    if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CopyLastFrom"); }
    auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
    child->SetParentPageId(recipient->GetPageId());

    assert(child->GetParentPageId() == recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
}

/**
 * @brief 
 * @tparam KeyType 
 * @tparam ValueType 
 * @tparam KeyComparator 
 * @param  pair             pair 是要贡献者的最后一个
 * @param  parent_index     接收 node 在父节点中对应的 index
 * @param  buffer_pool_managerdesc
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::CopyFirstFrom(
    const MappingType &pair, 
    int parent_index,
    BufferPoolManager *buffer_pool_manager)
{
    assert(GetValueSize() + 1 < GetMaxValueSize());

    auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CopyFirstFrom"); }
    auto parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());

    auto key = parent->KeyAt(parent_index);  // 备份一下
    parent->SetKeyAt(parent_index, pair.first);  // 直接修改掉

    InsertNodeAfter(array[0].second, key, array[0].second);
    array[0].second = pair.second;

    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue, BufferPoolManager *buffer_pool_manager)
{
    // 就是子节点入队的操作
    for (int i = 0; i < GetValueSize(); i++)
    {
        auto *page = buffer_pool_manager->FetchPage(array[i].second);
        if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while printing"); }
        auto *child = reinterpret_cast<BPlusTreePage *>(page->GetData());
        // std::printf("p id is %d and curr id is %d\n", child->GetParentPageId(), GetPageId());
        // assert(child->GetParentPageId() == GetPageId());
        queue->push(child);
    }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
std::string BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::ToString(bool verbose) const
{
    if (GetValueSize() == 0) {return "";}
    std::ostringstream os;
    if (verbose) {os << "[" << GetPageId() << ":" << GetParentPageId() << "] ———— "; }

    int entry = 0; // verbose ? 0 : 1;
    int end = GetValueSize(); // 包括了第一个的无效 k，所以这个 value size 最小得是2
    // std::cout << "internal end is " << end  << std::endl;
    bool first = true;

    while (entry < end)
    {
        if (first) {first = false;}
        else { os << " "; }
        os << std::dec << " " << array[entry].first.ToString();
        if (verbose) { os << "(" << array[entry].second << ")"; }  // 中间节点的 second 一定是一个 page id
        ++entry;
        os << " ";
    }
    // std::cout << os.str() << std::endl;
    return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;

} // namespace cmudb
