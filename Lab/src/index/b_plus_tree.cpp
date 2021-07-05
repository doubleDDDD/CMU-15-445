/**
 * b_plus_tree.cpp
 */

#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"


/**
 * @brief 
 * 利用旧金山大学的数据结构可视化工具去理解了一下b+tree
 * b+tree有一个属性称为秩。这个是要提前预设的，
 */

namespace cmudb
{

template <typename KeyType, typename ValueType, typename KeyComparator>
BPlusTree<KeyType, ValueType, KeyComparator>::BPlusTree(
    const std::string &name,
    BufferPoolManager *buffer_pool_manager,
    const KeyComparator &comparator,
    page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) 
    {}

template <typename KeyType, typename ValueType, typename KeyComparator>
// 类内 static 的变量必须初始化，因为它不依赖于类对象的实例化
thread_local bool BPlusTree<KeyType, ValueType, KeyComparator>::root_is_locked = false;

/*
 * Helper function to decide whether current b+tree is empty
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool
BPlusTree<KeyType, ValueType, KeyComparator>::IsEmpty() const
{
    return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * 这个是区别于范围查询的，当然范围查询也是先有一个 point 查询，然后顺着找就可以了               
 * @return : true means key exists
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTree<KeyType, ValueType, KeyComparator>::GetValue(
    const KeyType &key, std::vector<ValueType> &result,
    Transaction *transaction)
{
    // 根据key找到叶子节点页面
    auto *leaf = FindLeafPage(key, false, Operation::READONLY, transaction);

    bool ret = false;
    if (leaf != nullptr)
    {
        ValueType value;
        if (leaf->Lookup(key, value, comparator_))
        {
            result.push_back(value);
            ret = true;
        }

        // 释放锁
        UnlockUnpinPages(Operation::READONLY, transaction);

        if (transaction == nullptr)
        {
            auto page_id = leaf->GetPageId();
            buffer_pool_manager_->FetchPage(page_id)->RUnlatch();
            buffer_pool_manager_->UnpinPage(page_id, false);
        }
    }
    return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * 核心实际上插入的应该是叶子节点的 kv
 * if current tree is empty, start new tree, update root page id and insert
 * entry
 *  直接在 root 节点的第1/2个kv对中insert本次的kv就完事了，这个时候的操作与 radix 树基本是没有任何差异的
 * otherwise insert into leaf page
 *  可能需要分裂节点甚至增加树的高度等等
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTree<KeyType, ValueType, KeyComparator>::Insert(
    const KeyType &key, const ValueType &value, Transaction *transaction)
{
    // 互斥锁
    {
        std::lock_guard<std::mutex> lock(mutex_);
        // 如果树为空就新建一棵树
        if (IsEmpty())
        {
            StartNewTree(key, value);
            return true;
        }
    }
    /**
     * @brief 离开上面的{}范围之后lock就会释放
     * 插入操作一定是在叶子节点插入的，
     */
    return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 *  即插入第一个节点
 *  b+tree也是持久性设备上的内容，在数据库的层面上，b+tree的节点位于一整个数据库 page 中
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::StartNewTree(
    const KeyType &key, const ValueType &value)
{
    /**
     * @brief 可以看到 disk manager 不会去 care 这个 page 是存数据库的内容还是存索引
     * 反正你就顺序的去加就可以了
     * 就目前看来，数据库的内容与index是位于同一个文件在
     * 在 page 这个粒度上，DBMS并没有去care这些内容在磁盘上是否要顺序存放
     */
    auto *page = buffer_pool_manager_->NewPage(root_page_id_);
    if (page == nullptr) {
        throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while StartNewTree"); 
    }
    // 第一个节点就是b+tree的root节点，刚分配好的，本质是增加了file的size
    auto root =
        reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
    /**
     * @brief Construct a new Update Root Page Id object
     * header page 是用来存放表或索引的元数据的
     * 所以在创建表或创建index的过程中，同样需要在 page header page 中注册自己的元数据
     */
    UpdateRootPageId(true);
    // b+tree 的node节点是内存中的一个page，除了kv之外还有一些元数据被保存在 page 首部
    // 初始化 size=0，但是 maxsize 就是所有能够容纳kv的数量
    root->Init(root_page_id_, INVALID_PAGE_ID);
    /**
     * @brief root节点的插入方法，value在本例中是一个指向 数据 page 的指针
     * 从这里可以看出来在b+tree的创建过程中，最开始root节点就是叶子节点
     */
    root->Insert(key, value, comparator_);

    // unpin 是为了支持多线程并发的
    buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 *  插入节点是有可能会造成 b+tree 的分裂的
 *  现在知道了，在一个b+tree中，所谓的指针就是 page number，难道没有搞一个 page 内的 offset 么，不知道开销是否大
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool
BPlusTree<KeyType, ValueType, KeyComparator>::InsertIntoLeaf(
    const KeyType &key, const ValueType &value, Transaction *transaction)
{
    // 先找到正确的叶子节点，在这个过程中，什么都不需要care，只要找到即可，最次就是在最左边或最右边
    // 找到之后检查能够正常的 insert
    auto *leaf = FindLeafPage(key, false, Operation::INSERT, transaction);
    if (leaf == nullptr) { return false; }

    ValueType v;
    // 如果树中已经有值了，就返回false
    if (leaf->Lookup(key, v, comparator_))
    {
        UnlockUnpinPages(Operation::INSERT, transaction);
        return false;
    }
    /**
     * @brief 如果已经直接调用了叶子节点的 insert 方法，那么意味着是能够顺利 insert 的，即由调用方在确定是否可以顺利insert
     * 实际上调用方就是 b+tree 的对象，是具有全局视角的一个对象，由它去 control 所有的节点 
     * if L has enough space, and done
     */
    if (leaf->GetSize() < leaf->GetMaxSize()) { leaf->Insert(key, value, comparator_); }
    else
    {
        /**
         * @brief 如果一个叶子节点的空间不够了，如果插入后就会超过预设的秩
         * 这个时候就必须要分裂叶子节点了，因为 b+tree 所要求的 k 的个数最小是秩的一半，最大不超过秩
         * 所以就是创建一个新的节点 L2，然后平均分配 L 中的 kv 到 L 与 L2
         * 然后L的父节点中也应该insert一个pointer指向L1,所以这里是有向父节点 insert k 的，父节点也有分裂的可能
         * 这里不同于B tree，B+tree 的中间节点保存的是key的一个copy，所以L以及L2的父节点要新增一个 copy 的 key
         * 如果父节点需要分裂，则需要将key向上copy，最终可能传递到root节点，那么b+tree则新增一层
         * 分裂如果导致不满足 B+tree 的限制条件后，则一直传递到父节点，父节点分裂后，可能导致层数的增大
         * L的后一半丢给了L2
         * 分裂完成后，实际上并未插入，这个实现把一个节点用满了，所以是没有办法先插入，再处理的
         *  如果我规定B+tree的秩就是最大的一半，那么是完全可以先 insert 后 check 并分裂的
         * 这个写法有点奇怪，之后我自己 change 其中一部分
         */
        auto *leaf2 = Split<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>(leaf);  /* 创建新 node，并完成一半的 copy */
        /**
         * @brief 在分裂一半的基础上+1基本是不可能超出范围的，这个写法不是很好，之后修改掉
         */
        if (comparator_(key, leaf2->KeyAt(0)) < 0) { 
            // 这个key应该插入到L
            leaf->Insert(key, value, comparator_); 
        } else { 
            // key应该被插入到L2中
            leaf2->Insert(key, value, comparator_); 
        }

        // 更新前后关系
        if (comparator_(leaf->KeyAt(0), leaf2->KeyAt(0)) < 0)
        {
            leaf2->SetNextPageId(leaf->GetNextPageId());
            leaf->SetNextPageId(leaf2->GetPageId());
        }
        else
        {
            leaf2->SetNextPageId(leaf->GetPageId());
        }

        // 将分裂的节点插入到父节点, 这里是有可能导致父节点分裂的，如果父节点分裂，则继续向上传递
        InsertIntoParent(leaf, leaf2->KeyAt(0), leaf2, transaction);
    }

    UnlockUnpinPages(Operation::INSERT, transaction);
    return true;
}

/*
 * Split input page and return newly created page.
 *  在insert的过程中，叶子节点与中间节点都有可能面临分裂的情况
 *  分裂是需要向上传递的，一般中间节点就能cover住，否则一旦分裂传递到root节点，则b+tree层数增大
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N>
N *BPlusTree<KeyType, ValueType, KeyComparator>::Split(N *node)
{
    page_id_t page_id;
    // 新创建 page 的过程实际上是磁盘文件++的过程，之前有说过，创建 index 与创建 table 是一样的，它们都是磁盘上的文件
    auto *page = buffer_pool_manager_->NewPage(page_id);
    if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while Split"); }

    auto new_node = reinterpret_cast<N *>(page->GetData());
    /* N可能是叶子节点也可能是中间节点，叶子节点与中间节点都有自己的init方法 */
    new_node->Init(page_id);

    /* 被分裂节点的movehalfto方法 */
    node->MoveHalfTo(new_node, buffer_pool_manager_);
    return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 *  可能导致递归的分裂过程
 * 一个调用过程的实例 InsertIntoParent(leaf, leaf2->KeyAt(0), leaf2, transaction);
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::InsertIntoParent(
    BPlusTreePage *old_node, const KeyType &key,
    BPlusTreePage *new_node, Transaction *transaction)
{
    if (old_node->IsRootPage())
    {
        // 这相当于把 root 节点分裂了，显然是需要增加树的高度的，copy一个L或L2中的key到新的root节点
        auto *page = buffer_pool_manager_->NewPage(root_page_id_);
        if (page == nullptr) {
            throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while InsertIntoParent");
        }

        assert(page->GetPinCount() == 1);
        auto root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
        root->Init(root_page_id_);
        // key是L2的第一个key, 实际上就是为新的root节点add第一个kv对,kv也仅仅是子节点中kv的copy
        root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

        old_node->SetParentPageId(root_page_id_);
        new_node->SetParentPageId(root_page_id_);

        // 这时需要更新根节点页面id
        UpdateRootPageId(false);
        // 这一次分裂搞出两个new page来，极具的扩展了B+tree
        buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
    }
    else {
        // 新增元素的父节点是非root节点的中间节点，有可能递归的向上传递，并且可能最终导致b+tree层数+1
        auto *page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
        if (page == nullptr) {
            throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while InsertIntoParent");
        }

        // internal是 L与L2 的父节点
        auto internal =
            reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
    
        // 如果父节点还有空间，父节点直接就cover住了，分裂无需继续向上传递
        if (internal->GetSize() < internal->GetMaxSize()) {
            // 这个insert函数会导致 kv 数组整体后移
            internal->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
            new_node->SetParentPageId(internal->GetPageId());
            buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
        }
        else {
            // internal 作为 L与L2 的父节点，现在已经无法再容纳多一个kv了，所以 internal 也需要分裂
            // 最复杂的情形，父节点也需要分裂，并且需要向上传递一次分裂，明显是一个递归的过程
            page_id_t page_id;
            // 这个page与internal是同一级的，应该要 copy internal 的 kv 到该new page。这个page最后直接干掉了，没有保留，这里依然只是多了一个分裂的page
            auto *page = buffer_pool_manager_->NewPage(page_id);
            if (page == nullptr) {
                throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while InsertIntoParent");
            }

            assert(page->GetPinCount() == 1);
            
            /* copy is a new internal node, only a 中间过程的节点 */
            auto *copy =
                reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());

            copy->Init(page_id);
            copy->SetSize(internal->GetSize());
            // 现在 copy 的 kv 对是空的

            for (int i = 1, j = 0; i <= internal->GetSize(); ++i, ++j)
            {
                /**
                 * @brief old node 指的是 internal 的左子节点
                 * 这里是在遍历 internal，就是为了将最新的 key 插入
                 */
                if (internal->ValueAt(i - 1) == old_node->GetPageId())
                {
                    // 在 internal 中之前应该是有一个指针是指向 old_node 的（即，有一个kv的v是 old node 的 pageid），new node 产生之后，现在还没有父节点
                    // 所以这个条件只会被触发一次，执行完毕之后在 copy 中会存在一个 kv pair
                    copy->SetKeyAt(j, key);
                    copy->SetValueAt(j, new_node->GetPageId());
                    ++j;
                }
                if (i < internal->GetSize())
                {
                    copy->SetKeyAt(j, internal->KeyAt(i));
                    copy->SetValueAt(j, internal->ValueAt(i));
                }
            }

            // i=0 的

            assert(copy->GetSize() == copy->GetMaxSize());
            // copy 分裂会导致一个新page的产生
            auto internal2 = Split<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>(copy);

            internal->SetSize(copy->GetSize() + 1);
            for (int i = 0; i < copy->GetSize(); ++i) {
                internal->SetKeyAt(i + 1, copy->KeyAt(i));
                internal->SetValueAt(i + 1, copy->ValueAt(i));
            }

            if (comparator_(key, internal2->KeyAt(0)) < 0)
            {
                new_node->SetParentPageId(internal->GetPageId());
            }
            else if (comparator_(key, internal2->KeyAt(0)) == 0)
            {
                new_node->SetParentPageId(internal2->GetPageId());
            }
            else
            {
                new_node->SetParentPageId(internal2->GetPageId());
                old_node->SetParentPageId(internal2->GetPageId());
            }

            buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

            buffer_pool_manager_->UnpinPage(copy->GetPageId(), false);
            buffer_pool_manager_->DeletePage(copy->GetPageId());

            // 继续递归了这里
            InsertIntoParent(internal, internal2->KeyAt(0), internal2);
        }

        buffer_pool_manager_->UnpinPage(internal->GetPageId(), true);
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::
    Remove(const KeyType &key, Transaction *transaction)
{
  if (IsEmpty())
  {
    return;
  }

  auto *leaf = FindLeafPage(key, false, Operation::DELETE, transaction);
  if (leaf != nullptr)
  {
    int size_before_deletion = leaf->GetSize();
    if (leaf->RemoveAndDeleteRecord(key, comparator_) != size_before_deletion)
    {
      if (CoalesceOrRedistribute(leaf, transaction))
      {
        transaction->AddIntoDeletedPageSet(leaf->GetPageId());
      }
    }
    UnlockUnpinPages(Operation::DELETE, transaction);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N>
bool BPlusTree<KeyType, ValueType, KeyComparator>::
    CoalesceOrRedistribute(N *node, Transaction *transaction)
{
  if (node->IsRootPage())
  {
    return AdjustRoot(node);
  }
  if (node->IsLeafPage())
  {
    if (node->GetSize() >= node->GetMinSize())
    {
      return false;
    }
  }
  else
  {
    if (node->GetSize() > node->GetMinSize())
    {
      return false;
    }
  }

  auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (page == nullptr)
  {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while CoalesceOrRedistribute");
  }
  auto parent =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                             KeyComparator> *>(page->GetData());
  int value_index = parent->ValueIndex(node->GetPageId());

  assert(value_index != parent->GetSize());

  int sibling_page_id;
  if (value_index == 0)
  {
    sibling_page_id = parent->ValueAt(value_index + 1);
  }
  else
  {
    sibling_page_id = parent->ValueAt(value_index - 1);
  }

  page = buffer_pool_manager_->FetchPage(sibling_page_id);
  if (page == nullptr)
  {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while CoalesceOrRedistribute");
  }

  // lab3
  page->WLatch();
  transaction->AddIntoPageSet(page);
  auto sibling = reinterpret_cast<N *>(page->GetData());
  bool redistribute = false;

  if (sibling->GetSize() + node->GetSize() > node->GetMaxSize())
  {
    redistribute = true;
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  }

  if (redistribute)
  {
    if (value_index == 0)
    {
      Redistribute<N>(sibling, node, 1);
    }
    return false;
  }

  bool ret;
  if (value_index == 0)
  {
    // lab3
    Coalesce<N>(node, sibling, parent, 1, transaction);
    transaction->AddIntoDeletedPageSet(sibling_page_id);
    ret = false;
  }
  else
  {
    Coalesce<N>(sibling, node, parent, value_index, transaction);
    ret = true;
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  return ret;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happened
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N>
void BPlusTree<KeyType, ValueType, KeyComparator>::
    Coalesce(N *neighbor_node, N *node,
             BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent,
             int index, Transaction *transaction)
{

  node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);

  parent->Remove(index);

  if (CoalesceOrRedistribute(parent, transaction))
  {
    transaction->AddIntoDeletedPageSet(parent->GetPageId());
  }
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N>
void BPlusTree<KeyType, ValueType, KeyComparator>::
    Redistribute(N *neighbor_node, N *node, int index)
{
  if (index == 0)
  {
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
  }
  else
  {
    auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    if (page == nullptr)
    {
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while Redistribute");
    }
    auto parent =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(page->GetData());
    int idx = parent->ValueIndex(node->GetPageId());
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);

    neighbor_node->MoveLastToFrontOf(node, idx, buffer_pool_manager_);
  }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTree<KeyType, ValueType, KeyComparator>::
    AdjustRoot(BPlusTreePage *old_root_node)
{
  // 如果删除了最后一个节点
  if (old_root_node->IsLeafPage())
  {
    if (old_root_node->GetSize() == 0)
    {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(false);
      return true;
    }
    return false;
  }

  // 删除了还有最后一个节点
  if (old_root_node->GetSize() == 1)
  {
    auto root =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(old_root_node);
    root_page_id_ = root->ValueAt(0);
    UpdateRootPageId(false);

    auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
    if (page == nullptr)
    {
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while AdjustRoot");
    }
    auto new_root =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(page->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
IndexIterator<KeyType, ValueType, KeyComparator> BPlusTree<KeyType, ValueType, KeyComparator>::
    Begin()
{
  KeyType key{};
  return IndexIterator<KeyType, ValueType, KeyComparator>(
      FindLeafPage(key, true), 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
IndexIterator<KeyType, ValueType, KeyComparator> BPlusTree<KeyType, ValueType, KeyComparator>::
    Begin(const KeyType &key)
{
  auto *leaf = FindLeafPage(key, false);
  int index = 0;
  if (leaf != nullptr)
  {
    index = leaf->KeyIndex(key, comparator_);
  }
  return IndexIterator<KeyType, ValueType, KeyComparator>(
      leaf, index, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

// **************** lab3 ***********************
/*
 * Unlock all nodes current transaction hold according to op then unpin pages
 * and delete all pages if any
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::
    UnlockUnpinPages(Operation op, Transaction *transaction)
{
  if (transaction == nullptr)
  {
    return;
  }

  for (auto *page : *transaction->GetPageSet())
  {
    if (op == Operation::READONLY)
    {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
    else
    {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
  transaction->GetPageSet()->clear();

  for (auto page_id : *transaction->GetDeletedPageSet())
  {
    buffer_pool_manager_->DeletePage(page_id);
  }
  transaction->GetDeletedPageSet()->clear();

  if (root_is_locked)
  {
    root_is_locked = false;
    unlockRoot();
  }
}

/*
 * Note: leaf node and internal node have different MAXSIZE
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N>
bool BPlusTree<KeyType, ValueType, KeyComparator>::
    isSafe(N *node, Operation op)
{
  if (op == Operation::INSERT)
  {
    return node->GetSize() < node->GetMaxSize();
  }
  else if (op == Operation::DELETE)
  {
    // >=: keep same with `coalesce logic`
    return node->GetSize() > node->GetMinSize() + 1;
  }
  return true;
}
// **************** lab3 ***********************

/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * 返回的就是 B+tree 的叶子节点
 */
// 这个函数lab2和lab3有着很多不同
template <typename KeyType, typename ValueType, typename KeyComparator>
BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *
BPlusTree<KeyType, ValueType, KeyComparator>::FindLeafPage(
    const KeyType &key, bool leftMost, Operation op, Transaction *transaction)
{
    // 如果操作不是只读的，就要锁根节点
    if (op != Operation::READONLY)
    {
        lockRoot();
        root_is_locked = true;
    }

    if (IsEmpty()) { return nullptr; }

    // 先把root节点的page拿到手
    auto *parent = buffer_pool_manager_->FetchPage(root_page_id_);
    if (parent == nullptr)
    {
        throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while FindLeafPage");
    }

    if (op == Operation::READONLY) { parent->RLatch(); }
    else { parent->WLatch(); }

    if (transaction != nullptr) { transaction->AddIntoPageSet(parent);}
    
    // page 实际就是b+tree上的一个node
    auto *node = reinterpret_cast<BPlusTreePage *>(parent->GetData());

    /**
     * @brief 这里就是一个递归搜索的一个过程
     * 如果是叶子节点就直接返回了
     * 如果是中间节点的话才会继续向下寻找
     * 这玩意与面试时候的3种顺序打印二叉树异曲同工
     */
    while (!node->IsLeafPage())
    {
        // 第一个节点一定是root节点，所以这里一定会走一遭
        auto internal =
            reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);

        page_id_t parent_page_id = node->GetPageId(), child_page_id;

        if (leftMost) { child_page_id = internal->ValueAt(0); }
        else { child_page_id = internal->Lookup(key, comparator_); }

        // 直接拿着key到内部节点中去找，通常就是二分查找
        auto *child = buffer_pool_manager_->FetchPage(child_page_id);
        if (child == nullptr) {
            throw Exception(EXCEPTION_TYPE_INDEX,
                            "all page are pinned while FindLeafPage");
        }

        if (op == Operation::READONLY)
        {
            child->RLatch();
            UnlockUnpinPages(op, transaction);
        }
        else
        {
            child->WLatch();
        }

        node = reinterpret_cast<BPlusTreePage *>(child->GetData());
        assert(node->GetParentPageId() == parent_page_id);

        // 如果是安全的，就释放父节点那的锁
        if (op != Operation::READONLY && isSafe(node, op))
        {
            UnlockUnpinPages(op, transaction);
        }
        if (transaction != nullptr)
        {
            transaction->AddIntoPageSet(child);
        }
        else
        {
            parent->RUnlatch();
            buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
            parent = child;
        }
    }

    return reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 *  header page 保存的是表与 index 的元数据
 *  实际上，创建表与创建索引是一个差不多的操作
 * Call this method every time root page id is changed.
 * @parameter: insert_record default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 *  表名与索引名是不能重复的
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void 
BPlusTree<KeyType, ValueType, KeyComparator>::UpdateRootPageId(bool insert_record)
{
    auto *page = buffer_pool_manager_->FetchPage(HEADER_PAGE_ID);
    if (page == nullptr) {
        throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while UpdateRootPageId");
    }
    auto *header_page = reinterpret_cast<HeaderPage *>(page->GetData());

    if (insert_record)
    {
        // create a new record<index_name + root_page_id> in header_page
        header_page->InsertRecord(index_name_, root_page_id_);
    }
    else
    {
        // update root_page_id in header_page
        header_page->UpdateRecord(index_name_, root_page_id_);
    }

    buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree structure, rank by rank
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
std::string BPlusTree<KeyType, ValueType, KeyComparator>::
    ToString(bool verbose)
{
  if (IsEmpty())
  {
    return "Empty tree";
  }
  std::queue<BPlusTreePage *> todo, tmp;
  std::stringstream tree;
  auto node = reinterpret_cast<BPlusTreePage *>(
      buffer_pool_manager_->FetchPage(root_page_id_));
  if (node == nullptr)
  {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while printing");
  }
  todo.push(node);
  bool first = true;
  while (!todo.empty())
  {
    node = todo.front();
    if (first)
    {
      first = false;
      tree << "| ";
    }
    // leaf page, print all key-value pairs
    if (node->IsLeafPage())
    {
      auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
      tree << page->ToString(verbose) << "| ";
    }
    else
    {
      auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
      tree << page->ToString(verbose) << "| ";
      page->QueueUpChildren(&tmp, buffer_pool_manager_);
    }
    todo.pop();
    if (todo.empty() && !tmp.empty())
    {
      todo.swap(tmp);
      tree << '\n';
      first = true;
    }
    // unpin node when we are done
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  }
  return tree.str();
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::
    InsertFromFile(const std::string &file_name, Transaction *transaction)
{
  int64_t key;
  std::ifstream input(file_name);
  while (input)
  {
    input >> key;

    KeyType index_key{};
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}

/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::
    RemoveFromFile(const std::string &file_name, Transaction *transaction)
{
  int64_t key;
  std::ifstream input(file_name);
  while (input)
  {
    input >> key;
    KeyType index_key{};
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
