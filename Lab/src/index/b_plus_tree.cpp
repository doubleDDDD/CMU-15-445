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
 * b+tree有一个属性称为秩。这个是要提前预设的
 * 重新理解一下这个叶子节点中的 kv 对
 *  对于叶子节点，kv是真实对应的，磁盘中不会有指针这种东西，所以这个 v 实际上就是一个 tuple 在磁盘文件中的位置
 *  即 page number 以及 slot number，这个是 B+tree 中真实的数据结构，真正其 索引 作用的结构
 *  其它的中间节点可以理解为索引kv的 index，两种节点的 v 的类型是不一致的
 *  所以对于叶子节点，kv就直接对应上就完事了
 *  作者这里的 size 指的是 key 的数量，应该是比v的个数小 1 的
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
    // 互斥锁, 如果想要在一个函数中的一部分采用锁，则可以采用中括号用来作为作用域
    {
        std::lock_guard<std::mutex> lock(mutex_);
        // 如果树为空就新建一棵树
        if (IsEmpty()) {
            // 秩=3, insert 1,2,3,4,5 的例子，insert 1 走这里
            StartNewTree(key, value); // 一定是 insert 到 page 节点中的
            return true;
        }
    }

    /**
     * @brief 离开上面的{}范围之后lock就会释放
     * 插入操作一定是在叶子节点插入的
     * 秩=3, insert 1,2,3,4,5 的例子，insert 2,3,4,5 都会走到这里
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
     * 就目前看来，数据库的内容与index是位于同一个文件
     * 在 page 这个粒度上，DBMS 并没有去 care 这些内容在磁盘上是否要顺序存放
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
    // 在 init 之后再 set 一下 maxsize 作为B+ tree 的秩，方便测试的，key的数量是要小于秩的
    root->Init(root_page_id_, INVALID_PAGE_ID);  // parent id 是 -1 代表的是 root 节点
    // reset, if show debug is not defined, the func is a empty func
    // 规则也保证了 b+tree 的阶数至少是2
    ReSetPageOrder(root);
    root->SetLayerId(1); // root 节点的层是 1

    /**
     * @brief root节点的插入方法，value在本例中是一个指向 数据 page 的指针
     * 从这里可以看出来在 b+tree 的创建过程中，最开始 root 节点就是叶子节点
     */
    root->Insert(key, value, comparator_);

    // unpin 是为了支持多线程并发的
    buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 *  插入节点是有可能会造成 b+tree 的分裂的
 *  b+ tree 的本质是一个排序的 kv, 所以假如要 insert 的是 x，只要找到距离 x 最近的 v，然后 insert 即可
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
    // 找到之后检查能够正常的 insert，这里找到的一定是叶子节点
    // 要确定这里 insert 的目标是叶子节点，所以不存在 kv 错位的问题
    auto *leaf = FindLeafPage(key, false, Operation::INSERT, transaction);
    if (leaf == nullptr) { return false; }

    ValueType v;
    // 不支持重复 key，意味着一个域中的 属性不能有相同值，所以一般都是针对 primary key 去建立 index
    if (leaf->Lookup(key, v, comparator_)) {
        UnlockUnpinPages(Operation::INSERT, transaction);
        return false;
    }

    // 这个 insert 操作有可能使得叶子结点的key的数量超过 秩-1，即达到 秩
    // 二话不说先 insert
    leaf->Insert(key, value, comparator_);
    
    /**
     * @brief 秩=3, insert 1,2,3,4,5 的例子
     * 2 直接 insert
     * 3 首先直接 insert，在触发 leaf 的分裂，2上推，直接形成一个新的 root 节点
     * 4 直接 insert 到 2,3 所在的 node, 然后分裂为 2;3,4, 3继续上推去找2，root 节点不需要分裂
     * 到此为止还是正常的
     * 5 insert，直接 insert 3,4 所在的 node，然后 leaf node 分裂为 3;4,5, 其中4要上推到2,3所在的node中
     *      再触发 root 节点的分裂，将形成新的 root 节点
     *      目前就是 5 的 insert 有问题
     */
    if (leaf->GetKeySize() >= leaf->GetOrder()) {
        assert(leaf->GetKeySize()==leaf->GetOrder());  // 只允许大一个之后就需要 split 了
        /**
         * @brief 如果一个叶子节点的空间不够了，大于等于秩之后，意味着需要分直接裂叶子节点
         * 测试学习的秩 =3
         *      叶子节点中 kv 对的数量是 3
         *      所以 1 2 3 都是可以 insert 到
         * 目前作者的实现，是把一个节点的秩用满了，所以没法先insert，再check，只能先 check，再分裂，再 insert
         *  我认为是一个不太好的实现方式
         * 所以我还是想要保证一下用户指定的秩在 1/2 或 2/3 左右，能够支持先 insert，再 check，然后再 split
         *  算了，好像还是我的实现是不太合适的
         * 
         * 
         * 这个时候就必须要分裂叶子节点了，叶子节点分裂的结果是两个叶子节点，kv是没有错位情况的
         * k是不能大于秩 M 的，就能够保证 v 的最大值就是 M
         * 现在在叶子节点中 insert 一个 k 后，k的数量将达到秩
         * 
         * 创建一个新的 node，并将当前叶子节点中的 一般 copy 到新的 node 中
         * 如果我测试的 b+ tree 的秩为3，即一个节点中的k的数量最大是2，最小是1 
         * 3/2=1，所以一半最小是1
         * 后半部分 copy 到 L2
         * 
         * 最终还是实现了先 insert 再 split 的策略，如果无法平均分配，即节点的秩为奇数，那么后半段容纳多的那部分
         * 这里实现的是叶子节点的分裂
         */
        auto *leaf2 = Split<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>(leaf);

        // /**
        //  * @brief 在分裂一半的基础上+1基本是不可能超出范围的，这个写法不是很好，之后修改掉
        //  */
        // if (comparator_(key, leaf2->KeyAt(0)) < 0) { 
        //     // 这个key应该插入到L
        //     leaf->Insert(key, value, comparator_); 
        // } else { 
        //     // key应该被插入到L2中
        //     leaf2->Insert(key, value, comparator_); 
        // }

        // std::cout << "left is " << leaf->KeyAt(0) << "right is " << leaf2->KeyAt(0) << std::endl;
        assert(comparator_(leaf->KeyAt(0), leaf2->KeyAt(0)) < 0);  // 新节点总是后面的那个
        // 叶子结点生成之后，立刻成 list
        leaf2->SetNextPageId(leaf->GetNextPageId());
        leaf->SetNextPageId(leaf2->GetPageId());

        // // 更新前后关系
        // if (comparator_(leaf->KeyAt(0), leaf2->KeyAt(0)) < 0)
        // {
        //     leaf2->SetNextPageId(leaf->GetNextPageId());
        //     leaf->SetNextPageId(leaf2->GetPageId());
        // }
        // else
        // {
        //     leaf2->SetNextPageId(leaf->GetPageId());
        // }

        // 将分裂的节点插入到父节点, 这里是有可能导致父节点分裂的，如果父节点分裂，则继续向上传递
        // 大于等于在右边，所以应该将新分裂及节点最左侧的 key 向上推
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
 * b+ tree 的方法，分裂对两种节点的操作的框架是一致的，相当于这里包了一层，但是 copy 的函数是各自的
 *      对于叶子节点，新的 kv 已经 insert 了，即节点中 key 的数量已经达到了秩
 *      对于中间节点，新的 kv 也已经 insert 了。即节点中的 v 的数量已经超过了秩，k 的数量也达到了 秩
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
    // reset order
    ReSetPageOrder(new_node);

    /* 被分裂节点的 move half to 方法 */
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
 *      大于等于在右边
 *      父节点是一定没有即将被推上去的 key 的，即 leaf2->KeyAt(0)
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::InsertIntoParent(
    BPlusTreePage *old_node, 
    const KeyType &key,
    BPlusTreePage *new_node,
    Transaction *transaction)
{
    /**
     * @brief new_node 的父子关系还没有 build
     * new node 如果是叶子节点的话，则已经成 list 了
     */
    if (old_node->IsRootPage()) {
        // root节点的分裂，copy L2 中的第一个key到新的root节点
        auto *page = buffer_pool_manager_->NewPage(root_page_id_);
        if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while InsertIntoParent"); }

        assert(page->GetPinCount() == 1);
        auto root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());

        root->Init(root_page_id_);
        ReSetPageOrder(root);  // reset b+ tree 的秩，树的 秩 会set到 new node 上
        root->SetLayerId(1);  // 这是新的 root 节点

        // 这里就相当于将 L2 的第一个 key 推到上一层，但是上一层是root，新推的key也是 root 的第一个 key
        root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
        old_node->SetParentPageId(root_page_id_);
        new_node->SetParentPageId(root_page_id_);
        
        // 这两个节点要向下走一层，即层数+1
        old_node->SetLayerId(old_node->GetLayerId()+1);
        new_node->SetLayerId(new_node->GetLayerId()+1);

        // 这时需要更新根节点页面id
        UpdateRootPageId(false);
        // 这一次分裂搞出两个new page来，极具的扩展了B+tree
        buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
    } else {
        // 新增元素的父节点是 非root 节点的中间节点，有可能递归的向上传递，并且可能最终导致 b+tree 整体层数+1
        auto *page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
        if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while InsertIntoParent"); }

        // internal是 L与L2 原本的父节点
        auto internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());

        // 父节点保存了 v 的数量，这个上限就是 秩-1 的大小，所以能够满足先 insert 再 split 的需求
        // 这里准备采用自己的方式，即 二话不说 先insert，再判断是否要 split
        // InsertNodeAfter 会导致中间节点的kv都整体后移一下
        // new_node 的 kv 对已经 insert 到对应的 internal 中了
        internal->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

        /**
         * @brief
         * 这里的一个重要的操作就是给 new_node 找爸爸
         *  如果节点未分裂，则 new_node 的爸爸就是 internal
         *  如果 internal 也分裂了，那么 new_node 的爸爸可能是 internal 或 internal2
         *      如果 internal 分裂出了一个 internal2 节点，则 new_node 能不能找到爸爸取决于 上推的 kv 在 internal 中还是在 internal2 中
         * 确实新节点少了一个与父节点的 link 关系，但是这个不是必现的，找找是为啥
         * 待分裂的中间节点是 internal，分裂后得到的新节点是 internal2，如果internal之上的节点继续分裂的话，internal就没有父节点了
         * 所以对 new_node set 父节点的操作应该是无条件执行的，如果父节点发送了分裂，在对应节点的split函数中会处理这个问题
         */

        // 后续操作尽量的与 new node/old node 隔离开了，除非要调整该层节点的父节点
        if(internal->GetValueSize() > internal->GetOrder()) 
        {
            // 在第一次测试中，这个地方有问题，check again
            // 就是在 5 insert 完成之后，该 node 分裂为 3与45，4 要上推，4上推到 2,3,4
            // 2,3,4 需要继续分裂，就会 run here
            // 这个时候 new_node 依然是没有 set 父节点的

            // value 的数量已经比秩大了，但是一定是只会大1
            // 经过 insert，只有 internal 是不满足规则的，但是也仅仅只是规则不满足而已，指针的指向，即kv的个数是完全ok的
            //  虽然不合规，但是 tree 本身是完整的
            assert(internal->GetValueSize() == internal->GetOrder()+1);
            // 由于L分裂为L与L2，并且将L2的第一个key上举导致L与L1的父节点也需要 split 了
            /**
             * @brief 
             * 1. split
             *      将当前中间节点的 kv 对半分
             *      创建一个 new node，old node 中的一半 kv 移动到 new old 中
             *      两个节点的大小也全部被 adjust 到实际大小
             *      被移动 node 的子节点的父节点也都调整完事了
             * 2. 比 internal 与 internal2 低一层的部分节点（internal2中的value）需要调整父节点
             *      第一层的节点的赋值是 finish 的
             *      但是 new_node 就是低一层的节点啊
             *          会遍历 internal 的孩子节点，可能将其父节点调整为 internal2
             *      但是这里 internal 本身的父节点是没有赋值的
             *      这里之前竟然一直都没有发现
             * 3. internal2 再次被传递到函数 InsertIntoParent 中，就构成了 new_node
             *      new_node 的父节点都是在继续调用一层的地方搞定的
             * 
             * split 解决 new_node 找爸爸的问题
             */
            auto *internal2 = Split<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>(internal);

            // 首先set next page id
            // internal2->SetNextPageId(internal->GetNextPageId());
            // internal->SetNextPageId(internal2->GetPageId());
            // 真是自己傻了，叶子节点怎么可能有这个函数，真是没有过脑子直接 copy 过来了，但是需要set parent id

            // internal2 的第一个 kv 对的 k 需要继续向上 insert，internal2->KeyAt(0) 是无效的
            // 这里 internal2->KeyAt(0) 就是3了，要形成一个新的 root 节点
            InsertIntoParent(internal, internal2->KeyAt(0), internal2, transaction);
        } else { new_node->SetParentPageId(internal->GetPageId()); }

        buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(internal->GetPageId(), true);

        // // 分割线
        // if (internal->GetSize() < internal->GetMaxSize()) 
        // {
        //     // 这个insert函数会导致 kv 数组整体后移
        //     internal->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        //     new_node->SetParentPageId(internal->GetPageId());
        //     buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
        // }
        // else {
        //     // internal 作为 L与L2 的父节点，现在已经无法再容纳多一个kv了，所以 internal 也需要分裂
        //     // 最复杂的情形，父节点也需要分裂，并且需要向上传递一次分裂，明显是一个递归的过程
        //     page_id_t page_id;
        //     // 这个page与internal是同一级的，应该要 copy internal 的 kv 到该new page。这个page最后直接干掉了，没有保留，这里依然只是多了一个分裂的page
        //     auto *page = buffer_pool_manager_->NewPage(page_id);
        //     if (page == nullptr) {
        //         throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while InsertIntoParent");
        //     }

        //     assert(page->GetPinCount() == 1);
            
        //     /* copy is a new internal node, only a 中间过程的节点 */
        //     auto *copy =
        //         reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());

        //     copy->Init(page_id);
        //     copy->SetSize(internal->GetSize());
        //     // 现在 copy 的 kv 对是空的

        //     for (int i = 1, j = 0; i <= internal->GetSize(); ++i, ++j)
        //     {
        //         /**
        //          * @brief old node 指的是 internal 的左子节点
        //          * 这里是在遍历 internal，就是为了将最新的 key 插入
        //          */
        //         if (internal->ValueAt(i - 1) == old_node->GetPageId())
        //         {
        //             // 在 internal 中之前应该是有一个指针是指向 old_node 的（即，有一个kv的v是 old node 的 pageid），new node 产生之后，现在还没有父节点
        //             // 所以这个条件只会被触发一次，执行完毕之后在 copy 中会存在一个 kv pair
        //             copy->SetKeyAt(j, key);
        //             copy->SetValueAt(j, new_node->GetPageId());
        //             ++j;
        //         }
        //         if (i < internal->GetSize())
        //         {
        //             copy->SetKeyAt(j, internal->KeyAt(i));
        //             copy->SetValueAt(j, internal->ValueAt(i));
        //         }
        //     }

        //     // i=0 的

        //     assert(copy->GetSize() == copy->GetMaxSize());
        //     // copy 分裂会导致一个新page的产生
        //     auto internal2 = Split<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>(copy);

        //     internal->SetSize(copy->GetSize() + 1);
        //     for (int i = 0; i < copy->GetSize(); ++i) {
        //         internal->SetKeyAt(i + 1, copy->KeyAt(i));
        //         internal->SetValueAt(i + 1, copy->ValueAt(i));
        //     }

        //     if (comparator_(key, internal2->KeyAt(0)) < 0)
        //     {
        //         new_node->SetParentPageId(internal->GetPageId());
        //     }
        //     else if (comparator_(key, internal2->KeyAt(0)) == 0)
        //     {
        //         new_node->SetParentPageId(internal2->GetPageId());
        //     }
        //     else
        //     {
        //         new_node->SetParentPageId(internal2->GetPageId());
        //         old_node->SetParentPageId(internal2->GetPageId());
        //     }

        //     buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

        //     buffer_pool_manager_->UnpinPage(copy->GetPageId(), false);
        //     buffer_pool_manager_->DeletePage(copy->GetPageId());

        //     // 继续递归了这里
        //     InsertIntoParent(internal, internal2->KeyAt(0), internal2);
        // }
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. 
 * Remember to deal with redistribute or merge if necessary.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::Remove(
    const KeyType &key, Transaction *transaction)
{
    if (IsEmpty()) { return; }

    // std::printf("start remove\n");

    // 先找到要删除的key所在的叶子节点
    auto *leaf = FindLeafPage(key, false, Operation::DELETE, transaction);

    if (leaf != nullptr) {
        int size_before_deletion = leaf->GetKeySize();

        // 叶子节点的函数 RemoveAndDeleteRecord 仅仅是简单的删除一个 key，并且返回新的 key 的个数
        if (leaf->RemoveAndDeleteRecord(key, comparator_) != size_before_deletion) {
            // key 在叶子节点中删除成功
            if (CoalesceOrRedistribute(leaf, transaction)) { 
                // transaction->AddIntoDeletedPageSet(leaf->GetPageId()); 
            }
        }
        // 下面这个操作会清理 buffer pool manager 的缓存，最后导致出错，写并发的时候再考虑这里
        UnlockUnpinPages(Operation::DELETE, transaction);
    }
}

/**
 * @brief 这是自己加的一个辅助函数，是为了使得代码结构看起来更规整
 * read and check
 * 在 写锁内 执行的
 * @tparam KeyType 
 * @tparam ValueType 
 * @tparam KeyComparator 
 * @tparam N 
 * @param  node             desc
 * @param  sibling_page_id  desc
 * @param  transaction      desc
 * @return true @c 
 * @return false @c 
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N>
bool BPlusTree<KeyType, ValueType, KeyComparator>::_CoalesceOrRedistribute(
    N *sibling,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent) 
{
    if (sibling->IsLeafPage()){
        auto _sibling = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(sibling);
        if (_sibling->GetKeySize()-1 >= _sibling->GetMinKeySize()) {
            buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
            return true;
        } else { return false; }
    } else {
        auto _sibling = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(sibling);
        if (_sibling->GetValueSize()-1 >= _sibling->GetMinValueSize()) {
            buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
            return true;
        } else { return false; }
    }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. 
 *  兄弟节点的 kv 足够，则输入 page 先向兄弟节点借 kv
 * 
 * Otherwise, merge.
 *  兄弟节点也不够了，那就只能合并了
 * 
 * 合并或再分配
 * 
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 * 
 * Start at root, find leaf L where entry belongs
 * Remove the entry
 * If L is at least half-full, done!
 * If L has only M/2-1entries
 *      Try to re-distribute, borrowing from sibling (adjacent node with same parent as L, 相邻的节点，左右都可以的).
 *      If re-distribution fails, merge Land sibling.
 *          If merge occurred, must delete entry (pointing to Lor sibling) from parent of L
 *
 * 向兄弟节点借或合并兄弟节点，指的是 B+tree 中有相同父节点的节点称为兄弟节点，否则是堂兄弟节点
 * B+tree 的最小秩是2，所以B+tree除了 root 节点，一定有兄弟节点
 * 即使是对于边界上的节点，无论是左边还是右边，一定有一个 
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N>
bool BPlusTree<KeyType, ValueType, KeyComparator>::CoalesceOrRedistribute(
    N *node, Transaction *transaction)
{
    // std::printf("start coalesce or redistribute\n");

    // node 可能是叶子节点也可能是中间节点，只有一个共同特性，那就是都被删了东西，所以触发该函数的操作
    if (node->IsRootPage()) { return AdjustRoot(node); }

    // 删除完一个kv之后，b+tree 的规则并没有被破坏
    if (node->IsLeafPage()) {
        auto _node = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
        if (_node->GetKeySize() >= _node->GetMinKeySize()) { return false; }
    } else {
        auto _node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
        if (_node->GetValueSize() >= _node->GetMinValueSize()) { return false; }
    }

    // std::printf("leaf node is ok\n");

    // 节点删除完毕之后。节点已经不满足 b+tree 的条件了
    auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CoalesceOrRedistribute"); }
    auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());

    // 找到这个父节点中指向 该 page 的 kv 对 对应的 index
    int value_index = parent->ValueIndex(node->GetPageId());
    // ValueIndex失败就会返回超过下标的一个值，这个与迭代器中 end() 指向最后一个元素的后一个的用法是一致的
    assert(value_index != parent->GetValueSize());

    /**
     * @brief 选择一个兄弟节点
     *      原作者这里貌似是仅仅只借左边的兄弟节点，如果是第一个才会借右边的兄弟节点
     * 按照定义，左右兄弟节点都是 可以借的，左边的优先，边界上的节点只有一个选择
     *
     * 想到得到兄弟节点，就一定要去找爸爸，说不定还得去去找爷爷，表弟表哥也是兄弟么
     *
     * 能跑到这里来的，一定是有父节点的
     *
     * 自己错了，表兄弟节点不算是可以合并的兄弟节点，导致自己把问题高复杂了，就是没搞清楚就开始干代码
     * 也有点浪费时间
     */
    int left_sibling_page_id = -1;
    int right_sibling_page_id = -1;

    // 首先判断父节点是不是 root
    // if(parent->IsRootPage()){
    //     if (value_index == 0) {
    //         // 它就是整个 b+tree 的第一个节点，没有左兄弟，只能向右边的兄弟去借
    //         right_sibling_page_id = parent->ValueAt(value_index + 1); 
    //     } else if (value_index == parent->GetValueSize()-1) { 
    //         // 它是整个 B+tree 的最后一个节点，没有右兄弟，只能向左边的兄弟去借
    //         left_sibling_page_id = parent->ValueAt(value_index - 1); 
    //     } else {
    //         // 左右节点都准备好先
    //         left_sibling_page_id = parent->ValueAt(value_index - 1);
    //         right_sibling_page_id = parent->ValueAt(value_index + 1);
    //     }
    // } else {
    //     // 对于中间节点需要检查到爷爷
    //     auto *g_page = buffer_pool_manager_->FetchPage(parent->GetParentPageId());
    //     if (g_page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CoalesceOrRedistribute"); }
    //     auto grandparent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(g_page->GetData());
    //     int father_value_index = grandparent->ValueIndex(parent->GetPageId());

    //     if (value_index == 0) {
    //         // 它是父节点的第一个儿子
    //         right_sibling_page_id = parent->ValueAt(value_index + 1);
    //         // 是否有左节点要去检查 爷爷 节点
    //         if (father_value_index>0) {
    //             // uncle 最右边的就是 node 的左边的兄弟
    //             int uncle_id = grandparent->ValueAt(father_value_index-1);
    //             auto *uncle_paga = buffer_pool_manager_->FetchPage(uncle_id);
    //             if (uncle_paga == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CoalesceOrRedistribute"); }
    //             auto uncle = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(uncle_paga->GetData());
    //             left_sibling_page_id = uncle->ValueAt(uncle->GetValueSize()-1);
    //         }
    //     } else if (value_index == parent->GetValueSize()-1) { 
    //         // 它是父节点的最后一个儿子，是否有右兄弟，也要去检查爷爷
    //         left_sibling_page_id = parent->ValueAt(value_index - 1);
    //         if (father_value_index<grandparent->GetValueSize()-1){
    //             // 说明爸爸的右边还有兄弟
    //             int uncle_id = grandparent->ValueAt(father_value_index+1);
    //             auto *uncle_paga = buffer_pool_manager_->FetchPage(uncle_id);
    //             if (uncle_paga == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CoalesceOrRedistribute"); }
    //             auto uncle = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(uncle_paga->GetData());
    //             right_sibling_page_id = uncle->ValueAt(0);  //因为这个叔叔是右边的，所以叔叔最小的孩子就是我的兄弟
    //         }
    //     } else {
    //         // 左右节点都准备好先
    //         left_sibling_page_id = parent->ValueAt(value_index - 1);
    //         right_sibling_page_id = parent->ValueAt(value_index + 1);
    //     }
    // }

    // value index 是出问题的节点在父节点中的index
    if (value_index==0){
        // 只有右边有兄弟
        right_sibling_page_id = parent->ValueAt(value_index + 1);
    } else if(value_index == parent->GetValueSize()-1){
        // 只有左边有兄弟
        left_sibling_page_id = parent->ValueAt(value_index - 1);
    } else {
        // 左右倒是都有兄弟;
        left_sibling_page_id = parent->ValueAt(value_index - 1);
        right_sibling_page_id = parent->ValueAt(value_index + 1);
    }

    // TODO 并发的支持

    // Redistribute 是有更新父节点的操作的

    // for debug
    // std::printf(
    //     "value_index:%d, left:%d, right:%d\n", 
    //     value_index, left_sibling_page_id, right_sibling_page_id);

    // if-else
    if (left_sibling_page_id>0 && right_sibling_page_id>0)
    {
        // left and right are all ok, left first
        // 先尝试 左边的是否可以 no merge
        auto *siblingpage = buffer_pool_manager_->FetchPage(left_sibling_page_id);
        if (siblingpage == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CoalesceOrRedistribute"); }

        // siblingpage->WLatch();
        // transaction->AddIntoPageSet(siblingpage);

        auto sibling = reinterpret_cast<N *>(siblingpage->GetData());
        if (_CoalesceOrRedistribute(sibling, parent)) {
            // redistribution no merge
            // redistribute is true
            // Redistribute(N *neighbor_node, N *node, int index)
            // move from neighbor to node
            // indx=0 means neighbor's first, otherwise neighbor's last
            Redistribute<N>(sibling, node, 1);  // sibling is left neighbor, move last
            // siblingpage->WUnlatch();
            return true;
        }

        auto _siblingpage = buffer_pool_manager_->FetchPage(right_sibling_page_id);
        if (_siblingpage == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CoalesceOrRedistribute"); }

        auto _sibling = reinterpret_cast<N *>(_siblingpage->GetData());
        // 再尝试 右边是否可以 no merge
        if (_CoalesceOrRedistribute(_sibling, parent)) {
            // redistribution no merge
            Redistribute<N>(_sibling, node, 0);  // move sibling's first to the head of node
            return true;
        }

        // 必须 merge，如果 merge ，直接找左边的 merge，默认向左合并，向左合并，父节点可以直接删除对应的kv对
        // 如果是向右合并的，指向被合并的 page 的 kv 对的 v 需要被删除
        Coalesce<N>(sibling, node, parent, value_index, transaction);
        return true;
    } else {
        if (left_sibling_page_id>0) {
            // only left sibling
            auto *siblingpage = buffer_pool_manager_->FetchPage(left_sibling_page_id);
            if (siblingpage == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CoalesceOrRedistribute"); }
            auto sibling = reinterpret_cast<N *>(siblingpage->GetData());
            if (_CoalesceOrRedistribute(sibling, parent)){
                Redistribute<N>(sibling, node, 1);  // move sibling's last to the head of node
            } else {
                // 只有左边有兄弟，自己本身是最后一个，向左边合并
                Coalesce<N>(sibling, node, parent, value_index, transaction);
            }
        } else {
            // only right sibling
            // sibling 位于 node 右侧，但是这个地方我想要 sibling 的内容 merge 到 node 中
            // 然后保留 node
            auto *siblingpage = buffer_pool_manager_->FetchPage(right_sibling_page_id);
            if (siblingpage == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CoalesceOrRedistribute"); }
            auto sibling = reinterpret_cast<N *>(siblingpage->GetData());
            if (_CoalesceOrRedistribute(sibling, parent)){
                Redistribute<N>(sibling, node, 0);  // move sibling's first to the end of node
            } else {
                // 由右向左合并，这里调整一下

                std::printf("here!\n\n");

                value_index = parent->ValueIndex(sibling->GetPageId());
                Coalesce<N>(node, sibling, parent, value_index, transaction);
            }
        }
    }

    return true;

    // // 先写出来，再考虑结构，if-else 的分叉有点多哦
    // if (leftsiblingpage && rightsiblingpage) {
    //     // left or right are all ok, left firstly
    //     ;
    // } else {
    //     if(leftsiblingpage) {
    //         // only left
    //         leftsiblingpage->WLatch();
    //         transaction->AddIntoPageSet(leftsiblingpage);

    //         auto sibling = reinterpret_cast<N *>(leftsiblingpage->GetData());

    //         if (node->IsLeafPage()){
    //             // 叶子节点, 反正自己是不够了，左边的叶子节点能够借给自己 kv
    //             // 搞一个更加直观的判断方法 等价于 sibling->GetKeySize() - 1 >= node->GetMinKeySize()  
    //             // if (sibling->GetKeySize() + node->GetKeySize() > node->GetMaxKeySize()) {
    //             //     redistribute = true;
    //             //     buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    //             // }
    //             // node是叶子节点，那么 sibling 才是叶子节点
    //             if (sibling->GetKeySize() - 1 >= sibling->GetMinKeySize()) {
    //                 // sibling 少一个也问题不大
    //                 buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    //             }
    //         } else {
    //             // 中间节点             
    //             if (sibling->GetValueSize() - 1 >= sibling->GetMinValueSize()) {
    //                 buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    //             }
    //         }
    //     } else {
    //         // only right;
    //     }
    // }


    // return true;


    // page->WLatch();
    // transaction->AddIntoPageSet(page);

    // auto sibling = reinterpret_cast<N *>(page->GetData());
    // bool redistribute = false;

    // if (sibling->GetSize() + node->GetSize() > node->GetMaxSize())
    // {
    //     // 
    //     redistribute = true;
    //     buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    // }

    // if (redistribute)
    // {
    //     if (value_index == 0) { Redistribute<N>(sibling, node, 1); }
    //     return false;
    // }

    // bool ret;
    // if (value_index == 0)
    // {
    //     // lab3
    //     Coalesce<N>(node, sibling, parent, 1, transaction);
    //     transaction->AddIntoDeletedPageSet(sibling_page_id);
    //     ret = false;
    // }
    // else
    // {
    //     Coalesce<N>(sibling, node, parent, value_index, transaction);
    //     ret = true;
    // }
    // buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    // return ret;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page
 
 * Parent page must be adjusted to take info of deletion into account
 * Remember to deal with coalesce or redistribute recursively if necessary.
 *
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happened
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N>
void BPlusTree<KeyType, ValueType, KeyComparator>::Coalesce(
    N *neighbor_node, 
    N *node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent,
    int index, Transaction *transaction)
{
    // move kv from node to neighbor
    // index 位置处的 kv 的 v 原本指向的是被 删掉的 node
    // 无论向左合并还是向右合并，被合并掉的node在父节点中对应的index均直接删除即可
    // 这里有一个比较重要的思考，确实无需调整被合并的page在父节点中对应的kv对的值，是直接合规的
    node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);
    parent->Remove(index);  // 所以这个 remove 方法一定只有中间节点才有的，只是一个简单的 remove
    // buffer_pool_manager_->DeletePage(node->GetPageId());

    // 递归操作
    if (CoalesceOrRedistribute(parent, transaction)) { transaction->AddIntoDeletedPageSet(parent->GetPageId()); }
}

/*
 * Redistribute key & value pairs from one page to its sibling page.
 *  实际上仅仅只会 move 一个 kv 对，并非平均分配
 * 
 * If index == 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * 
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 *
 * 这两个节点一定是有相同的父节点的，正儿八经的兄弟节点
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
template <typename N>
void BPlusTree<KeyType, ValueType, KeyComparator>::Redistribute(N *neighbor_node, N *node, int index)
{
    // 由节点 neighbor_node 拿一个 kv 给到 node, 对于 neighbor_node 来说，不是第一个就是最后一个，只有这两种情况
    if (index == 0) {
        // 选择的是右边的兄弟，兄弟的第一个来自己 node 做最后一个
        // neighbor_node's first to node， 对应父节点中的 kv 对也要一起 update
        neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
    } else {
        // 选择的是最左边的兄弟，最左边兄弟的最大值来自己这里做第一个
        // neighbor_node's last to node
        auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
        if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while Redistribute"); }
        auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
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
 * 
 * 节点的删除传递到了 b+tree 的 root 节点，即 root 节点删除了一个 kv
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
bool BPlusTree<KeyType, ValueType, KeyComparator>::AdjustRoot(
    BPlusTreePage *old_root_node)
{
    // 如果删除了最后一个节点
    if (old_root_node->IsLeafPage()) {
        auto _old_root_node = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*> (old_root_node);
        if (_old_root_node->GetKeySize() == 0) {
            root_page_id_ = INVALID_PAGE_ID;
            UpdateRootPageId(false);  // 修改索引表，以能够正确的找到 b+ tree 的 root 节点
            return true;
        }
        return false;
    }

    // root 一定是中间节点
    auto _old_root_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>*> (old_root_node);

    // 删除了还有最后一个节点
    if (_old_root_node->GetValueSize() == 1) {
        // b+ tree 的高度在这个地方要降低了
        auto root =
            reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(_old_root_node);
        // int old_root_page_id = root_page_id_;
        root_page_id_ = root->ValueAt(0);  // root 节点发生了修改
        
        UpdateRootPageId(false);
        // buffer_pool_manager_->DeletePage(old_root_page_id);

        auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
        if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while AdjustRoot"); }
        auto new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
        new_root->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
        return true;
    }

    // root 少了之后没有什么影响
    return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 * begin 指向的是第一个对象
 * 由于测试代码中没有测试到 tree.Begin() 的情况，所以作者在这里偷懒了，实现的其实不太对
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
IndexIterator<KeyType, ValueType, KeyComparator> BPlusTree<KeyType, ValueType, KeyComparator>::Begin()
{
    // std::printf("yeap, you are here\n");
    // 得要找到最小的叶子节点
    if(IsEmpty()) {
        // 我其实不是很清楚这个边界条件的处理
        return IndexIterator<KeyType, ValueType, KeyComparator>(nullptr, 0, buffer_pool_manager_);
    }

    auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
    // 将某些函数修改成为了虚函数，有个模板类，类模板的相互派生关系，这个东西我还弄不清楚
    while (!node->IsLeafPage()) {
        // 不是叶子节点一定是中间节点
        auto internode = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
        page_id_t page_id = internode->ValueAt(0);
        node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id));
    }

    auto leafnode = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
    KeyType key = leafnode->KeyAt(0);
    return IndexIterator<KeyType, ValueType, KeyComparator>(FindLeafPage(key, false), 0, buffer_pool_manager_);

    // 分割线，作者写的
    // KeyType key {};
    // return IndexIterator<KeyType, ValueType, KeyComparator>(FindLeafPage(key, true), 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
IndexIterator<KeyType, ValueType, KeyComparator> BPlusTree<KeyType, ValueType, KeyComparator>::Begin(
    const KeyType &key)
{
    auto *leaf = FindLeafPage(key, false);
    int index = 0;
    if (leaf != nullptr) { index = leaf->KeyIndex(key, comparator_); }
    return IndexIterator<KeyType, ValueType, KeyComparator>(leaf, index, buffer_pool_manager_);
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
    // 先无条件返回，先保证b+tree删除的逻辑基本正确先
    if (transaction == nullptr) { return; }

    for (auto *page : *transaction->GetPageSet())
    {
        if (op == Operation::READONLY) {
            page->RUnlatch();
            buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
        }
        else {
            page->WUnlatch();
            buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        }
    }
    transaction->GetPageSet()->clear();

    // for (auto page_id : *transaction->GetDeletedPageSet()) {
    //     buffer_pool_manager_->DeletePage(page_id);
    // }
    transaction->GetDeletedPageSet()->clear();

    if (root_is_locked) {
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
//   if (op == Operation::INSERT)
//   {
//     return node->GetSize() < node->GetMaxSize();
//   }
//   else if (op == Operation::DELETE)
//   {
//     // >=: keep same with `coalesce logic`
//     return node->GetSize() > node->GetMinSize() + 1;
//   }
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
    if (op != Operation::READONLY) {
        lockRoot();
        root_is_locked = true;
    }

    if (IsEmpty()) { return nullptr; }

    // 先把root节点的page拿到手
    auto *parent = buffer_pool_manager_->FetchPage(root_page_id_);
    if (parent == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while FindLeafPage"); }

    if (op == Operation::READONLY) { parent->RLatch(); }
    else { parent->WLatch(); }

    if (transaction != nullptr) { transaction->AddIntoPageSet(parent);}
    
    // page 实际就是 b+tree 上的一个node
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
        if (child == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while FindLeafPage"); }

        if (op == Operation::READONLY) {
            child->RLatch();
            UnlockUnpinPages(op, transaction);
        }
        else { child->WLatch(); }

        node = reinterpret_cast<BPlusTreePage *>(child->GetData());

        // only for debug
        if(node->GetParentPageId() != parent_page_id) {
            std::cout << "error key is: " << key 
                << " and pid from self is " << node->GetParentPageId() 
                << " and curr id is " << parent_page_id << std::endl;
            BackTracePlus(); 
        }

        assert(node->GetParentPageId() == parent_page_id);

        // 如果是安全的，就释放父节点那的锁
        if (op != Operation::READONLY && isSafe(node, op)) { UnlockUnpinPages(op, transaction); }
        if (transaction != nullptr) { transaction->AddIntoPageSet(child); }
        else {
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
    if (page == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while UpdateRootPageId"); }
    auto *header_page = reinterpret_cast<HeaderPage *>(page->GetData());
    if (insert_record) {
        // create a new record<index_name + root_page_id> in header_page
        header_page->InsertRecord(index_name_, root_page_id_);
    }
    else {
        // update root_page_id in header_page
        header_page->UpdateRecord(index_name_, root_page_id_);
    }

    buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree structure, rank by rank
 * 基本思路就是二叉树的层次遍历，要用一个队列，首先root入队，然后一直出队，如果出队了，就需要将出队节点的所有children入队
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
std::string BPlusTree<KeyType, ValueType, KeyComparator>::ToString(bool verbose)
{
    if (IsEmpty()) { return "Empty tree"; }
    std::queue<BPlusTreePage *> todo, tmp;
    std::stringstream tree;
    auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
    if (node == nullptr) { throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while printing"); }

    todo.push(node);
    bool first = true;

    while (!todo.empty())
    {
        node = todo.front();
        if (first) {
            first = false;
            tree << "| ";
        }

        // leaf page, print all key-value pairs
        if (node->IsLeafPage()) {
            auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
            tree << page->ToString(verbose) << "| ";
        }
        else {
            auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
            tree << page->ToString(verbose) << "| ";
            page->QueueUpChildren(&tmp, buffer_pool_manager_);  // 这里就是所有子节点入临时队列
        }

        // 这里多加了一个临时的 queue,一层的节点全部处理完成之后，才会处理下一层的数据
        // TODO 不为空，说明这一层还没有被处理完，这个有利于输出
        todo.pop();
        if (todo.empty() && !tmp.empty()) {
            todo.swap(tmp);
            tree << '\n' << '\n';
            first = true;
        }

        // 如果本层的没有处理完成的话，tmp就相当于是攒下来了，一层一层处理的这个过程非常贴切
        // unpin node when we are done
        buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    }
    std::cout << tree.str() << std::endl;
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

// 分割线
template <typename KeyType, typename ValueType, typename KeyComparator>
void 
BPlusTree<KeyType, ValueType, KeyComparator>::SetOrder(int _order)
{
    order = _order;
    return;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::ReSetPageOrder(BPlusTreePage *node)
{
    /**
     * @brief 指定秩，B+ tree 的秩的最小值是2，最大不能超过容量
     * 如果秩与最大容量的大小是一致的，则无法做到先 insert 再 split，所以这里留一点余量
     * 就保留了一个空间的余量
     * 这个容量也不一样啊，之前作者的工作在这里看起来是有些欠缺的
     */
// #ifdef DEBUG_TREE_SHOW
    if(order > node->GetMaxCapacity() - 1 || order <= 1) {
        // B+ tree 最少二阶，阶指的是 v 的数量，反正就是 kv 对的数量，k 需要空一个出来
        throw Exception(EXCEPTION_TYPE_OUT_OF_RANGE, "order of b+ tree is too big!");
    }
    node->SetOrder(order);
// #endif
    return;
}

// /**
//   * @brief 层次遍历节点，额外的空间保存节点
//   * 这里可以参考二叉树的层次遍历，思路是一样的
//   *  root 节点入队，开始遍历队列，并输出
//   *  节点出队后，检查左右节点是否为空，不为空则入队，左右子树均入队后，准备出队
//   *    任何节点出队，都要检查是否是否有左右子树，如果有的话，则继续入队
//   * 忽然发现人家有对应的 debug show
//   */
// template <typename KeyType, typename ValueType, typename KeyComparator>
// void BPlusTree<KeyType, ValueType, KeyComparator>::Show() const
// {
// #ifdef DEBUG_TREE_SHOW
//     std::queue<BPlusTreePage *> nodequeu;  // 用于存放同一层次的 node
//     auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
//     nodequeu.push(node);  // root 节点入队

//     while(!nodequeu.empty()){
//         // 尝试出队节点，都是一个一个出队的，但是入队可能是一批一批 (二叉树的话就是左右子树，度多的树的话就是所有的子节点) 入队的
//         node = nodequeu.front();
//         if(node->IsLeafPage()) {
//             auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
//             page->ToString(true);
//         } else { 
//             // 这里写错了，中间节点的模板参数list，在internal.cpp中就已经定义的就是 page_id_t
//             auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
//             page->ToString(true);
//         }

//         nodequeu.pop();
//         // for(){
//         //     // 所有子节点入队
//         // }
//     }
// #endif
//     return;
// }

// template <typename KeyType, typename ValueType, typename KeyComparator>
// void
// BPlusTree<KeyType, ValueType, KeyComparator>::PrintSingleNode(BPlusTreePage *node) const 
// {

//     // std::queue<BPlusTreePage> _children;
//     // _children.clear();

//     // node->GetLayerId();
//     // node->GetMaxSize();
//     // node->GetPageId();
//     // node->GetSize()
//     // node->GetMinSize()


//     // for(  ){
//     //     // 打印自己，push_back 子节点
//     //     if(!node->IsLeafPage) { _children.push_back(childnode); }
//     // }

//     // if(!_children.empty()){
//     //     for() { PrintSingleNode(); }
//     // }

//     return;
// }


template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb