/**
 * b_plus_tree_test.cpp
 */

#include <algorithm>
#include <cstdio>
#include <iostream>
#include <sstream>

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "index/b_plus_tree.h"
#include "vtable/virtual_table.h"
#include "gtest/gtest.h"

namespace cmudb {

TEST(BPlusTreeTests, InsertTest1) 
{
    // create KeyComparator and index schema
    Schema *key_schema = ParseCreateStatement("a bigint");  // 创建一个schema，即结构化的数据，属性名为a,类型为bigint
    GenericComparator<8> comparator(key_schema);

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
    /**
     * @brief create b+ tree
     * keytype is GenericKey
     * value is rid, menas row id, including pageid and slotnum, 你原来是干这个的啊
     * key的比较器是 GenericComparator
     * 这里创建B+ tree的过程中，并没有指定秩的大小
     */
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);

// #ifdef DEBUG_TREE_SHOW
    tree.SetOrder(3);  // set B+ tree 的阶数为 3，实际上是key的个数不能超过3
// #endif

    GenericKey<8> index_key;  // 数据部分是一个char数组，数组长度8
    RID rid;
    // create transaction
    Transaction *transaction = new Transaction(0);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(page_id);
    (void) header_page;

    std::vector<int64_t> keys = {1, 2, 3, 4, 5};  // 这个就很巧妙了，代码的情况都可以跑到。得把秩set到3就刚好都能跑到
    for (auto key : keys) {
        int64_t value = key & 0xFFFFFFFF;
        rid.Set((int32_t) (key >> 32), value);  // 这里set的是pageid与slotnum。pageid均为0
        index_key.SetFromInteger(key); // 每次insert一个字符数组，8字节，只有第一个slot有值，就是1,2那样的递增关系
        tree.Insert(index_key, rid, transaction);
        // tree.ToString(true);
        // std::printf("tree finish\n");
    }

    // tree.ToString(true);

    std::vector<RID> rids;
    for (auto key : keys) {
        rids.clear();
        index_key.SetFromInteger(key);
        tree.GetValue(index_key, rids);
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].GetSlotNum(), value);
    }

    int64_t start_key = 1;
    int64_t current_key = start_key;
    index_key.SetFromInteger(start_key);

    for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false; ++iterator) 
    {
        auto location = (*iterator).second;
        EXPECT_EQ(location.GetPageId(), 0);
        EXPECT_EQ(location.GetSlotNum(), current_key);
        current_key = current_key + 1;
    }

    start_key = 1;
    current_key = start_key;
    index_key.SetFromInteger(start_key);

    for (auto iterator = tree.Begin(); iterator.isEnd() == false; ++iterator) 
    {
        auto location = (*iterator).second;
        EXPECT_EQ(location.GetPageId(), 0);
        EXPECT_EQ(location.GetSlotNum(), current_key);
        current_key = current_key + 1;
    }

    EXPECT_EQ(current_key, keys.size() + 1);

    bpm->UnpinPage(HEADER_PAGE_ID, true);
    delete transaction;
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
}

TEST(BPlusTreeTests, InsertTest2) {
    // create KeyComparator and index schema
    Schema *key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema);

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
    // create b+ tree
    // RID 通过 pageid 与 slotnum 来表示一个 tuple 在 db 中的位置
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    
    tree.SetOrder(3);

    GenericKey<8> index_key;
    RID rid;
    // create transaction
    Transaction *transaction = new Transaction(0);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(page_id);
    (void) header_page;

    // 配合旧金山大学的数据结构可视化工具很好理解，同样的 key 按不同的顺序 insert 最后的结果可能是不同的
    std::vector<int64_t> keys = {5, 4, 3, 2, 1};
    for (auto key : keys) {
        int64_t value = key & 0xFFFFFFFF;
        // set 的参数，第一个是 page id，第二个是 slot num
        // key 右移 32 位 目前 都是 0
        rid.Set((int32_t) (key >> 32), value);
        index_key.SetFromInteger(key);
        tree.Insert(index_key, rid, transaction);
    }

    // tree.ToString(true);

    std::vector<RID> rids;
    for (auto key : keys) {
        rids.clear();
        index_key.SetFromInteger(key);
        tree.GetValue(index_key, rids);
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].GetSlotNum(), value);
    }

    int64_t start_key = 1;
    int64_t current_key = start_key;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
        ++iterator) 
    {
        auto location = (*iterator).second;
        EXPECT_EQ(location.GetPageId(), 0);  // 这个是 rid 的 pageid
        EXPECT_EQ(location.GetSlotNum(), current_key);  // 这个是 rid 的 slotnum
        current_key = current_key + 1;
    }

    EXPECT_EQ(current_key, keys.size() + 1);

    start_key = 3;
    current_key = start_key;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
        ++iterator) {
        auto location = (*iterator).second;
        EXPECT_EQ(location.GetPageId(), 0);  // pageid 是与 b+tree 的秩相关的，所以这里直接不测了
        EXPECT_EQ(location.GetSlotNum(), current_key);
        current_key = current_key + 1;
    }

    EXPECT_EQ(6, current_key);

    bpm->UnpinPage(HEADER_PAGE_ID, true);
    delete transaction;
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
}

TEST(BPlusTreeTests, InsertScale) {
    // create KeyComparator and index schema
    Schema *key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema);

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManager(5000, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    tree.SetOrder(31);

    GenericKey<8> index_key;
    RID rid;
    // create transaction
    Transaction *transaction = new Transaction(0);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(page_id);
    (void) header_page;

    int scale = 10000; // 数量讲道理还是毛毛雨
    // 关于一些 assert 判定失效的问题很可能是 buffer_pool 的容量有限导致的
    // int scale = 10000;  // 在 scale=10 的时候有测试出 bug，最后一个 10 在 insert 的时候会出问题
    std::vector<int64_t> keys;
    for (int i = 0; i < scale; ++i) { keys.push_back(i + 1); }

    for (auto key : keys) {
        int64_t value = key & 0xFFFFFFFF;
        rid.Set((int32_t) (key >> 32), value);
        index_key.SetFromInteger(key);
        tree.Insert(index_key, rid, transaction);
    }

    // tree.ToString(true);
    // buffer pool 的数量足够大，这些测试都能 pass
    // 50 个 4KB 太小气了

    // check all value is in the tree
    std::vector<RID> rids;
    for (auto key : keys) {
        rids.clear();
        index_key.SetFromInteger(key);
        tree.GetValue(index_key, rids);
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].GetSlotNum(), value);
    }

    // range query
    int64_t start_key = 1;
    int64_t current_key = start_key;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
        ++iterator) {
        auto location = (*iterator).second;
        EXPECT_EQ(location.GetPageId(), 0);
        EXPECT_EQ(location.GetSlotNum(), current_key);
        current_key = current_key + 1;
    }

    EXPECT_EQ(current_key, keys.size() + 1);

    bpm->UnpinPage(HEADER_PAGE_ID, true);
    delete transaction;
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
}

TEST(BPlusTreeTests, InsertRandom) {
    // create KeyComparator and index schema
    Schema *key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema);

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManager(5000, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    
    // 后面依然发生了一样的问题，奇数秩没什么问题，但是偶数秩是有问题的
    tree.SetOrder(32);

    GenericKey<8> index_key;
    RID rid;
    // create transaction
    Transaction *transaction = new Transaction(0);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(page_id);
    (void) header_page;

    std::vector<int64_t> keys;
    // for debug
    // 最后的原因是少为新节点赋值了，没有给新节点找爸爸
    // std::vector<int64_t> keys = {5,11,12,16,15,17,18,2,7,10,4,8,9,3,1,13,6,14};
    int scale = 10000;

    // for debug again
    // 蛋疼除了没有赋值的之外还有赋值出错的，把这个再来看看，秩=6，序列如下
    // insert13的时候出的问题，原因是有一个节点的父节点给错了，即 set 的时候出了问题
    // std::vector<int64_t> keys = {5,11,12,16,25,17,18,2,7,10,4,23,20,3,1,24,21,19,14,9,6,22,8,13,15};

    // int scale = 25;
    for (int i = 0; i < scale; ++i) { keys.push_back(i + 1); }

    std::random_shuffle(keys.begin(), keys.end());
    // std::for_each(keys.begin(), keys.end(), [](int i) {
    //  std::cerr << i << " ";
    // });
    // std::cerr << std::endl << std::endl;

    for (auto key : keys) {
        int64_t value = key & 0xFFFFFFFF;
        rid.Set((int32_t) (key >> 32), value);
        index_key.SetFromInteger(key);
        tree.Insert(index_key, rid, transaction);
        // tree.ToString(true);
        // std::printf("tree finish, key %ld has been inserted\n", key);
        // std::printf("\n");
    } 

    // tree.ToString(true);

    // check all value is in the tree
    std::vector<RID> rids;
    for (auto key : keys) {
        rids.clear();
        index_key.SetFromInteger(key);
        tree.GetValue(index_key, rids);
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].GetSlotNum(), value);
    }

    // range query
    int64_t start_key = 1;
    int64_t current_key = start_key;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
        ++iterator) {
        auto location = (*iterator).second;
        EXPECT_EQ(location.GetPageId(), 0);
        EXPECT_EQ(location.GetSlotNum(), current_key);
        current_key = current_key + 1;
    }

    EXPECT_EQ(current_key, keys.size() + 1);

    bpm->UnpinPage(HEADER_PAGE_ID, true);
    delete transaction;
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
}

TEST(BPlusTreeTests, DeleteBasic) {
    // create KeyComparator and index schema
    std::string createStmt = "a bigint";
    Schema *key_schema = ParseCreateStatement(createStmt);
    GenericComparator<8> comparator(key_schema);

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);

    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    tree.SetOrder(3);

    GenericKey<8> index_key;
    RID rid;
    // create transaction
    Transaction *transaction = new Transaction(0);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(page_id);
    (void) header_page;

    std::vector<int64_t> keys = {1, 2, 3, 4, 5};
    for (auto key : keys) {
        int64_t value = key & 0xFFFFFFFF;
        rid.Set((int32_t) (key >> 32), value);
        index_key.SetFromInteger(key);
        tree.Insert(index_key, rid, transaction);
    }

    std::vector<RID> rids;
    for (auto key : keys) {
        rids.clear();
        index_key.SetFromInteger(key);
        tree.GetValue(index_key, rids);
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].GetSlotNum(), value);
    }

    int64_t start_key = keys[0];
    int64_t current_key = start_key;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
        ++iterator) {
        auto location = (*iterator).second;
        EXPECT_EQ(location.GetPageId(), 0);
        EXPECT_EQ(location.GetSlotNum(), current_key);
        ++current_key;
    }

    EXPECT_EQ(current_key, keys.size() + start_key);

    // tree.ToString(true);
    // return;
    
    std::vector<int64_t> remove_keys = {2, 5, 3, 1, 4};
    for (auto key : remove_keys) {
        index_key.SetFromInteger(key);
        tree.Remove(index_key, transaction);
    }

    // all gone
    rids.clear();
    for (auto key : keys) {
        rids.clear();
        index_key.SetFromInteger(key);
        tree.GetValue(index_key, rids);
        EXPECT_EQ(rids.size(), 0);
    }

    bpm->UnpinPage(HEADER_PAGE_ID, true);
    delete transaction;
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
}

TEST(BPlusTreeTests, DeleteScale) {
    // create KeyComparator and index schema
    Schema *key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema);

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManager(500, disk_manager);

    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    tree.SetOrder(4);

    GenericKey<8> index_key;
    RID rid;
    // create transaction
    Transaction *transaction = new Transaction(0);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(page_id);
    (void) header_page;

    int scale = 100;
    // int scale = 12;
    std::vector<int64_t> keys;
    for (int i = 0; i < scale; ++i) {
        keys.push_back(i + 1);
    }

    for (auto key : keys) {
        int64_t value = key & 0xFFFFFFFF;
        rid.Set((int32_t) (key >> 32), value);
        index_key.SetFromInteger(key);
        tree.Insert(index_key, rid, transaction);
    }

    // check all value is in the tree
    std::vector<RID> rids;
    for (auto key : keys) {
        rids.clear();
        index_key.SetFromInteger(key);
        tree.GetValue(index_key, rids);
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].GetSlotNum(), value);
    }

    // range query
    int64_t start_key = 1;
    int64_t current_key = start_key;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
        ++iterator) {
        auto location = (*iterator).second;
        EXPECT_EQ(location.GetPageId(), 0);
        EXPECT_EQ(location.GetSlotNum(), current_key);
        current_key = current_key + 1;
    }

    EXPECT_EQ(current_key, keys.size() + 1);


    // delete all
    for (auto key :keys) {
        index_key.SetFromInteger(key);
        tree.Remove(index_key, transaction);
    }

    // check all value is in the tree
    for (auto key : keys) {
        rids.clear();
        index_key.SetFromInteger(key);
        tree.GetValue(index_key, rids);
        EXPECT_EQ(rids.size(), 0);
    }

    bpm->UnpinPage(HEADER_PAGE_ID, true);
    delete transaction;
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
}

// /*
//   std::vector<int64_t> keys = {
//       31, 37, 24, 38, 33, 17, 30, 32, 6, 29, 7, 18, 20, 34, 40,
//       46, 28, 44, 1, 23, 2, 35, 27, 26, 3, 9, 12, 45, 43, 39, 36,
//       16, 41, 10, 13, 21, 22, 15, 42, 4, 14, 8, 19, 5, 11, 25
//   };

//   std::vector<int64_t> remove_keys = {
//       29, 34, 37
//   };

//   std::for_each(keys.begin(), keys.end(), [](int i) {
//     std::cerr << i << ", ";
//   });
//   std::cerr << std::endl;
//  */


TEST(BPlusTreeTests, DeleteRandom) {
    // create KeyComparator and index schema
    Schema *key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema);

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);

    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    tree.SetOrder(32);

    GenericKey<8> index_key;
    RID rid;
    // create transaction
    Transaction *transaction = new Transaction(0);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(page_id);
    (void) header_page;

    std::vector<int64_t> keys;
    int scale = 1000;
    for (int i = 0; i < scale; ++i) { keys.push_back(i + 1); }
    std::random_shuffle(keys.begin(), keys.end());

    for (auto key : keys) {
        int64_t value = key & 0xFFFFFFFF;
        rid.Set((int32_t) (key >> 32), value);
        index_key.SetFromInteger(key);
        tree.Insert(index_key, rid, transaction);
    }

    std::random_shuffle(keys.begin(), keys.end());
    // delete all
    for (auto key :keys) {
        index_key.SetFromInteger(key);
        tree.Remove(index_key, transaction);
    }

    bpm->UnpinPage(HEADER_PAGE_ID, true);
    delete transaction;
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
}

TEST(BPlusTreeTests, DeleteTest1) {
    // create KeyComparator and index schema
    std::string createStmt = "a bigint";
    Schema *key_schema = ParseCreateStatement(createStmt);
    GenericComparator<8> comparator(key_schema);

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);

    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    tree.SetOrder(3);

    GenericKey<8> index_key;
    RID rid;
    // create transaction
    Transaction *transaction = new Transaction(0);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(page_id);
    (void) header_page;

    std::vector<int64_t> keys = {1, 2, 3, 4, 5};
    for (auto key : keys) {
        int64_t value = key & 0xFFFFFFFF;
        rid.Set((int32_t) (key >> 32), value);
        index_key.SetFromInteger(key);
        tree.Insert(index_key, rid, transaction);
    }

    std::vector<RID> rids;
    for (auto key : keys) {
        rids.clear();
        index_key.SetFromInteger(key);
        tree.GetValue(index_key, rids);
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].GetSlotNum(), value);
    }

    int64_t start_key = 1;
    int64_t current_key = start_key;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
        ++iterator) {
        auto location = (*iterator).second;
        EXPECT_EQ(location.GetPageId(), 0);
        EXPECT_EQ(location.GetSlotNum(), current_key);
        current_key = current_key + 1;
    }

    EXPECT_EQ(current_key, keys.size() + 1);

    std::vector<int64_t> remove_keys = {1, 5};
    for (auto key : remove_keys) {
        index_key.SetFromInteger(key);
        tree.Remove(index_key, transaction);
    }

    start_key = 2;
    current_key = start_key;
    int64_t size = 0;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
        ++iterator) {
        auto location = (*iterator).second;
        EXPECT_EQ(location.GetPageId(), 0);
        EXPECT_EQ(location.GetSlotNum(), current_key);
        current_key = current_key + 1;
        size = size + 1;
    }

    EXPECT_EQ(size, 3);

    bpm->UnpinPage(HEADER_PAGE_ID, true);
    delete transaction;
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
}

TEST(BPlusTreeTests, DeleteTest2) {
    // create KeyComparator and index schema
    Schema *key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema);

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    tree.SetOrder(3);

    GenericKey<8> index_key;
    RID rid;
    // create transaction
    Transaction *transaction = new Transaction(0);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(page_id);
    (void) header_page;

    std::vector<int64_t> keys = {1, 2, 3, 4, 5};
    for (auto key : keys) {
        int64_t value = key & 0xFFFFFFFF;
        rid.Set((int32_t) (key >> 32), value);
        index_key.SetFromInteger(key);
        tree.Insert(index_key, rid, transaction);
    }

    std::vector<RID> rids;
    for (auto key : keys) {
        rids.clear();
        index_key.SetFromInteger(key);
        tree.GetValue(index_key, rids);
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].GetSlotNum(), value);
    }

    int64_t start_key = 1;
    int64_t current_key = start_key;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
        ++iterator) {
        auto location = (*iterator).second;
        EXPECT_EQ(location.GetPageId(), 0);
        EXPECT_EQ(location.GetSlotNum(), current_key);
        current_key = current_key + 1;
    }

    EXPECT_EQ(current_key, keys.size() + 1);

    std::vector<int64_t> remove_keys = {1, 5, 3, 4};
    for (auto key : remove_keys) {
        index_key.SetFromInteger(key);
        tree.Remove(index_key, transaction);
    }

    start_key = 2;
    current_key = start_key;
    int64_t size = 0;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
        ++iterator) {
        auto location = (*iterator).second;
        EXPECT_EQ(location.GetPageId(), 0);
        EXPECT_EQ(location.GetSlotNum(), current_key);
        current_key = current_key + 1;
        size = size + 1;
    }

    EXPECT_EQ(size, 1);

    bpm->UnpinPage(HEADER_PAGE_ID, true);
    delete transaction;
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
}

TEST(BPlusTreeTests, ScaleTest) {
    // create KeyComparator and index schema
    Schema *key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema);

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManager(3000, disk_manager);

    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    tree.SetOrder(32); 
    
    GenericKey<8> index_key;
    RID rid;
    // create transaction
    Transaction *transaction = new Transaction(0);
    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(page_id);
    (void) header_page;

    int64_t scale = 10000;
    std::vector<int64_t> keys;
    for (int64_t key = 1; key < scale; key++) {
        keys.push_back(key);
    }

    for (auto key : keys) {
        int64_t value = key & 0xFFFFFFFF;
        rid.Set((int32_t) (key >> 32), value);
        index_key.SetFromInteger(key);
        tree.Insert(index_key, rid, transaction);
    }
    std::vector<RID> rids;
    for (auto key : keys) {
        rids.clear();
        index_key.SetFromInteger(key);
        tree.GetValue(index_key, rids);
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].GetSlotNum(), value);
    }

    int64_t start_key = 1;
    int64_t current_key = start_key;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
        ++iterator) {
        current_key = current_key + 1;
    }
    EXPECT_EQ(current_key, keys.size() + 1);

    int64_t remove_scale = 9900;
    std::vector<int64_t> remove_keys;
    for (int64_t key = 1; key < remove_scale; key++) {
        remove_keys.push_back(key);
    }
    // std::random_shuffle(remove_keys.begin(), remove_keys.end());
    for (auto key : remove_keys) {
        index_key.SetFromInteger(key);
        tree.Remove(index_key, transaction);
    }

    start_key = 9900;
    current_key = start_key;
    int64_t size = 0;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
        ++iterator) {
        current_key = current_key + 1;
        size = size + 1;
    }

    EXPECT_EQ(size, 100);

    bpm->UnpinPage(HEADER_PAGE_ID, true);
    delete transaction;
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
}

TEST(BPlusTreeTests, RandomTest) {
    // create KeyComparator and index schema
    Schema *key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema);

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManager(20000, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    tree.SetOrder(99);

    GenericKey<8> index_key;
    RID rid;
    // create transaction
    Transaction *transaction = new Transaction(0);
    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(page_id);
    (void) header_page;

    int64_t scale = 10000;
    std::vector<int64_t> keys;
    for (int64_t key = 1; key < scale; key++) { keys.push_back(key); }

    std::random_shuffle(keys.begin(), keys.end());

    for (auto key : keys) {
        int64_t value = key & 0xFFFFFFFF;
        rid.Set((int32_t) (key >> 32), value);
        index_key.SetFromInteger(key);
        tree.Insert(index_key, rid, transaction);
    }
    std::vector<RID> rids;
    for (auto key : keys) {
        rids.clear();
        index_key.SetFromInteger(key);
        tree.GetValue(index_key, rids);
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].GetSlotNum(), value);
    }

    int64_t start_key = 1;
    int64_t current_key = start_key;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
        ++iterator) {
        current_key = current_key + 1;
    }
    EXPECT_EQ(current_key, keys.size() + 1);

    int64_t remove_scale = 9900;
    std::vector<int64_t> remove_keys;
    for (int64_t key = 1; key < remove_scale; key++) {
        remove_keys.push_back(key);
    }

    std::random_shuffle(remove_keys.begin(), remove_keys.end());
    for (auto key : remove_keys) {
        index_key.SetFromInteger(key);
        tree.Remove(index_key, transaction);
    }

    start_key = 9900;
    current_key = start_key;
    int64_t size = 0;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
        ++iterator) {
        current_key = current_key + 1;
        size = size + 1;
    }

    EXPECT_EQ(size, 100);

    bpm->UnpinPage(HEADER_PAGE_ID, true);
    delete transaction;
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
}

} // namespace cmudb
