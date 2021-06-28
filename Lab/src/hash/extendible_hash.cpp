#include <cassert>
#include <functional>
#include <list>
#include <bitset>
#include <iostream>

#include "hash/extendible_hash.h"
#include "page/page.h"

/**
 * vector
 * |——| -> map
 * |——| -> map
 * |——| -> map
 * |——| -> map
 * |——| -> map
 * https://cloud.tencent.com/developer/article/1020586
 * 数组的个数是2的D次方，桶的容量是2的L次方，D称为全局深度，L称为局部深度
 * 假设D=2,L=2。那么一个桶的最大数据量就是4，如果insert的时候，数据超过了4，就需要分裂新的桶
 * 
 * hash 函数与桶的 depth 是有关的
 * 所以分裂新桶后，depth 会发生变化, hash func is change, 所以 element 会被重新分配到不同的桶中
 *
 * 可扩展 hash 表示 buffer_pool_manager 在构造是创建的
 *
 * 全局depth决定了 array 的数量，2^depth=sizeof(array)
 *
 * bucket_size_ has a fix value, for example, 50
 *
 * size_t position = HashKey(key) & ((1 << depth) - 1);
 *  hashkey 的低位
 */
namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) 
    : bucket_size_(size), bucket_count_(0), pair_count_(0), depth(0) 
{
    /**
     * @brief 
     * emplace_back 向vector尾端插入元素，与push_back有所区别，效率更高一些
     * 全局 depth 的初始值是0，所以数组长度仅为 1
     * depth=0, array size=1  // depth=0的时候，hash result必然是0
     * depth=1, array size=2  // depth=1的时候，hash result是HashKey的最后一位
     * depth=2, array size=4  // 依次类推，hash result 是 HashKey 的低 depth 位
     * ...
     */
    bucket_.emplace_back(new Bucket(0, 0));
    bucket_count_ = 1;
}

/*
 * helper function to calculate the hashing address of input key
 * the result of HashKey(key) & ((1 << depth) - 1) is like
 * hash后的这个结果差别还蛮大的
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key)
{
    return std::hash<K>()(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const 
{
    /* 全局桶深度 */
    std::lock_guard<std::mutex> lock(mutex_);  /* 防止其它线程的修改，保证并发安全 */
    return depth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const 
{
    /** 
    * bucket_id 就是数组的偏移
    * 返回给定偏移的局部深度 
    */
    std::lock_guard<std::mutex> lock(mutex_);
    if(bucket_[bucket_id]) {
        return bucket_[bucket_id]->depth;
    }
    return -1;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const 
{
    /* 返回桶总数 */
    std::lock_guard<std::mutex> lock(mutex_);
    return bucket_count_;
}

/*
 * lookup function to find value associate with input key
 * 查找桶里的哈希表是否有该值
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) 
{
    std::lock_guard<std::mutex> lock(mutex_);
    size_t position = HashKey(key) & ((1 << depth) - 1);

    if(bucket_[position]) {
        if(bucket_[position]->items.find(key) != bucket_[position]->items.end()) {
            value = bucket_[position]->items[key];  /* 该值存在 */
            return true;
        }
    }
    return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 * 移除元素
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) 
{
    std::lock_guard<std::mutex> lock(mutex_);
    size_t position = HashKey(key) & ((1 << depth) - 1);
    size_t cnt = 0;

    if(bucket_[position]) {
        auto tmp_bucket = bucket_[position];
        cnt = tmp_bucket->items.erase(key);
        pair_count_ -= cnt;
    }
    return cnt != 0;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) 
{
    std::lock_guard<std::mutex> lock(mutex_);
    /**
     * @brief hash 函数
     * (1<<0)-1 = 0. 这谁来 & 都是 0 啊
     * 所以最开始在没有达到预设的桶的阈值之后，所有的 value 都是被插到第一个桶中
     * 超过之后，开始扩展桶
     * 全局 depth++，然后开始取 hash key 的最后一位，理论上是能够把已有的key平均分到两个桶中的
     *  hash func确实发生了变化
     * if (1 << depth) - 1 is 1, 奇数与它与是 1，偶数则为 0
     */
    size_t bucket_id = HashKey(key) & ((1 << depth) - 1);
#ifdef EX_HASH_DEBUG
    std::printf(
        "new insert, depth: %d, bucket count: %d, pair count:%ld, "
        "hashkey: %ld, bucket id: %ld\n",
        depth, bucket_count_, pair_count_, HashKey(key), bucket_id);
#endif
    if(bucket_[bucket_id] == nullptr) {
        /**
         * @brief 数组中的每一个元素是一个指向 Bucket 的智能指针
         * hash table 的类是没有析构函数
         * 拥有一个 std::map 的资源
         */
        bucket_[bucket_id] = std::make_shared<Bucket>(bucket_id, depth);
        ++bucket_count_;
    }

    auto bucket = bucket_[bucket_id];

    /* update, 比如说一个代表 page id 的 page change  */
    if(bucket->items.find(key) != bucket->items.end()) {
        bucket->items[key] = value;
        return;
    }

    /* insert，STL map 的 insert 函数，std::map 是有序的 map，对key按照默认规则排序 */
    bucket->items.insert({key, value});
    ++pair_count_;

    /**
     * @brief 当桶的长度超过预设的值，这里预设值为50，将分裂桶
     * 如果单桶的值没有超过一个预设值，则就是一个简单的 insert，insert 之后就完事了
     * 如果是第一次分裂桶，之前的桶深度是 0
     */
    if(bucket->items.size() > bucket_size_) 
    {
#ifdef EX_HASH_DEBUG
        std::printf("after insert, the bucket size is over\n");
        Show();  // for debug
#endif
        auto old_index = bucket->id;  // first split 0
        auto old_depth = bucket->depth;  // first split 0
        /**
         * @brief 分裂一个新桶出来，分裂一个桶是通过增加 depth，多选一个 key 的位作为 hashkey
         * 桶的 depth 增加了一位，有可能超过全局的 depth
         * 即 hash 表 slot size，即 sizeof(array) 不能太短
         * 要跟着扩展，大于等于最长的 local depth
         */
        std::shared_ptr<Bucket> new_bucket = split(bucket);

        if(new_bucket == nullptr) {
            bucket->depth = old_depth;
            return;
        }

        /**
         * @brief 新桶旧桶的 depth 是一样的
         * 若插入的桶的局部深度大于全局深度，则要扩展桶数组
         */
        if(bucket->depth > depth) {
            auto old_depth = depth;
            auto size = bucket_.size();  // 当前的 array size, bucket_是一个 vector
            /**
             * @brief 看看大了2的几次方出去
             * 例如桶分裂之后深度是 4，当前的全局 depth=0, 那么 factor=1<4=16
             */
            auto factor = (1 << (bucket->depth - depth));
            depth = bucket->depth;
            /* 调整大小后的 array size */
            bucket_.resize(bucket_.size() * factor);
            size_t newsize = bucket_.size();

            /**
             * @brief array 扩展了，但是 kv 对还在之前 array 的 slot 中
             * bucket以及new_bucket是指向Bucket(std::map)的指针
             * 先把目标桶以及split出来的新桶
             */
            bucket_[bucket->id] = bucket;
            bucket_[new_bucket->id] = new_bucket;
#ifdef EX_HASH_DEBUG
            std::printf(
                "after split, ready to extend the num of array\n"
                "   pre depth: %d, new depth: %d, factor: %d, "
                "pre size: %ld, new size: %ld\n", 
                old_depth, depth, factor, size, newsize);
            Show();
#endif
            /**
             * @brief size 是调整之前的大小
             * 没反应过来，扩展 array 的大小之后，调整后的这部分内容下面的代码不涉及
             */
            for(size_t i=0;i<size;++i) {
                if(bucket_[i]) {
                    if(i < bucket_[i]->id){
                        std::cout << "reset index: " << i << std::endl;
                        bucket_[i].reset();  /* 一定已经被copy到另一个桶中了，智能指针的reset方法，这里已经被rehash掉了 */
                    } else {
                        auto step = 1 << bucket_[i]->depth;
                        std::printf("i:%ld,step:%d\n", i, step);
                        for(size_t j = i + step; j < bucket_.size(); j += step) {
                            std::printf("rehash,i:%ld,j:%ld\n",i,j);
                            bucket_[j] = bucket_[i];  /* rehash */
                        }
                    }
                }
            }
        } else {
            std::printf("i do not know\n");
            for (size_t i = old_index; i < bucket_.size(); i += (1 << old_depth)) {
                bucket_[i].reset();
            }

            bucket_[bucket->id] = bucket;
            bucket_[new_bucket->id] = new_bucket;

            auto step = 1 << bucket->depth;
            for (size_t i = bucket->id + step; i < bucket_.size(); i += step) {
                bucket_[i] = bucket;
            }
            for (size_t i = new_bucket->id + step; i < bucket_.size(); i += step) {
                bucket_[i] = new_bucket;
            }
        }
    }
}

/**
 * @brief 分裂新桶
 * @param  b                desc
 * @return template <typename K, typename V> @c 
 */
template <typename K, typename V>
std::shared_ptr<typename ExtendibleHash<K, V>::Bucket>
ExtendibleHash<K, V>::split(std::shared_ptr<Bucket> &b) 
{
    /**
     * 分裂目标桶 b，以 ref 的形式接受，这里直接操作调用函数上的 b，即目标分裂的桶
     * 先利用 make_shared 创建一个新桶
     *  调用函数中也有这样的一个对象类型在接受 std::shared_ptr<Bucket> new_bucket = split(bucket);s
     */
#ifdef EX_HASH_DEBUG
    std::cout << "start split" << std::endl;
#endif
    // auto p = std::make_shared<int>(10); 可以理解为 int 类型的参数是 10
    /**
     * @brief 括号里是指针类的参数，会返回给被调用函数
     * 如果 rehash 失败的话（没有）
     */
    auto res = std::make_shared<Bucket>(0, b->depth);  // res 是新桶
    while(res->items.empty())
    {
        b->depth++;  // firstly is 1 now
        res->depth++;  // firstly is 1 now
#ifdef EX_HASH_DEBUG
        std::cout << "extend depth, new depth: " << b->depth << std::endl;
#endif
        /**
         * @brief 遍历桶，在 for 的过程中会修改数据，这里展示了一个比较巧妙的方式
         * 现在已经在一个桶中的元素，其 hashkey 的低 depth 位一定是相等的
         * 所以在多向前要一位，那只能是一分为二，只有在一分为2不ok的情况下，才会继续向前再取一位
         */
        for(auto it = b->items.begin(); it!=b->items.end();) {
            // hashkey 不为0, 由旧桶 move to 新桶
            if (HashKey(it->first) & (1 << (b->depth - 1))) {
                res->items.insert(*it);
                res->id = HashKey(it->first) & ((1 << b->depth) - 1);
#ifdef EX_HASH_DEBUG
                std::cout 
                    << "new bucket: " << res->id 
                    << ", org key id: " << it->first 
                    << ", Hash: " << HashKey(it->first) 
                    << ", & v: " << (1 << (b->depth - 1))
                    << ", hash & v: " << 1 << std::endl;
#endif
                it = b->items.erase(it);
            } else {
#ifdef EX_HASH_DEBUG
                std::cout 
                    << "old bucket: " << b->id 
                    << ", org key id: " << it->first 
                    << ", Hash: " << HashKey(it->first) 
                    << ", & v: " << (1 << (b->depth - 1)) 
                    << ", Hash & v: " << 0 << std::endl;
#endif
                ++it;
            }
        }

        /**
         * @brief 如果 depth++ 后，是要取所有 key 的后两位去分配桶，如果忽然发现旧桶中的数据全部被 rehash 了
         */
        if(b->items.empty()) 
        {
            // 先恢复原状
            b->items.swap(res->items);
            b->id = res->id;  // 能得到一个桶下标的最大值

            /**
             * @brief for debug
             */
            // std::cout << "After swap: " << std::endl;
            // Show();
            // std::cout << "New bucket" << std::endl;
            // std::cout 
            //     << " id is: " << res->id 
            //     << " depth is: " << res->depth << " keys: ";
            // std::map<K, V> tmp = res->items;
            // for(auto it = tmp.begin(); it != tmp.end(); ++it){
            //     std::cout << it->first << " ";
            // }
            // std::cout << std::endl;
        }
    }

    ++bucket_count_;
    return res;
}

template <typename K, typename V>
void ExtendibleHash<K, V>::Show() const 
{
#ifdef EX_HASH_DEBUG
    size_t i;
    auto size = bucket_.size();

    std::cout << "Show" << std::endl;
    for(i=0; i<size; ++i) {
        auto curr_bucket = bucket_[i];
        if(curr_bucket){
            std::cout 
                << "    index: " << i 
                << " id: " << curr_bucket->id 
                << " depth: " << curr_bucket->depth << " ,keys: ";

            std::map<K, V> tmp = curr_bucket->items;
            for(auto it = tmp.begin(); it != tmp.end(); ++it){
                std::cout << it->first << " ";
            }
            std::cout << std::endl;
        }
    }
#endif
    return;
}

// std::map<K, V>::iterator
// template <typename K, typename V>
// friend std::ostream& operator << (std::ostream& os, const std::map<K, V>& tmp)
// {
//     os << "key:" << tmp.frist << ",value" << tmp.second << std::endl;
//     return os ;
// }

// 为了模板类的实现能够与 .h 分离
template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
