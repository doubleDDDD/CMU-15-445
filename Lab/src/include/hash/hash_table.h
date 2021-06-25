/**
 * hash_table.h
 *
 * Abstract class for hash table implementation
 */

#pragma once

namespace cmudb {

/** 
  自行实现的hash table 
  纯虚函数，所以只能是个抽象类
*/
template <typename K, typename V> class HashTable {
public:
  HashTable() {}
  virtual ~HashTable() {}  /* 基类虚析构函数 */

  // lookup and modifier
  virtual bool Find(const K &key, V &value) = 0;
  virtual bool Remove(const K &key) = 0;
  virtual void Insert(const K &key, const V &value) = 0;
  virtual size_t Size() const = 0;
};

} // namespace cmudb
