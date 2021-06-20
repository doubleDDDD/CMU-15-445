/**
 * replacer.h
 *
 * Abstract class for replacer, your LRU should implement those methods
 */
#pragma once

#include <cstdlib>

namespace cmudb {

template <typename T> class Replacer {
public:
  Replacer() {}
  // 基类的析构全部写成虚函数
  virtual ~Replacer() {}
  // 以下均为纯虚函数
  virtual void Insert(const T &value) = 0;
  virtual bool Victim(T &value) = 0;
  virtual bool Erase(const T &value) = 0;
  virtual size_t Size() = 0;
};

} // namespace cmudb
