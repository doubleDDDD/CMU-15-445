/**
 * header_page.h
 *
 * Database use the first page (page_id = 0) as header page to store metadata, in
 * our case, we will contain information about table/index name (length less than
 * 32 bytes) and their corresponding root_id
 *
 * Format (size in byte):
 *  -----------------------------------------------------------------
 * | RecordCount (4) | Entry_1 name (32) | Entry_1 root_id (4) | ... |
 *  -----------------------------------------------------------------
 * 
 * 我知道为啥在创建索引（即b+tree的时候，并没有考虑分隔之类的情形）
 * cmudb视table与index是一类东西
 *  创建表的话就是分配page写数据库的记录
 *  创建索引的话，与持久化一个数据库表，在存储引擎的表现上看来差异是不大的
 * 以b+tree为例，节点是由数据库中page的概念来代表的，所以b+tree索引的创建确实与创建一张表异曲同工
 *  看样子索引的名字与表的名字是不能重复的
 */

#pragma once

#include "page/page.h"

#include <cstring>

namespace cmudb {

class HeaderPage : public Page {
public:
    void Init() { SetRecordCount(0); }
    /**
    * Record related
    */
    bool InsertRecord(const std::string &name, const page_id_t root_id);
    bool DeleteRecord(const std::string &name);
    bool UpdateRecord(const std::string &name, const page_id_t root_id);

    // return root_id if success
    bool GetRootId(const std::string &name, page_id_t &root_id);
    int GetRecordCount();

    // public 一下
    bool TableExist(const std::string &name) { return FindRecord(name) != -1; }

private:
    /**
    * helper functions
    */
    int FindRecord(const std::string &name);

    void SetRecordCount(int record_count);
};
} // namespace cmudb