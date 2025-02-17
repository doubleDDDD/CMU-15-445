/**
 * index.h
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "catalog/schema.h"
#include "table/tuple.h"
#include "type/value.h"

namespace cmudb {

/**
 * class IndexMetadata - Holds metadata of an index object
 *
 * The metadata object maintains the tuple schema and key attribute of an
 * index, since the external callers does not know the actual structure of
 * the index key, so it is the index's responsibility to maintain such a
 * mapping relation and does the conversion between tuple key and index key
 *
 * 可能需要在多个列上创建组合索引，所以需要一个数据结构来表示索引
 * 组合索引也有自己的 schema，是 table schema 的一个子集
 */
class Transaction;
class IndexMetadata {
    IndexMetadata() = delete;

public:
    IndexMetadata(std::string index_name, std::string table_name,
                    const Schema *tuple_schema, const std::vector<int> &key_attrs)
        : name_(index_name), table_name_(table_name), key_attrs_(key_attrs) 
    {
        key_schema_ = Schema::CopySchema(tuple_schema, key_attrs_);
    }

    ~IndexMetadata() { delete key_schema_; };

    inline const std::string &GetName() const { return name_; }

    inline const std::string &GetTableName() { return table_name_; }

    // Returns a schema object pointer that represents the indexed key
    inline Schema *GetKeySchema() const { return key_schema_; }

    // Return the number of columns inside index key (not in tuple key)
    // Note that this must be defined inside the cpp source file
    // because it uses the member of catalog::Schema which is not known here
    int GetIndexColumnCount() const { return (int) key_attrs_.size(); }

    //  Returns the mapping relation between indexed columns  and base table
    //  columns
    inline const std::vector<int> &GetKeyAttrs() const { return key_attrs_; }

    // Get a string representation for debugging
    const std::string ToString() const {
        std::stringstream os;

        os << "IndexMetadata["
            << "Name = " << name_ << ", "
            << "Type = B+Tree, "
            << "Table name = " << table_name_ << "] :: ";
        os << key_schema_->ToString();

        return os.str();
    }

private:
    std::string name_;
    std::string table_name_;
    // The mapping relation between key schema and tuple schema
    const std::vector<int> key_attrs_;
    // schema of the indexed key
    Schema *key_schema_;
};

/////////////////////////////////////////////////////////////////////
// Index class definition
/////////////////////////////////////////////////////////////////////

/**
 * class Index - Base class for derived indices of different types
 *
 * The index structure majorly maintains information on the schema of the
 * schema of the underlying table and the mapping relation between index key
 * and tuple key, and provides an abstracted way for the external world to
 * interact with the underlying index implementation without exposing
 * the actual implementation's interface.
 *
 * Index object also handles predicate scan, in addition to simple insert,
 * delete, predicate insert, point query, and full index scan. Predicate scan
 * only supports conjunction, and may or may not be optimized depending on
 * the type of expressions inside the predicate.
 */
class Index {
public:
    Index(IndexMetadata *metadata) : metadata_(metadata) {}

    virtual ~Index() { delete metadata_; }

    // Return the metadata object associated with the index
    IndexMetadata *GetMetadata() const { return metadata_; }

    int GetIndexColumnCount() const { return metadata_->GetIndexColumnCount(); }

    const std::string &GetName() const { return metadata_->GetName(); }

    Schema *GetKeySchema() const { return metadata_->GetKeySchema(); }

    const std::vector<int> &GetKeyAttrs() const { return metadata_->GetKeyAttrs(); }

    // Get a string representation for debugging
    const std::string ToString() const {
        std::stringstream os;

        os << "INDEX: (" << GetName() << ")";
        os << metadata_->ToString();
        return os.str();
    }

    ///////////////////////////////////////////////////////////////////
    // Point Modification
    ///////////////////////////////////////////////////////////////////
    // designed for secondary indexes.
    virtual void InsertEntry(const Tuple &key, RID rid,
                            Transaction *transaction = nullptr) = 0;

    // delete the index entry linked to given tuple
    virtual void DeleteEntry(const Tuple &key,
                            Transaction *transaction = nullptr) = 0;

    virtual void ScanKey(const Tuple &key, std::vector<RID> &result,
                        Transaction *transaction = nullptr) = 0;

private:
    //===--------------------------------------------------------------------===//
    //  Data members
    //===--------------------------------------------------------------------===//
    IndexMetadata *metadata_;
};

} // namespace cmudb
