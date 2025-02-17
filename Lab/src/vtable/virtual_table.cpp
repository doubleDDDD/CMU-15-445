/**
 * virtual_table.cpp
 */
#include <algorithm>
#include <cstring>
#include <iostream>
#include <sys/stat.h>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/string_utility.h"
#include "page/header_page.h"
#include "vtable/virtual_table.h"

namespace cmudb {

SQLITE_EXTENSION_INIT1

/* API implementation */

/** 创建虚拟表
 * @brief 
 * @param  db               desc 一个db连接的实例
 * @param  pAux             desc
 * @param  argc             desc
 * @param  argv             desc
 * @param  ppVtab           desc
 * @param  pzErr            desc
 * @return int @c 
 */
int VtabCreate(
    sqlite3 *db, void *pAux, int argc, const char *const *argv,
    sqlite3_vtab **ppVtab, char **pzErr) 
{
    // BackTracePlus();
    std::printf(
        "\nCreate vtable!,tid=%d\n\n", static_cast<int>(gettid()));
    /**
    * @brief 
    * storage_engine_ is a global var
    * in sqlite3_vtable_init 
    * storage_engine_ = new StorageEngine(db_file_name);
    */
    BufferPoolManager *buffer_pool_manager = storage_engine_->buffer_pool_manager_;
    LockManager *lock_manager = storage_engine_->lock_manager_;
    LogManager *log_manager = storage_engine_->log_manager_;

    /**
    * @brief fetch header page from buffer pool
        这里有一个类型转换 page->HeaderPage
        HeaderPage存放表的元数据
        这个page已经 new 过了，所以这里基本是一定可以fetch到page的
    */
    HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager->FetchPage(HEADER_PAGE_ID));

    // the first three parameter:(1) module name (2) database name (3)table name
    assert(argc >= 4);
    // parse arg[3](string that defines table schema)
    std::string schema_string(argv[3]);
    schema_string = schema_string.substr(1, (schema_string.size() - 2));
    Schema *schema = ParseCreateStatement(schema_string);  // 根据字符串创建 schema 对象

    // 如果对应的虚拟表存在，就按照connect来处理
    if(header_page->TableExist(std::string(argv[2]))){
        // std::printf("table %s has already exists!\n", std::string(argv[2]).c_str());
        return VtabConnect(db, pAux, argc, argv, ppVtab, pzErr);
        // return SQLITE_ERROR;
    }

    // parse arg[4](string that defines table index)
    Index *index = nullptr;
    if (argc > 4) {
        // 如果大于4，则说明需要 为表 创建索引
        // 正常的操作是在表的某一列（多列）上创建索引，且索引是有名字的
        std::string index_string(argv[4]);  // 在哪些列上创建索引  (column1, column2) column name
        index_string = index_string.substr(1, (index_string.size() - 2));  // 这个地方是为了把括号去掉
        // create index object, allocate memory space
        IndexMetadata *index_metadata = ParseIndexStatement(index_string, std::string(argv[2]), schema);
        // 组合索引，B+tree也仅仅只有一个
        index = ConstructIndex(index_metadata, buffer_pool_manager);
    }

    // create table object, allocate memory space
    VirtualTable *table = new VirtualTable(schema, buffer_pool_manager, lock_manager, log_manager, index);
    // insert table root page info into header page
    header_page->InsertRecord(std::string(argv[2]), table->GetFirstPageId());

    // // for debug
    // Page *res = nullptr;
    // HashTable<page_id_t, Page *> *page_table____ = buffer_pool_manager->GetPageTable();
    // page_table____->Find(HEADER_PAGE_ID, res);
    buffer_pool_manager->UnpinPage(HEADER_PAGE_ID, true);
    // register virtual table within sqlite system
    schema_string = "CREATE TABLE X(" + schema_string + ");";
    assert(sqlite3_declare_vtab(db, schema_string.c_str()) == SQLITE_OK);

    *ppVtab = reinterpret_cast<sqlite3_vtab *>(table);
    return SQLITE_OK;
}

/**
 * @brief
 * @param  db               desc
 * @param  pAux             desc
 * @param  argc             de  sc
 * @param  argv             desc
 * @param  ppVtab           desc
 * @param  pzErr            desc
 */
int VtabConnect(
    sqlite3 *db, void *pAux, int argc, const char *const *argv,
    sqlite3_vtab **ppVtab, char **pzErr) 
{
    // BackTracePlus();
    std::printf(
        "\nConnect vtable!,tid=%d\n\n", static_cast<int>(gettid()));

    assert(argc >= 4);
    std::string schema_string(argv[3]);
    // remove the very first and last character
    schema_string = schema_string.substr(1, (schema_string.size() - 2));
    // new virtual table object, allocate memory space
    Schema *schema = ParseCreateStatement(schema_string);

    BufferPoolManager *buffer_pool_manager = storage_engine_->buffer_pool_manager_;
    LockManager *lock_manager = storage_engine_->lock_manager_;
    LogManager *log_manager = storage_engine_->log_manager_;

    // Retrieve table root page info from header page
    HeaderPage *header_page =
        static_cast<HeaderPage *>(buffer_pool_manager->FetchPage(HEADER_PAGE_ID));
    page_id_t table_root_id;
    header_page->GetRootId(std::string(argv[2]), table_root_id);

    // parse arg[4](string that defines table index)
    Index *index = nullptr;
    if (argc > 4) {
        std::string index_string(argv[4]);
        index_string = index_string.substr(1, (index_string.size() - 2));
        // create index object, allocate memory space
        IndexMetadata *index_metadata = ParseIndexStatement(index_string, std::string(argv[2]), schema);
        // Retrieve index root page info from header page
        page_id_t index_root_id;
        header_page->GetRootId(index_metadata->GetName(), index_root_id);
        index = ConstructIndex(index_metadata, buffer_pool_manager, index_root_id);
    }

    VirtualTable *table = new VirtualTable(
        schema, buffer_pool_manager, lock_manager, log_manager, index, table_root_id);

    // register virtual table within sqlite system
    schema_string = "CREATE TABLE X(" + schema_string + ");";
    assert(sqlite3_declare_vtab(db, schema_string.c_str()) == SQLITE_OK);

    *ppVtab = reinterpret_cast<sqlite3_vtab *>(table);
    buffer_pool_manager->UnpinPage(HEADER_PAGE_ID, false);
    return SQLITE_OK;
}

/*
 * we only support
 * (1) equlity check. e.g select * from foo where a = 1
 * (2) indexed column == predicated column
 */
int VtabBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo) {
  // LOG_DEBUG("VtabBestIndex");
  VirtualTable *table = reinterpret_cast<VirtualTable *>(tab);
  if (table->GetIndex() == nullptr)
    return SQLITE_OK;
  const std::vector<int> key_attrs = table->GetIndex()->GetKeyAttrs();
  // make sure indexed column == predicate column
  // e.g select * from foo where a = 1 and b =2; indexed column must be {a,b}
  if (pIdxInfo->nConstraint != (int)(key_attrs.size()))
    return SQLITE_OK;

  int counter = 0;
  bool is_index_scan = true;
  for (int i = 0; i < pIdxInfo->nConstraint; i++) {
    if (pIdxInfo->aConstraint[i].usable == 0)
      continue;
    int item = pIdxInfo->aConstraint[i].iColumn;
    // if predicate column is part of indexed column
    if (std::find(key_attrs.begin(), key_attrs.end(), item) !=
        key_attrs.end()) {
      // equlity check
      if (pIdxInfo->aConstraint[i].op != SQLITE_INDEX_CONSTRAINT_EQ) {
        is_index_scan = false;
        break;
      }
      pIdxInfo->aConstraintUsage[i].argvIndex = (i + 1);
      counter++;
    }
  }

  if (counter == (int)key_attrs.size() && is_index_scan) {
    pIdxInfo->idxNum = 1;
  }
  return SQLITE_OK;
}

/**
 * @brief 增加脏页写回的步骤，即将数据持久化一下
 * @param  pVtab            desc
 * @return int @c 
 */
int VtabDisconnect(sqlite3_vtab *pVtab) {
  std::printf("disconnect vtable!\n");
  VirtualTable *virtual_table = reinterpret_cast<VirtualTable *>(pVtab);
  // 将所有的脏页写回
  storage_engine_->buffer_pool_manager_->FlushAllDirtyPage();
  delete virtual_table;
  // delete all the global managers
  delete storage_engine_;
  return SQLITE_OK;
}

int VtabOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
    // LOG_DEBUG("VtabOpen");
    // if read operation, begin transaction here
    if (global_transaction_ == nullptr) {
        VtabBegin(pVtab);
    }
    // 线程id与事务id都给一下
    std::printf(
        "Open vtable!,Tid=%d,tid=%d\n", 
        static_cast<int>(global_transaction_->GetTransactionId()),
        static_cast<int>(gettid()));
    // 以下操作一定在事务中
    VirtualTable *virtual_table = reinterpret_cast<VirtualTable *>(pVtab);
    Cursor *cursor = new Cursor(virtual_table);
    *ppCursor = reinterpret_cast<sqlite3_vtab_cursor *>(cursor);

    // 如果出错这里是要返回错误的啊，这全部无脑返回OK不行啊，检查一下事务的状态
    if(global_transaction_->GetState() == TransactionState::ABORTED) {return SQLITE_ABORT;}
    return SQLITE_OK;
}

int VtabClose(sqlite3_vtab_cursor *cur) {
    if(global_transaction_){
        std::printf(
            "Close vtable!,Tid=%d,tid=%d\n", 
            static_cast<int>(global_transaction_->GetTransactionId()),
            static_cast<int>(gettid()));
    }

    // LOG_DEBUG("VtabClose");
    Cursor *cursor = reinterpret_cast<Cursor *>(cur);
    // if read operation, commit transaction here
    // 这里提交事务的话，是个分号就要提交事务，不太合理啊
    // VtabCommit(nullptr);
    delete cursor;
    return SQLITE_OK;
}

/*
** This method is called to "rewind" the cursor object back
** to the first row of output. This method is always called at least
** once prior to any call to VtabColumn() or VtabRowid() or
** VtabEof().
*/
int VtabFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum, const char *idxStr,
               int argc, sqlite3_value **argv) {
  // LOG_DEBUG("VtabFilter");
  Cursor *cursor = reinterpret_cast<Cursor *>(pVtabCursor);
  Schema *key_schema;
  // if indexed scan
  if (idxNum == 1) {
    cursor->SetScanFlag(true);
    // Construct the tuple for point query
    key_schema = cursor->GetKeySchema();
    Tuple scan_tuple = ConstructTuple(key_schema, argv);
    cursor->ScanKey(scan_tuple);
  }
  return SQLITE_OK;
}

int VtabNext(sqlite3_vtab_cursor *cur) {
  // LOG_DEBUG("VtabNext");
  Cursor *cursor = reinterpret_cast<Cursor *>(cur);
  ++(*cursor);
  return SQLITE_OK;
}

int VtabEof(sqlite3_vtab_cursor *cur) {
  // LOG_DEBUG("VtabEof");
  Cursor *cursor = reinterpret_cast<Cursor *>(cur);
  return cursor->isEof();
}

int VtabColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i) {
  Cursor *cursor = reinterpret_cast<Cursor *>(cur);
  cursor->GetTableIterator();  // 这里只有调用一次gdb的时候才可以访问，无实际意义
  Schema *schema = cursor->GetVirtualTable()->GetSchema();
  cursor->GetVirtualTable()->GetTableHeap();  // 这里只有调用一次gdb的时候才可以访问，无实际意义
  // get column type and value
  TypeId type = schema->GetType(i);
  Value v = cursor->GetCurrentValue(schema, i);

  switch (type) {
  case TypeId::TINYINT:
  case TypeId::BOOLEAN:
    sqlite3_result_int(ctx, (int)v.GetAs<int8_t>());
    break;
  case TypeId::SMALLINT:
    sqlite3_result_int(ctx, (int)v.GetAs<int16_t>());
    break;
  case TypeId::INTEGER:
    sqlite3_result_int(ctx, (int)v.GetAs<int32_t>());
    break;
  case TypeId::BIGINT:
    sqlite3_result_int64(ctx, (sqlite3_int64)v.GetAs<int64_t>());
    break;
  case TypeId::DECIMAL:
    sqlite3_result_double(ctx, v.GetAs<double>());
    break;
  case TypeId::VARCHAR:
    sqlite3_result_text(ctx, v.GetData(), -1, SQLITE_TRANSIENT);
    break;
  default:
    return SQLITE_ERROR;
  } // End of switch
  return SQLITE_OK;
}

// 这里读取的仅仅是rowID
int VtabRowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *pRowid) {
  // LOG_DEBUG("VtabRowid");
  Cursor *cursor = reinterpret_cast<Cursor *>(cur);
  // return rid of the tuple that cursor currently points at
  *pRowid = cursor->GetCurrentRid();
  return SQLITE_OK;
}

int VtabUpdate(
    sqlite3_vtab *pVTab, int argc, sqlite3_value **argv,
    sqlite_int64 *pRowid) 
{
    // LOG_DEBUG("VtabUpdate");
    std::printf("VtabUpdate, tid=%d\n", global_transaction_->GetTransactionId());
    VirtualTable *table = reinterpret_cast<VirtualTable *>(pVTab);

    // RID帮忙索引一个tuple所在的pageid与slotid

    // The single row with rowid equal to argv[0] is deleted
    // 删除单行操作
    if (argc == 1) {
        const RID rid(sqlite3_value_int64(argv[0]));
        // delete entry from index
        table->DeleteEntry(rid);
        // delete tuple from table heap
        table->DeleteTuple(rid);
    }
    // A new row is inserted with a rowid argv[1] and column values in argv[2] and
    // following. If argv[1] is an SQL NULL, the a new unique rowid is generated
    // automatically.
    // 增加新行
    else if (argc > 1 && sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        Schema *schema = table->GetSchema();
        Tuple tuple = ConstructTuple(schema, (argv + 2));
        // insert into table heap
        RID rid;
        table->InsertTuple(tuple, rid);
        // insert into index
        table->InsertEntry(tuple, rid);
    }
    // The row with rowid argv[0] is updated with new values in argv[2] and
    // following parameters.
    else if (argc > 1 && sqlite3_value_type(argv[0]) != SQLITE_NULL) {
        Schema *schema = table->GetSchema();
        Tuple tuple = ConstructTuple(schema, (argv + 2));
        RID rid(sqlite3_value_int64(argv[0]));
        // for update, index always delete and insert
        // because you have no clue key has been updated or not
        table->DeleteEntry(rid);  // 没有索引直接返回 
        // if true, then update succeed, rid keep the same
        // else, delete & insert
        // tuple与rid都是新构建的
        if (table->UpdateTuple(tuple, rid) == false) {
            table->DeleteTuple(rid);
            // rid should be different
            table->InsertTuple(tuple, rid);
        }
        table->InsertEntry(tuple, rid);
    }
    return SQLITE_OK;
}

/**
 * @brief 尝试保证事务T1先被执行
 * @param  pVTab            desc
 * @return int @c 
 */
int VtabBegin(sqlite3_vtab *pVTab) {
    // LOG_DEBUG("VtabBegin");
    // create new transaction(write operation will call this method)
    global_transaction_ = storage_engine_->transaction_manager_->Begin();

    std::printf(
        "\nBegin vtable!,Tid=%d,tid=%d\n\n", 
        static_cast<int>(global_transaction_->GetTransactionId()),
        static_cast<int>(gettid()));
    // std::printf("\n");
    // BackTracePlus();
    // std::printf("\n");
    return SQLITE_OK;
}

int 
VtabCommit(sqlite3_vtab *pVTab) {
    // LOG_DEBUG("VtabCommit");
    auto transaction = GetTransaction();
    if (transaction == nullptr)
        return SQLITE_OK;
    
    std::printf(
        "\nCommit vtable!,Tid=%d,tid=%d\n\n", 
        static_cast<int>(transaction->GetTransactionId()),
        static_cast<int>(gettid()));    
    // std::printf("\n");
    // BackTracePlus();
    // std::printf("\n");
    // get global txn manager
    auto transaction_manager = storage_engine_->transaction_manager_;
    // invoke transaction manager to commit(this txn can't fail)
    transaction_manager->Commit(transaction);
    // when commit, delete transaction pointer and set to null
    delete transaction;
    global_transaction_ = nullptr;

    return SQLITE_OK;
}

sqlite3_module VtableModule = {
    0,              /* iVersion */
    VtabCreate,     /* xCreate */
    VtabConnect,    /* xConnect */
    VtabBestIndex,  /* xBestIndex */
    VtabDisconnect, /* xDisconnect */
    VtabDisconnect, /* xDestroy */
    VtabOpen,       /* xOpen - open a cursor */
    VtabClose,      /* xClose - close a cursor */
    VtabFilter,     /* xFilter - configure scan constraints */
    VtabNext,       /* xNext - advance a cursor */
    VtabEof,        /* xEof - check for end of scan */
    VtabColumn,     /* xColumn - read data */
    VtabRowid,      /* xRowid - read data */
    VtabUpdate,     /* xUpdate */
    VtabBegin,      /* xBegin */
    0,              /* xSync */
    VtabCommit,     /* xCommit */
    0,              /* xRollback */
    0,              /* xFindMethod */
    0,              /* xRename */
    0,              /* xSavepoint */
    0,              /* xRelease */
    0,              /* xRollbackTo */
};

/* start in extension, 调用的时候尝试保证一下线程安全 */
#ifdef _WIN32
__declspec(dllexport)
#endif
extern "C" int sqlite3_vtable_init(sqlite3 *db, char **pzErrMsg,
                                       const sqlite3_api_routines *pApi) {
    std::lock_guard<std::mutex> guard(thread_mutex);

    SQLITE_EXTENSION_INIT2(pApi);
    /* 该数据库的存储形式是一个文件，文件名vtable.db */
    std::string db_file_name = "vtable.db";
    struct stat buffer;  /* stat命令检查文件是否存在 */
    bool is_file_exist = (stat(db_file_name.c_str(), &buffer) == 0);
    std::printf("Prepare to init vtable in load!\n");

    /**
     * @brief Construct a new if object
     * 如果模块已经被创建了
     * 则应该直接复用
     * 针对当前的数据库连接db,创建模块vtable，这个模块需要再connect一个虚拟表
     * 就可以正常工作了
     */
    if(storage_engine_){
        int rc = sqlite3_create_module(
            db, "vtable", &VtableModule, nullptr);
        return rc;
    }

    /**
     * @brief init storage engine
     */
    storage_engine_ = new StorageEngine(db_file_name);
    // start the logging set ENABLE_LOGGING = true;
    storage_engine_->log_manager_->RunFlushThread();
    // create header page from BufferPoolManager if necessary
    if (!is_file_exist) {
        std::printf("file is not exist!\n");
        page_id_t header_page_id;
        // 从free_list中取一个page到hash_table,ref++ 
        storage_engine_->buffer_pool_manager_->NewPage(header_page_id);
        assert(header_page_id == HEADER_PAGE_ID);
        // 把这个page设置为脏然后刷下去
        // firstpage->SetDirty();  unpin会set page为脏
        /* unpin表示不再使用page */
        storage_engine_->buffer_pool_manager_->UnpinPage(header_page_id, true);
        storage_engine_->buffer_pool_manager_->FlushPage(header_page_id);
    }

    int rc = sqlite3_create_module(db, "vtable", &VtableModule, nullptr);
    return rc;
}

/* Helpers */
Schema *ParseCreateStatement(const std::string &sql_base) {
    std::string::size_type n;
    std::vector<Column> v;
    std::string column_name;
    std::string column_type;
    int column_length = 0;
    TypeId type = INVALID;
    // create a copy of the sql query
    std::string sql = sql_base;
    // prepocess, transform sql string into lower case（小写）
    // 怪不得说 sql 是大小写不敏感的
    std::transform(sql.begin(), sql.end(), sql.begin(), ::tolower);
    // 创建table格式的时候是这个样子的 (a int, b varchar, ...)  逗号是列的分割符
    std::vector<std::string> tok = StringUtility::Split(sql, ',');
    // iterate through returned result
    for (std::string &t : tok) {
        type = INVALID;
        column_length = 0;
        // whitespace seperate column name and type
        // 原来这里是这样来处理的
        n = t.find_first_of(' ');
        column_name = t.substr(0, n);  // 列名 
        column_type = t.substr(n + 1);  // 列类型
        // deal with varchar(size) situation varchar会指定字符串的宽度
        n = column_type.find_first_of('(');
        if (n != std::string::npos) {
            column_length = std::stoi(column_type.substr(n + 1));  // 该列字符串所占的长度，该长度是用户指定的 varchar(13), 则 column_length=13
            column_type = column_type.substr(0, n);
        }
        if (column_type == "bool" || column_type == "boolean") {
            type = BOOLEAN;
        } else if (column_type == "tinyint") {
            type = TINYINT;
        } else if (column_type == "smallint") {
            type = SMALLINT;
        } else if (column_type == "int" || column_type == "integer") {
            type = INTEGER;
        } else if (column_type == "bigint") {
            type = BIGINT;
        } else if (column_type == "double" || column_type == "float") {
            type = DECIMAL;
        } else if (column_type == "varchar" || column_type == "char") {
            type = VARCHAR;
            column_length = (column_length == 0) ? 32 : column_length;  // 测试代码都是非常的严谨
        }

        // construct each column
        if (type == INVALID) {
            throw Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "unknown type for create table");
        } else if (type == VARCHAR) {
            Column col(type, column_length, column_name);
            v.emplace_back(col);
        } else {
            Column col(type, Type::GetTypeSize(type), column_name);
            v.emplace_back(col);
        }
    }
    Schema *schema = new Schema(v);
    // LOG_DEBUG("%s", schema->ToString().c_str());

    return schema;
}

/**
 * @brief 
 * @param  sql              desc 在哪些列上创建索引 （column1,column2,...）
 * @param  table_name       desc
 * @param  schema           desc 整个 table 的 schema
 * @return IndexMetadata* @c 
 */
IndexMetadata *ParseIndexStatement(
    std::string &sql,
    const std::string &table_name,
    Schema *schema) 
{
    std::string::size_type n;
    std::string index_name;  // 索引名就是一个组合 column1column2...
    std::vector<int> key_attrs;
    int column_id = -1;
   
    // prepocess, transform sql string into lower case，小写
    std::transform(sql.begin(), sql.end(), sql.begin(), ::tolower);
    n = sql.find_first_of(' ');
    // NOTE: must use whitespace to seperate index name and indexed column names
    assert(n != std::string::npos);
    index_name = sql.substr(0, n);
    sql = sql.substr(n + 1);

    std::vector<std::string> tok = StringUtility::Split(sql, ',');
    // iterate through returned result
    // 组合索引，需要check都要在哪些列上创建索引
    for (std::string &t : tok) {
        StringUtility::Trim(t);
        column_id = schema->GetColumnID(t);
        if (column_id != -1)
        key_attrs.emplace_back(column_id);
    }
    if ((int)key_attrs.size() > schema->GetColumnCount())
        throw Exception(EXCEPTION_TYPE_INDEX, "can't create index, format error");

    // 需要在哪些列上创建组合索引
    IndexMetadata *metadata =
        new IndexMetadata(index_name, table_name, schema, key_attrs);

    // LOG_DEBUG("%s", metadata->ToString().c_str());
    return metadata;
}

Tuple ConstructTuple(Schema *schema, sqlite3_value **argv) {
  int column_count = schema->GetColumnCount();
  Value v(TypeId::INVALID);
  std::vector<Value> values;
  // iterate through schema, generate column value to insert
  for (int i = 0; i < column_count; i++) {
    TypeId type = schema->GetType(i);

    switch (type) {
    case TypeId::BOOLEAN:
    case TypeId::INTEGER:
    case TypeId::SMALLINT:
    case TypeId::TINYINT:
      v = Value(type, (int32_t)sqlite3_value_int(argv[i]));
      break;
    case TypeId::BIGINT:
      v = Value(type, (int64_t)sqlite3_value_int64(argv[i]));
      break;
    case TypeId::DECIMAL:
      v = Value(type, sqlite3_value_double(argv[i]));
      break;
    case TypeId::VARCHAR:
      v = Value(type, std::string(reinterpret_cast<const char *>(
                          sqlite3_value_text(argv[i]))));
      break;
    default:
      break;
    } // End of switch
    values.emplace_back(v);
  }
  Tuple tuple(values, schema);

  return tuple;
}

// serve the functionality of index factory
Index *ConstructIndex(
    IndexMetadata *metadata,
    BufferPoolManager *buffer_pool_manager,
    page_id_t root_id) 
{
    // The size of the key in bytes
    Schema *key_schema = metadata->GetKeySchema();  // table schema 的子集
    int key_size = key_schema->GetLength();
    // for each varchar attribute, we assume the largest size is 16 bytes
    key_size += 16 * key_schema->GetUnlinedColumnCount();

    if (key_size <= 4) {
        return new BPlusTreeIndex<GenericKey<4>, RID, GenericComparator<4>>(
            metadata, buffer_pool_manager, root_id);
    } else if (key_size <= 8) {
        return new BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>(
            metadata, buffer_pool_manager, root_id);
    } else if (key_size <= 16) {
        return new BPlusTreeIndex<GenericKey<16>, RID, GenericComparator<16>>(
            metadata, buffer_pool_manager, root_id);
    } else if (key_size <= 32) {
        return new BPlusTreeIndex<GenericKey<32>, RID, GenericComparator<32>>(
            metadata, buffer_pool_manager, root_id);
    } else {
        return new BPlusTreeIndex<GenericKey<64>, RID, GenericComparator<64>>(
            metadata, buffer_pool_manager, root_id);
    }
}

// 虚拟表的全局函数
Transaction *GetTransaction() { return global_transaction_; }

} // namespace cmudb
