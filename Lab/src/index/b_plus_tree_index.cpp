/**
 * b_plus_tree_index.cpp
 * 就是B+tree的insert,delete以及getvalue
 */

#include "index/b_plus_tree_index.h"

namespace cmudb {
/*
 * Constructor
 */
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_INDEX_TYPE::BPlusTreeIndex(IndexMetadata *metadata,
                                     BufferPoolManager *buffer_pool_manager,
                                     page_id_t root_page_id)
    : Index(metadata), comparator_(metadata->GetKeySchema()),
      container_(metadata->GetName(), buffer_pool_manager, comparator_,
                 root_page_id) {}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::InsertEntry(const Tuple &key, RID rid,
                                       Transaction *transaction) {
    // construct insert index key
    KeyType index_key;
    index_key.SetFromKey(key);

    container_.Insert(index_key, rid, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::DeleteEntry(const Tuple &key,
                                       Transaction *transaction) {
    // construct delete index key
    KeyType index_key;
    index_key.SetFromKey(key);

    container_.Remove(index_key, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::ScanKey(const Tuple &key, std::vector<RID> &result,
                                   Transaction *transaction) {
    // construct scan index key
    KeyType index_key;
    index_key.SetFromKey(key);

    container_.GetValue(index_key, result, transaction);
}

template class BPlusTreeIndex<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeIndex<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeIndex<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeIndex<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
