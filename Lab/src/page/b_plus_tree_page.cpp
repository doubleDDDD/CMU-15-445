/**
 * b_plus_tree_page.cpp
 */
#include "page/b_plus_tree_page.h"

namespace cmudb {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 * 可以是root节点，也可以不是root节点，这个判断是足够的
 */
// bool
// BPlusTreePage::IsLeafPage() const {
//     return page_type_ == IndexPageType::LEAF_PAGE;
// }

// // only root has no parent
// bool
// BPlusTreePage::IsRootPage() const {
//     return parent_page_id_ == INVALID_PAGE_ID;
// }

// void
// BPlusTreePage::SetPageType(IndexPageType page_type) {
//     page_type_ = page_type;
// }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
// int BPlusTreePage::GetSize() const { return size_; }
// void BPlusTreePage::SetSize(int size) { size_ = size; }
// void BPlusTreePage::IncreaseSize(int amount) { size_ += amount; }

/*
 * Helper methods to get/set max size (capacity) of the page
 */
// int BPlusTreePage::GetMaxCapacity() const { return max_kv_capacity_; }
// void BPlusTreePage::SetMaxCapacity(int capacity_) { max_kv_capacity_ = capacity_; }
    
// int BPlusTreePage::GetOrder() const { return real_order_; }
// void BPlusTreePage::SetOrder(int order_) { real_order_= order_; }

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
// int BPlusTreePage::GetMinSize() const { return max_size_/2; }

/*
 * Helper methods to get/set parent page id
 */
page_id_t BPlusTreePage::GetParentPageId() const { return parent_page_id_; }
void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) { parent_page_id_ = parent_page_id; }

/*
 * Helper methods to get/set self page id
 */
page_id_t BPlusTreePage::GetPageId() const { return page_id_; }
void BPlusTreePage::SetPageId(page_id_t page_id) { page_id_ = page_id; }

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

// void BPlusTreePage::SetLayerId(int _layer) { layer = _layer; }
// int BPlusTreePage::GetLayerId() const { return layer; }

} // namespace cmudb
