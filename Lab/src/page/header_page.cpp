/**
 * header_page.cpp
 */
#include <cassert>
#include <iostream>

#include "page/header_page.h"

namespace cmudb {

/**
 * Record related
 * 这边写的是page，但是整个向下刷是按page的
 */
bool 
HeaderPage::InsertRecord(const std::string &name, const page_id_t root_id) {
    /* root id should be 0 */
    assert(name.length() < 32);
    assert(root_id > INVALID_PAGE_ID);

    int record_num = GetRecordCount();
    int offset = 4 + record_num * 36;
    // check for duplicate name
    if (FindRecord(name) != -1){
        return false;
    }
    // copy record content
    memcpy(GetData() + offset, name.c_str(), (name.length() + 1));
    memcpy((GetData() + offset + 32), &root_id, 4);

    SetRecordCount(record_num + 1);
    return true;
}

bool HeaderPage::DeleteRecord(const std::string &name) {
    int record_num = GetRecordCount();
    assert(record_num > 0);

    int index = FindRecord(name);
    // record does not exsit
    if (index == -1)
        return false;

    int offset = index * 36 + 4;
    /* 后面的整体前移 */
    memmove(GetData() + offset, GetData() + offset + 36,
            (record_num - index - 1) * 36);

    SetRecordCount(record_num - 1);
    return true;
}

bool HeaderPage::UpdateRecord(const std::string &name, const page_id_t root_id) {
    assert(name.length() < 32);

    int index = FindRecord(name);
    // record does not exsit
    if (index == -1)
        return false;
    int offset = index * 36 + 4;
    // update record content, only root_id
    memcpy((GetData() + offset + 32), &root_id, 4);

    return true;
}

bool HeaderPage::GetRootId(const std::string &name, page_id_t &root_id) {
    assert(name.length() < 32);

    int index = FindRecord(name);
    // record does not exist
    if (index == -1)
        return false;
    int offset = (index + 1)*36;
    root_id = *reinterpret_cast<page_id_t *>(GetData() + offset);

    return true;
}

/**
 * helper functions
 */
// record count
int HeaderPage::GetRecordCount() { return *reinterpret_cast<int *>(GetData()); }

void HeaderPage::SetRecordCount(int record_count) {
    memcpy(GetData(), &record_count, 4);
}

int HeaderPage::FindRecord(const std::string &name) {
    /* 查找是否有相同的记录名 */
    int record_num = GetRecordCount();

    for (int i = 0; i < record_num; i++) {
        char *raw_name = reinterpret_cast<char *>(GetData() + (4 + i*36));
        if (strcmp(raw_name, name.c_str()) == 0)
            return i;
        }
    return -1;
}
} // namespace cmudb
