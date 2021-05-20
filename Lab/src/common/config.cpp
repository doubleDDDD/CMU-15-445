#include "common/config.h"

namespace cmudb {
    std::atomic<bool> ENABLE_LOGGING(false);  // for virtual table
    /* 登录超时时间 set 为 1s */
    std::chrono::duration<long long int> LOG_TIMEOUT = std::chrono::seconds(1);
}