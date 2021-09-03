#pragma once

#include <cstdint>

#include "protocols/common/transaction_id.hpp"
#include "protocols/silo/include/tidword.hpp"

struct Value {
    alignas(64) TidWord tidword;
    void* rec;
};

struct ValueTest {
    alignas(64) TidWord tidword;
    void* rec;
    TxID txid;
};