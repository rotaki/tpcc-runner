#pragma once

#include "benchmarks/ycsb/include/record_key.hpp"
#include "benchmarks/ycsb/include/record_layout.hpp"
#include "protocols/common/schema.hpp"

template <typename Record>
inline TableID get_id() {
    return sizeof(Record);
}
