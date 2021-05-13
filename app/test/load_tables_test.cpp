#include "gtest/gtest.h"

// Glue code //////////////////////////////////////////////////////////////////
#include "db_wrapper.hpp"
#include "initializer.hpp"

// TPCC code //////////////////////////////////////////////////////////////////
#include "record_generator.hpp"
#include "record_key.hpp"
#include "record_layout.hpp"
#include "utils.hpp"

using namespace RecordGenerator;

TEST(LoadTablesSuit, LoadItemsTable) {
    // Config& c = get_mutable_config();
    // c.set_num_warehouses(3);
    Initializer::load_items_table();

    Item i;
    uint32_t i_id = 1;
    DBWrapper::get_db().get_item_record(i, i_id);
    EXPECT_EQ(i.i_id, i_id);
    EXPECT_GE(i.i_im_id, 1);
    EXPECT_LE(i.i_im_id, 10000);
}
