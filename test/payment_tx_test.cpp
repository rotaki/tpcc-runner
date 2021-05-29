#include "gtest/gtest.h"

// Glue code
#include "database.hpp"
#include "initializer.hpp"
#include "transaction.hpp"

// TPCC
#include "logger.hpp"
#include "payment_tx.hpp"
#include "tx_runner.hpp"

class PaymentTest : public ::testing::Test {
protected:
    void SetUp() override {
        Config& c = get_mutable_config();
        c.set_num_warehouses(1);
        Initializer::load_items_table();
        Initializer::load_warehouses_table();
    }
};

TEST_F(PaymentTest, CheckRunWithRetry) {
    Database& db = Database::get_db();
    Transaction tx(db);
    Output out;
    run_with_retry<PaymentTx>(tx, out);
}