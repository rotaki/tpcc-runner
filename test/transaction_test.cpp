#include "gtest/gtest.h"

// Glue code
#include "database.hpp"
#include "initializer.hpp"
#include "transaction.hpp"

// TPCC
#include "record_generator.hpp"
#include "transaction_input_data.hpp"
#include "transaction_runner.hpp"

using namespace TransactionRunner;

class TransactionTest : public ::testing::Test {
protected:
    void SetUp() override {
        uint16_t num_warehouse = 1;
        Config& c = get_mutable_config();
        c.set_num_warehouses(num_warehouse);
        Initializer::load_items_table();
        Initializer::load_warehouses_table();
    }
};

TEST_F(TransactionTest, CheckNewOrder) {
    Database& db = Database::get_db();
    InputData::NewOrder input;
    uint16_t w_id = 1;
    input.generate(w_id);
    OutputData::NewOrder output;
    Transaction tx(db);
    run_with_retry(input, output, tx);
}

TEST_F(TransactionTest, CheckPayment) {
    Database& db = Database::get_db();
    InputData::Payment input;
    uint16_t w_id = 1;
    input.generate(w_id);
    OutputData::Payment output;
    Transaction tx(db);
    run_with_retry(input, output, tx);
}

TEST(TransactionSuite, CheckOrderStatus) {
    Database& db = Database::get_db();
    InputData::OrderStatus input;
    uint16_t w_id = 1;
    input.generate(w_id);
    OutputData::OrderStatus output;
    Transaction tx(db);
    run_with_retry(input, output, tx);
}

TEST(TransactionSuite, CheckDelivery) {
    Database& db = Database::get_db();
    InputData::Delivery input;
    uint16_t w_id = 1;
    input.generate(w_id);
    OutputData::Delivery output;
    Transaction tx(db);
    run_with_retry(input, output, tx);
}

TEST(TransactionSuite, CheckStockLevel) {
    Database& db = Database::get_db();
    InputData::StockLevel input;
    uint16_t w_id = 1;
    uint8_t d_id = 1;
    input.generate(w_id, d_id);
    OutputData::StockLevel output;
    Transaction tx(db);
    run_with_retry(input, output, tx);
}