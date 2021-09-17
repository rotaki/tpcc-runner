#include "benchmarks/tpcc/include/payment_tx.hpp"
#include "benchmarks/tpcc/include/record_layout.hpp"
#include "benchmarks/tpcc/include/tx_runner.hpp"
#include "gtest/gtest.h"
#include "protocols/naive/include/database.hpp"
#include "protocols/naive/include/initializer.hpp"
#include "protocols/naive/include/transaction.hpp"
#include "utils/logger.hpp"

class AtomicityTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        Config& c = get_mutable_config();
        c.set_num_warehouses(num_warehouse);
        Initializer::load_all_tables();
    }

    static constexpr uint16_t num_warehouse = 1;
};


TEST_F(AtomicityTest, Test1) {
    Database& db = Database::get_db();
    Transaction tx(db);
    Stat stat;
    Output out;
    PaymentTx payment(1);
    while (payment.input.by_last_name == true) payment.input.generate(1);
    uint16_t w_id = payment.input.w_id;
    uint8_t d_id = payment.input.d_id;
    uint32_t c_id = payment.input.c_id;
    double h_amount = payment.input.h_amount;

    ASSERT_GE(c_id, 1);

    const Warehouse* w;
    Warehouse::Key w_key = Warehouse::Key::create_key(w_id);
    db.get_record(w, w_key);
    ASSERT_TRUE(w != nullptr);
    double w_ytd = w->w_ytd;

    const District* d;
    District::Key d_key = District::Key::create_key(w_id, d_id);
    db.get_record(d, d_key);
    ASSERT_TRUE(d != nullptr);
    double d_ytd = d->d_ytd;

    const Customer* c;
    Customer::Key c_key = Customer::Key::create_key(w_id, d_id, c_id);
    db.get_record(c, c_key);
    ASSERT_TRUE(c != nullptr);
    double c_balance = c->c_balance;
    double c_ytd_payment = c->c_ytd_payment;
    uint16_t c_payment_cnt = c->c_payment_cnt;

    if (c->c_credit[0] == 'B' && c->c_credit[1] == 'C') {
        LOG_TRACE("c_data before: %s", c->c_data);
    }

    payment.run(tx, stat, out);

    db.get_record(w, w_key);
    ASSERT_TRUE(w != nullptr);
    ASSERT_EQ(w_ytd + h_amount, w->w_ytd);

    db.get_record(d, d_key);
    ASSERT_TRUE(d != nullptr);
    ASSERT_EQ(d_ytd + h_amount, d->d_ytd);

    db.get_record(c, c_key);
    ASSERT_TRUE(c != nullptr);
    ASSERT_EQ(c_ytd_payment + h_amount, c->c_ytd_payment);
    ASSERT_EQ(c_balance - h_amount, c->c_balance);
    ASSERT_EQ(c_payment_cnt + 1, c->c_payment_cnt);

    if (c->c_credit[0] == 'B' && c->c_credit[1] == 'C') {
        LOG_TRACE("c_data after: %s", c->c_data);
    }
}

/* TEST_F(AtomicityTest, Test2) {
    Database& db = Database::get_db();
    Transaction tx(db);
    Output out;
    PaymentTx payment(1);
    while (payment.input.by_last_name == true) payment.input.generate(1);
    payment.input.ROLLBACK_MODE_FOR_TEST = 1;
    uint16_t w_id = payment.input.w_id;
    uint8_t d_id = payment.input.d_id;
    uint32_t c_id = payment.input.c_id;

    ASSERT_GE(c_id, 1);

    Warehouse w;
    Warehouse::Key w_key = Warehouse::Key::create_key(w_id);
    ASSERT_TRUE(db.get_record(w, w_key));
    double w_ytd = w.w_ytd;

    District d;
    District::Key d_key = District::Key::create_key(w_id, d_id);
    ASSERT_TRUE(db.get_record(d, d_key));
    double d_ytd = d.d_ytd;

    Customer c;
    Customer::Key c_key = Customer::Key::create_key(w_id, d_id, c_id);
    ASSERT_TRUE(db.get_record(c, c_key));
    double c_balance = c.c_balance;
    double c_ytd_payment = c.c_ytd_payment;
    uint16_t c_payment_cnt = c.c_payment_cnt;

    if (c.c_credit[0] == 'B' && c.c_credit[1] == 'C') {
        LOG_TRACE("c_data before: %s", c.c_data);
    }

    payment.run(tx, out);

    ASSERT_TRUE(db.get_record(w, w_key));
    ASSERT_EQ(w_ytd, w.w_ytd);

    ASSERT_TRUE(db.get_record(d, d_key));
    ASSERT_EQ(d_ytd, d.d_ytd);

    ASSERT_TRUE(db.get_record(c, c_key));
    ASSERT_EQ(c_ytd_payment, c.c_ytd_payment);
    ASSERT_EQ(c_balance, c.c_balance);
    ASSERT_EQ(c_payment_cnt, c.c_payment_cnt);

    if (c.c_credit[0] == 'B' && c.c_credit[1] == 'C') {
        LOG_TRACE("c_data after: %s", c.c_data);
    }
} */
