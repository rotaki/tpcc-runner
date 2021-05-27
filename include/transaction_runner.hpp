#pragma once

#include <iostream>
#include <set>
#include <stdexcept>

#include "logger.hpp"
#include "record_generator.hpp"
#include "record_layout.hpp"
#include "transaction_input_data.hpp"
#include "transaction_output_data.hpp"
#include "utils.hpp"

namespace TransactionRunnerUtils {
namespace NewOrderUtils {
void create_neworder(NewOrder& no, uint16_t w_id, uint16_t d_id, uint32_t o_id);
void create_order(
    Order& o, uint16_t w_id, uint8_t d_id, uint32_t c_id, uint32_t o_id, uint8_t ol_cnt,
    bool is_remote);
void create_orderline(
    OrderLine& ol, uint16_t w_id, uint8_t d_id, uint32_t o_id, uint8_t ol_num, uint32_t ol_i_id,
    uint16_t ol_supply_w_id, uint8_t ol_quantity, double ol_amount, const Stock& s);
void modify_stock(Stock& s, uint8_t ol_quantity, bool is_remote);
}  // namespace NewOrderUtils

namespace PaymentUtils {
void modify_customer(
    Customer& c, uint16_t w_id, uint8_t d_id, uint16_t c_w_id, uint8_t c_d_id, double h_amount);
void create_history(
    History& h, uint16_t w_id, uint8_t d_id, uint32_t c_id, uint16_t c_w_id, uint8_t c_d_id,
    double h_amount, const char* w_name, const char* d_name);

}  // namespace PaymentUtils
}  // namespace TransactionRunnerUtils

namespace TransactionRunner {
using namespace TransactionRunnerUtils;
enum Status {
    SUCCESS = 0,   // if all stages of transaction return Result::SUCCESS
    USER_ABORT,    // if rollback defined in the specification occurs (e.g. 1% of NewOrder Tx)
    SYSTEM_ABORT,  // if any stage of a transaction returns Result::ABORT
    BUG            // if any stage of a transaciton returns unexpected Result::FAIL
};

template <typename Transaction>
bool not_succeeded(Transaction& tx, typename Transaction::Result& res) {
    const Config& c = get_config();
    bool flag = c.get_random_abort_flag();
    if (flag && res == Transaction::Result::SUCCESS
        && RecordGeneratorUtils::urand_int(1, 100) == 1) {
        tx.abort();
        res = Transaction::Result::ABORT;
    }
    return res != Transaction::Result::SUCCESS;
}

template <typename Transaction>
Status kill_tx(Transaction& tx, typename Transaction::Result res) {
    assert(not_succeeded(tx, res));
    if (res == Transaction::Result::FAIL) {
        return Status::BUG;
    } else {
        return Status::SYSTEM_ABORT;
    }
}


template <typename Transaction>
Status run(const InputData::NewOrder& input, OutputData::NewOrder& output, Transaction& tx) {
    typename Transaction::Result res;


    bool is_remote = input.is_remote;
    uint16_t w_id = input.w_id;
    uint8_t d_id = input.d_id;
    uint32_t c_id = input.c_id;
    uint8_t ol_cnt = input.ol_cnt;

    Warehouse w;
    res = tx.get_record(w, Warehouse::Key::create_key(w_id));
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) return kill_tx(tx, res);

    District d;
    District::Key d_key = District::Key::create_key(w_id, d_id);
    res = tx.get_record(d, d_key);
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) return kill_tx(tx, res);
    uint32_t o_id = (d.d_next_o_id)++;
    res = tx.update_record(d_key, d);
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) return kill_tx(tx, res);

    Customer c;
    res = tx.get_record(c, Customer::Key::create_key(w_id, d_id, c_id));
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) return kill_tx(tx, res);

    NewOrder no;
    NewOrderUtils::create_neworder(no, w_id, d_id, o_id);
    res = tx.insert_record(no);
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) return kill_tx(tx, res);

    Order o;
    NewOrderUtils::create_order(o, w_id, d_id, c_id, o_id, ol_cnt, is_remote);
    res = tx.insert_record(o);
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) return kill_tx(tx, res);

    for (uint8_t ol_num = 1; ol_num <= ol_cnt; ol_num++) {
        uint32_t ol_i_id = input.items[ol_num - 1].ol_i_id;
        uint16_t ol_supply_w_id = input.items[ol_num - 1].ol_supply_w_id;
        uint8_t ol_quantity = input.items[ol_num - 1].ol_quantity;

        Item i;
        if (ol_i_id == Item::UNUSED_ID) return Status::USER_ABORT;
        res = tx.get_record(i, Item::Key::create_key(ol_i_id));
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        Stock s;
        Stock::Key s_key = Stock::Key::create_key(ol_supply_w_id, ol_i_id);
        res = tx.get_record(s, s_key);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);
        NewOrderUtils::modify_stock(s, ol_quantity, is_remote);

        res = tx.update_record(s_key, s);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        double ol_amount = ol_quantity * i.i_price;
        OrderLine ol;
        NewOrderUtils::create_orderline(
            ol, w_id, d_id, o_id, ol_num, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, s);
        res = tx.insert_record(ol);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);
    }

    if (tx.commit()) {
        LOG_TRACE("commit success");
        return Status::SUCCESS;
    } else {
        LOG_TRACE("commit fail");
        return Status::SYSTEM_ABORT;
    }
}

template <typename Transaction>
Status run(const InputData::Payment& input, OutputData::Payment& output, Transaction& tx) {
    typename Transaction::Result res;

    uint16_t w_id = input.w_id;
    uint8_t d_id = input.d_id;
    uint32_t c_id = input.c_id;
    uint16_t c_w_id = input.c_w_id;
    uint8_t c_d_id = input.c_d_id;
    double h_amount = input.h_amount;
    const char* c_last = input.c_last;
    bool by_last_name = input.by_last_name;

    Warehouse w;
    Warehouse::Key w_key = Warehouse::Key::create_key(w_id);
    res = tx.get_record(w, w_key);
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) return kill_tx(tx, res);
    w.w_ytd += h_amount;
    res = tx.update_record(w_key, w);
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) return kill_tx(tx, res);

    District d;
    District::Key d_key = District::Key::create_key(w_id, d_id);
    res = tx.get_record(d, d_key);
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) return kill_tx(tx, res);
    d.d_ytd += h_amount;
    res = tx.update_record(d_key, d);
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) return kill_tx(tx, res);

    Customer c;
    if (by_last_name) {
        assert(c_id == Customer::UNUSED_ID);
        CustomerSecondary::Key c_last_key =
            CustomerSecondary::Key::create_key(c_w_id, c_d_id, c_last);
        res = tx.get_customer_by_last_name(c, c_last_key);
    } else {
        assert(c_id != Customer::UNUSED_ID);
        res = tx.get_record(c, Customer::Key::create_key(c_w_id, c_d_id, c_id));
    }
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) return kill_tx(tx, res);
    PaymentUtils::modify_customer(c, w_id, d_id, c_w_id, c_d_id, h_amount);
    res = tx.update_record(Customer::Key::create_key(c_w_id, c_d_id, c.c_id), c);
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) return kill_tx(tx, res);

    History h;
    PaymentUtils::create_history(
        h, w_id, d_id, c.c_id, c_w_id, c_d_id, h_amount, w.w_name, d.d_name);
    res = tx.insert_record(h);
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) return kill_tx(tx, res);

    if (tx.commit()) {
        LOG_TRACE("commit success");
        return Status::SUCCESS;
    } else {
        LOG_TRACE("commit fail");
        return Status::SYSTEM_ABORT;
    }
}

template <typename Transaction>
Status run(const InputData::OrderStatus& input, OutputData::OrderStatus& output, Transaction& tx) {
    typename Transaction::Result res;

    uint16_t c_w_id = input.w_id;
    uint8_t c_d_id = input.d_id;
    uint32_t c_id = input.c_id;
    const char* c_last = input.c_last;
    bool by_last_name = input.by_last_name;

    Customer c;
    if (by_last_name) {
        assert(c_id == Customer::UNUSED_ID);
        CustomerSecondary::Key c_last_key =
            CustomerSecondary::Key::create_key(c_w_id, c_d_id, c_last);
        res = tx.get_customer_by_last_name(c, c_last_key);
    } else {
        assert(c_id != Customer::UNUSED_ID);
        res = tx.get_record(c, Customer::Key::create_key(c_w_id, c_d_id, c_id));
    }
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) return kill_tx(tx, res);

    Order o;
    res = tx.get_order_by_customer_id(o, OrderSecondary::Key::create_key(c_w_id, c_d_id, c_id));
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) return kill_tx(tx, res);

    std::set<uint32_t> ol_i_ids;
    std::set<uint16_t> ol_supply_w_ids;
    std::set<uint8_t> ol_quantities;
    std::set<double> ol_amounts;
    std::set<Timestamp> ol_delivery_ds;
    OrderLine::Key low = OrderLine::Key::create_key(o.o_w_id, o.o_d_id, o.o_c_id, 0);
    OrderLine::Key up = OrderLine::Key::create_key(o.o_w_id, o.o_d_id, o.o_c_id + 1, 0);
    res = tx.template range_query<OrderLine>(
        low, up,
        [&ol_i_ids, &ol_supply_w_ids, &ol_quantities, &ol_amounts, &ol_delivery_ds](OrderLine& ol) {
            ol_i_ids.insert(ol.ol_i_id);
            ol_supply_w_ids.insert(ol.ol_supply_w_id);
            ol_quantities.insert(ol.ol_quantity);
            ol_amounts.insert(ol.ol_amount);
            ol_delivery_ds.insert(ol.ol_delivery_d);
        });
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) return kill_tx(tx, res);

    if (tx.commit()) {
        LOG_TRACE("commit success");
        return Status::SUCCESS;
    } else {
        LOG_TRACE("commit fail");
        return Status::SYSTEM_ABORT;
    }
}

template <typename Transaction>
Status run(const InputData::Delivery& input, OutputData::Delivery& output, Transaction& tx) {
    typename Transaction::Result res;

    uint16_t w_id = input.w_id;
    uint8_t o_carrier_id = input.o_carrier_id;

    std::vector<uint8_t> delivery_skipped_dists;
    for (uint8_t d_id = 1; d_id <= District::DISTS_PER_WARE; d_id++) {
        NewOrder no;
        NewOrder::Key no_low = NewOrder::Key::create_key(w_id, d_id, 0);
        res = tx.get_neworder_with_smallest_key_no_less_than(no, no_low);
        if (res == Transaction::Result::FAIL) {
            delivery_skipped_dists.emplace_back(d_id);
            continue;
        }
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);
        res = tx.template delete_record<NewOrder>(NewOrder::Key::create_key(no));

        Order o;
        Order::Key o_key = Order::Key::create_key(w_id, d_id, no.no_o_id);
        res = tx.get_record(o, o_key);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);
        o.o_carrier_id = o_carrier_id;
        res = tx.update_record(o_key, o);
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        double total_ol_amount = 0.0;
        OrderLine::Key o_low = OrderLine::Key::create_key(o.o_w_id, o.o_d_id, o.o_id, 0);
        OrderLine::Key o_up = OrderLine::Key::create_key(o.o_w_id, o.o_d_id, o.o_id + 1, 0);
        res = tx.template range_update<OrderLine>(o_low, o_up, [&total_ol_amount](OrderLine& ol) {
            ol.ol_amount = get_timestamp();
            total_ol_amount += ol.ol_amount;
        });
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);

        Customer c;
        Customer::Key c_key = Customer::Key::create_key(w_id, d_id, o.o_c_id);
        res = tx.get_record(c, c_key);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);
        c.c_balance += total_ol_amount;
        c.c_delivery_cnt += 1;
        res = tx.update_record(c_key, c);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);
    }

    if (tx.commit()) {
        LOG_TRACE("commit success");
        return Status::SUCCESS;
    } else {
        LOG_TRACE("commit fail");
        return Status::SYSTEM_ABORT;
    }
}

template <typename Transaction>
Status run(const InputData::StockLevel& input, OutputData::StockLevel& output, Transaction& tx) {
    typename Transaction::Result res;

    uint16_t w_id = input.w_id;
    uint8_t d_id = input.d_id;
    uint8_t threshold = input.threshold;

    District d;
    res = tx.get_record(d, District::Key::create_key(w_id, d_id));
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) return kill_tx(tx, res);

    std::set<uint32_t> s_i_ids;
    OrderLine::Key low = OrderLine::Key::create_key(w_id, d_id, d.d_next_o_id - 20, 0);
    OrderLine::Key up = OrderLine::Key::create_key(w_id, d_id, d.d_next_o_id + 1, 0);
    res = tx.template range_query<OrderLine>(low, up, [&s_i_ids](const OrderLine& ol) {
        if (ol.ol_i_id != Item::UNUSED_ID) s_i_ids.insert(ol.ol_i_id);
    });
    LOG_TRACE("res: %d", static_cast<int>(res));
    if (not_succeeded(tx, res)) kill_tx(tx, res);

    auto it = s_i_ids.begin();
    Stock s;
    while (it != s_i_ids.end()) {
        res = tx.get_record(s, Stock::Key::create_key(w_id, *it));
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return kill_tx(tx, res);
        if (s.s_quantity >= threshold) {
            it = s_i_ids.erase(it);
        } else {
            it++;
        }
    }

    if (tx.commit()) {
        LOG_TRACE("commit success");
        return Status::SUCCESS;
    } else {
        LOG_TRACE("commit fail");
        return Status::SYSTEM_ABORT;
    }
}

template <typename Input, typename Output, typename Transaction>
bool run_with_retry(const Input& input, Output& output, Transaction& tx) {
    for (;;) {
        Status res = run(input, output, tx);
        switch (res) {
        case SUCCESS: LOG_TRACE("success"); return true;
        case USER_ABORT:
            LOG_TRACE("user abort");
            tx.abort();
            return false;                                        // aborted by the user
        case SYSTEM_ABORT: LOG_TRACE("system abort"); continue;  // aborted by the tx engine
        case BUG: throw std::runtime_error("Unexpected Transaction Bug");
        default: assert(false);
        }
    }
}

}  // namespace TransactionRunner