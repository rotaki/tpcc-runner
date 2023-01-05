#pragma once

#include <inttypes.h>

#include <cstdint>
#include <deque>

#include "benchmarks/tpcc/include/record_key.hpp"
#include "benchmarks/tpcc/include/record_layout.hpp"
#include "benchmarks/tpcc/include/tx_utils.hpp"
#include "utils/logger.hpp"
#include "utils/tsc.hpp"

class OrderStatusTx {
public:
    OrderStatusTx(uint16_t w_id0) {
        input.generate(w_id0);
        input.print();
    }

    static constexpr char name[] = "OrderStatus";
    static constexpr TxProfileID id = TxProfileID::ORDERSTATUS_TX;

    struct Input {
        uint16_t w_id;
        uint8_t d_id;
        uint32_t c_id;
        bool by_last_name;
        char c_last[Customer::MAX_DATA + 1];

        void generate(uint16_t w_id0) {
            w_id = w_id0;
            d_id = urand_int(1, District::DISTS_PER_WARE);
            by_last_name = ((urand_int(1, 100) <= 60) ? 1 : 0);
            if (by_last_name) {
                c_id = Customer::UNUSED_ID;
                make_clast(c_last, nurand_int<255>(0, 999));
            } else {
                c_id = nurand_int<1023>(1, 3000);
            }
        }

        void print() {
            if (by_last_name) {
                LOG_TRACE(
                    "ordstts: w_id=%" PRIu16 " d_id=%" PRIu8 " by_last_name=t c_last=%s", w_id,
                    d_id, c_last);
            } else {
                LOG_TRACE(
                    "ordstts: w_id=%" PRIu16 " d_id=%" PRIu8 " by_last_name=f c_id=%" PRIu32, w_id,
                    d_id, c_id);
            }
        }

    } input;

    enum AbortID : uint8_t {
        GET_CUSTOMER_BY_LAST_NAME = 0,
        GET_CUSTOMER = 1,
        GET_ORDER_BY_CUSTOMER_ID = 2,
        RANGE_GET_ORDERLINE = 3,
        PRECOMMIT = 4,
        MAX = 5
    };

    template <AbortID a>
    static constexpr const char* abort_reason() {
        if constexpr (a == AbortID::GET_CUSTOMER_BY_LAST_NAME)
            return "GET_CUSTOMER_BY_LAST_NAME";
        else if constexpr (a == AbortID::GET_CUSTOMER)
            return "GET_CUSTOMER";
        else if constexpr (a == AbortID::GET_ORDER_BY_CUSTOMER_ID)
            return "GET_ORDER_BY_CUSTOMER_ID";
        else if constexpr (a == AbortID::RANGE_GET_ORDERLINE)
            return "RANGE_GET_ORDERLINE";
        else if constexpr (a == AbortID::PRECOMMIT)
            return "PRECOMMIT";
        else
            static_assert(false_v<a>, "undefined abort reason");
    }

    template <typename Transaction>
    Status run(Transaction& tx, Stat& stat, Output& out) {
        uint64_t start, end;
        start = rdtscp();
        typename Transaction::Result res;
        TxHelper<Transaction> helper(tx, stat[TxProfileID::ORDERSTATUS_TX]);

        uint16_t c_w_id = input.w_id;
        uint8_t c_d_id = input.d_id;
        uint32_t c_id = input.c_id;
        const char* c_last = input.c_last;
        bool by_last_name = input.by_last_name;

        out << c_w_id << c_d_id << c_id;

        const Customer* c;
        LOG_TRACE("by_last_name %s", by_last_name ? "true" : "false");
        if (by_last_name) {
            LOG_TRACE("c_last: %s", c_last);
            assert(c_id == Customer::UNUSED_ID);
            res = tx.get_customer_by_last_name(c, c_w_id, c_d_id, c_last);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res, GET_CUSTOMER_BY_LAST_NAME);
        } else {
            assert(c_id != Customer::UNUSED_ID);
            res = tx.get_record(c, Customer::Key::create_key(c_w_id, c_d_id, c_id));
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res, GET_CUSTOMER);
        }

        c_id = c->c_id;
        out << c->c_first << c->c_middle << c->c_last << c->c_balance;

        const Order* o = nullptr;
        res = tx.get_order_by_customer_id(o, c_w_id, c_d_id, c_id);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return helper.kill(res, GET_ORDER_BY_CUSTOMER_ID);

        out << o->o_id << o->o_entry_d << o->o_carrier_id;

        OrderLine::Key low = OrderLine::Key::create_key(o->o_w_id, o->o_d_id, o->o_id, 1);
        OrderLine::Key up = OrderLine::Key::create_key(o->o_w_id, o->o_d_id, o->o_id + 1, 1);

        res = tx.template range_query<OrderLine>(low, up, [&out](const OrderLine& ol) {
            out << ol.ol_supply_w_id << ol.ol_i_id << ol.ol_quantity << ol.ol_amount
                << ol.ol_delivery_d;
        });

        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return helper.kill(res, RANGE_GET_ORDERLINE);

        end = rdtscp();
        return helper.commit(PRECOMMIT, end - start);
    }
};
