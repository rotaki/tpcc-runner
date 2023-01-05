#pragma once

#include <inttypes.h>

#include <cstdint>

#include "benchmarks/tpcc/include/record_key.hpp"
#include "benchmarks/tpcc/include/record_layout.hpp"
#include "benchmarks/tpcc/include/tx_utils.hpp"
#include "utils/logger.hpp"
#include "utils/tsc.hpp"


class DeliveryTx {
public:
    DeliveryTx(uint16_t w_id0) {
        input.generate(w_id0);
        input.print();
    }

    static constexpr char name[] = "Delivery";
    static constexpr TxProfileID id = TxProfileID::DELIVERY_TX;

    struct Input {
        uint16_t w_id;
        uint8_t o_carrier_id;
        Timestamp ol_delivery_d;

        void generate(uint16_t w_id0) {
            w_id = w_id0;
            o_carrier_id = urand_int(1, 10);
            ol_delivery_d = get_timestamp();
        }

        void print() {
            LOG_TRACE(
                "del: w_id=%" PRIu16 " o_carrier_id=%" PRIu8 " ol_delivery_d=%ld", w_id,
                o_carrier_id, ol_delivery_d);
        }
    } input;

    enum AbortID : uint8_t {
        GET_NEWORDER_WITH_SMALLEST_KEY = 0,
        DELETE_NEWORDER = 1,
        FINISH_DELETE_NEWORDER = 2,
        PREPARE_UPDATE_ORDER = 3,
        FINISH_UPDATE_ORDER = 4,
        RANGE_UPDATE_ORDERLINE = 5,
        PREPARE_UPDATE_CUSTOMER = 6,
        FINISH_UPDATE_CUSTOMER = 7,
        PRECOMMIT = 8,
        MAX = 9
    };

    template <const AbortID a>
    static constexpr const char* abort_reason() {
        if constexpr (a == AbortID::GET_NEWORDER_WITH_SMALLEST_KEY)
            return "GET_NEWORDER_WITH_SMALLEST_KEY";
        else if constexpr (a == AbortID::DELETE_NEWORDER)
            return "DELETE_NEWORDER";
        else if constexpr (a == AbortID::FINISH_DELETE_NEWORDER)
            return "FINISH_DELETE_NEWORDER";
        else if constexpr (a == AbortID::PREPARE_UPDATE_ORDER)
            return "PREPARE_UPDATE_ORDER";
        else if constexpr (a == AbortID::FINISH_UPDATE_ORDER)
            return "FINISH_UPDATE_ORDER";
        else if constexpr (a == AbortID::RANGE_UPDATE_ORDERLINE)
            return "RANGE_UPDATE_ORDERLINE";
        else if constexpr (a == AbortID::PREPARE_UPDATE_CUSTOMER)
            return "PREPARE_UPDATE_CUSTOMER";
        else if constexpr (a == AbortID::FINISH_UPDATE_CUSTOMER)
            return "FINISH_UPDATE_CUSTOMER";
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
        TxHelper<Transaction> helper(tx, stat[TxProfileID::DELIVERY_TX]);

        uint16_t w_id = input.w_id;
        uint8_t o_carrier_id = input.o_carrier_id;

        out << w_id << o_carrier_id;

        for (uint8_t d_id = 1; d_id <= District::DISTS_PER_WARE; d_id++) {
            const NewOrder* no = nullptr;
            NewOrder::Key no_low = NewOrder::Key::create_key(w_id, d_id, 1);
            res = tx.get_neworder_with_smallest_key_no_less_than(no, no_low);
            if (res == Transaction::Result::FAIL) continue;
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res, GET_NEWORDER_WITH_SMALLEST_KEY);
            res =
                tx.template prepare_record_for_delete<NewOrder>(no, NewOrder::Key::create_key(*no));
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res, DELETE_NEWORDER);
            res = tx.finish_delete(no);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res, FINISH_DELETE_NEWORDER);

            Order* o;
            Order::Key o_key = Order::Key::create_key(w_id, d_id, no->no_o_id);
            res = tx.prepare_record_for_update(o, o_key);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res, PREPARE_UPDATE_ORDER);
            o->o_carrier_id = o_carrier_id;
            res = tx.finish_update(o);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res, FINISH_UPDATE_ORDER);

            double total_ol_amount = 0.0;
            OrderLine::Key o_low = OrderLine::Key::create_key(o->o_w_id, o->o_d_id, o->o_id, 1);
            OrderLine::Key o_up = OrderLine::Key::create_key(o->o_w_id, o->o_d_id, o->o_id + 1, 1);
            res =
                tx.template range_update<OrderLine>(o_low, o_up, [&total_ol_amount](OrderLine& ol) {
                    ol.ol_delivery_d = get_timestamp();
                    total_ol_amount += ol.ol_amount;
                });
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res, RANGE_UPDATE_ORDERLINE);

            Customer* c;
            Customer::Key c_key = Customer::Key::create_key(w_id, d_id, o->o_c_id);
            res = tx.prepare_record_for_update(c, c_key);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res, PREPARE_UPDATE_CUSTOMER);
            c->c_balance += total_ol_amount;
            c->c_delivery_cnt += 1;
            res = tx.finish_update(c);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res, FINISH_UPDATE_CUSTOMER);

            out << d_id << o->o_id;
        }
        end = rdtscp();
        return helper.commit(PRECOMMIT, end - start);
    };
};
