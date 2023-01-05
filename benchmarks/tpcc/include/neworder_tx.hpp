#pragma once

#include <inttypes.h>

#include <cstdint>

#include "benchmarks/tpcc/include/record_key.hpp"
#include "benchmarks/tpcc/include/record_layout.hpp"
#include "benchmarks/tpcc/include/tx_utils.hpp"
#include "utils/logger.hpp"
#include "utils/tsc.hpp"


class NewOrderTx {
public:
    NewOrderTx(uint16_t w_id0) {
        input.generate(w_id0);
        input.print();
    }

    static constexpr char name[] = "NewOrder";
    static constexpr TxProfileID id = TxProfileID::NEWORDER_TX;

    struct Input {
        uint16_t w_id;
        uint8_t d_id;
        uint32_t c_id;
        uint8_t ol_cnt;
        Timestamp o_entry_d;
        bool rbk;
        bool is_remote;
        struct {
            uint16_t ol_supply_w_id;
            uint32_t ol_i_id;
            uint8_t ol_quantity;
        } items[OrderLine::MAX_ORDLINES_PER_ORD];

        void generate(uint16_t w_id0) {
            const Config& c = get_config();
            uint16_t num_warehouses = c.get_num_warehouses();
            w_id = w_id0;
            d_id = urand_int(1, District::DISTS_PER_WARE);
            c_id = nurand_int<1023>(1, Customer::CUSTS_PER_DIST);
            ol_cnt = urand_int(OrderLine::MIN_ORDLINES_PER_ORD, OrderLine::MAX_ORDLINES_PER_ORD);
            o_entry_d = get_timestamp();
            rbk = (urand_int(1, 100) == 1 ? 1 : 0);
            is_remote = (urand_int(1, 100) == 1 ? 1 : 0);
            for (int i = 1; i <= ol_cnt; i++) {
                if (i == ol_cnt && rbk) {
                    items[i - 1].ol_i_id = Item::UNUSED_ID; /* set to an unused value */
                } else {
                    items[i - 1].ol_i_id = nurand_int<8191>(1, Item::ITEMS);
                }
                if (is_remote && num_warehouses > 1) {
                    uint16_t remote_w_id;
                    while ((remote_w_id = urand_int(1, num_warehouses)) == w_id) {
                    }
                    assert(remote_w_id != w_id);
                    items[i - 1].ol_supply_w_id = remote_w_id;
                } else {
                    items[i - 1].ol_supply_w_id = w_id;
                }
                items[i - 1].ol_quantity = urand_int(1, 10);
            }
        }

        void print() {
            LOG_TRACE(
                "nod: w_id=%" PRIu16 " d_id=%" PRIu8 " c_id=%" PRIu32 " rbk=%" PRIu8
                " remote=%s ol_cnt=%" PRIu8,
                w_id, d_id, c_id, rbk, is_remote ? "t" : "f", ol_cnt);
            for (unsigned int i = 1; i <= ol_cnt; i++) {
                LOG_TRACE(
                    " [%d]: ol_i_id=%" PRIu32 " ol_supply_w_id=%" PRIu16 " c_quantity=%" PRIu8, i,
                    items[i - 1].ol_i_id, items[i - 1].ol_supply_w_id, items[i - 1].ol_quantity);
            }
        }

    } input;

    enum AbortID : uint8_t {
        GET_WAREHOUSE = 0,
        PREPARE_UPDATE_DISTRICT = 1,
        FINISH_UPDATE_DISTRICT = 2,
        GET_CUSTOMER = 3,
        PREPARE_INSERT_NEWORDER = 4,
        FINISH_INSERT_NEWORDER = 5,
        PREPARE_INSERT_ORDER = 6,
        FINISH_INSERT_ORDER = 7,
        GET_ITEM = 8,
        PREPARE_UPDATE_STOCK = 9,
        FINISH_UPDATE_STOCK = 10,
        PREPARE_INSERT_ORDERLINE = 11,
        FINISH_INSERT_ORDERLINE = 12,
        PRECOMMIT = 13,
        MAX = 14,
    };

    template <AbortID a>
    static constexpr const char* abort_reason() {
        if constexpr (a == AbortID::GET_WAREHOUSE)
            return "GET_WAREHOUSE";
        else if constexpr (a == AbortID::PREPARE_UPDATE_DISTRICT)
            return "PREPARE_UPDATE_DISTRICT";
        else if constexpr (a == AbortID::FINISH_UPDATE_DISTRICT)
            return "FINISH_UPDATE_DISTRICT";
        else if constexpr (a == AbortID::GET_CUSTOMER)
            return "GET_CUSTOMER";
        else if constexpr (a == AbortID::PREPARE_INSERT_NEWORDER)
            return "PREPARE_INSERT_NEWORDER";
        else if constexpr (a == AbortID::FINISH_INSERT_NEWORDER)
            return "FINISH_INSERT_NEWORDER";
        else if constexpr (a == AbortID::PREPARE_INSERT_ORDER)
            return "PREPARE_INSERT_ORDER";
        else if constexpr (a == AbortID::FINISH_INSERT_ORDER)
            return "FINISH_INSERT_ORDER";
        else if constexpr (a == AbortID::GET_ITEM)
            return "GET_ITEM";
        else if constexpr (a == AbortID::PREPARE_UPDATE_STOCK)
            return "PREPARE_UPDATE_STOCK";
        else if constexpr (a == AbortID::FINISH_UPDATE_STOCK)
            return "FINISH_UPDATE_STOCK";
        else if constexpr (a == AbortID::PREPARE_INSERT_ORDERLINE)
            return "PREPARE_INSERT_ORDERLINE";
        else if constexpr (a == AbortID::FINISH_INSERT_ORDERLINE)
            return "FINISH_INSERT_ORDERLINE";
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
        TxHelper<Transaction> helper(tx, stat[TxProfileID::NEWORDER_TX]);

        bool is_remote = input.is_remote;
        uint16_t w_id = input.w_id;
        uint8_t d_id = input.d_id;
        uint32_t c_id = input.c_id;
        uint8_t ol_cnt = input.ol_cnt;
        Timestamp o_entry_d = input.o_entry_d;

        out << w_id << d_id << c_id;

        const Warehouse* w = nullptr;
        Warehouse::Key w_key = Warehouse::Key::create_key(w_id);
        res = tx.get_record(w, w_key);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return helper.kill(res, GET_WAREHOUSE);

        District* d = nullptr;
        District::Key d_key = District::Key::create_key(w_id, d_id);
        res = tx.prepare_record_for_update(d, d_key);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return helper.kill(res, PREPARE_UPDATE_DISTRICT);
        uint32_t o_id = d->d_next_o_id;
        d->d_next_o_id++;
        res = tx.finish_update(d);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return helper.kill(res, FINISH_UPDATE_DISTRICT);

        const Customer* c = nullptr;
        Customer::Key c_key = Customer::Key::create_key(w_id, d_id, c_id);
        res = tx.get_record(c, c_key);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return helper.kill(res, GET_CUSTOMER);

        out << c->c_last << c->c_credit << c->c_discount << w->w_tax << d->d_tax << ol_cnt
            << d->d_next_o_id << o_entry_d;

        NewOrder* no = nullptr;
        NewOrder::Key no_key = NewOrder::Key::create_key(w_id, d_id, o_id);
        res = tx.prepare_record_for_insert(no, no_key);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return helper.kill(res, PREPARE_INSERT_NEWORDER);
        create_neworder(*no, w_id, d_id, o_id);
        res = tx.finish_insert(no);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return helper.kill(res, FINISH_INSERT_NEWORDER);

        Order* o = nullptr;
        Order::Key o_key = Order::Key::create_key(w_id, d_id, o_id);
        res = tx.prepare_record_for_insert(o, o_key);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return helper.kill(res, PREPARE_INSERT_ORDER);
        create_order(*o, w_id, d_id, c_id, o_id, ol_cnt, is_remote);
        res = tx.finish_insert(o);
        LOG_TRACE("res: %d", static_cast<int>(res));
        if (not_succeeded(tx, res)) return helper.kill(res, FINISH_INSERT_ORDER);

        double total = 0;

        for (uint8_t ol_num = 1; ol_num <= ol_cnt; ol_num++) {
            uint16_t ol_supply_w_id = input.items[ol_num - 1].ol_supply_w_id;
            uint32_t ol_i_id = input.items[ol_num - 1].ol_i_id;
            uint8_t ol_quantity = input.items[ol_num - 1].ol_quantity;

            if (ol_i_id == Item::UNUSED_ID) return helper.usr_abort();

            const Item* i = nullptr;
            Item::Key i_key = Item::Key::create_key(ol_i_id);
            res = tx.get_record(i, i_key);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res, GET_ITEM);

            Stock* s;
            Stock::Key s_key = Stock::Key::create_key(ol_supply_w_id, ol_i_id);
            res = tx.prepare_record_for_update(s, s_key);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res, PREPARE_UPDATE_STOCK);
            char brand_generic;
            if (strstr(i->i_data, "ORIGINAL") && strstr(s->s_data, "ORIGINAL")) {
                brand_generic = 'B';
            } else {
                brand_generic = 'G';
            }
            modify_stock(*s, ol_quantity, is_remote);
            res = tx.finish_update(s);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res, FINISH_UPDATE_STOCK);

            double ol_amount = ol_quantity * i->i_price;
            total += ol_amount;

            OrderLine* ol = nullptr;
            OrderLine::Key ol_key = OrderLine::Key::create_key(w_id, d_id, o_id, ol_num);
            res = tx.prepare_record_for_insert(ol, ol_key);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res, PREPARE_INSERT_ORDERLINE);
            create_orderline(
                *ol, w_id, d_id, o_id, ol_num, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, *s);
            res = tx.finish_insert(ol);
            LOG_TRACE("res: %d", static_cast<int>(res));
            if (not_succeeded(tx, res)) return helper.kill(res, FINISH_INSERT_ORDERLINE);

            out << ol_supply_w_id << ol_i_id << i->i_name << ol_quantity << s->s_quantity
                << brand_generic << i->i_price << ol_amount;
        }

        total *= (1 - c->c_discount) * (1 + w->w_tax + d->d_tax);
        out << total;
        end = rdtscp();
        return helper.commit(PRECOMMIT, end - start);
    }

private:
    void create_neworder(NewOrder& no, uint16_t w_id, uint16_t d_id, uint32_t o_id) {
        no.no_w_id = w_id;
        no.no_d_id = d_id;
        no.no_o_id = o_id;
    }

    void create_order(
        Order& o, uint16_t w_id, uint8_t d_id, uint32_t c_id, uint32_t o_id, uint8_t ol_cnt,
        bool is_remote) {
        o.o_w_id = w_id;
        o.o_d_id = d_id;
        o.o_c_id = c_id;
        o.o_id = o_id;
        o.o_carrier_id = 0;
        o.o_ol_cnt = ol_cnt;
        o.o_all_local = !is_remote;
        o.o_entry_d = get_timestamp();
    }

    void create_orderline(
        OrderLine& ol, uint16_t w_id, uint8_t d_id, uint32_t o_id, uint8_t ol_num, uint32_t ol_i_id,
        uint16_t ol_supply_w_id, uint8_t ol_quantity, double ol_amount, const Stock& s) {
        ol.ol_w_id = w_id;
        ol.ol_d_id = d_id;
        ol.ol_o_id = o_id;
        ol.ol_number = ol_num;
        ol.ol_i_id = ol_i_id;
        ol.ol_supply_w_id = ol_supply_w_id;
        ol.ol_delivery_d = 0;
        ol.ol_quantity = ol_quantity;
        ol.ol_amount = ol_amount;
        copy_cstr(ol.ol_dist_info, s.s_dist[d_id - 1], sizeof(ol.ol_dist_info));
    }

    void modify_stock(Stock& s, uint8_t ol_quantity, bool is_remote) {
        if (s.s_quantity > ol_quantity + 10)
            s.s_quantity -= ol_quantity;
        else
            s.s_quantity = (s.s_quantity - ol_quantity) + 91;
        s.s_ytd += ol_quantity;
        s.s_order_cnt += 1;
        if (is_remote) s.s_remote_cnt += 1;
    }
};
