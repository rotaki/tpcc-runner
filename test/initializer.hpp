#pragma once

#include <cstdint>

namespace Initializer {
void create_and_insert_item_record(uint32_t i_id);
void create_and_insert_warehouse_record(uint16_t w_id);
void create_and_insert_stock_record(uint16_t s_w_id, uint32_t s_i_id);
void create_and_insert_district_record(uint16_t d_w_id, uint8_t d_id);
void create_and_insert_customer_record(uint16_t c_w_id, uint8_t c_d_id, uint32_t c_id, int64_t t);
void create_and_insert_history_record(
    uint16_t h_c_w_id, uint8_t h_c_d_id, uint32_t h_c_id, uint16_t h_w_id, uint8_t h_d_id);
// returns o_entry_d which will be used for loading orderlines
int64_t create_and_insert_order_record(
    uint16_t o_w_id, uint8_t o_d_id, uint32_t o_c_id, uint32_t o_id);
void create_and_insert_neworder_record(uint16_t no_w_id, uint8_t no_d_id, uint32_t no_o_id);
void create_and_insert_orderline_record(
    uint16_t ol_w_id, uint8_t ol_d_id, uint32_t ol_o_id, uint16_t ol_supply_w_id, uint32_t ol_i_id,
    uint8_t ol_number, int64_t o_entry_d);

void load_items_table();
void load_warehouses_table();
void load_stocks_table(uint16_t w_id);
void load_districts_table(uint16_t d_w_id);
void load_customers_table(uint16_t c_w_id, uint8_t c_d_id);
void load_histories_table(uint16_t w_id, uint8_t d_id, uint32_t c_id);
void load_orders_table(uint16_t o_w_id, uint8_t o_d_id);
void load_orderlines_table(uint16_t ol_w_id, uint8_t ol_d_id, uint32_t ol_o_id, int64_t o_entry_d);
void load_neworders_table(uint16_t no_w_id, uint8_t no_d_id, uint32_t no_o_id);
}  // namespace Initializer
