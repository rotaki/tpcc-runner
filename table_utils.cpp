#include "table_utils.hpp"

template <typename T>
std::string to_string(const T& t) {
    return std::string(reinterpret_cast<const char*>(&t), sizeof(t));
}
std::string TableUtils::serialize_warehouse(const Warehouse* w) {
    return to_string(*w);
}

void TableUtils::deserialize_warehouse(Warehouse* w, const std::string& serialized) {
    const Warehouse* x = reinterpret_cast<const Warehouse*>(serialized.data());
    w->w_id = x->w_id;
    w->w_tax = x->w_tax;
    w->w_ytd = x->w_ytd;
    strcpy(w->w_name, x->w_name);
    w->w_address = x->w_address;
}

std::string TableUtils::serialize_district(const District* d) {
    return to_string(*d);
}

void TableUtils::deserialize_district(District* d, const std::string& serialized) {
    const District* x = reinterpret_cast<const District*>(serialized.data());
    d->d_id = x->d_id;
    d->d_w_id = x->d_w_id;
    d->d_next_o_id = x->d_next_o_id;
    d->d_tax = x->d_tax;
    d->d_ytd = x->d_ytd;
    strcpy(d->d_name, x->d_name);
    d->d_address = x->d_address;
}

std::string TableUtils::serialize_customer(const Customer* c) {
    return to_string(*c);
}

void TableUtils::deserialize_customer(Customer* c, const std::string& serialized) {
    const Customer* x = reinterpret_cast<const Customer*>(serialized.data());
    c->c_id = x->c_id;
    c->c_d_id = x->c_d_id;
    c->c_w_id = x->c_w_id;
    c->c_payment_cnt = x->c_payment_cnt;
    c->c_delivery_cnt = x->c_delivery_cnt;
    c->c_since = x->c_since;
    c->c_credit_lim = x->c_credit_lim;
    c->c_discount = x->c_discount;
    c->c_balance = x->c_balance;
    c->c_ytd_payment = x->c_ytd_payment;
    strcpy(c->c_first, x->c_first);
    strcpy(c->c_middle, x->c_middle);
    strcpy(c->c_last, x->c_last);
    strcpy(c->c_phone, x->c_phone);
    strcpy(c->c_credit, x->c_credit);
    strcpy(c->c_data, x->c_data);
    c->c_address = x->c_address;
}

std::string TableUtils::serialize_history(const History* h) {
    return to_string(*h);
}

void TableUtils::deserialize_history(History* h, const std::string& serialized) {
    const History* x = reinterpret_cast<const History*>(serialized.data());
    h->h_c_id = x->h_c_id;
    h->h_c_d_id = x->h_c_d_id;
    h->h_c_w_id = x->h_c_w_id;
    h->h_d_id = x->h_d_id;
    h->h_w_id = x->h_w_id;
    h->h_date = x->h_date;
    h->h_amount = x->h_amount;
    strcpy(h->h_data, x->h_data);
}

std::string TableUtils::serialize_order(const Order* o) {
    return to_string(*o);
}

void TableUtils::deserialize_order(Order* o, const std::string& serialized) {
    const Order* x = reinterpret_cast<const Order*>(serialized.data());
    o->o_id = x->o_id;
    o->o_d_id = x->o_d_id;
    o->o_w_id = x->o_w_id;
    o->o_c_id = x->o_c_id;
    o->o_carrier_id = x->o_carrier_id;
    o->o_ol_cnt = x->o_ol_cnt;
    o->o_all_local = x->o_all_local;
    o->o_entry_d = x->o_entry_d;
}


std::string TableUtils::serialize_neworder(const NewOrder* n) {
    return to_string(*n);
}

void TableUtils::deserialize_neworder(NewOrder* n, const std::string& serialized) {
    const NewOrder* x = reinterpret_cast<const NewOrder*>(serialized.data());
    n->no_o_id = x->no_o_id;
    n->no_d_id = x->no_d_id;
    n->no_w_id = x->no_w_id;
}

std::string TableUtils::serialize_orderline(const OrderLine* o) {
    return to_string(*o);
}

void TableUtils::deserialize_orderline(OrderLine* o, const std::string& serialized) {
    const OrderLine* x = reinterpret_cast<const OrderLine*>(serialized.data());
    o->ol_o_id = x->ol_o_id;
    o->ol_d_id = x->ol_d_id;
    o->ol_w_id = x->ol_w_id;
    o->ol_number = x->ol_number;
    o->ol_i_id = x->ol_i_id;
    o->ol_supply_w_id = x->ol_supply_w_id;
    o->ol_delivery_d = x->ol_delivery_d;
    o->ol_quantity = x->ol_quantity;
    o->ol_amount = x->ol_amount;
    strcpy(o->ol_dist_info, x->ol_dist_info);
}

std::string TableUtils::serialize_item(const Item* i) {
    return to_string(*i);
}

void TableUtils::deserialize_item(Item* i, const std::string& serialized) {
    const Item* x = reinterpret_cast<const Item*>(serialized.data());
    i->i_id = x->i_id;
    i->i_im_id = x->i_im_id;
    i->i_price = x->i_price;
    strcpy(i->i_name, x->i_name);
    strcpy(i->i_data, x->i_data);
}

std::string TableUtils::serialize_stock(const Stock* s) {
    return to_string(*s);
}

void TableUtils::deserialize_stock(Stock* s, const std::string& serialized) {
    const Stock* x = reinterpret_cast<const Stock*>(serialized.data());
    s->s_i_id = x->s_i_id;
    s->s_w_id = x->s_w_id;
    s->s_quantity = x->s_quantity;
    s->s_ytd = x->s_ytd;
    s->s_order_cnt = x->s_order_cnt;
    s->s_remote_cnt = x->s_remote_cnt;
    strcpy(s->s_dist_01, x->s_dist_01);
    strcpy(s->s_dist_02, x->s_dist_02);
    strcpy(s->s_dist_03, x->s_dist_03);
    strcpy(s->s_dist_04, x->s_dist_04);
    strcpy(s->s_dist_05, x->s_dist_05);
    strcpy(s->s_dist_06, x->s_dist_06);
    strcpy(s->s_dist_07, x->s_dist_07);
    strcpy(s->s_dist_08, x->s_dist_08);
    strcpy(s->s_dist_09, x->s_dist_09);
    strcpy(s->s_dist_10, x->s_dist_10);
    strcpy(s->s_data, x->s_data);
}
