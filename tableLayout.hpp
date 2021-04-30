#include <cstdint>

static const int DATETIME = 14;

struct Address {
  static const int MAX_STREET = 10;
  static const int MAX_CITY = 10;
  static const int STATE = 2;
  static const int ZIP = 9;
private:
  Address();
};

// Primary Key w_id
struct Warehouse {
  static const int MAX_NAME = 10;
  uint32_t w_id;
  float w_tax;                  // Sales tx
  float w_ytd;                  // Year to date balance
  char w_name[MAX_NAME+1];
  char w_street_1[Address::MAX_STREET+1];
  char w_street_2[Address::MAX_STREET+1];
  char w_city[Address::MAX_CITY+1];
  char w_state[Address::STATE+1];
  char w_zip[Address::ZIP+1];
};

// Primary Key (d_w_id, d_id)
// Foreign Key d_w_id references w_id
struct District {
  static const int MAX_NAME = 10;
  uint32_t d_id;
  uint32_t d_w_id;
  uint32_t d_next_o_id;              // next available order number
  float d_tax;                    // sales tax
  float d_ytd;                    // year to date balance
  char d_name[MAX_NAME+1];
  char d_street_1[Address::MAX_STREET+1];
  char d_street_2[Address::MAX_STREET+1];
  char d_city[Address::MAX_CITY+1];
  char d_state[Address::STATE+1];
  char d_zip[Address::ZIP+1];
};

// Primary Key (c_w_id, c_d_id, c_id)
// Foreign Key (c_w_id, c_d_id) references (d_w_id, d_id)
struct Customer {
  static const int MAX_FIRST = 16;
  static const int MAX_MIDDLE = 2;
  static const int MAX_LAST = 16;
  static const int PHONE = 16;
  static const int CREDIT = 2;
  static const int MAX_DATA = 500;
  uint32_t c_id;
  uint32_t c_d_id;
  uint32_t c_w_id;
  uint32_t c_payment_cnt;
  uint32_t c_delivery_cnt;
  float c_credit_lim;
  float c_discount;
  float c_balance;
  float c_ytd_payment;
  char c_first[MAX_FIRST+1];
  char c_middle[MAX_MIDDLE+1];
  char c_last[MAX_LAST+1];
  char c_street_1[Address::MAX_STREET+1];
  char c_street_2[Address::MAX_STREET+1];
  char c_city[Address::MAX_CITY+1];
  char c_state[Address::STATE+1];
  char c_zip[Address::ZIP+1];
  char c_phone[PHONE+1];
  char c_since[DATETIME+1];           // date and time
  char c_credit[CREDIT+1];           // "GC"=good, "BC"=bad
  char c_data[MAX_DATA+1];           // Miscellaneous information
};

// Primary Key None
// Foreign Key (h_c_w_id, h_c_d_id, h_c_id) references (c_w_id, c_d_id, c_id)
// Foreign Key (h_w_id, h_d_id) references (d_w_id, d_id)
struct History {
  static const int MAX_DATA = 24;
  uint32_t h_c_id;
  uint32_t h_c_d_id;
  uint32_t h_c_w_id;
  uint32_t h_d_id;
  uint32_t h_w_id;
  float h_amount;
  char h_date[DATETIME+1];
  char h_data[MAX_DATA+1];
};

// Primary Key (no_w_id, no_d_id, no_o_id)
// Foreign Key (no_w_id, no_d_id, no_o_id) references (o_w_id, o_d_id, o_id)
struct NewOrder {
  uint32_t no_o_id;
  uint32_t no_d_id;
  uint32_t no_w_id;
};

// Primary Key (o_w_id, o_d_id, o_id)
// Foreign Key (o_w_id, o_d_id, o_c_id) references (c_w_id, c_d_id, c_id)
struct Order {
  uint32_t o_id;
  uint32_t o_d_id;
  uint32_t o_w_id;
  uint32_t o_c_id;
  uint32_t carrier_id;
  uint32_t o_ol_cnt;            // count of order-lines
  uint32_t o_all_local;
  char o_entry_d[DATETIME+1];
};

// Primary Key i_id
struct Item {
  static const int MAX_DATA = 50;
  uint32_t i_id;
  uint32_t i_im_id;
  uint32_t i_name;
  float i_price;
  char i_data[MAX_DATA+1];
};

// Primary Key (s_w_id, s_i_id)
// Foreign Key s_w_id references w_id
struct Stock {
  static const int DIST = 24;
  static const int MAX_DATA = 50;
  uint32_t s_i_id;
  uint32_t s_w_id;
  int32_t s_quantity;
  uint32_t s_ytd;
  uint32_t s_order_cnt;
  uint32_t s_remote_cnt;
  char s_dist_01[DIST+1];
  char s_dist_02[DIST+1];
  char s_dist_03[DIST+1];
  char s_dist_04[DIST+1];
  char s_dist_05[DIST+1];
  char s_dist_06[DIST+1];
  char s_dist_07[DIST+1];
  char s_dist_08[DIST+1];
  char s_dist_09[DIST+1];
  char s_dist_10[DIST+1];
  char s_data[MAX_DATA+1];
};


