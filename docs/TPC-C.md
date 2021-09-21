# Optimization
1. The specification of TPC-C requires to print the output of each transaction in a certain format. This is simplified by creating a 64-bit checksum of the specified output.
2. Timestamp is implemented as `int64_t`.
3. No transaction reads the history table and it is an append only table. Also, it does not have a primary key. Therefore, it is implemented as thread local deque. See `protocols/tpcc-common/record_misc.hpp` for details.
4. Secondary table is required for Order and Customer Table. While Order Secondary Table is implemented using the thread-safe index structure, Customer Secondary Table is implemented using the thread-unsafe `std::multimap` where the value is the Customer record **pointer**. This is possible because no new Customer record is inserted during the execution. Cusotomer records are and only read or updated (fields that consist the secondary key is not updated), which means that the secondary index does not have to be thread-safe. See `protocols/tpcc-common/record_misc.hpp` for details.

# Phantom protection

## What is it?

Phantoms are anomalies caused when insert and range query happen concurrently. If we scanned a particular range but only kept track of the records that were present during the scan, membership in the range could change without being detected by the protocol, thus violating serializability. A typical phantom looks like the following schedule.

```
r1[P] ... w2[y in P] ... (c1 or a1)
```

## Where in TPC-C is phantom likely to occur?

Existing range queries in current implementation of TPC-C are

| Transaction Type | Index              | Range Query Interface                              | Possible Concurrent Insert? |
| ---------------- | ------------------ | -------------------------------------------------- | --------------------------- |
| Payment          | Customer Secondary | `get_customer_by_last_name_and_prepare_for_update` | NO                          |
| OrderStatus      | Customer Secondary | `get_customer_by_last_name`                        | NO                          |
| OrderStatus      | Order Secondary    | `get_order_by_customer_id`                         | YES (by NewOrder)           |
| OrderStatus      | OrderLine          | `range_query`                                      | YES (by NewOrder)           |
| Delivery         | NewOrder           | `get_neworder_with_smallest_key_no_less_than`      | YES (by NewOrder)           |
| Delivery         | OrderLine          | `range_update`                                     | YES (by NewOrder)           |
| StockLevel       | OrderLine          | `range_query`                                      | YES (by NewOrder)           |

Those that have "YES" in the fourth column are likely to cause phantoms.
Possible ways to protect range queries from phantoms are
1. Table Lock
2. Predicate Lock
3. Gap Lock
4. Node Verify (OCC)

Masstree provides good interface to implement Node Verify style phantom protection. See https://github.com/wattlebirdaz/masstree-wrapper for more detail.

Note that phantoms are also possible due to lookups and deletes that fail. In the current implementation of TPC-C, as soon as failure is detected a transaction will abort, thus this is not a problem. However, if you are trying to implement a protocol that continues even when a failure is detected, you need to implement phantom protection for these in addition to range queries.