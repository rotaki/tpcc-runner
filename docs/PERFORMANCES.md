# Performance Notes

## Optimizations and cheats

The following is a list of optimizations and cheats which have been applied or not applied but scrutinized for better performance.

- The specification of TPC-C defines the contents of output of the transactions and orders to print it in a certain format. This is simplified by creating a 64bit checksum of the specified output.

- The current naive transaction engine uses WriteSet (which is respective to the transaction instances) to temporarily insert and update records before commit. This would allow us to abort transactions by clearing contents of the WriteSet and without touching the shared memory. This seems good but not in terms of performance compared with the in-place inserts and updates. The WriteSet increases the number of copies of the records and making copies is a heavy operation. In TPC-C, it is possible to update the shared memory in-place and create an undo buffer **only when aborting is certain**. This is because in TPC-C, you can know beforehand whether a transaction aborts. Although this would increase performance (because less copies will be made), in my opinion, this is impractical because it is impossible to know whether a transaction aborts before the start of the transaction in real world. Therefore, WriteSet was adopted in this implementation.

- Further optimizations for this naive transaction engine would be to decrease the number of tree traverse by
    - using unordered map instead of ordered map for tables which do not require range queries or lower/upper bounds
    - decreasing the number of lower/upper bound calls when manipulating the tables
    - remembering the iterator of the table from which writeset copied the record when preparing for update
    
## Transaction Concurrency

Executing `./naive 1 1 20` yields the following output on `8 core Intel(R) Core(TM) i9-9900 CPU @ 3.10GHz` with `32GB RAM`

```
Loading all tables with 1 warehouse(s)
Loaded
1 warehouse(s), 1 thread(s), 20 second(s)
    commits: 2436911
    sys aborts: 0
    usr aborts: 11196
Throughput: 121845 txns/s

Details:
    NewOrder    c: 1089872(0.45%)   ua: 11196  sa:     0
    Payment     c: 1053197(0.43%)   ua:     0  sa:     0
    OrderStatus c:   97858(0.04%)   ua:     0  sa:     0
    Delivery    c:   98044(0.04%)   ua:     0  sa:     0
    StockLevel  c:   97940(0.04%)   ua:     0  sa:     0
```

- usr aborts are aborts specified in TPC-C. (1% of the NewOrder Transaction should rollback according to the specification.)
- sys aborts are aborts that occurs due to the concurrency control. (One example of sys aborts would be aborts due to no-wait in 2PL.) Since there are no concurrency in the current implementation, there are no sys aborts.

Since the current implementation locks the whole database while performing a transaction, execution using multiple threads is **possible**. This, however, decreases performance due to contention.

- 2 threads
```
1 warehouse(s), 2 thread(s), 20 second(s)
    commits: 2131902
    sys aborts: 0
    usr aborts: 9834
Throughput: 106595 txns/s

Details:
    NewOrder    c:  953095(0.45%)   ua:  9834  sa:     0
    Payment     c:  921336(0.43%)   ua:     0  sa:     0
    OrderStatus c:   85672(0.04%)   ua:     0  sa:     0
    Delivery    c:   86054(0.04%)   ua:     0  sa:     0
    StockLevel  c:   85745(0.04%)   ua:     0  sa:     0
```

- 5 threads
```
1 warehouse(s), 5 thread(s), 20 second(s)
    commits: 1998028
    sys aborts: 0
    usr aborts: 9204
Throughput: 99901 txns/s

Details:
    NewOrder    c:  894493(0.45%)   ua:  9204  sa:     0
    Payment     c:  862455(0.43%)   ua:     0  sa:     0
    OrderStatus c:   80360(0.04%)   ua:     0  sa:     0
    Delivery    c:   79834(0.04%)   ua:     0  sa:     0
    StockLevel  c:   80886(0.04%)   ua:     0  sa:     0
```
