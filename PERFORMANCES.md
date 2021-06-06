# Performance Notes

## TPC-C

The following is the list of optimizations and cheats which have been applied or not applied but scrutinized for better performance.

- The specification of TPC-C defines the contents of output of the transactions and orders to print it in a certain format. This is simplified by creating a 64bit checksum of the specified output.

- The current naive transaction engine uses WriteSet (which is respective to the transaction instances) to temporarily insert and update records before commit. This would allow us to abort transactions by clearing contents of the WriteSet and without touching the shared memory. This seems good but not in terms of performance compared with the in-place inserts and updates. The WriteSet increases the number of copies of the records and making copies is a heavy operation. In TPC-C, it is possible to update the shared memory in-place and create an undo buffer **only when aborting is certain**. This is because in TPC-C, you can know beforehand whether a transaction aborts. Although this would increase performance (because less copies will be made), in my opinion, this is impractical because it is impossible to know whether a transaction aborts before the start of the transaction in real world. Therefore, WriteSet was adopted in this implementation.

## Transaction Concurrency

Executing `./main 1 1 20` yields the following output on `8 core Intel(R) Core(TM) i9-9900 CPU @ 3.10GHz` with `32GB RAM`

```
Loading all tables with 1 warehouse(s)
Loaded
1 warehouse(s), 1 thread(s), 20 second(s)
    commits: 1329712
    sys aborts: 0
    usr aborts: 6081
Throughput: 66485 txns/s

Details:
    NewOrder    c:595148(44.76%)   ua:6081  sa:0
    Payment     c:573984(43.17%)   ua:0  sa:0
    OrderStatus c:53265(4.01%)   ua:0  sa:0
    Delivery    c:53500(4.02%)   ua:0  sa:0
    StockLevel  c:53815(4.05%)   ua:0  sa:0
```

- usr aborts are aborts specified in TPC-C. (1% of the NewOrder Transaction should rollback according to the specification.)
- sys aborts are aborts that occurs due to the concurrency control. (One example of sys aborts would be aborts due to no-wait in 2PL.) Since there are no concurrency in the current implementation, there are no sys aborts.

Since the current implementation locks the whole database while performing a transaction, execution using multiple threads is **possible**. This, however, decreases performance due to contention.

- 2 threads
```
1 warehouse(s), 2 thread(s), 20 second(s)
    commits: 1203014
    sys aborts: 0
    usr aborts: 5505
Throughput: 60150 txns/s

Details:
    NewOrder    c:537842(44.71%)   ua:5505  sa:0
    Payment     c:519590(43.19%)   ua:0  sa:0
    OrderStatus c:48515(4.03%)   ua:0  sa:0
    Delivery    c:48609(4.04%)   ua:0  sa:0
    StockLevel  c:48458(4.03%)   ua:0  sa:0
```

- 5 threads
```
1 warehouse(s), 5 thread(s), 20 second(s)
    commits: 1143756
    sys aborts: 0
    usr aborts: 5405
Throughput: 57187 txns/s

Details:
    NewOrder    c:511939(44.76%)   ua:5405  sa:0
    Payment     c:493250(43.13%)   ua:0  sa:0
    OrderStatus c:46360(4.05%)   ua:0  sa:0
    Delivery    c:46242(4.04%)   ua:0  sa:0
    StockLevel  c:45965(4.02%)   ua:0  sa:0
```

Implementing concurrent transactions is left for future work.

