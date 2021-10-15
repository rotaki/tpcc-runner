**WIP**

# Protocols
Details for the protocols implemented in this repository.

# Overview

| Protocol | Type               | Read       | Update        | Phantom Protection | Update GC                   | Delete GC                   | Lock    | Version Storage | Version Head Indirection |
| -------- | ------------------ | ---------- | ------------- | ------------------ | --------------------------- | --------------------------- | ------- | --------------- | ------------------------ |
| SILO     | Optimistic         | By Pointer | Copy on Write | Node Verify        | Epoch Based Tuple Level     | Epoch Based Tuple Level     | Spin    | -               | -                        |
| NOWAIT   | Pessimistic        | By Pointer | Copy on Write | Next-Key Lock      | -                           | Epoch Based Tuple Level     | Spin    | -               | -                        |
| MVTO     | Timestamp Ordering | By Pointer | Copy on Write | Node Timestamp     | Timestamp Based Tuple Level | Timestamp Based Tuple Level | Spin    | N2O             | No                       |
| NOWAIT   | Pessimistic        | By Pointer | Copy on Write | Next-Key Lock      | -                           | Epoch Based Tuple Level     | WaitDie | -               | -                        |
## Type
### Pessimistic
Pessimistic approach locks record on read.
### Optimistic 
Optimistic approach does not lock on read. It verifies whether the value read has not been changed in pre-commit phase. If it is changed, the transaction will abort.

### Timestamp Ordering
The schedule of the transactions is determined beforehand based on the timestamp attached to each transaction.

## Read
### By Pointer 
Read by Pointer does not allocate new memory on read. It copies the record pointer of the shared index and place it in the readset. 
### By Copy
Read by Copy will allocate memory on read and copies the record data from the index. Generally Read by Pointer is faster than Read by Copy because Read by Copy will cause additional memory allocation, memcpy, and GC.
## Update
### Copy on Write
In Copy on Write approach, a record is updated by a replacing it with a copy.

### Modify Original
Modify Original approach will not create another copy and update the data directly.
## Phantom Protection
### Node Verify
Node Verify approach uses index leaf nodes to optimistically verify whether inserts to a certain range has been executed during the transaction that calls the range query.
### Next-Key Lock
Next-Key Lock approach will lock the next key before inserting a key. This prevent inserts into the range of concurrent range queries. 

### Node Timestamp
Node Timestamp approach embeds timestamp to index leaf nodes to detect conflicts between range-read and insert. When insert detects a leaf node with a bigger timestamp, it will rollback its insertion.
## GC
### Epoch Based
TODO

### Timestamp Based
TODO
## Lock
### Spin
TODO