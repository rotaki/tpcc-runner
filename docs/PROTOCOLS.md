**WIP**

# Protocols
Details for the protocols implemented in this repository.

# Overview

| Protocols | Type        | Read       | Update        | Phantom Protection | GC          | Lock |
| --------- | ----------- | ---------- | ------------- | ------------------ | ----------- | ---- |
| SILO      | Optimistic  | By Pointer | Copy on Write | Node Verify        | Epoch Based | Spin |
| NOWAIT    | Pessimistic | By Pointer | Copy on Write | Next-Key Lock      | Epoch Based | Spin |


## Type
### Pessimistic vs Optimistic
Pessimistic approach locks record on read while Optimistic approach does not.

## Read
### By Pointer vs By Copy
Generally Read by Pointer is faster than Read by Copy. Read by Copy will cause additional memory allocation, memcpy, and GC.

## Update
### Copy on Write vs Modify Original
In Copy on Write approach, a record is updated by a replacing it with a copy while Modify Original approach will not create another copy and update the data directly.

## Phantom Protection
### Node Verify vs Next-Key Lock
Next-Key Lock approach will lock the next key before inserting a key. This prevent inserts into the range of concurrent range queries. Node Verify approach will optimistically verify whether inserts to a certain range has been executed during the transaction that calls the range query.

## GC
### Epoch Based
TODO

## Lock
### Spin
TODO