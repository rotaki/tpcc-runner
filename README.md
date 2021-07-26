# tpcc-runner

A portable TPC-C benchmark for various in-memory database engines. 

# Description

tpcc-runner is yet another implementation of [TPC-C](http://www.tpc.org/tpcc/) which is a benchmark for online transaction processing systems.
TPC-C executes a mix of five different concurrent transactions of different types and complexity to measure the various performances of transaction engines.
Although TPC-C has its own problems, it is still one of the standard benchmarks used in academia along with the [YCSB](https://github.com/brianfrankcooper/YCSB) for concurrency control protocols.

# Motivation

tpcc-runner aims to provide an Open Source C++ implementation of in-memory TPC-C which __separates the implementation of transaction profiles from the transaction engine__.
As mentioned above, TPC-C will run transactions of five different profiles which are NewOrder, Payment, OrderStatus, Delivery, and StockLevel. 
Ideally these profiles should be independent from the backend transaction engine in the implementation level so that users can port these to different kinds of backends only by implementing the nessesary interfaces.
This is better not only in terms of usability but in terms of fairness as a benchmark.
Although there are already some implementations of TPC-C in C++ such as [tpccbench](https://github.com/evanj/tpccbench), these implementations tightly couples the transaction profiles with the backend so that it takes a lot of effort to customize them for uses in different backends.
To address this issue, tpcc-runner provides an implementation of TPC-C where transaction profiles are loosely coupled with the backend.

# Getting Started

## Dependencies
- Ubuntu 20.04
- C++20

## Build
To build, 

```sh
mkdir build
cd build
cmake ..
make
```

## Execute
After building, the executable will be stored into the `build/bin` directory.
To execute, 

```sh
cd build/bin
./main w t s
```

This will create tables with `w` warehouses and execute transactions using `t` threads for `s` seconds. For example, `./main 2 5 20` will create tables with 2 warehouses and executes TPC-C using 5 threads for 20 seconds.

For more information on usage, see [USAGE.md](DOCS/USAGE.md).

# Performance

Currently tpcc-runner has a simple transaction engine which locks the entire database while performing a transaction, which obviously does not scale with the number of threads. However, as a benchmark of the TPC-C itself (not the transaction engine) measuring the performance of a single-threaded execution should suffice.
Benchmarking on transaction engines that utilizes multi-threaded concurrency control (such as Silo, ERMIA, TicToc, Cicada, and so on) is left for future work. 

Executing `./main 1 1 20` yields the following output on `8 core Intel(R) Core(TM) i9-9900 CPU @ 3.10GHz` with `32GB RAM`

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

Read more about performance in [PERFORMACES.md](DOCS/PERFORMANCES.md).

# Author

Riki Otaki

# Licensing

[MIT License](https://github.com/wattlebirdaz/tpcc-runner/blob/master/LICENSE)