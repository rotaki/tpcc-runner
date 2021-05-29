# tpcc-runner

A portable TPC-C benchmark for various transaction engines. 

__Current Status: WIP__

# Description

tpcc-runner is yet another implementation of [TPC-C](http://www.tpc.org/tpcc/) which is a benchmark for online transaction processing systems.
TPC-C executes a mix of five different concurrent transactions of different types and complexity to measure the various performance of transaction engines.
Although TPC-C has its own problems, it is still one of the standard benchmark used in academia along with the [YCSB](https://github.com/brianfrankcooper/YCSB) for concurrency control protocols.

__tpcc-runner__ aims to provide an Open Source C++ implementation of TPC-C which __separates the implementation of transaction profiles with the transaction engine__.
As mentioned above, TPC-C will run transactions of five different profiles which are NewOrder, Payment, OrderStatus, Delivery, and StockLevel. 
Ideally these profiles should be independent from the backend transaction engine in the implementation level so that users can port these to different kinds of backends only by implementing the nessesary interfaces.
This is better not only in terms of usability but in terms of fairness as a benchmark.
Although there are already some implementations of TPC-C in C++ such as [tpccbench](https://github.com/evanj/tpccbench), these implementations tightly couples the transaction profiles with the backend so that it takes a lot of effort to customize them for uses in different backends.
To address this issue, tpcc-runner provides an implementation TPC-C where transaction profiles are loosely coupled with the backend.

# Getting Started

## Dependencies
- Tested on MaxOS Catalina, Ubuntu 20.04
- Uses C++20

## Build
To build, 

```sh
mkdir build
cd build
cmake ..
make
```

To format code, 

```sh
cd build
cmake ..
make
make format
```

To set to a different log level, 

```sh
cd build
ccmake .. ## set log level in GUI
```

To test all, 

```sh
cd build
cmake ..
make
ctest
```

To execute a single test, 

```sh
cd build
cmake ..
make
cd test/
./test_name
```

# Author

Riki Otaki

# Licensing

[MIT License](https://github.com/wattlebirdaz/tpcc-runner/blob/master/LICENSE)