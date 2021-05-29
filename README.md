# tpcc-runner

A portable TPC-C benchmark for various transaction engines. 

__Current Status: WIP__

# Description

tpcc-runner is yet another implementation of [TPC-C](http://www.tpc.org/tpcc/) which is a benchmark for online transaction processing systems.
TPC-C executes a mix of five different concurrent transactions of different types and complexity to measure the various performance of transaction engines.
Although TPC-C has its own problems, it is still one of the standard benchmark used in academia along with the [YCSB](https://github.com/brianfrankcooper/YCSB) for concurrency control protocols.

__tpcc-runner__ aims to provide a C++ implementation of TPC-C which __separates the implementation of transaction profiles with the transaction engine__.
As mentioned, TPC-C will run transactions of five diffrent profiles (e.g. NewOrder) and these should be independent from the backend transction engine in the implementation level.
This will allow users to port tpcc-runner to different kinds of backend engines just by implementing the nessesary interfaces, which is easier and fairer as a benchmark.
There are already some implementation of TPC-C in C++ such as [tpccbench](https://github.com/evanj/tpccbench). 
However, these implementations are based on a dense connection between the transaction profile and the backend so that it takes a lot of effort to customize them for uses in different backends.

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

To format code
```sh
cd build
cmake ..
make
make format
```

To set differnt log level
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

## Author

Riki Otaki

# Licensing

[MIT License](https://github.com/wattlebirdaz/tpcc-runner/blob/master/LICENSE)