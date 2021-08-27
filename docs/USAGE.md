# General Usage

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
./naive w t s
```
(w: num_warehouse, t: num_threads, s: seconds)

## Format code
This requires installation of clang-format. (`sudo apt-get install clang-format`)

```sh
cd build
cmake ..
make
make format
```

## Configuration

One of the ways to set configuration is to use cmake GUI.

```sh
cd build
ccmake .. ## set configurations in GUI
```

Important parameters are:
- `CMAKE_BUILD_TYPE`
- `LOG_LEVEL` 
- `CC_ALG`

- Use `CMAKE_BUILD_TYPE=Debug`, `LOG_LEVEL=5` for debugging.
- Use `CMAKE_BUILD_TYPE=Release`, `LOG_LEVEL=0` for measuring performance.
- For `CC_ALG`, use `SILO` or `NAIVE`

## Test

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

# Customizing Transaction Engines

The implementation of tpcc-runner can be divided mainly into two parts, tpcc and transaction engine, which are located in [./tpcc](./tpcc) and concurrency control specific directories/

[./tpcc](./tpcc) contains code that are specialized for running transactions described in the [TPC-C specification](http://tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf). This includes not only the profiles of the five transactions (NewOrder, Payment, OrderStatus, Delivery, StockLevel) but also the schema and the generator of the tables, and runner of the transactions.

On the other hand, CC specific directories contains code that are not specialized to TPC-C such as the database and the transaction. One can use a different transaction engine simply by implementing the glue code. It is essential to implement these interfaces because they are called in [./tpcc](./tpcc) or [./tpcc](./test). Although some of the interfaces are specific to TPC-C, efforts have been made to keep them generic to a certain level for easier implementation.