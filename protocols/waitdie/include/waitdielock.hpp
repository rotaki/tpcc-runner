#pragma once
#include <cassert>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>

#include "utils/atomic_wrapper.hpp"
#include "utils/logger.hpp"

template <typename Node>
struct TimestampSortedList {
private:
    using TS = uint64_t;

    struct Item {
        Item(TS ts, Node node)
            : ts(ts)
            , node(node) {}
        TS ts;
        Node node;
    };

public:
    TimestampSortedList() { clear(); }

    bool empty() { return item_list.empty(); }

    void clear() { item_list.clear(); }

    void insert(TS ts, Node node) {
        // insert a node to the sorted list
        for (auto iter = item_list.begin(); iter != item_list.end(); ++iter) {
            if (iter->ts < ts) {
                item_list.emplace(iter, ts, node);
                return;
            }
        }
        assert(item_list.empty() || item_list.back().ts >= ts);
        item_list.emplace_back(ts, node);
        return;
    }

    void remove(TS ts) {
        for (auto iter = item_list.begin(); iter != item_list.end(); ++iter) {
            if (iter->ts == ts) {
                item_list.erase(iter);
                return;
            }
        }
        throw std::runtime_error("timestamp not in list");
    }

    Node& front() { return item_list.front().node; }

    void pop() { item_list.pop_front(); }

    TS get_back_timestamp() { return item_list.back().ts; }

    uint64_t get_size() { return item_list.size(); }

    void trace() {
        printf("[ ");
        for (auto iter = item_list.begin(); iter != item_list.end(); ++iter) {
            printf("%lu ", iter->ts);
        }
        printf("]\n");
    }

private:
    std::list<Item> item_list;
};

class WaitDieLock {
private:
    using TS = uint64_t;
    enum Operation : uint8_t {
        I,  // invalid
        S,  // shared
        E,  // exclusive
        U   // upgrade
    };
    struct WaiterNode {
        WaiterNode(TS ts, Operation op, bool waiting)
            : waiting(waiting)
            , ts(ts)
            , op(op) {}
        alignas(64) bool waiting;  // spin variable
        char pad[64 - sizeof(bool)] = {};
        TS ts;         // timestamp
        Operation op;  // S, E, U
    };

    // sorted (ts big -> ts small) list of waiters
    using WaiterList = TimestampSortedList<std::shared_ptr<WaiterNode>>;

    struct OwnerNode {
        OwnerNode(TS ts)
            : ts(ts) {}
        TS ts;
    };

    struct OwnerList {
        Operation op = I;  // I, S, E
        // sorted (ts big -> ts small) list of owners
        TimestampSortedList<std::shared_ptr<OwnerNode>> owners;
        void insert(TS ts, std::shared_ptr<OwnerNode> node) { owners.insert(ts, node); }
        void remove(TS ts) {
            owners.remove(ts);
            if (owners.empty()) op = I;
        }
        uint64_t get_size() { return owners.get_size(); }
        // get the smallest timestamp in owner_list
        TS get_min_timestamp() { return owners.get_back_timestamp(); }
        void trace() {
            printf(op == I ? "(I):" : op == S ? "(S):" : "(E):");
            return owners.trace();
        }
    };

    std::mutex latch;
    WaiterList waiter_list;
    OwnerList owner_list;

public:
    WaitDieLock() {}

    void trace() {
        latch.lock();
        printf("Waiter: ");
        waiter_list.trace();
        printf("Owner: ");
        owner_list.trace();
        latch.unlock();
    }

    void trace_without_latch() {
        printf("Waiter: ");
        waiter_list.trace();
        printf("Owner: ");
        owner_list.trace();
    }

    bool try_lock_shared(uint64_t ts) {
        latch.lock();
        /**
         * STATE -> ACTION
         *
         * owner(I, S), no waiter -> add to owner_list, change it's op to S, unlock latch and return
         *true
         *
         * owner(I), waiter -> add to waiter_list, unlock latch and spin
         *
         * owner(S), waiter -> compare with min ts of owner with this ts, if this ts is smaller, add
         *to waiter_list, unlock latch and spin else unlock latch and return false
         *
         * owner(E) -> compare with min ts of owner with this ts, if this ts is smaller, add to
         *waiter_list, unlock latch and spin else unlock latch and return false
         **/
        bool no_waiter = waiter_list.empty();
        Operation op = owner_list.op;
        if ((op == I || op == S) && no_waiter) {
            auto node = std::make_shared<OwnerNode>(ts);
            owner_list.insert(ts, node);
            owner_list.op = S;
            latch.unlock();
            return true;
        }
        if ((op == I) && !no_waiter) {
            auto node = std::make_shared<WaiterNode>(ts, S, true);
            waiter_list.insert(ts, node);
            latch.unlock();
            while (load_acquire(node->waiting))
                ;  // spin
            return true;
        }
        if (((op == S) && !no_waiter) || op == E) {
            if (owner_list.get_min_timestamp() > ts) {
                auto node = std::make_shared<WaiterNode>(ts, S, true);
                waiter_list.insert(ts, node);
                latch.unlock();
                while (load_acquire(node->waiting))
                    ;  // spin
                return true;
            } else {
                latch.unlock();
                return false;
            }
        }
        throw std::runtime_error("Unhandled State Found");
    };

    bool try_lock(uint64_t ts) {
        latch.lock();
        /**
         * STATE -> ACTION
         *
         * owner(I), no waiter -> add to owner_list, change it's op to E, unlock latch and return
         *true
         *
         * owner(I), waiter -> add to waiter_list, unlock latch and spin
         *
         * owner(S, E) -> compare with min ts of owner with this ts, if this ts is smaller, add to
         *waiter_list, unlock latch and spin else unlock latch and return false
         *
         **/
        bool no_waiter = waiter_list.empty();
        Operation op = owner_list.op;
        if (op == I && no_waiter) {
            // add to owner_list and return
            auto node = std::make_shared<OwnerNode>(ts);
            owner_list.insert(ts, node);
            owner_list.op = E;
            latch.unlock();
            return true;
        }
        if (op == I && !no_waiter) {
            // add to waiter_list and spin
            auto node = std::make_shared<WaiterNode>(ts, E, true);
            waiter_list.insert(ts, node);
            latch.unlock();
            while (load_acquire(node->waiting))
                ;  // spin
            return true;
        }
        if (op == S || op == E) {
            if (owner_list.get_min_timestamp() > ts) {
                auto node = std::make_shared<WaiterNode>(ts, E, true);
                waiter_list.insert(ts, node);
                latch.unlock();
                while (load_acquire(node->waiting))
                    ;  // spin
                return true;
            } else {
                latch.unlock();
                return false;
            }
        }
        throw std::runtime_error("Unhandled State Found");
    }

    bool try_lock_upgrade(uint64_t ts) {
        latch.lock();
        /**
         * STATE -> ACTION
         *
         * owner(I, E) -> throw
         *
         * owner(S) -> compare with min ts of owner with this ts, if they are same, and multiple
         *owners exist, add to waiter_list, unlock latch and spin if they are same, and single owner
         *exists, change owner state and return true else unlock latch and return false
         **/
        Operation op = owner_list.op;
        if (op == I || op == E) {
            throw std::runtime_error("No lock to upgrade");
        } else if (op == S) {
            TS min_ts = owner_list.get_min_timestamp();
            uint64_t num_owners = owner_list.get_size();
            if (min_ts == ts && num_owners > 1) {
                auto node = std::make_shared<WaiterNode>(ts, U, true);
                waiter_list.insert(ts, node);  // this should come to the head of waiter_list
                latch.unlock();
                while (load_acquire(node->waiting))
                    ;
                return true;
            } else if (min_ts == ts && num_owners == 1) {
                owner_list.op = E;
                latch.unlock();
                return true;
            } else {
                latch.unlock();
                return false;
            }
        }
        throw std::runtime_error("Unhandled State Found");
    }

    void unlock_shared(uint64_t ts) {
        latch.lock();
        /**
         * STATE -> ACTION
         *
         * owner(I, E) -> throw
         *
         * owner(S) -> remove this ts from owner_list, promote waiters, unlock latch, return
         **/
        Operation op = owner_list.op;
        if (op == I || op == E) {
            throw std::runtime_error("No shared lock to unlock");
        } else if (op == S) {
            owner_list.remove(ts);
            promote_waiters();
            latch.unlock();
            return;
        }
        throw std::runtime_error("Unhandled State Found");
    }

    void unlock(uint64_t ts) {
        latch.lock();
        /**
         * STATE -> ACTION
         *
         * owner(I, S) -> throw
         *
         * owner(E) -> remove this ts from owners if found, promote_waiters, unlock latch, return
         **/
        Operation op = owner_list.op;
        if (op == I || op == S) {
            throw std::runtime_error("No exclusive lock to unlock");
        } else if (op == E) {
            owner_list.remove(ts);
            promote_waiters();
            latch.unlock();
            return;
        }
        throw std::runtime_error("Unhandled State Found");
    }

private:
    // Get latch before calling this function
    void promote_waiters() {
        /**
         * STATE -> ACTION
         * no_waiter -> return
         *
         * waiter(S), owner(I, S) -> promote waiter, change owner mode, pop from waiter_list, set
         *waiting to false, loop
         *
         * waiter(S), owner(E) -> return waiter(E), owner(S, E) -> return
         *
         * waiter(E), owner(I) -> promote waiter, change owner mode, pop from waiter_list, set
         *waiting to false, loop
         *
         * waiter(U), owner(I, E) -> throw
         *
         * waiter(U), owner(S), multiple owners -> return
         *
         * waiter(U), owner(S), single owner -> promote waiter, change owner mode, pop from
         *waiter_list, set waiting to false, loop
         *
         **/
        Operation o_op;
        Operation w_op;
        uint64_t num_owners;
        while (true) {
            if (waiter_list.empty()) return;

            o_op = owner_list.op;
            num_owners = owner_list.get_size();
            w_op = waiter_list.front()->op;

            bool finish = (w_op == S && o_op == E) || (w_op == E && (o_op == S || o_op == E))
                || (w_op == U && o_op == S && num_owners > 1);
            if (finish) return;

            bool error = (w_op == U && (o_op == I || o_op == E));
            if (error) throw std::runtime_error("Failed Promoting Waiter");

            // loop
            if (w_op == S && (o_op == I || o_op == S)) {
                // promote waiter
                std::shared_ptr<WaiterNode> waiter = waiter_list.front();
                TS ts = waiter->ts;
                auto o_node = std::make_shared<OwnerNode>(ts);
                owner_list.insert(ts, o_node);
                owner_list.op = S;
                // pop
                waiter_list.pop();
                // set waiting to false
                store_release(waiter->waiting, false);
                // loop
                continue;
            } else if (w_op == E && o_op == I) {
                // promote waiter
                std::shared_ptr<WaiterNode> waiter = waiter_list.front();
                TS ts = waiter->ts;
                auto o_node = std::make_shared<OwnerNode>(ts);
                owner_list.insert(ts, o_node);
                owner_list.op = E;
                // pop
                waiter_list.pop();
                // set waiting to false
                store_release(waiter->waiting, false);
                // loop
                continue;
            } else if (w_op == U && o_op == S && num_owners == 1) {
                // promote waiter
                std::shared_ptr<WaiterNode> waiter = waiter_list.front();
                assert(owner_list.get_min_timestamp() == waiter->ts);
                owner_list.op = E;
                // pop
                waiter_list.pop();
                // set waiting to false
                store_release(waiter->waiting, false);
                continue;
            }
            throw std::runtime_error("Unhandled State Found");
        }
    }
};