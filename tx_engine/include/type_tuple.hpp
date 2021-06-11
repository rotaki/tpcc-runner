#pragma once
/*
 * Simple type tuple and its utilities.
 *
 * Usage:
 *
 * using TT = TypeTuple<A, B, C>;
 *
 * TT tt;
 * get<A>(tt) returns the reference of A in tt.
 *
 * Duplicates of the elements of same type is not allowed.
 * For example, TypeTuple<A, B, A, A> does not work well.
 * The non-1st A elements are not accessible with get<TT, A>.
 */

#include <tuple>
#include <type_traits>


template <typename... Args>
using TypeTuple = std::tuple<Args...>;


template <typename TT>
concept IsTypeTuple = requires(TT& t) {
    {std::tuple_size_v<TT>};
    typename std::tuple_element<0, TT>::type;
    {std::get<0>(t)};
};


namespace {

template <bool b, size_t t, size_t f>
inline constexpr size_t conditional_v = t;

template <size_t t, size_t f>
inline constexpr size_t conditional_v<false, t, f> = f;


template <IsTypeTuple TT, typename T, size_t i>
struct GetTupleIndexDetail {
    static constexpr size_t nr_ = std::tuple_size_v<TT>;
    static constexpr size_t value = conditional_v<
        std::is_same_v<typename std::tuple_element<nr_ - i, TT>::type, T>, nr_ - i,
        GetTupleIndexDetail<TT, T, i - 1>::value>;
};

template <IsTypeTuple TT, typename T>
struct GetTupleIndexDetail<TT, T, 0> {
    static constexpr size_t value = std::tuple_size_v<TT>;  // not found.
};

}  // anonymous namespace


/**
 * GetTupleIndex<TT, T>::value is the index of the element of T.
 * If the element of T not found,
 */
template <IsTypeTuple TT, typename T>
using GetTupleIndex = GetTupleIndexDetail<TT, T, std::tuple_size_v<TT>>;


template <typename T, IsTypeTuple TT>
inline T& get(TT& tt) {
    constexpr size_t i = GetTupleIndex<TT, T>::value;
    return std::get<i>(tt);
}

template <typename T, IsTypeTuple TT>
inline const T& get(const TT& tt) {
    constexpr size_t i = GetTupleIndex<TT, T>::value;
    return std::get<i>(tt);
}
