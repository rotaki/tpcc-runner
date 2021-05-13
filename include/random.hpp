#pragma once

#include <cstdint>
#include <cstring>
#include <random>

/*
This algorithm is developed in 2015 by Sebastiano Vigna (vigna@acm.org)
https://prng.di.unimi.it/splitmix64.c

Modified by Riki Otaki
*/

class SplitMix64 {
    uint64_t x_;

public:
    explicit SplitMix64(uint64_t seed)
        : x_(seed) {}
    uint64_t operator()() {
        uint64_t z = (x_ += UINT64_C(0x9e3779b97f4a7c15));
        z = (z ^ (z >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)) * UINT64_C(0x94d049bb133111eb);
        return z ^ (z >> 31);
    }
};

/*
This algorithm is developed in 2019 by David Blackman and Sebastiano Vigna (vigna@acm.org)
https://prng.di.unimi.it/xoshiro128plusplus.c

Modified by Riki Otaki
*/

class Xoshiro256PlusPlus {
public:
    struct State {
        uint64_t s[4];
        const uint64_t& operator[](std::size_t i) const { return s[i]; }
        uint64_t& operator[](std::size_t i) { return s[i]; }
        void operator+=(uint64_t v) { s[0] += v; }
    };

private:
    State s_;

public:
    explicit Xoshiro256PlusPlus(uint64_t seed, std::size_t num_jumps = 0) {
        SplitMix64 initializer(seed);
        s_[0] = seed;
        s_[1] = initializer();
        s_[2] = initializer();
        s_[3] = initializer();
        jump(num_jumps);
    }

    uint64_t operator()() {
        const uint64_t result = rotl(s_[0] + s_[3], 23) + s_[0];
        const uint64_t t = s_[1] << 17;
        s_[2] ^= s_[0];
        s_[3] ^= s_[1];
        s_[1] ^= s_[2];
        s_[0] ^= s_[3];
        s_[2] ^= t;
        s_[3] = rotl(s_[3], 45);
        return result;
    }

    void jump(std::size_t num_jumps) {
        for (std::size_t i = 0; i < num_jumps; i++) {
            jump_once();
        }
    }

private:
    uint64_t rotl(const uint64_t x, int k) { return (x << k) | (x >> (64 - k)); }

    /* This is the jump function for the generator. It is equivalent
     to 2^128 calls to next(); it can be used to generate 2^128
     non-overlapping subsequences for parallel computations. */
    void jump_once() {
        static const uint64_t JUMP[] = {
            0x180ec6d33cfd0aba, 0xd5a61266f0c9392c, 0xa9582618e03fc9aa, 0x39abdc4529b1661c};
        uint64_t s[4] = {0, 0, 0, 0};
        for (std::size_t i = 0; i < sizeof JUMP / sizeof *JUMP; i++)
            for (std::size_t b = 0; b < 64; b++) {
                if (JUMP[i] & UINT64_C(1) << b) {
                    s[0] ^= s_[0];
                    s[1] ^= s_[1];
                    s[2] ^= s_[2];
                    s[3] ^= s_[3];
                }
                operator()();
            }
        ::memcpy(&s_[0], &s[0], sizeof(s_));
    }

    /* This is the long-jump function for the generator. It is equivalent to
     2^192 calls to next(); it can be used to generate 2^64 starting points,
     from each of which jump() will generate 2^64 non-overlapping
     subsequences for parallel distributed computations. */
    void long_jump(void) {
        static const uint64_t LONG_JUMP[] = {
            0x76e15d3efefdcbbf, 0xc5004e441c522fb3, 0x77710069854ee241, 0x39109bb02acbe635};

        uint64_t s[4] = {0, 0, 0, 0};
        for (std::size_t i = 0; i < sizeof LONG_JUMP / sizeof *LONG_JUMP; i++)
            for (std::size_t b = 0; b < 64; b++) {
                if (LONG_JUMP[i] & UINT64_C(1) << b) {
                    s[0] ^= s_[0];
                    s[1] ^= s_[1];
                    s[2] ^= s_[2];
                    s[3] ^= s_[3];
                }
                operator()();
            }

        ::memcpy(&s_[0], &s[0], sizeof(s_));
    }
};
