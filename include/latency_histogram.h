#pragma once

#include <array>
#include <cstdint>
#include <limits>

// Lightweight, mergeable histogram for latency samples (e.g., TSC cycles).
class LatencyHistogram {
public:
    static constexpr int SUB_BUCKETS = 16;
    static constexpr int MAX_EXP = 63;
    static constexpr int BUCKETS = (MAX_EXP + 1) * SUB_BUCKETS;

    LatencyHistogram() { counts_.fill(0); }

    inline void record(uint64_t value) noexcept {
        counts_[index_for(value)]++;
        total_++;
    }

    inline void add_bucket(int idx, uint64_t cnt) noexcept {
        if (cnt == 0 || idx < 0 || idx >= BUCKETS) {
            return;
        }
        counts_[idx] += cnt;
        total_ += cnt;
    }

    inline void merge(const LatencyHistogram& other) noexcept {
        total_ += other.total_;
        for (int i = 0; i < BUCKETS; ++i) {
            counts_[i] += other.counts_[i];
        }
    }

    inline uint64_t total() const noexcept { return total_; }
    inline const std::array<uint64_t, BUCKETS>& counts() const noexcept { return counts_; }

    uint64_t quantile_value(double q) const noexcept {
        if (total_ == 0) {
            return 0;
        }
        if (q <= 0.0) {
            q = 0.0;
        } else if (q >= 1.0) {
            q = 1.0;
        }

        uint64_t target = static_cast<uint64_t>(q * static_cast<double>(total_));
        if (target == 0) {
            target = 1;
        }

        uint64_t cum = 0;
        for (int i = 0; i < BUCKETS; ++i) {
            cum += counts_[i];
            if (cum >= target) {
                return bucket_lower_value(i);
            }
        }
        return bucket_lower_value(BUCKETS - 1);
    }

    static uint64_t bucket_lower_value(int idx) noexcept {
        if (idx <= 0) {
            return 0;
        }

        int exp = idx / SUB_BUCKETS;
        int sub = idx % SUB_BUCKETS;
        if (exp < 0) {
            exp = 0;
        } else if (exp > MAX_EXP) {
            exp = MAX_EXP;
        }

        uint64_t base = (exp == 0) ? 1ull : (1ull << exp);
        uint64_t next = (exp == MAX_EXP) ? std::numeric_limits<uint64_t>::max() : (base << 1);
        uint64_t width = (next > base) ? ((next - base) / static_cast<uint64_t>(SUB_BUCKETS)) : 1ull;
        if (width == 0) {
            width = 1;
        }

        return base + static_cast<uint64_t>(sub) * width;
    }

private:
    static inline int index_for(uint64_t value) noexcept {
        if (value == 0) {
            return 0;
        }

        int exp = 63 - __builtin_clzll(value);
        if (exp < 0) {
            exp = 0;
        } else if (exp > MAX_EXP) {
            exp = MAX_EXP;
        }

        uint64_t base = (exp == 0) ? 1ull : (1ull << exp);
        uint64_t next = (exp == MAX_EXP) ? std::numeric_limits<uint64_t>::max() : (base << 1);
        uint64_t width = (next > base) ? ((next - base) / static_cast<uint64_t>(SUB_BUCKETS)) : 1ull;
        if (width == 0) {
            width = 1;
        }

        uint64_t off = (value <= base) ? 0ull : (value - base);
        int sub = static_cast<int>(off / width);
        if (sub >= SUB_BUCKETS) {
            sub = SUB_BUCKETS - 1;
        }

        return exp * SUB_BUCKETS + sub;
    }

private:
    std::array<uint64_t, BUCKETS> counts_;
    uint64_t total_{0};
};
