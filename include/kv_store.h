#pragma once
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <shared_mutex>
#include <vector>
#include <mutex>

struct TransparentStringHash {
    using is_transparent = void;

    size_t operator()(std::string_view sv) const noexcept {
        return std::hash<std::string_view>{}(sv);
    }
    size_t operator()(const std::string& s) const noexcept {
        return std::hash<std::string_view>{}(std::string_view(s));
    }
};

struct TransparentStringEq {
    using is_transparent = void;

    bool operator()(std::string_view a, std::string_view b) const noexcept { return a == b; }
    bool operator()(const std::string& a, const std::string& b) const noexcept { return a == b; }
    bool operator()(const std::string& a, std::string_view b) const noexcept { return std::string_view(a) == b; }
    bool operator()(std::string_view a, const std::string& b) const noexcept { return a == std::string_view(b); }
};

class KVStore {
public:
    using MapType = std::unordered_map<std::string, std::string, TransparentStringHash, TransparentStringEq>;
    static constexpr size_t kNumShards = 256;
    using ApplyProgressCallback = void (*)(void* ctx, size_t applied_ops);

    KVStore();

    struct ApplyOp {
        char op = 'N';
        uint16_t shard = 0;
        std::string_view key;
        std::string_view value;
    };
    
    void put(const std::string& key, const std::string& value);
    void put(std::string&& key, std::string&& value);
    void put(std::string_view key, std::string&& value);
    std::string get(const std::string& key) const;
    std::string get(std::string_view key) const;
    bool update(const std::string& key, const std::string& value);
    bool update(std::string_view key, std::string&& value);
    bool erase(const std::string& key);
    bool erase(std::string_view key);

    void reserve_keys(size_t total_keys);

    // Apply a batch of operations in the given order (no reordering).
    // Optimized for consecutive operations hitting the same shard.
    void apply_ops_in_order(const ApplyOp* ops, size_t n);
    void apply_ops_in_order_progressive(const ApplyOp* ops,
                                        size_t n,
                                        size_t progress_stride,
                                        ApplyProgressCallback progress_cb,
                                        void* progress_ctx);
    
    MapType take_snapshot() const;
    void apply_snapshot(const MapType& snapshot);
    
    bool is_valid() const;
    
private:
    struct alignas(64) Shard {
        mutable std::mutex mu;
        MapType map;
    };

    std::vector<Shard> shards_;

    Shard& get_shard(std::string_view key) const;
};
