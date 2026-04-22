#include "kv_store.h"
#include <iostream>
#include <limits>
#include <utility>

KVStore::KVStore() : shards_(kNumShards) {
    for (auto& shard : shards_) {
        shard.map.reserve(4096);
    }
}

void KVStore::reserve_keys(size_t total_keys) {
    if (total_keys == 0) {
        return;
    }

    const size_t per_shard = (total_keys + kNumShards - 1) / kNumShards;
    const size_t target = std::max<size_t>(4096, per_shard + (per_shard / 8) + 64);
    for (auto& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard.mu);
        if (shard.map.bucket_count() < target) {
            shard.map.reserve(target);
        }
    }
}

// Helper to get shard index
inline size_t get_shard_idx(std::string_view key) {
    return TransparentStringHash{}(key) % KVStore::kNumShards;
}

void KVStore::put(const std::string& key, const std::string& value) {
    auto& shard = shards_[TransparentStringHash{}(key) % kNumShards];
    std::lock_guard<std::mutex> lock(shard.mu);
    shard.map.insert_or_assign(key, value);
}

void KVStore::put(std::string&& key, std::string&& value) {
    auto& shard = shards_[TransparentStringHash{}(key) % kNumShards];
    std::lock_guard<std::mutex> lock(shard.mu);
    shard.map.insert_or_assign(std::move(key), std::move(value));
}

void KVStore::put(std::string_view key, std::string&& value) {
    auto& shard = shards_[TransparentStringHash{}(key) % kNumShards];
    std::lock_guard<std::mutex> lock(shard.mu);
    auto it = shard.map.find(key);
    if (it != shard.map.end()) {
        it->second = std::move(value);
    } else {
        shard.map.emplace(std::string(key), std::move(value));
    }
}

std::string KVStore::get(const std::string& key) const {
    const auto& shard = shards_[TransparentStringHash{}(key) % kNumShards];
    std::lock_guard<std::mutex> lock(shard.mu);
    auto it = shard.map.find(key);
    if (it == shard.map.cend()) {
        return "";
    }
    return it->second;
}

std::string KVStore::get(std::string_view key) const {
    const auto& shard = shards_[TransparentStringHash{}(key) % kNumShards];
    std::lock_guard<std::mutex> lock(shard.mu);
    auto it = shard.map.find(key);
    if (it == shard.map.cend()) {
        return "";
    }
    return it->second;
}

bool KVStore::update(const std::string& key, const std::string& value) {
    auto& shard = shards_[TransparentStringHash{}(key) % kNumShards];
    std::lock_guard<std::mutex> lock(shard.mu);
    auto it = shard.map.find(key);
    if (it == shard.map.end()) {
        return false;
    }
    it->second = value;
    return true;
}

bool KVStore::update(std::string_view key, std::string&& value) {
    auto& shard = shards_[TransparentStringHash{}(key) % kNumShards];
    std::lock_guard<std::mutex> lock(shard.mu);
    auto it = shard.map.find(key);
    if (it == shard.map.end()) {
        return false;
    }
    it->second = std::move(value);
    return true;
}

bool KVStore::erase(const std::string& key) {
    auto& shard = shards_[TransparentStringHash{}(key) % kNumShards];
    std::lock_guard<std::mutex> lock(shard.mu);
    return shard.map.erase(key) > 0;
}

bool KVStore::erase(std::string_view key) {
    auto& shard = shards_[TransparentStringHash{}(key) % kNumShards];
    std::lock_guard<std::mutex> lock(shard.mu);
    auto it = shard.map.find(key);
    if (it != shard.map.end()) {
        shard.map.erase(it);
        return true;
    }
    return false;
}

KVStore::MapType KVStore::take_snapshot() const {
    KVStore::MapType result;
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard.mu);
        result.insert(shard.map.begin(), shard.map.end());
    }
    return result;
}

void KVStore::apply_snapshot(const KVStore::MapType& snapshot) {
    for (auto& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard.mu);
        shard.map.clear();
    }
    for (const auto& kv : snapshot) {
        auto& shard = shards_[TransparentStringHash{}(kv.first) % kNumShards];
        std::lock_guard<std::mutex> lock(shard.mu);
        shard.map.insert(kv);
    }
}

bool KVStore::is_valid() const {
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard.mu);
        if (!shard.map.empty()) return true;
    }
    return false;
}

KVStore::Shard& KVStore::get_shard(std::string_view key) const {
   return const_cast<Shard&>(shards_[TransparentStringHash{}(key) % kNumShards]);
}

void KVStore::apply_ops_in_order(const ApplyOp* ops, size_t n) {
    apply_ops_in_order_progressive(ops, n, 0, nullptr, nullptr);
}

void KVStore::apply_ops_in_order_progressive(const ApplyOp* ops,
                                             size_t n,
                                             size_t progress_stride,
                                             ApplyProgressCallback progress_cb,
                                             void* progress_ctx) {
    if (n == 0 || ops == nullptr) {
        return;
    }

    std::unique_lock<std::mutex> held_lock;
    size_t held_shard = std::numeric_limits<size_t>::max();
    size_t applied = 0;
    size_t applied_since_progress = 0;

    for (size_t i = 0; i < n; ++i) {
        const ApplyOp& op = ops[i];
        const size_t shard_idx = static_cast<size_t>(op.shard);

        if (shard_idx != held_shard) {
            held_lock = std::unique_lock<std::mutex>(shards_[shard_idx].mu);
            held_shard = shard_idx;
        }

        auto& map = shards_[shard_idx].map;
        if (op.op == 'P') {
            // Put: insert or overwrite.
            auto it = map.find(op.key);
            if (it != map.end()) {
                it->second.assign(op.value.data(), op.value.size());
            } else {
                map.emplace(std::string(op.key), std::string(op.value));
            }
        } else if (op.op == 'U') {
            // Update: only if exists.
            auto it = map.find(op.key);
            if (it != map.end()) {
                it->second.assign(op.value.data(), op.value.size());
            }
        } else if (op.op == 'D') {
            // Erase: avoid constructing a std::string key.
            auto it = map.find(op.key);
            if (it != map.end()) {
                map.erase(it);
            }
        }

        ++applied;
        if (progress_cb != nullptr && progress_stride != 0) {
            ++applied_since_progress;
            if (applied_since_progress >= progress_stride) {
                progress_cb(progress_ctx, applied);
                applied_since_progress = 0;
            }
        }
    }

    if (progress_cb != nullptr && progress_stride != 0 && applied_since_progress != 0) {
        progress_cb(progress_ctx, applied);
    }
}
