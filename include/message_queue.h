#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <immintrin.h>
#include <type_traits>

#include "cxl_memory.h"

constexpr size_t kMaxBandleNodes = 3;
constexpr size_t kBandleRingCapacity = 32768;
constexpr size_t kMaxValueBytes = 1088;
constexpr size_t kCompletionSlots = 1u << 20;

enum class BandleMsgType : uint8_t {
    PROPOSAL = 1,
    P1 = 2,
    P2 = 3,
    DECIDE = 4,
};

struct BandlePayload {
    uint64_t seq = 0;
    uint64_t round = 1;
    uint64_t sender = 0;
    uint64_t proposer = 0;
    uint64_t client_req_id = 0;
    uint64_t start_tsc = 0;
    uint8_t value = 1;
    uint8_t promise = 0;
    char op = 'N';
    uint8_t reserved0 = 0;
    uint32_t key_len = 0;
    uint32_t value_len = 0;
    char data[kMaxValueBytes];
};

struct BandleDescriptor {
    uint64_t slot_seq = 0;
    BandleMsgType type = BandleMsgType::PROPOSAL;
    uint8_t reserved[7]{};
    uint64_t seq = 0;
    uint64_t round = 1;
    uint64_t sender = 0;
    uint64_t proposer = 0;
    uint64_t client_req_id = 0;
    uint64_t start_tsc = 0;
    uint8_t value = 1;
    uint8_t promise = 0;
    char op = 'N';
    uint8_t reserved0 = 0;
    uint32_t key_len = 0;
    uint32_t value_len = 0;
};

struct BandleMessage {
    uint64_t slot_seq = 0;
    BandleMsgType type = BandleMsgType::PROPOSAL;
    BandlePayload body;
};

static_assert(std::is_trivially_copyable<BandleDescriptor>::value, "BandleDescriptor must be trivially copyable");
static_assert(std::is_trivially_copyable<BandleMessage>::value, "BandleMessage must be trivially copyable");

struct alignas(64) SpscRingMeta {
    std::atomic<uint64_t> head{0};
    std::atomic<uint64_t> tail{0};
};

struct alignas(64) CompletionSlot {
    std::atomic<uint64_t> client_req_id{0};
    std::atomic<uint64_t> tsc{0};
};

struct alignas(64) BandleSharedState {
    std::atomic<uint64_t> run_id{0};
    std::atomic<uint64_t> node_finished[kMaxBandleNodes + 1];
    CompletionSlot completion[kCompletionSlots];
    SpscRingMeta rings[kMaxBandleNodes + 1][kMaxBandleNodes + 1];
    BandleDescriptor descs[kMaxBandleNodes + 1][kMaxBandleNodes + 1][kBandleRingCapacity];
};

struct alignas(64) BandleNonHwccState {
    char payloads[kMaxBandleNodes + 1][kMaxBandleNodes + 1][kBandleRingCapacity][kMaxValueBytes];
};

template <size_t Capacity>
class SpscRingView {
public:
    static constexpr size_t MASK = Capacity - 1;
    static_assert((Capacity & MASK) == 0, "Capacity must be power-of-two");

    SpscRingView(CXLMemoryPool* pool,
                 SpscRingMeta* meta,
                 BandleDescriptor* descs,
                 char (*payloads)[kMaxValueBytes])
        : pool_(pool), meta_(meta), descs_(descs), payloads_(payloads) {}

    bool push(const BandleMessage& msg) {
        uint64_t h = meta_->head.load(std::memory_order_relaxed);
        uint64_t t = meta_->tail.load(std::memory_order_acquire);
        if ((h - t) >= Capacity) {
            return false;
        }

        const uint64_t idx = h & MASK;
        BandleDescriptor d{};
        d.slot_seq = h + 1;
        d.type = msg.type;
        d.seq = msg.body.seq;
        d.round = msg.body.round;
        d.sender = msg.body.sender;
        d.proposer = msg.body.proposer;
        d.client_req_id = msg.body.client_req_id;
        d.start_tsc = msg.body.start_tsc;
        d.value = msg.body.value;
        d.promise = msg.body.promise;
        d.op = msg.body.op;
        d.key_len = msg.body.key_len;
        d.value_len = msg.body.value_len;

        const uint64_t bytes = static_cast<uint64_t>(d.key_len) + static_cast<uint64_t>(d.value_len);
        if (bytes > kMaxValueBytes) {
            return false;
        }
        if (bytes != 0) {
            pool_->nt_memcpy(payloads_[idx], msg.body.data, static_cast<size_t>(bytes));
            pool_->sfence();
        }
        descs_[idx] = d;
        meta_->head.store(h + 1, std::memory_order_release);
        return true;
    }

    bool push_batch(const BandleMessage* msgs, size_t count) {
        if (count == 0) {
            return true;
        }
        uint64_t h = meta_->head.load(std::memory_order_relaxed);
        uint64_t t = meta_->tail.load(std::memory_order_acquire);
        if ((h - t) + count > Capacity) {
            return false;
        }

        bool wrote_payload = false;
        for (size_t i = 0; i < count; ++i) {
            const uint64_t idx = (h + i) & MASK;
            const BandleMessage& msg = msgs[i];
            BandleDescriptor d{};
            d.slot_seq = h + i + 1;
            d.type = msg.type;
            d.seq = msg.body.seq;
            d.round = msg.body.round;
            d.sender = msg.body.sender;
            d.proposer = msg.body.proposer;
            d.client_req_id = msg.body.client_req_id;
            d.start_tsc = msg.body.start_tsc;
            d.value = msg.body.value;
            d.promise = msg.body.promise;
            d.op = msg.body.op;
            d.key_len = msg.body.key_len;
            d.value_len = msg.body.value_len;

            const uint64_t bytes = static_cast<uint64_t>(d.key_len) + static_cast<uint64_t>(d.value_len);
            if (bytes > kMaxValueBytes) {
                return false;
            }
            if (bytes != 0) {
                pool_->nt_memcpy(payloads_[idx], msg.body.data, static_cast<size_t>(bytes));
                wrote_payload = true;
            }
            descs_[idx] = d;
        }
        if (wrote_payload) {
            pool_->sfence();
        }
        meta_->head.store(h + count, std::memory_order_release);
        return true;
    }

    bool pop(BandleMessage& out) {
        uint64_t t = meta_->tail.load(std::memory_order_relaxed);
        uint64_t h = meta_->head.load(std::memory_order_acquire);
        if (t == h) {
            return false;
        }
        const uint64_t idx = t & MASK;
        if (!read_slot(idx, t + 1, out)) {
            return false;
        }
        meta_->tail.store(t + 1, std::memory_order_release);
        return true;
    }

    uint64_t head() const {
        return meta_->head.load(std::memory_order_acquire);
    }

    uint64_t tail() const {
        return meta_->tail.load(std::memory_order_relaxed);
    }

    void commit_tail(uint64_t tail) {
        meta_->tail.store(tail, std::memory_order_release);
    }

    bool read_at_abs(uint64_t abs, BandleMessage& out) const {
        return read_slot(abs & MASK, abs + 1, out);
    }

    bool is_full() const {
        const uint64_t h = meta_->head.load(std::memory_order_relaxed);
        const uint64_t t = meta_->tail.load(std::memory_order_acquire);
        return (h - t) >= Capacity;
    }

private:
    bool read_slot(uint64_t idx, uint64_t expected, BandleMessage& out) const {
        constexpr int kRetries = 64;
        for (int i = 0; i < kRetries; ++i) {
            BandleDescriptor d = descs_[idx];
            if (d.slot_seq != expected) {
                _mm_pause();
                continue;
            }
            const uint64_t bytes = static_cast<uint64_t>(d.key_len) + static_cast<uint64_t>(d.value_len);
            if (bytes > kMaxValueBytes) {
                return false;
            }
            out.slot_seq = d.slot_seq;
            out.type = d.type;
            out.body.seq = d.seq;
            out.body.round = d.round;
            out.body.sender = d.sender;
            out.body.proposer = d.proposer;
            out.body.client_req_id = d.client_req_id;
            out.body.start_tsc = d.start_tsc;
            out.body.value = d.value;
            out.body.promise = d.promise;
            out.body.op = d.op;
            out.body.key_len = d.key_len;
            out.body.value_len = d.value_len;
            if (bytes != 0) {
                pool_->clflush(payloads_[idx], static_cast<size_t>(bytes));
                std::atomic_thread_fence(std::memory_order_seq_cst);
                std::memcpy(out.body.data, payloads_[idx], static_cast<size_t>(bytes));
            }
            return true;
        }
        return false;
    }

    CXLMemoryPool* pool_;
    SpscRingMeta* meta_;
    BandleDescriptor* descs_;
    char (*payloads_)[kMaxValueBytes];
};
