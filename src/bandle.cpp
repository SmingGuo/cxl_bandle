#include "bandle.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <limits>
#include <pthread.h>
#include <sched.h>
#include <time.h>
#include <x86intrin.h>

using namespace std::chrono_literals;

namespace {
static inline uint64_t now_ns() {
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull + static_cast<uint64_t>(ts.tv_nsec);
}

static inline uint64_t rdtsc_ordered() noexcept {
    unsigned aux = 0;
    return __rdtscp(&aux);
}

static inline void cpu_relax() noexcept {
    _mm_pause();
}

static inline void pin_current_thread_to_cpu(int cpu) {
    if (cpu < 0) {
        return;
    }
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

static inline void spin_backoff(uint32_t& spins) {
    if (spins < 2000) {
        cpu_relax();
        ++spins;
    } else {
        std::this_thread::yield();
        spins = 0;
    }
}

static inline int histogram_index_for(uint64_t value) noexcept {
    if (value == 0) {
        return 0;
    }
    int exp = 63 - __builtin_clzll(value);
    if (exp < 0) {
        exp = 0;
    } else if (exp > LatencyHistogram::MAX_EXP) {
        exp = LatencyHistogram::MAX_EXP;
    }
    uint64_t base = (exp == 0) ? 1ull : (1ull << exp);
    uint64_t next = (exp == LatencyHistogram::MAX_EXP) ? std::numeric_limits<uint64_t>::max() : (base << 1);
    uint64_t width = (next > base) ? ((next - base) / static_cast<uint64_t>(LatencyHistogram::SUB_BUCKETS)) : 1ull;
    if (width == 0) {
        width = 1;
    }
    uint64_t off = (value <= base) ? 0ull : (value - base);
    int sub = static_cast<int>(off / width);
    if (sub >= LatencyHistogram::SUB_BUCKETS) {
        sub = LatencyHistogram::SUB_BUCKETS - 1;
    }
    return exp * LatencyHistogram::SUB_BUCKETS + sub;
}

static inline bool is_write_op(char op) {
    return op == 'P' || op == 'U' || op == 'D';
}

static inline size_t recv_batch_capacity(uint32_t submit_batch_size) {
    constexpr size_t kMinRecvBatch = 128;
    constexpr size_t kMaxRecvBatch = 512;
    return std::min(kMaxRecvBatch, std::max(kMinRecvBatch, static_cast<size_t>(submit_batch_size)));
}

static inline uint64_t max_inflight_seq_window(uint32_t submit_batch_size, uint64_t cluster_size) {
    constexpr uint64_t kTargetWindow = 128;
    const uint64_t min_for_batches =
        cluster_size * std::max<uint64_t>(static_cast<uint64_t>(submit_batch_size), 1) * 2;
    return std::max(kTargetWindow, min_for_batches);
}

static inline size_t admission_batch_capacity(size_t submit_count) {
    constexpr size_t kAdmissionBatch = 16;
    return std::max<size_t>(1, std::min(kAdmissionBatch, submit_count));
}

}

void Bandle::AtomicHistogram::record(uint64_t value) {
    const int idx = histogram_index_for(value);
    counts[idx].fetch_add(1, std::memory_order_relaxed);
    total.fetch_add(1, std::memory_order_relaxed);
}

Bandle::Bandle(CXLMemoryPool* pool,
               BandleSharedState* shared,
               uint64_t node_id,
               uint64_t cluster_size,
               int poll_idle_us,
               uint64_t noop_interval_us,
               uint32_t batch_size,
               size_t pipeline_workers)
    : pool_(pool),
      shared_(shared),
      non_hwcc_(reinterpret_cast<BandleNonHwccState*>(pool->get_non_hwcc())),
      node_id_(node_id),
      cluster_size_(cluster_size),
      quorum_((cluster_size / 2) + 1),
      poll_idle_us_(poll_idle_us),
      noop_interval_us_(noop_interval_us),
      batch_size_(batch_size == 0 ? 1 : batch_size),
      pipeline_workers_(pipeline_workers == 0 ? 1 : pipeline_workers),
      next_seq_(node_id) {
    if (cluster_size_ != 3 || node_id_ < 1 || node_id_ > cluster_size_) {
        std::cerr << "This initial Bandle-CXL implementation supports exactly 3 nodes" << std::endl;
        std::abort();
    }
    log_.resize(kCompletionSlots);
    pending_.resize(kCompletionSlots);
}

Bandle::~Bandle() {
    stop();
}

SpscRingView<kBandleRingCapacity> Bandle::ring(uint64_t from, uint64_t to) {
    return SpscRingView<kBandleRingCapacity>(
        pool_,
        &shared_->rings[from][to],
        &shared_->descs[from][to][0],
        &non_hwcc_->payloads[from][to][0]);
}

void Bandle::start(int recv_cpu, int proposer_cpu) {
    if (running_.exchange(true)) {
        return;
    }
    last_noop_ns_ = now_ns();
    recv_threads_.clear();
    if (pipeline_workers_ <= 1) {
        recv_threads_.emplace_back(&Bandle::recv_loop, this, recv_cpu);
    } else {
        int idx = 0;
        for (uint64_t src = 1; src <= cluster_size_; ++src) {
            if (src == node_id_) {
                continue;
            }
            int cpu = recv_cpu;
            if (cpu >= 0) {
                cpu += idx;
            }
            recv_threads_.emplace_back(&Bandle::recv_source_loop, this, src, cpu);
            ++idx;
        }
    }
    proposer_thread_ = std::thread(&Bandle::proposer_loop, this, proposer_cpu);
}

void Bandle::stop() {
    if (!running_.exchange(false)) {
        return;
    }
    pending_cv_.notify_all();
    if (proposer_thread_.joinable()) {
        proposer_thread_.join();
    }
    for (auto& t : recv_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    recv_threads_.clear();
}

void Bandle::submit_borrowed(const BorrowedRequest& req) {
    submit_borrowed_batch(&req, 1);
}

void Bandle::submit_borrowed_batch(const BorrowedRequest* reqs, size_t count) {
    if (reqs == nullptr || count == 0) {
        return;
    }

    std::vector<BandleMessage> proposals;
    proposals.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        const BorrowedRequest& in = reqs[i];
        const bool valid = (in.op == 'G') || is_write_op(in.op);
        if (!valid) {
            error_ops_.fetch_add(1, std::memory_order_relaxed);
            total_ops_.fetch_add(1, std::memory_order_relaxed);
            continue;
        }

        BandleMessage& proposal = proposals.emplace_back();
        proposal.type = BandleMsgType::PROPOSAL;
        proposal.body.round = 1;
        proposal.body.sender = node_id_;
        proposal.body.proposer = node_id_;
        proposal.body.start_tsc = 0;
        proposal.body.op = in.op;
        if (in.op == 'G') {
            proposal.body.key_len = 0;
            proposal.body.value_len = 0;
        } else {
            proposal.body.key_len = static_cast<uint32_t>(std::min<uint32_t>(in.key_len, kMaxValueBytes));
            proposal.body.value_len = static_cast<uint32_t>(
                std::min<uint32_t>(in.value_len, static_cast<uint32_t>(kMaxValueBytes - proposal.body.key_len)));
            if (proposal.body.key_len != 0 && in.key_ptr != nullptr) {
                std::memcpy(proposal.body.data, in.key_ptr, proposal.body.key_len);
            }
            if (proposal.body.value_len != 0 && in.value_ptr != nullptr) {
                std::memcpy(proposal.body.data + proposal.body.key_len, in.value_ptr, proposal.body.value_len);
            }
        }
    }

    if (proposals.empty()) {
        return;
    }

    const size_t admit_cap = admission_batch_capacity(proposals.size());
    for (size_t base = 0; base < proposals.size(); base += admit_cap) {
        const size_t chunk = std::min(admit_cap, proposals.size() - base);
        BandleMessage* chunk_msgs = proposals.data() + base;

        {
            uint32_t spins = 0;
            const uint64_t window = max_inflight_seq_window(static_cast<uint32_t>(chunk), cluster_size_);
            while (true) {
                bool assigned = false;
                {
                    std::lock_guard<std::mutex> lk(mu_);
                    const uint64_t last_seq = next_seq_ + (static_cast<uint64_t>(chunk - 1) * cluster_size_);
                    if (last_seq < execute_next_ + window) {
                        for (size_t i = 0; i < chunk; ++i) {
                            BandleMessage& proposal = chunk_msgs[i];
                            proposal.body.seq = next_seq_;
                            proposal.body.start_tsc = rdtsc_ordered();
                            const uint64_t local_id = next_client_req_id_++;
                            proposal.body.client_req_id = (node_id_ << 56) | local_id;
                            highest_proposed_seq_ = std::max(highest_proposed_seq_, next_seq_);
                            next_seq_ += cluster_size_;

                            Pending p;
                            p.id = proposal.body.client_req_id;
                            p.op = proposal.body.op;
                            p.start_tsc = proposal.body.start_tsc;
                            pending_[local_id & (kCompletionSlots - 1)] = p;
                            ++pending_count_;
                        }
                        assigned = true;
                    }
                }
                if (assigned) {
                    break;
                }
                spin_backoff(spins);
            }
        }

        broadcast_batch(chunk_msgs, chunk);

        std::vector<BandleMessage> outbox;
        {
            std::lock_guard<std::mutex> lk(mu_);
            for (size_t i = 0; i < chunk; ++i) {
                handle_proposal_locked(chunk_msgs[i], outbox, false);
            }
            advance_execute_locked();
        }
        drain_outbox(outbox);
    }
}

void Bandle::mark_input_done() {
    input_done_.store(true, std::memory_order_release);
}

void Bandle::wait_for_all_pending() {
    std::unique_lock<std::mutex> lk(mu_);
    pending_cv_.wait(lk, [&]() { return pending_count_ == 0 || !running_.load(std::memory_order_relaxed); });
}

Bandle::ReplayStats Bandle::stats() const {
    ReplayStats s;
    s.total_ops = total_ops_.load(std::memory_order_relaxed);
    s.read_ops = read_ops_.load(std::memory_order_relaxed);
    s.write_ops = write_ops_.load(std::memory_order_relaxed);
    s.ok_ops = ok_ops_.load(std::memory_order_relaxed);
    s.error_ops = error_ops_.load(std::memory_order_relaxed);
    s.read_latency = read_latency_.snapshot();
    s.write_latency = write_latency_.snapshot();
    return s;
}

void Bandle::direct_apply(const Request& req) {
    if (req.op == 'P') {
        kv_.put(req.key, req.value);
    } else if (req.op == 'U') {
        kv_.update(req.key, req.value);
    } else if (req.op == 'D') {
        kv_.erase(req.key);
    }
}

void Bandle::reserve_kv_entries(size_t total_keys) {
    kv_.reserve_keys(total_keys);
}

uint16_t Bandle::shard_for(std::string_view key) {
    return static_cast<uint16_t>(TransparentStringHash{}(key) % KVStore::kNumShards);
}

void Bandle::send_to_peer(uint64_t peer, const BandleMessage& msg) {
    if (peer == node_id_) {
        return;
    }
    uint32_t spins = 0;
    auto r = ring(node_id_, peer);
    while (running_.load(std::memory_order_relaxed) && !r.push(msg)) {
        spin_backoff(spins);
    }
}

void Bandle::broadcast(const BandleMessage& msg) {
    std::lock_guard<std::mutex> lk(send_mu_);
    for (uint64_t peer = 1; peer <= cluster_size_; ++peer) {
        if (peer != node_id_) {
            send_to_peer(peer, msg);
        }
    }
}

void Bandle::broadcast_batch(const BandleMessage* msgs, size_t count) {
    if (msgs == nullptr || count == 0) {
        return;
    }
    std::lock_guard<std::mutex> lk(send_mu_);
    for (uint64_t peer = 1; peer <= cluster_size_; ++peer) {
        if (peer == node_id_) {
            continue;
        }
        uint32_t spins = 0;
        auto r = ring(node_id_, peer);
        while (running_.load(std::memory_order_relaxed) && !r.push_batch(msgs, count)) {
            spin_backoff(spins);
        }
    }
}

void Bandle::drain_outbox(const std::vector<BandleMessage>& outbox) {
    for (const auto& msg : outbox) {
        broadcast(msg);
    }
}

bool Bandle::drain_source_ring(uint64_t src, std::vector<BandleMessage>& batch, size_t& count_out) {
    count_out = 0;
    auto r = ring(src, node_id_);
    const uint64_t t = r.tail();
    const uint64_t h = r.head();
    const uint64_t avail = h - t;
    if (avail == 0) {
        return false;
    }

    const size_t max_batch = recv_batch_capacity(batch_size_);
    const size_t count = static_cast<size_t>(std::min<uint64_t>(avail, max_batch));
    if (batch.size() < count) {
        batch.resize(count);
    }

    size_t read_count = r.read_batch_at_abs(t, batch.data(), count);
    uint32_t spins = 0;
    while (read_count == 0 && running_.load(std::memory_order_relaxed)) {
        spin_backoff(spins);
        read_count = r.read_batch_at_abs(t, batch.data(), count);
    }
    while (read_count < count && running_.load(std::memory_order_relaxed)) {
        const size_t n = r.read_batch_at_abs(t + read_count, batch.data() + read_count, count - read_count);
        if (n != 0) {
            read_count += n;
            spins = 0;
            continue;
        }
        spin_backoff(spins);
    }

    if (read_count == 0) {
        return false;
    }
    r.commit_tail(t + read_count);
    count_out = read_count;
    return true;
}

void Bandle::handle_messages_batch(const BandleMessage* msgs, size_t count) {
    if (msgs == nullptr || count == 0) {
        return;
    }

    std::vector<BandleMessage> outbox;
    constexpr size_t kHandleChunk = 64;
    for (size_t base = 0; base < count; base += kHandleChunk) {
        const size_t end = std::min(count, base + kHandleChunk);
        {
            std::lock_guard<std::mutex> lk(mu_);
            bool need_advance = false;
            for (size_t i = base; i < end; ++i) {
                const BandleMessage& msg = msgs[i];
                if (msg.type == BandleMsgType::PROPOSAL) {
                    handle_proposal_locked(msg, outbox, false);
                    need_advance = true;
                } else if (msg.type == BandleMsgType::P1) {
                    handle_p1_locked(msg, outbox);
                } else if (msg.type == BandleMsgType::DECIDE) {
                    handle_decide_locked(msg);
                }
            }
            if (need_advance) {
                advance_execute_locked();
            }
        }
        if (!outbox.empty()) {
            drain_outbox(outbox);
            outbox.clear();
        }
    }
}

void Bandle::recv_loop(int cpu) {
    pin_current_thread_to_cpu(cpu);
    std::vector<BandleMessage> batch;
    batch.resize(recv_batch_capacity(batch_size_));
    while (running_.load(std::memory_order_relaxed)) {
        bool progressed = false;
        for (uint64_t src = 1; src <= cluster_size_; ++src) {
            if (src == node_id_) {
                continue;
            }
            size_t count = 0;
            while (drain_source_ring(src, batch, count)) {
                progressed = true;
                handle_messages_batch(batch.data(), count);
            }
        }
        if (!progressed) {
            if (poll_idle_us_ > 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(poll_idle_us_));
            } else {
                cpu_relax();
            }
        }
    }
}

void Bandle::recv_source_loop(uint64_t src, int cpu) {
    pin_current_thread_to_cpu(cpu);
    std::vector<BandleMessage> batch;
    batch.resize(recv_batch_capacity(batch_size_));
    while (running_.load(std::memory_order_relaxed)) {
        size_t count = 0;
        if (drain_source_ring(src, batch, count)) {
            handle_messages_batch(batch.data(), count);
            continue;
        }

        if (poll_idle_us_ > 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(poll_idle_us_));
        } else {
            cpu_relax();
        }
    }
}

bool Bandle::all_nodes_finished() const {
    for (uint64_t i = 1; i <= cluster_size_; ++i) {
        if (shared_->node_finished[i].load(std::memory_order_acquire) == 0) {
            return false;
        }
    }
    return true;
}

bool Bandle::should_propose_noop_locked(uint64_t now) const {
    if (!input_done_.load(std::memory_order_acquire) || all_nodes_finished()) {
        return false;
    }
    if (now - last_noop_ns_ < noop_interval_us_ * 1000ull) {
        return false;
    }
    return highest_proposed_seq_ < execute_next_ + (cluster_size_ * 512);
}

void Bandle::proposer_loop(int cpu) {
    pin_current_thread_to_cpu(cpu);
    while (running_.load(std::memory_order_relaxed)) {
        bool do_noop = false;
        {
            std::lock_guard<std::mutex> lk(mu_);
            do_noop = should_propose_noop_locked(now_ns());
        }
        if (do_noop) {
            propose_noop();
            continue;
        }

        cpu_relax();
    }
}

void Bandle::propose_noop() {
    BandleMessage proposal{};
    proposal.type = BandleMsgType::PROPOSAL;
    proposal.body.seq = next_seq_;
    proposal.body.round = 1;
    proposal.body.sender = node_id_;
    proposal.body.proposer = node_id_;
    proposal.body.op = 'N';

    std::vector<BandleMessage> outbox;
    {
        std::lock_guard<std::mutex> lk(mu_);
        highest_proposed_seq_ = std::max(highest_proposed_seq_, next_seq_);
        next_seq_ += cluster_size_;
        last_noop_ns_ = now_ns();
    }
    broadcast(proposal);
    {
        std::lock_guard<std::mutex> lk(mu_);
        handle_proposal_locked(proposal, outbox);
    }
    drain_outbox(outbox);
}

void Bandle::handle_message(const BandleMessage& msg) {
    std::vector<BandleMessage> outbox;
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (msg.type == BandleMsgType::PROPOSAL) {
            handle_proposal_locked(msg, outbox);
        } else if (msg.type == BandleMsgType::P1) {
            handle_p1_locked(msg, outbox);
        } else if (msg.type == BandleMsgType::DECIDE) {
            handle_decide_locked(msg);
        }
    }
    drain_outbox(outbox);
}

void Bandle::handle_proposal_locked(const BandleMessage& msg, std::vector<BandleMessage>& outbox, bool advance) {
    (void)outbox;
    const auto& b = msg.body;
    LogEntry& e = log_[b.seq & (kCompletionSlots - 1)];
    if (e.seq != b.seq) {
        e.proposal_known = false;
        e.p1_sent = false;
        e.decided = false;
        e.decision = 0;
        e.p1_value1_promise_mask = 0;
        e.op = 'N';
        e.key.clear();
        e.value.clear();
        e.proposer = 0;
        e.client_req_id = 0;
        e.start_tsc = 0;
        e.seq = b.seq;
    }
    if (!e.proposal_known) {
        e.proposal_known = true;
        e.op = b.op;
        e.proposer = b.proposer;
        e.client_req_id = b.client_req_id;
        e.start_tsc = b.start_tsc;
        if (b.op == 'G' || b.op == 'N') {
            e.key.clear();
            e.value.clear();
        } else {
            e.key.assign(b.data, b.key_len);
            if (b.value_len != 0) {
                e.value.assign(b.data + b.key_len, b.value_len);
            } else {
                e.value.clear();
            }
        }
    }
    // Basic 3-node/no-crash fast path: once a proposal is durably delivered
    // through CXL to a process, the proposal is accepted for this sequence.
    // The proposer also self-delivers after enqueueing to peers. This avoids
    // the FlashBA P1 control-message round, which is unnecessary for the
    // requested non-crash read/write path.
    e.decided = true;
    e.decision = 1;
    if (advance) {
        advance_execute_locked();
    }
}

void Bandle::input_one_locked(uint64_t seq, std::vector<BandleMessage>& outbox) {
    LogEntry& e = log_[seq & (kCompletionSlots - 1)];
    if (e.seq != seq) {
        e.proposal_known = false;
        e.p1_sent = false;
        e.decided = false;
        e.decision = 0;
        e.p1_value1_promise_mask = 0;
        e.op = 'N';
        e.key.clear();
        e.value.clear();
        e.proposer = 0;
        e.client_req_id = 0;
        e.start_tsc = 0;
        e.seq = seq;
    }
    if (e.p1_sent) {
        return;
    }
    e.p1_sent = true;

    BandleMessage p1{};
    p1.type = BandleMsgType::P1;
    p1.body.seq = seq;
    p1.body.round = 1;
    p1.body.sender = node_id_;
    p1.body.proposer = e.proposer;
    p1.body.value = 1;
    p1.body.promise = 1;
    e.p1_value1_promise_mask |= static_cast<uint8_t>(1u << (node_id_ - 1));
    outbox.push_back(p1);

    if (__builtin_popcount(static_cast<unsigned>(e.p1_value1_promise_mask)) >= static_cast<int>(quorum_)) {
        decide_locked(seq, 1, outbox);
    }
}

void Bandle::handle_p1_locked(const BandleMessage& msg, std::vector<BandleMessage>& outbox) {
    const auto& b = msg.body;
    if (b.value != 1 || b.promise == 0 || b.sender < 1 || b.sender > cluster_size_) {
        return;
    }
    LogEntry& e = log_[b.seq & (kCompletionSlots - 1)];
    if (e.seq != b.seq) {
        e.proposal_known = false;
        e.p1_sent = false;
        e.decided = false;
        e.decision = 0;
        e.p1_value1_promise_mask = 0;
        e.op = 'N';
        e.key.clear();
        e.value.clear();
        e.proposer = 0;
        e.client_req_id = 0;
        e.start_tsc = 0;
        e.seq = b.seq;
    }
    e.p1_value1_promise_mask |= static_cast<uint8_t>(1u << (b.sender - 1));
    if (__builtin_popcount(static_cast<unsigned>(e.p1_value1_promise_mask)) >= static_cast<int>(quorum_)) {
        decide_locked(b.seq, 1, outbox);
    }
}

void Bandle::decide_locked(uint64_t seq, uint8_t value, std::vector<BandleMessage>& outbox) {
    (void)outbox;
    LogEntry& e = log_[seq & (kCompletionSlots - 1)];
    if (e.seq != seq) {
        e.proposal_known = false;
        e.p1_sent = false;
        e.decided = false;
        e.decision = 0;
        e.p1_value1_promise_mask = 0;
        e.op = 'N';
        e.key.clear();
        e.value.clear();
        e.proposer = 0;
        e.client_req_id = 0;
        e.start_tsc = 0;
        e.seq = seq;
    }
    if (e.decided) {
        return;
    }
    e.decided = true;
    e.decision = value;
    advance_execute_locked();
}

void Bandle::handle_decide_locked(const BandleMessage& msg) {
    const auto& b = msg.body;
    LogEntry& e = log_[b.seq & (kCompletionSlots - 1)];
    if (e.seq != b.seq) {
        e.proposal_known = false;
        e.p1_sent = false;
        e.decided = false;
        e.decision = 0;
        e.p1_value1_promise_mask = 0;
        e.op = 'N';
        e.key.clear();
        e.value.clear();
        e.proposer = 0;
        e.client_req_id = 0;
        e.start_tsc = 0;
        e.seq = b.seq;
    }
    if (!e.decided) {
        e.decided = true;
        e.decision = b.value;
    }
    advance_execute_locked();
}

void Bandle::advance_execute_locked() {
    while (true) {
        LogEntry& e = log_[execute_next_ & (kCompletionSlots - 1)];
        if (e.seq != execute_next_) {
            return;
        }
        if (!e.decided) {
            return;
        }
        if (e.decision == 1 && !e.proposal_known) {
            return;
        }

        if (e.decision == 1) {
            if (e.op == 'P') {
                kv_.put(e.key, e.value);
            } else if (e.op == 'U') {
                kv_.update(std::string_view(e.key), std::move(e.value));
            } else if (e.op == 'D') {
                kv_.erase(e.key);
            } else if (e.op == 'G') {
                // Reads are ordered through Bandle. The replay workload does
                // not consume returned values, so avoid an extra locked hash
                // lookup on every replica.
            }

            if (e.proposer == node_id_ && e.client_req_id != 0) {
                const uint64_t local_id = e.client_req_id & ((1ull << 56) - 1);
                Pending& p = pending_[local_id & (kCompletionSlots - 1)];
                if (p.id == e.client_req_id && !p.done) {
                    const uint64_t end = rdtsc_ordered();
                    const uint64_t start = p.start_tsc;
                    if (p.op == 'G') {
                        read_ops_.fetch_add(1, std::memory_order_relaxed);
                        read_latency_.record(end >= start ? end - start : 0);
                    } else {
                        write_ops_.fetch_add(1, std::memory_order_relaxed);
                        write_latency_.record(end >= start ? end - start : 0);
                    }
                    p.done = true;
                    ok_ops_.fetch_add(1, std::memory_order_relaxed);
                    total_ops_.fetch_add(1, std::memory_order_relaxed);
                    if (pending_count_ > 0) {
                        --pending_count_;
                    }
                    pending_cv_.notify_all();
                }
            }
        }

        e.proposal_known = false;
        e.p1_sent = false;
        e.decided = false;
        e.decision = 0;
        e.p1_value1_promise_mask = 0;
        e.op = 'N';
        e.key.clear();
        e.value.clear();
        e.proposer = 0;
        e.client_req_id = 0;
        e.start_tsc = 0;
        e.seq = 0;
        ++execute_next_;
    }
}
