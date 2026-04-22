#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cxl_memory.h"
#include "kv_store.h"
#include "latency_histogram.h"
#include "message_queue.h"

class Bandle {
public:
    struct Request {
        char op = 'G';
        std::string key;
        std::string value;
    };

    struct BorrowedRequest {
        char op = 'G';
        const char* key_ptr = nullptr;
        uint32_t key_len = 0;
        const char* value_ptr = nullptr;
        uint32_t value_len = 0;
    };

    struct ReplayStats {
        uint64_t total_ops = 0;
        uint64_t read_ops = 0;
        uint64_t write_ops = 0;
        uint64_t ok_ops = 0;
        uint64_t error_ops = 0;
        uint64_t real_start_ns = 0;
        uint64_t work_done_ns = 0;
        uint64_t real_end_ns = 0;
        uint64_t tsc_hz = 0;
        LatencyHistogram read_latency;
        LatencyHistogram write_latency;
    };

    Bandle(CXLMemoryPool* pool,
           BandleSharedState* shared,
           uint64_t node_id,
           uint64_t cluster_size,
           int poll_idle_us,
           uint64_t noop_interval_us,
           uint32_t batch_size,
           size_t pipeline_workers);
    ~Bandle();

    void start(int recv_cpu = -1, int proposer_cpu = -1);
    void stop();

    void submit_borrowed(const BorrowedRequest& req);
    void submit_borrowed_batch(const BorrowedRequest* reqs, size_t count);
    void mark_input_done();
    void wait_for_all_pending();
    ReplayStats stats() const;
    void direct_apply(const Request& req);
    void reserve_kv_entries(size_t total_keys);

private:
    struct ClientRequest {
        uint64_t id = 0;
        uint64_t start_tsc = 0;
        char op = 'G';
        std::string key;
        std::string value;
    };

    struct Pending {
        uint64_t id = 0;
        char op = 'G';
        uint64_t start_tsc = 0;
        bool done = false;
    };

    struct LogEntry {
        uint64_t seq = 0;
        bool proposal_known = false;
        bool p1_sent = false;
        bool decided = false;
        uint8_t decision = 0;
        uint8_t p1_value1_promise_mask = 0;
        char op = 'N';
        std::string key;
        std::string value;
        uint64_t proposer = 0;
        uint64_t client_req_id = 0;
        uint64_t start_tsc = 0;
    };

    struct AtomicHistogram {
        std::array<std::atomic<uint64_t>, LatencyHistogram::BUCKETS> counts;
        std::atomic<uint64_t> total{0};

        AtomicHistogram() {
            for (auto& c : counts) {
                c.store(0, std::memory_order_relaxed);
            }
        }

        void record(uint64_t value);

        LatencyHistogram snapshot() const {
            LatencyHistogram h;
            for (int i = 0; i < LatencyHistogram::BUCKETS; ++i) {
                uint64_t c = counts[i].load(std::memory_order_relaxed);
                if (c != 0) {
                    h.add_bucket(i, c);
                }
            }
            return h;
        }
    };

    SpscRingView<kBandleRingCapacity> ring(uint64_t from, uint64_t to);
    void recv_loop(int cpu);
    void recv_source_loop(uint64_t src, int cpu);
    bool drain_source_ring(uint64_t src, std::vector<BandleMessage>& batch, size_t& count);
    void handle_messages_batch(const BandleMessage* msgs, size_t count);
    void proposer_loop(int cpu);
    void handle_message(const BandleMessage& msg);
    void handle_proposal_locked(const BandleMessage& msg, std::vector<BandleMessage>& outbox, bool advance = true);
    void handle_p1_locked(const BandleMessage& msg, std::vector<BandleMessage>& outbox);
    void handle_decide_locked(const BandleMessage& msg);
    void input_one_locked(uint64_t seq, std::vector<BandleMessage>& outbox);
    void decide_locked(uint64_t seq, uint8_t value, std::vector<BandleMessage>& outbox);
    void advance_execute_locked();
    void broadcast(const BandleMessage& msg);
    void broadcast_batch(const BandleMessage* msgs, size_t count);
    void send_to_peer(uint64_t peer, const BandleMessage& msg);
    void drain_outbox(const std::vector<BandleMessage>& outbox);
    bool all_nodes_finished() const;
    bool should_propose_noop_locked(uint64_t now_ns) const;
    void propose_noop();
    static uint16_t shard_for(std::string_view key);

    CXLMemoryPool* pool_;
    BandleSharedState* shared_;
    BandleNonHwccState* non_hwcc_;
    uint64_t node_id_;
    uint64_t cluster_size_;
    uint64_t quorum_;
    int poll_idle_us_;
    uint64_t noop_interval_us_;
    uint32_t batch_size_;
    size_t pipeline_workers_;

    KVStore kv_;

    std::atomic<bool> running_{false};
    std::vector<std::thread> recv_threads_;
    std::thread proposer_thread_;

    mutable std::mutex mu_;
    std::vector<LogEntry> log_;
    uint64_t execute_next_ = 1;
    uint64_t next_seq_;
    uint64_t highest_proposed_seq_ = 0;
    uint64_t last_noop_ns_ = 0;

    std::deque<ClientRequest> request_q_;
    std::vector<Pending> pending_;
    std::condition_variable pending_cv_;
    uint64_t next_client_req_id_ = 1;
    uint64_t pending_count_ = 0;
    std::atomic<bool> input_done_{false};

    std::mutex send_mu_;

    std::atomic<uint64_t> total_ops_{0};
    std::atomic<uint64_t> read_ops_{0};
    std::atomic<uint64_t> write_ops_{0};
    std::atomic<uint64_t> ok_ops_{0};
    std::atomic<uint64_t> error_ops_{0};
    AtomicHistogram read_latency_;
    AtomicHistogram write_latency_;
};
