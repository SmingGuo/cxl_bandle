#include "bandle.h"
#include "cxl_memory.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <time.h>
#include <vector>
#include <x86intrin.h>

using namespace std::chrono_literals;

struct ParsedLineView {
    uint64_t ts_us = 0;
    char op = 'G';
    const char* key_ptr = nullptr;
    uint32_t key_len = 0;
    const char* value_ptr = nullptr;
    uint32_t value_len = 0;
};

static inline uint64_t now_ns() {
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull + static_cast<uint64_t>(ts.tv_nsec);
}

static inline uint64_t rdtsc_ordered() noexcept {
    unsigned aux = 0;
    return __rdtscp(&aux);
}

static uint64_t calibrate_tsc_hz() noexcept {
    timespec req;
    req.tv_sec = 0;
    req.tv_nsec = 10L * 1000L * 1000L;
    uint64_t t0 = now_ns();
    uint64_t c0 = rdtsc_ordered();
    nanosleep(&req, nullptr);
    uint64_t t1 = now_ns();
    uint64_t c1 = rdtsc_ordered();
    if (t1 <= t0 || c1 <= c0) {
        return 0;
    }
    return static_cast<uint64_t>((static_cast<long double>(c1 - c0) * 1.0e9L) /
                                 static_cast<long double>(t1 - t0));
}

static bool parse_u64(const char* p, const char* end, uint64_t& out) {
    uint64_t v = 0;
    if (p >= end) {
        return false;
    }
    while (p < end) {
        if (*p < '0' || *p > '9') {
            return false;
        }
        v = v * 10 + static_cast<uint64_t>(*p - '0');
        ++p;
    }
    out = v;
    return true;
}

static bool parse_line_view(const char* data, size_t len, ParsedLineView& out) {
    if (data == nullptr || len == 0 || data[0] == '#') {
        return false;
    }
    while (len > 0 && (data[len - 1] == '\n' || data[len - 1] == '\r')) {
        --len;
    }
    const char* end = data + len;
    const char* comma = static_cast<const char*>(memchr(data, ',', len));
    if (comma == nullptr || comma + 3 > end) {
        return false;
    }
    uint64_t ts = 0;
    if (!parse_u64(data, comma, ts)) {
        return false;
    }
    const char op = comma[1];
    if (comma[2] != ' ') {
        return false;
    }
    const char* key = comma + 3;
    const char* value = nullptr;
    const char* colon = static_cast<const char*>(memchr(key, ':', static_cast<size_t>(end - key)));
    if (colon != nullptr) {
        value = colon + 1;
    } else {
        colon = end;
    }
    if (key >= colon) {
        return false;
    }
    out.ts_us = ts;
    out.op = op;
    out.key_ptr = key;
    out.key_len = static_cast<uint32_t>(colon - key);
    if (value != nullptr) {
        out.value_ptr = value;
        out.value_len = static_cast<uint32_t>(end - value);
    } else {
        out.value_ptr = nullptr;
        out.value_len = 0;
    }
    return true;
}

static void write_stats_csv(const std::string& path, uint64_t node_id, const Bandle::ReplayStats& s) {
    std::ofstream out(path);
    if (!out.is_open()) {
        std::cerr << "Failed to open stats output file: " << path << std::endl;
        return;
    }
    out << "META," << node_id << "," << s.real_start_ns << "," << s.work_done_ns << "," << s.real_end_ns << ","
        << s.ok_ops << "," << s.read_ops << "," << s.write_ops << "," << s.tsc_hz << "\n";
    const auto& rc = s.read_latency.counts();
    for (int i = 0; i < LatencyHistogram::BUCKETS; ++i) {
        if (rc[i] != 0) {
            out << "READ," << i << "," << rc[i] << "\n";
        }
    }
    const auto& wc = s.write_latency.counts();
    for (int i = 0; i < LatencyHistogram::BUCKETS; ++i) {
        if (wc[i] != 0) {
            out << "WRITE," << i << "," << wc[i] << "\n";
        }
    }
}

static void usage(const char* prog) {
    std::cerr << "Usage: " << prog << " <dax_path> <node_id> <port> --dataset <file> "
              << "[--preload-file <file>] [--preload-count <n>] [--recordcount <n>] "
              << "[--cluster-size <n>] [--run-id <id>] [--start-signal <file>] [--ready-file <file>] "
              << "[--poll-idle-us <n>] [--noop-us <n>] [--batch-size <n>] "
              << "[--pipeline-workers <n>] [--stats-out <file>]\n";
}

int main(int argc, char** argv) {
    if (argc < 4) {
        usage(argv[0]);
        return 1;
    }

    std::string dax_path = argv[1];
    uint64_t node_id = std::stoull(argv[2]);
    (void)std::stoull(argv[3]);

    std::string dataset_path;
    std::string preload_file;
    std::string start_signal;
    std::string ready_file;
    std::string stats_out;
    uint64_t preload_count = 0;
    uint64_t recordcount = 0;
    uint64_t cluster_size = 3;
    uint64_t run_id = 0;
    int poll_idle_us = 0;
    uint64_t noop_us = 100;
    uint32_t batch_size = 64;
    size_t pipeline_workers = 1;

    for (int i = 4; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--dataset" && i + 1 < argc) {
            dataset_path = argv[++i];
        } else if (arg == "--preload-file" && i + 1 < argc) {
            preload_file = argv[++i];
        } else if (arg == "--preload-count" && i + 1 < argc) {
            preload_count = std::stoull(argv[++i]);
        } else if (arg == "--recordcount" && i + 1 < argc) {
            recordcount = std::stoull(argv[++i]);
        } else if (arg == "--cluster-size" && i + 1 < argc) {
            cluster_size = std::stoull(argv[++i]);
        } else if (arg == "--run-id" && i + 1 < argc) {
            run_id = std::stoull(argv[++i]);
        } else if (arg == "--start-signal" && i + 1 < argc) {
            start_signal = argv[++i];
        } else if (arg == "--ready-file" && i + 1 < argc) {
            ready_file = argv[++i];
        } else if (arg == "--poll-idle-us" && i + 1 < argc) {
            poll_idle_us = std::stoi(argv[++i]);
        } else if (arg == "--noop-us" && i + 1 < argc) {
            noop_us = std::stoull(argv[++i]);
        } else if (arg == "--batch-size" && i + 1 < argc) {
            batch_size = static_cast<uint32_t>(std::stoul(argv[++i]));
        } else if (arg == "--pipeline-workers" && i + 1 < argc) {
            pipeline_workers = static_cast<size_t>(std::stoul(argv[++i]));
        } else if (arg == "--stats-out" && i + 1 < argc) {
            stats_out = argv[++i];
        }
    }

    if (dataset_path.empty()) {
        std::cerr << "--dataset is required" << std::endl;
        return 1;
    }

    const size_t map_length = CXLMemoryPool::HWCC_SIZE + CXLMemoryPool::NON_HWCC_SIZE;
    int dax_fd = open(dax_path.c_str(), O_RDWR);
    if (dax_fd < 0) {
        std::cerr << "Failed to open " << dax_path << ": " << strerror(errno) << std::endl;
        return 1;
    }
    void* shared_memory = mmap(nullptr, map_length, PROT_READ | PROT_WRITE, MAP_SHARED, dax_fd, 0);
    if (shared_memory == MAP_FAILED) {
        std::cerr << "mmap failed on " << dax_path << ": " << strerror(errno) << std::endl;
        close(dax_fd);
        return 1;
    }
    close(dax_fd);

    void* hwcc_ptr = shared_memory;
    void* non_hwcc_ptr = static_cast<char*>(shared_memory) + CXLMemoryPool::HWCC_SIZE;
    auto* shared_state = reinterpret_cast<BandleSharedState*>(hwcc_ptr);
    auto* non_hwcc_state = reinterpret_cast<BandleNonHwccState*>(non_hwcc_ptr);

    if (node_id == 1) {
        std::memset(shared_state, 0, sizeof(BandleSharedState));
        std::memset(non_hwcc_state, 0, sizeof(BandleNonHwccState));
        if (run_id != 0) {
            shared_state->run_id.store(run_id, std::memory_order_release);
        }
    } else if (run_id != 0) {
        while (shared_state->run_id.load(std::memory_order_acquire) != run_id) {
            std::this_thread::sleep_for(10ms);
        }
    }

    {
        volatile char* base = static_cast<volatile char*>(non_hwcc_ptr);
        const size_t warmup_bytes = sizeof(BandleNonHwccState);
        for (size_t off = 0; off < warmup_bytes; off += 4096) {
            if (node_id == 1) {
                base[off] = 0;
            } else {
                volatile char v = base[off];
                (void)v;
            }
        }
    }

    CXLMemoryPool pool(hwcc_ptr, non_hwcc_ptr);
    Bandle bandle(&pool, shared_state, node_id, cluster_size, poll_idle_us, noop_us, batch_size, pipeline_workers);
    bandle.reserve_kv_entries(std::max<uint64_t>(recordcount, preload_count));

    if (preload_count > 0) {
        std::string path = preload_file.empty() ? dataset_path : preload_file;
        std::ifstream in(path);
        std::string line;
        uint64_t loaded = 0;
        while (loaded < preload_count && std::getline(in, line)) {
            ParsedLineView v;
            if (!parse_line_view(line.data(), line.size(), v)) {
                continue;
            }
            if (v.op == 'P' || v.op == 'U' || v.op == 'D') {
                Bandle::Request req;
                req.op = v.op;
                req.key.assign(v.key_ptr, v.key_len);
                if (v.value_ptr != nullptr && v.value_len != 0) {
                    req.value.assign(v.value_ptr, v.value_len);
                }
                bandle.direct_apply(req);
                ++loaded;
            }
        }
    }

    int dataset_fd = open(dataset_path.c_str(), O_RDONLY);
    if (dataset_fd < 0) {
        std::cerr << "Failed to open dataset " << dataset_path << ": " << strerror(errno) << std::endl;
        return 1;
    }
    struct stat st;
    if (fstat(dataset_fd, &st) != 0 || st.st_size <= 0) {
        std::cerr << "Invalid dataset file" << std::endl;
        close(dataset_fd);
        return 1;
    }
    const size_t dataset_len = static_cast<size_t>(st.st_size);
    const char* dataset = static_cast<const char*>(
        mmap(nullptr, dataset_len, PROT_READ, MAP_PRIVATE, dataset_fd, 0));
    close(dataset_fd);
    if (dataset == MAP_FAILED) {
        std::cerr << "mmap dataset failed: " << strerror(errno) << std::endl;
        return 1;
    }

    const uint64_t tsc_hz = calibrate_tsc_hz();
    bandle.start();

    if (!ready_file.empty()) {
        std::ofstream out(ready_file);
        out << "ready\n";
    }
    if (!start_signal.empty()) {
        while (access(start_signal.c_str(), F_OK) != 0) {
            std::this_thread::sleep_for(10ms);
        }
    }

    const auto real_start = std::chrono::steady_clock::now();
    const uint64_t real_start_ns = now_ns();

    std::vector<Bandle::BorrowedRequest> batch;
    batch.reserve(batch_size);
    const char* ptr = dataset;
    const char* end = dataset + dataset_len;
    uint64_t submitted = 0;
    while (ptr < end) {
        const char* nl = static_cast<const char*>(memchr(ptr, '\n', static_cast<size_t>(end - ptr)));
        const char* line_end = nl ? nl : end;
        ParsedLineView v;
        if (parse_line_view(ptr, static_cast<size_t>(line_end - ptr), v)) {
            auto target = real_start + std::chrono::microseconds(v.ts_us);
            auto now = std::chrono::steady_clock::now();
            if (target > now) {
                if (!batch.empty()) {
                    bandle.submit_borrowed_batch(batch.data(), batch.size());
                    batch.clear();
                }
                std::this_thread::sleep_until(target);
            }

            Bandle::BorrowedRequest req;
            req.op = v.op;
            req.key_ptr = v.key_ptr;
            req.key_len = v.key_len;
            req.value_ptr = v.value_ptr;
            req.value_len = v.value_len;
            batch.push_back(req);
            ++submitted;
            if ((submitted % 1000000ull) == 0) {
                std::cerr << "node " << node_id << " submitted " << submitted << " ops" << std::endl;
            }
            if (batch.size() >= batch_size) {
                bandle.submit_borrowed_batch(batch.data(), batch.size());
                batch.clear();
            }
        }
        ptr = nl ? nl + 1 : end;
    }
    if (!batch.empty()) {
        bandle.submit_borrowed_batch(batch.data(), batch.size());
        batch.clear();
    }
    munmap(const_cast<char*>(dataset), dataset_len);

    bandle.mark_input_done();
    bandle.wait_for_all_pending();
    std::cerr << "node " << node_id << " local pending complete" << std::endl;
    shared_state->node_finished[node_id].store(1, std::memory_order_release);
    while (true) {
        bool all_done = true;
        for (uint64_t i = 1; i <= cluster_size; ++i) {
            if (shared_state->node_finished[i].load(std::memory_order_acquire) == 0) {
                all_done = false;
                break;
            }
        }
        if (all_done) {
            break;
        }
        std::this_thread::sleep_for(1ms);
    }

    const uint64_t work_done_ns = now_ns();
    bandle.stop();

    auto stats = bandle.stats();
    stats.real_start_ns = real_start_ns;
    stats.work_done_ns = work_done_ns;
    stats.real_end_ns = now_ns();
    stats.tsc_hz = tsc_hz;
    std::cout << "Replay finished. submitted=" << submitted
              << " total_ops=" << stats.total_ops
              << " reads=" << stats.read_ops
              << " writes=" << stats.write_ops
              << " ok=" << stats.ok_ops
              << " errors=" << stats.error_ops << std::endl;

    if (!stats_out.empty()) {
        write_stats_csv(stats_out, node_id, stats);
    }

    munmap(shared_memory, map_length);
    return 0;
}
