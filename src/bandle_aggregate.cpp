#include "latency_histogram.h"

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace {

struct NodeStats {
    uint64_t node_id = 0;
    uint64_t real_start_ns = 0;
    uint64_t work_done_ns = 0;
    uint64_t real_end_ns = 0;
    uint64_t ok_ops = 0;
    uint64_t read_ops = 0;
    uint64_t write_ops = 0;
    uint64_t tsc_hz = 0;

    LatencyHistogram read_latency;
    LatencyHistogram write_latency;

    // Optional perf counters (present only when --perf-stats=1).
    std::unordered_map<std::string, uint64_t> perf;
};

static inline long double ns_to_s(uint64_t ns) { return static_cast<long double>(ns) / 1.0e9L; }

static inline long double cycles_to_us(uint64_t cycles, uint64_t tsc_hz) {
    if (tsc_hz == 0) {
        return 0.0L;
    }
    return (static_cast<long double>(cycles) * 1.0e6L) / static_cast<long double>(tsc_hz);
}

static inline uint64_t bucket_upper_value(int idx) noexcept {
    if (idx + 1 < LatencyHistogram::BUCKETS) {
        return LatencyHistogram::bucket_lower_value(idx + 1);
    }
    uint64_t lower = LatencyHistogram::bucket_lower_value(idx);
    return (lower == 0) ? 1 : (lower << 1);
}

static long double mean_cycles_from_hist(const LatencyHistogram& h) {
    const auto& c = h.counts();
    long double sum = 0.0L;
    uint64_t tot = 0;

    for (int i = 0; i < LatencyHistogram::BUCKETS; ++i) {
        uint64_t cnt = c[i];
        if (cnt == 0) {
            continue;
        }
        uint64_t lo = LatencyHistogram::bucket_lower_value(i);
        uint64_t hi = bucket_upper_value(i);
        long double mid = (static_cast<long double>(lo) + static_cast<long double>(hi)) / 2.0L;
        sum += mid * static_cast<long double>(cnt);
        tot += cnt;
    }

    if (tot == 0) {
        return 0.0L;
    }
    return sum / static_cast<long double>(tot);
}

static bool parse_csv_file(const std::string& path, NodeStats& out) {
    std::ifstream in(path);
    if (!in.is_open()) {
        std::cerr << "Failed to open stats file: " << path << "\n";
        return false;
    }

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        std::stringstream ss(line);
        std::string tag;
        std::getline(ss, tag, ',');

        if (tag == "META") {
            std::string tok;
            std::getline(ss, tok, ',');
            out.node_id = std::stoull(tok);
            std::getline(ss, tok, ',');
            out.real_start_ns = std::stoull(tok);
            std::getline(ss, tok, ',');
            // New format adds work_done_ns between real_start_ns and real_end_ns.
            // Backward compatible: if the next token count matches the old format,
            // treat it as real_end_ns.
            uint64_t third = std::stoull(tok);
            std::string next_tok;
            if (std::getline(ss, next_tok, ',')) {
                // New format: third=work_done_ns, next=real_end_ns
                out.work_done_ns = third;
                out.real_end_ns = std::stoull(next_tok);
            } else {
                // Old format: third=real_end_ns
                out.work_done_ns = 0;
                out.real_end_ns = third;
                continue;
            }
            std::getline(ss, tok, ',');
            out.ok_ops = std::stoull(tok);
            std::getline(ss, tok, ',');
            out.read_ops = std::stoull(tok);
            std::getline(ss, tok, ',');
            out.write_ops = std::stoull(tok);
            std::getline(ss, tok, ',');
            out.tsc_hz = std::stoull(tok);
        } else if (tag == "READ" || tag == "WRITE") {
            std::string tok;
            std::getline(ss, tok, ',');
            int idx = std::stoi(tok);
            std::getline(ss, tok, ',');
            uint64_t cnt = std::stoull(tok);
            if (tag == "READ") {
                out.read_latency.add_bucket(idx, cnt);
            } else {
                out.write_latency.add_bucket(idx, cnt);
            }
        } else if (tag == "PERF") {
            std::string name;
            std::string val;
            std::getline(ss, name, ',');
            std::getline(ss, val, ',');
            if (!name.empty() && !val.empty()) {
                out.perf[name] = std::stoull(val);
            }
        }
    }

    return true;
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <stats_node1.csv> [stats_node2.csv ...]\n";
        return 2;
    }

    std::vector<NodeStats> nodes;
    nodes.reserve(static_cast<size_t>(argc - 1));

    for (int i = 1; i < argc; ++i) {
        NodeStats ns;
        if (!parse_csv_file(argv[i], ns)) {
            return 1;
        }
        nodes.push_back(std::move(ns));
    }

    uint64_t cluster_start = 0;
    uint64_t cluster_end = 0;
    uint64_t total_ok = 0;
    uint64_t total_reads = 0;
    uint64_t total_writes = 0;

    uint64_t tsc_hz = 0;
    bool tsc_mismatch = false;

    LatencyHistogram cluster_read;
    LatencyHistogram cluster_write;

    for (const auto& n : nodes) {
        if (n.real_start_ns != 0 && (cluster_start == 0 || n.real_start_ns < cluster_start)) {
            cluster_start = n.real_start_ns;
        }
        if (n.real_end_ns != 0 && n.real_end_ns > cluster_end) {
            cluster_end = n.real_end_ns;
        }

        total_ok += n.ok_ops;
        total_reads += n.read_ops;
        total_writes += n.write_ops;

        cluster_read.merge(n.read_latency);
        cluster_write.merge(n.write_latency);

        if (n.tsc_hz != 0) {
            if (tsc_hz == 0) {
                tsc_hz = n.tsc_hz;
            } else if (tsc_hz != n.tsc_hz) {
                tsc_mismatch = true;
            }
        }
    }

    // IMPORTANT: real_start_ns/real_end_ns are based on each node's local monotonic clock.
    // Across multiple VMs these clocks are not synchronized, so we must NOT compute
    // duration using min(start) and max(end) across nodes.
    // Instead, compute cluster duration as the maximum per-node duration.
    long double duration_s = 0.0L;
    for (const auto& n : nodes) {
        if (n.real_end_ns > n.real_start_ns) {
            long double node_dur = ns_to_s(n.real_end_ns - n.real_start_ns);
            if (node_dur > duration_s) {
                duration_s = node_dur;
            }
        }
    }

    long double throughput = 0.0L;
    if (duration_s > 0.0L) {
        throughput = static_cast<long double>(total_ok) / duration_s;
    }

    uint64_t r_p50 = cluster_read.quantile_value(0.50);
    uint64_t r_p99 = cluster_read.quantile_value(0.99);
    uint64_t w_p50 = cluster_write.quantile_value(0.50);
    uint64_t w_p99 = cluster_write.quantile_value(0.99);

    long double r_mean = mean_cycles_from_hist(cluster_read);
    long double w_mean = mean_cycles_from_hist(cluster_write);

    std::cout << "================ Cluster Summary ================\n";
    std::cout << "Nodes:            " << nodes.size() << "\n";
    std::cout << "Duration (s):      " << static_cast<double>(duration_s) << "\n";
    std::cout << "OK ops:            " << total_ok << "\n";
    std::cout << "Read ops:          " << total_reads << "\n";
    std::cout << "Write ops:         " << total_writes << "\n";
    std::cout << "Throughput (ops/s):" << static_cast<double>(throughput) << "\n";

    // Optional perf summary if present.
    bool has_perf = false;
    for (const auto& n : nodes) {
        if (!n.perf.empty()) {
            has_perf = true;
            break;
        }
    }
    if (has_perf) {
        auto sum_key = [&](const char* k) {
            uint64_t sum = 0;
            for (const auto& n : nodes) {
                auto it = n.perf.find(k);
                if (it != n.perf.end()) {
                    sum += it->second;
                }
            }
            return sum;
        };
        auto max_key = [&](const char* k) {
            uint64_t mx = 0;
            for (const auto& n : nodes) {
                auto it = n.perf.find(k);
                if (it != n.perf.end()) {
                    mx = std::max(mx, it->second);
                }
            }
            return mx;
        };

        auto div_u64 = [&](uint64_t num, uint64_t den) -> long double {
            if (den == 0) return 0.0L;
            return static_cast<long double>(num) / static_cast<long double>(den);
        };

        std::cout << "---------------- Perf Counters ----------------\n";
        const uint64_t submit_fail = sum_key("submit_push_failures");
        const uint64_t submit_wait = sum_key("submit_push_wait_cycles");
        const uint64_t submit_batches = sum_key("submit_push_batches");
        const uint64_t submit_msgs = sum_key("submit_push_msgs");

        const uint64_t recv_retry = sum_key("recv_read_retry");
        const uint64_t recv_empty = sum_key("recv_coord_empty_loops");
        const uint64_t recv_max_avail = max_key("recv_ring_max_avail");
        const uint64_t recv_commits = sum_key("recv_commit_batches");

        const uint64_t lq_wait = sum_key("localq_publish_wait_cycles");
        const uint64_t lq_wait_max = max_key("localq_publish_wait_max_cycles");
        const uint64_t lq_backlog_max = max_key("localq_backlog_max");

        const uint64_t lq_empty = sum_key("recv_localq_empty_loops");

        const uint64_t hacc_cnt = sum_key("handle_accept_count");
        const uint64_t hack_cnt = sum_key("handle_ack_count");
        const uint64_t hacc_cyc = sum_key("handle_accept_cycles");
        const uint64_t hack_cyc = sum_key("handle_ack_cycles");
        const uint64_t hacc_apply_cnt = sum_key("handle_accept_apply_count");
        const uint64_t hack_apply_cnt = sum_key("handle_ack_apply_count");
        const uint64_t hacc_apply_cyc = sum_key("handle_accept_apply_cycles");
        const uint64_t hack_apply_cyc = sum_key("handle_ack_apply_cycles");
        const uint64_t publish_cnt = hacc_cnt + hack_cnt;

        std::cout << "submit_push_failures:            " << submit_fail << "\n";
        std::cout << "submit_push_batches:             " << submit_batches << "\n";
        std::cout << "submit_push_msgs:                " << submit_msgs << "\n";
        std::cout << "submit_push_wait_cycles(sum):    " << submit_wait << "\n";
        std::cout << "recv_read_retry:                 " << recv_retry << "\n";
        std::cout << "recv_ring_max_avail(max):        " << recv_max_avail << "\n";
        std::cout << "recv_commit_batches:             " << recv_commits << "\n";
        std::cout << "localq_publish_wait_cycles(sum): " << lq_wait << "\n";
        std::cout << "localq_publish_wait_max_cycles:  " << lq_wait_max << "\n";
        std::cout << "localq_backlog_max(max):         " << lq_backlog_max << "\n";
        std::cout << "recv_coord_empty_loops:          " << recv_empty << "\n";
        std::cout << "recv_localq_empty_loops:         " << lq_empty << "\n";
        std::cout << "handle_accept_count:             " << hacc_cnt << "\n";
        std::cout << "handle_ack_count:                " << hack_cnt << "\n";
        std::cout << "handle_accept_apply_count:       " << hacc_apply_cnt << "\n";
        std::cout << "handle_ack_apply_count:          " << hack_apply_cnt << "\n";

        std::cout << "---------------- Perf Derived ----------------\n";
        const long double submit_wait_per_batch = div_u64(submit_wait, submit_batches);
        const long double submit_fail_per_batch = div_u64(submit_fail, submit_batches);
        const long double lq_wait_per_publish = div_u64(lq_wait, publish_cnt);
        const long double hacc_cyc_per = div_u64(hacc_cyc, hacc_cnt);
        const long double hack_cyc_per = div_u64(hack_cyc, hack_cnt);
        const long double hacc_apply_cyc_per = div_u64(hacc_apply_cyc, hacc_apply_cnt);
        const long double hack_apply_cyc_per = div_u64(hack_apply_cyc, hack_apply_cnt);

        std::cout << "submit_push_wait_cycles/batch:    " << static_cast<double>(submit_wait_per_batch);
        if (tsc_hz != 0) {
            std::cout << " (" << static_cast<double>(cycles_to_us(static_cast<uint64_t>(submit_wait_per_batch), tsc_hz)) << " us)";
        }
        std::cout << "\n";
        std::cout << "submit_push_failures/batch:      " << static_cast<double>(submit_fail_per_batch) << "\n";

        std::cout << "localq_publish_wait_cycles/msg:  " << static_cast<double>(lq_wait_per_publish);
        if (tsc_hz != 0) {
            std::cout << " (" << static_cast<double>(cycles_to_us(static_cast<uint64_t>(lq_wait_per_publish), tsc_hz)) << " us)";
        }
        std::cout << "\n";

        std::cout << "handle_accept_cycles/msg:        " << static_cast<double>(hacc_cyc_per);
        if (tsc_hz != 0) {
            std::cout << " (" << static_cast<double>(cycles_to_us(static_cast<uint64_t>(hacc_cyc_per), tsc_hz)) << " us)";
        }
        std::cout << "\n";
        std::cout << "handle_accept_apply_cycles/apply:" << static_cast<double>(hacc_apply_cyc_per);
        if (tsc_hz != 0) {
            std::cout << " (" << static_cast<double>(cycles_to_us(static_cast<uint64_t>(hacc_apply_cyc_per), tsc_hz)) << " us)";
        }
        std::cout << "\n";

        std::cout << "handle_ack_cycles/msg:           " << static_cast<double>(hack_cyc_per);
        if (tsc_hz != 0) {
            std::cout << " (" << static_cast<double>(cycles_to_us(static_cast<uint64_t>(hack_cyc_per), tsc_hz)) << " us)";
        }
        std::cout << "\n";
        std::cout << "handle_ack_apply_cycles/apply:   " << static_cast<double>(hack_apply_cyc_per);
        if (tsc_hz != 0) {
            std::cout << " (" << static_cast<double>(cycles_to_us(static_cast<uint64_t>(hack_apply_cyc_per), tsc_hz)) << " us)";
        }
        std::cout << "\n";
    }

    if (tsc_hz == 0) {
        std::cout << "TSC Hz:            0 (cannot convert to us)\n";
        std::cout << "Read  p50/p99/mean (cycles): " << r_p50 << " / " << r_p99 << " / " << static_cast<double>(r_mean)
                  << "\n";
        std::cout << "Write p50/p99/mean (cycles): " << w_p50 << " / " << w_p99 << " / " << static_cast<double>(w_mean)
                  << "\n";
    } else {
        if (tsc_mismatch) {
            std::cout << "TSC Hz:            " << tsc_hz << " (warning: mismatch across nodes)\n";
        } else {
            std::cout << "TSC Hz:            " << tsc_hz << "\n";
        }

        std::cout << "Read  p50/p99/mean (us): " << static_cast<double>(cycles_to_us(r_p50, tsc_hz)) << " / "
                  << static_cast<double>(cycles_to_us(r_p99, tsc_hz)) << " / "
                  << static_cast<double>(cycles_to_us(static_cast<uint64_t>(r_mean), tsc_hz)) << "\n";
        std::cout << "Write p50/p99/mean (us): " << static_cast<double>(cycles_to_us(w_p50, tsc_hz)) << " / "
                  << static_cast<double>(cycles_to_us(w_p99, tsc_hz)) << " / "
                  << static_cast<double>(cycles_to_us(static_cast<uint64_t>(w_mean), tsc_hz)) << "\n";
    }

    std::cout << "\n================ Per-Node Summary ================\n";
    for (const auto& n : nodes) {
        uint64_t end_ns = (n.work_done_ns != 0) ? n.work_done_ns : n.real_end_ns;
        long double node_dur_s = 0.0L;
        if (end_ns > n.real_start_ns) {
            node_dur_s = ns_to_s(end_ns - n.real_start_ns);
        }
        long double node_thr = 0.0L;
        if (node_dur_s > 0.0L) {
            node_thr = static_cast<long double>(n.ok_ops) / node_dur_s;
        }

        uint64_t nr_p50 = n.read_latency.quantile_value(0.50);
        uint64_t nr_p99 = n.read_latency.quantile_value(0.99);
        uint64_t nw_p50 = n.write_latency.quantile_value(0.50);
        uint64_t nw_p99 = n.write_latency.quantile_value(0.99);
        long double nr_mean = mean_cycles_from_hist(n.read_latency);
        long double nw_mean = mean_cycles_from_hist(n.write_latency);

        std::cout << "Node " << n.node_id << ": ops=" << n.ok_ops
                  << " reads=" << n.read_ops << " writes=" << n.write_ops
                  << " done_s=" << static_cast<double>(node_dur_s)
                  << " thr_ops_s=" << static_cast<double>(node_thr);
        if (n.work_done_ns != 0 && n.real_end_ns > n.work_done_ns) {
            long double extra_wait_s = ns_to_s(n.real_end_ns - n.work_done_ns);
            std::cout << " wait_after_done_s=" << static_cast<double>(extra_wait_s);
        }
        std::cout << "\n";

        if (n.tsc_hz == 0) {
            std::cout << "  Read  p50/p99/mean (cycles): " << nr_p50 << " / " << nr_p99 << " / " << static_cast<double>(nr_mean) << "\n";
            std::cout << "  Write p50/p99/mean (cycles): " << nw_p50 << " / " << nw_p99 << " / " << static_cast<double>(nw_mean) << "\n";
        } else {
            std::cout << "  Read  p50/p99/mean (us): " << static_cast<double>(cycles_to_us(nr_p50, n.tsc_hz)) << " / "
                      << static_cast<double>(cycles_to_us(nr_p99, n.tsc_hz)) << " / "
                      << static_cast<double>(cycles_to_us(static_cast<uint64_t>(nr_mean), n.tsc_hz)) << "\n";
            std::cout << "  Write p50/p99/mean (us): " << static_cast<double>(cycles_to_us(nw_p50, n.tsc_hz)) << " / "
                      << static_cast<double>(cycles_to_us(nw_p99, n.tsc_hz)) << " / "
                      << static_cast<double>(cycles_to_us(static_cast<uint64_t>(nw_mean), n.tsc_hz)) << "\n";
        }
    }

    return 0;
}
