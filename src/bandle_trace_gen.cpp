#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace {

struct ParsedOp {
    uint64_t ts_us = 0;
    char op = 'G';
    std::string key;
    std::string value;
};

bool parse_line(const std::string& line, ParsedOp& out) {
    if (line.empty() || line[0] == '#') {
        return false;
    }
    auto comma = line.find(',');
    if (comma == std::string::npos) {
        return false;
    }
    std::string ts_str = line.substr(0, comma);
    std::string req = line.substr(comma + 1);
    while (!req.empty() && (req.back() == '\n' || req.back() == '\r')) {
        req.pop_back();
    }
    if (req.size() < 3 || req[1] != ' ') {
        return false;
    }
    out.op = req[0];
    std::string rest = req.substr(2);
    if (out.op == 'P' || out.op == 'U') {
        auto pos = rest.find(':');
        if (pos == std::string::npos) {
            return false;
        }
        out.key = rest.substr(0, pos);
        out.value = rest.substr(pos + 1);
    } else if (out.op == 'D' || out.op == 'G') {
        out.key = rest;
    } else {
        return false;
    }
    try {
        out.ts_us = std::stoull(ts_str);
    } catch (...) {
        return false;
    }
    return true;
}

void usage(const char* prog) {
    std::cerr << "Usage: " << prog << " <input_dataset> <output_prefix> <nodes> <max_ops> [multinode_writes 0|1]" << std::endl;
    std::cerr << "  Reads are balanced across nodes. Writes are balanced when multinode_writes=1." << std::endl;
}

}

int main(int argc, char* argv[]) {
    if (argc < 5) {
        usage(argv[0]);
        return 1;
    }

    std::string input_path = argv[1];
    std::string output_prefix = argv[2];
    int nodes = std::stoi(argv[3]);
    uint64_t max_ops = std::stoull(argv[4]);
    bool multinode_writes = true;
    if (argc >= 6) {
        std::string v = argv[5];
        multinode_writes = (v == "1" || v == "true" || v == "yes");
    }
    if (nodes < 1) {
        std::cerr << "nodes must be >=1" << std::endl;
        return 1;
    }

    std::ifstream in(input_path);
    if (!in.is_open()) {
        std::cerr << "Failed to open input dataset: " << input_path << std::endl;
        return 1;
    }

    // Pass 1: determine global end timestamp (streaming, no buffering).
    uint64_t end_ts_us = 0;
    {
        uint64_t total = 0;
        std::string line;
        ParsedOp op{};
        while (std::getline(in, line)) {
            if (!parse_line(line, op)) {
                continue;
            }
            if (max_ops > 0 && total >= max_ops) {
                break;
            }
            ++total;
            end_ts_us = op.ts_us;
        }
    }
    if (end_ts_us == 0) {
        std::cerr << "No valid operations found in input dataset" << std::endl;
        return 1;
    }

    // Rewind for pass 2.
    in.clear();
    in.seekg(0);

    std::vector<std::ofstream> outs;
    outs.reserve(nodes);
    for (int i = 0; i < nodes; ++i) {
        std::string path = output_prefix + std::to_string(i + 1) + "_dataset.txt";
        outs.emplace_back(path);
        if (!outs.back().is_open()) {
            std::cerr << "Failed to open output file: " << path << std::endl;
            return 1;
        }
    }

    uint64_t total = 0;
    std::vector<uint64_t> last_ts(nodes, 0);
    std::vector<uint64_t> read_counts(nodes, 0);
    std::vector<uint64_t> write_counts(nodes, 0);
    std::string line;
    ParsedOp op{};
    while (std::getline(in, line)) {
        if (!parse_line(line, op)) {
            continue;
        }
        if (max_ops > 0 && total >= max_ops) {
            break;
        }
        ++total;

        int target_node = 1; // 1-based
        if (op.op == 'G') {
            // Balance reads by count, then by last assigned timestamp (keeps per-node end times close).
            uint64_t best_count = UINT64_MAX;
            uint64_t best_last_ts = UINT64_MAX;
            int best = 1;
            for (int i = 1; i <= nodes; ++i) {
                uint64_t c = read_counts[i - 1];
                uint64_t l = last_ts[i - 1];
                if (c < best_count || (c == best_count && l < best_last_ts)) {
                    best_count = c;
                    best_last_ts = l;
                    best = i;
                }
            }
            target_node = best;
            read_counts[target_node - 1]++;
        } else {
            if (multinode_writes) {
                uint64_t best_count = UINT64_MAX;
                uint64_t best_last_ts = UINT64_MAX;
                int best = 1;
                for (int i = 1; i <= nodes; ++i) {
                    uint64_t c = write_counts[i - 1];
                    uint64_t l = last_ts[i - 1];
                    if (c < best_count || (c == best_count && l < best_last_ts)) {
                        best_count = c;
                        best_last_ts = l;
                        best = i;
                    }
                }
                target_node = best;
            } else {
                target_node = 1;
            }
            write_counts[target_node - 1]++;
        }

        outs[target_node - 1] << line << '\n';
        last_ts[target_node - 1] = op.ts_us;
    }

    // Pad each node with a final no-op read at end_ts_us so replay wall-clock ends around the same time.
    // This does NOT affect correctness and keeps the forwarding threads busy until the end.
    for (int i = 1; i <= nodes; ++i) {
        if (last_ts[i - 1] < end_ts_us) {
            outs[i - 1] << end_ts_us << ",G __pad_node" << i << "\n";
            last_ts[i - 1] = end_ts_us;
            read_counts[i - 1]++; // counts as an extra read
            ++total;
        }
    }

    std::cout << "Processed " << total << " operations (max_ops=" << max_ops << ")" << std::endl;
    std::cout << "Global end_ts_us=" << end_ts_us << std::endl;
    for (int i = 1; i <= nodes; ++i) {
        std::cout << "  node" << i << ": reads=" << read_counts[i - 1]
                  << " writes=" << write_counts[i - 1]
                  << " last_ts_us=" << last_ts[i - 1] << std::endl;
    }
    std::cout << "Output prefix: " << output_prefix << std::endl;
    return 0;
}
