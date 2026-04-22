# cxl_bandle

Initial 3-node Bandle-over-CXL prototype.

- `bandle_replay`: runs YCSB trace replay with all reads and writes ordered through Bandle.
- `bandle_trace_gen`: reuses the ChainPaxos trace splitter format.
- `bandle_aggregate`: aggregates per-node CSV stats.
- `scripts/run_bandle_poisson_vm.sh`: multi-VM runner matching the existing CXL VM workflow.

This first version targets basic read/write functionality without crash testing.
It currently supports exactly 3 replicas. HWCC is configured as 512 MiB; the
allocated shared descriptor state is about 109 MiB, and the pairwise payload
ring state is about 570 MiB in the non-HWCC region.
# cxl_bandle
