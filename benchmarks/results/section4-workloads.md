# Section 4 workload benchmark results

This file is populated from `benchmarks/section4_workloads.py`. Full workload
figures are calculations over deterministic size lists and do not allocate or
store payloads. Bounded actual zfec timing is recorded separately.
Run command:
```bash
.venv/bin/python -m benchmarks.section4_workloads
```
Environment: Python 3.12.2, deterministic seed 2026.

## Full workload calculations

| Workload | Strategy | Objects | Logical bytes | Physical bytes | Overhead | Repair bytes |
|---|---|---:|---:|---:|---:|---:|
| many-small | replication 3x | 5,000 | 129,582,627 | 388,747,881 | 3.000x | 259,165,254 |
| many-small | erasure 4+2 | 5,000 | 129,582,627 | 194,385,222 | 1.500x | 161,987,685 |
| few-large | replication 3x | 10 | 4,425,804,152 | 13,277,412,456 | 3.000x | 8,851,608,304 |
| few-large | erasure 4+2 | 10 | 4,425,804,152 | 6,638,706,258 | 1.500x | 5,532,255,215 |

These are theoretical/calculated results over all required object sizes. No
payload bytes were generated for this phase.
## Bounded actual zfec measurement
| Sample | Objects | Logical bytes processed | Physical encoded bytes | Encode + decode time | Throughput |
|---|---:|---:|---:|---:|---:|
| First 250 generated small objects | 250 | 6,929,891 | 10,395,402 | 0.008 s | 880.37 MiB/s |
| One 100 MiB large object | 1 | 104,857,600 | 157,286,400 | 0.257 s | 389.70 MiB/s |
The actual phase measured in-memory zfec encode plus decode and verified exact
reconstruction. It did not measure Docker networking, PostgreSQL, disk I/O, or
the entire 4.43 GB large-file workload. Payloads were transient and were not
written to disk. Do not describe calculated full-workload figures as measured
I/O.