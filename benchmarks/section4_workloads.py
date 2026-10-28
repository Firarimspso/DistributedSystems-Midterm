"""Reproducible Section 4 workload generator and redundancy benchmark.

The full workload is represented as deterministic object sizes, so calculating
its logical/physical bytes never creates payload files. Actual codec timing is
run on bounded in-memory samples and is always labelled separately.
"""

import argparse
import math
import random
import sys
import time
from dataclasses import dataclass

from metadata_service.redundancy import decode_shards, encode_shards


KIB = 1024
MIB = 1024 * KIB
GIB = 1024 * MIB


@dataclass(frozen=True)
class Workload:
    name: str
    sizes: list[int]


def generate_workloads(seed: int = 2026) -> tuple[Workload, Workload]:
    generator = random.Random(seed)
    small = [generator.randint(1 * KIB, 50 * KIB) for _ in range(5_000)]
    large = [generator.randint(100 * MIB, 1 * GIB) for _ in range(10)]
    return Workload("many-small", small), Workload("few-large", large)


def theoretical_rows(workload: Workload, n: int = 3, k: int = 4, m: int = 2) -> list[dict[str, int | float | str]]:
    logical = sum(workload.sizes)
    ec_physical = sum(math.ceil(size / k) * (k + m) for size in workload.sizes)
    ec_repair = sum(math.ceil(size / k) * (k + 1) for size in workload.sizes)
    return [
        {
            "workload": workload.name,
            "strategy": f"replication {n}x",
            "objects": len(workload.sizes),
            "logical": logical,
            "physical": logical * n,
            "overhead": n,
            "repair": logical * 2,
        },
        {
            "workload": workload.name,
            "strategy": f"erasure {k}+{m}",
            "objects": len(workload.sizes),
            "logical": logical,
            "physical": ec_physical,
            "overhead": ec_physical / logical,
            "repair": ec_repair,
        },
    ]


def payload(size: int) -> bytes:
    block = b"distributed-systems-section-4-benchmark\0"
    return (block * (size // len(block) + 1))[:size]


def actual_codec_sample(label: str, sizes: list[int], k: int = 4, m: int = 2) -> dict[str, int | float | str]:
    logical = physical = 0
    started = time.perf_counter()
    for size in sizes:
        original = payload(size)
        shards = encode_shards(original, k, m)
        indexes = list(range(k))
        reconstructed = decode_shards(shards[:k], indexes, k, m, size)
        if reconstructed != original:
            raise RuntimeError("Reed-Solomon reconstruction mismatch")
        logical += size
        physical += sum(len(shard) for shard in shards)
    elapsed = time.perf_counter() - started
    return {
        "sample": label,
        "objects": len(sizes),
        "logical": logical,
        "physical": physical,
        "seconds": elapsed,
        "throughput_mib_s": logical / MIB / elapsed,
    }


def print_theoretical(rows: list[dict[str, int | float | str]]) -> None:
    print("FULL WORKLOAD — THEORETICAL/CALCULATED (no payload files generated)")
    print("workload    strategy          objects       logical       physical  overhead       repair")
    for row in rows:
        print(
            f"{row['workload']:<11} {row['strategy']:<16} {row['objects']:>7} "
            f"{row['logical']:>13} {row['physical']:>14} {row['overhead']:>8.3f}x {row['repair']:>12}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--actual-small-count", type=int, default=250)
    parser.add_argument("--actual-large-count", type=int, default=1)
    parser.add_argument("--actual-large-mib", type=int, default=100)
    parser.add_argument("--no-actual", action="store_true")
    args = parser.parse_args()
    small, large = generate_workloads(args.seed)
    rows = theoretical_rows(small) + theoretical_rows(large)
    print_theoretical(rows)
    if args.no_actual:
        return

    small_sizes = small.sizes[: args.actual_small_count]
    large_sizes = [args.actual_large_mib * MIB] * args.actual_large_count
    print("\nBOUNDED ACTUAL ZFEC ENCODE+DECODE (in-memory; no payload files retained)")
    print("sample                objects       logical       physical   seconds   throughput")
    for result in (
        actual_codec_sample("small sample", small_sizes),
        actual_codec_sample("large 100 MiB sample", large_sizes),
    ):
        print(
            f"{result['sample']:<21} {result['objects']:>7} {result['logical']:>13} "
            f"{result['physical']:>14} {result['seconds']:>9.3f} "
            f"{result['throughput_mib_s']:>9.2f} MiB/s"
        )
    print(f"\nPython {sys.version.split()[0]} | deterministic seed {args.seed}")


if __name__ == "__main__":
    main()

