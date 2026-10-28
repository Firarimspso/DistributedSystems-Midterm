from dataclasses import dataclass

from metadata_service.redundancy import encode_shards


@dataclass(frozen=True)
class ComparisonRow:
    workload: str
    strategy: str
    logical_bytes: int
    physical_bytes: int
    overhead_ratio: float
    repair_bytes: int


def compare_workload(name: str, object_sizes: list[int], n: int = 3, k: int = 4, m: int = 2) -> list[ComparisonRow]:
    logical = sum(object_sizes)
    replication_physical = logical * n
    replication_repair = logical * 2
    erasure_physical = 0
    erasure_repair = 0
    for size in object_sizes:
        shards = encode_shards(b"x" * size, k, m)
        erasure_physical += sum(len(shard) for shard in shards)
        erasure_repair += sum(len(shard) for shard in shards[:k]) + len(shards[0])
    return [
        ComparisonRow(name, f"replication {n}x", logical, replication_physical, replication_physical / logical, replication_repair),
        ComparisonRow(name, f"erasure {k}+{m}", logical, erasure_physical, erasure_physical / logical, erasure_repair),
    ]


def comparison_rows() -> list[ComparisonRow]:
    return compare_workload("many-small", [4096] * 1000) + compare_workload("few-large", [4 * 1024 * 1024] * 8)


def main() -> None:
    print("CALCULATED REDUNDANCY COMPARISON (one replica/shard loss)")
    print("workload    strategy          logical      physical   overhead   repair traffic")
    for row in comparison_rows():
        print(
            f"{row.workload:<11} {row.strategy:<16} {row.logical_bytes:>11} "
            f"{row.physical_bytes:>13} {row.overhead_ratio:>9.3f}x {row.repair_bytes:>16}"
        )


if __name__ == "__main__":
    main()
