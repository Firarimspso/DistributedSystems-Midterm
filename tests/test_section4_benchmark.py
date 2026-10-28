from benchmarks.section4_workloads import GIB, KIB, MIB, generate_workloads, theoretical_rows


def test_professor_workload_shapes_are_deterministic() -> None:
    first_small, first_large = generate_workloads()
    second_small, second_large = generate_workloads()
    assert first_small.sizes == second_small.sizes
    assert first_large.sizes == second_large.sizes
    assert len(first_small.sizes) == 5_000
    assert all(1 * KIB <= size <= 50 * KIB for size in first_small.sizes)
    assert len(first_large.sizes) == 10
    assert all(100 * MIB <= size <= 1 * GIB for size in first_large.sizes)


def test_full_workload_comparison_requires_no_payload_allocation() -> None:
    small, large = generate_workloads()
    for workload in (small, large):
        replication, erasure = theoretical_rows(workload)
        assert replication["logical"] == erasure["logical"] == sum(workload.sizes)
        assert replication["overhead"] == 3
        assert 1.5 <= erasure["overhead"] < 1.501
        assert erasure["repair"] < replication["repair"]

