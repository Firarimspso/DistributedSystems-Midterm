"""Deterministic, lightweight storage-node scale-out demonstration."""

import hashlib
from dataclasses import dataclass

from metadata_service.hashing import ConsistentHashRing


@dataclass(frozen=True)
class ScaleOutResult:
    chunks: int
    moved_chunks: int
    moved_percentage: float
    repair_traffic_bytes: int
    integrity_verified: bool


def run_scale_out_demo(sample_size: int = 1_000) -> ScaleOutResult:
    before_ring = ConsistentHashRing(["node1", "node2", "node3", "node4"], virtual_nodes=128)
    after_ring = ConsistentHashRing(["node1", "node2", "node3", "node4", "node5"], virtual_nodes=128)
    physical: dict[str, dict[str, bytes]] = {f"node{i}": {} for i in range(1, 6)}
    chunks: dict[str, bytes] = {}

    for index in range(sample_size):
        data = f"sample-{index}".encode()
        chunk_hash = hashlib.sha256(data).hexdigest()
        chunks[chunk_hash] = data
        physical[before_ring.get_node(chunk_hash)][chunk_hash] = data

    moved = traffic = 0
    for chunk_hash, data in chunks.items():
        old_node = before_ring.get_node(chunk_hash)
        new_node = after_ring.get_node(chunk_hash)
        if old_node != new_node:
            # This copy/delete sequence is the same content-addressed operation
            # used by the repair/rebalance layer, but against an in-memory store.
            physical[new_node][chunk_hash] = physical[old_node][chunk_hash]
            del physical[old_node][chunk_hash]
            moved += 1
            traffic += len(data)

    integrity = all(
        hashlib.sha256(physical[after_ring.get_node(chunk_hash)][chunk_hash]).hexdigest() == chunk_hash
        for chunk_hash in chunks
    )
    return ScaleOutResult(sample_size, moved, moved * 100 / sample_size, traffic, integrity)


def main() -> None:
    result = run_scale_out_demo()
    print("SECTION 8 STORAGE SCALE-OUT DEMONSTRATION")
    print("Initial nodes: node1, node2, node3, node4")
    print("Added node:    node5")
    print(f"Chunks:        {result.chunks}")
    print(f"Moved:         {result.moved_chunks} ({result.moved_percentage:.2f}%)")
    print(f"Traffic:       {result.repair_traffic_bytes} bytes")
    print(f"SHA-256:       {'PASS' if result.integrity_verified else 'FAIL'}")


if __name__ == "__main__":
    main()

