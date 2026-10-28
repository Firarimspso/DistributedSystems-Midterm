import hashlib

from metadata_service.hashing import ConsistentHashRing


SAMPLE_HASHES = [hashlib.sha256(f"sample-{index}".encode()).hexdigest() for index in range(1000)]
INITIAL_NODES = ["node1", "node2", "node3", "node4"]


def placements(ring: ConsistentHashRing) -> list[str]:
    return [ring.get_node(chunk_hash) for chunk_hash in SAMPLE_HASHES]


def movement_percentage(before: list[str], after: list[str]) -> float:
    moved = sum(old != new for old, new in zip(before, after, strict=True))
    return moved * 100 / len(before)


def test_consistent_hashing_is_deterministic_and_selects_distinct_nodes() -> None:
    first = ConsistentHashRing(INITIAL_NODES, virtual_nodes=128)
    second = ConsistentHashRing(reversed(INITIAL_NODES), virtual_nodes=128)
    assert placements(first) == placements(second)
    assert len(set(first.get_nodes(SAMPLE_HASHES[0], 3))) == 3


def test_adding_node_measures_movement() -> None:
    ring = ConsistentHashRing(INITIAL_NODES, virtual_nodes=128)
    before = placements(ring)
    ring.add_node("node5")
    percentage = movement_percentage(before, placements(ring))
    print(f"add-node primary movement: {percentage:.2f}% (sample=1000)")
    assert 0 < percentage < 100


def test_removing_node_measures_movement() -> None:
    ring = ConsistentHashRing(INITIAL_NODES, virtual_nodes=128)
    before = placements(ring)
    ring.remove_node("node4")
    percentage = movement_percentage(before, placements(ring))
    print(f"remove-node primary movement: {percentage:.2f}% (sample=1000)")
    assert 0 < percentage < 100

