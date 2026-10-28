import hashlib
import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, selectinload

from metadata_service.app import MetadataSettings, create_app
from metadata_service.comparison import comparison_rows
from metadata_service.hashing import ConsistentHashRing
from metadata_service.models import Base, ChunkProtection, NodeHeartbeat
from metadata_service.redundancy import (
    QuorumError,
    RedundancyError,
    RedundancyManager,
    decode_shards,
    encode_shards,
)


TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+psycopg://storage:storage@localhost:5434/storage_metadata_test",
)
NODE_URLS = {f"node{i}": f"http://node{i}:8000" for i in range(1, 7)}


class MemoryStorage:
    def __init__(self) -> None:
        self.chunks: dict[tuple[str, str], bytes] = {}
        self.unavailable: set[str] = set()

    def exists(self, node_id: str, chunk_hash: str) -> bool:
        self._check(node_id)
        return (node_id, chunk_hash) in self.chunks

    def put(self, node_id: str, chunk_hash: str, data: bytes) -> None:
        self._check(node_id)
        self.chunks[node_id, chunk_hash] = data

    def get(self, node_id: str, chunk_hash: str) -> bytes:
        self._check(node_id)
        return self.chunks[node_id, chunk_hash]

    def delete(self, node_id: str, chunk_hash: str) -> None:
        self._check(node_id)
        self.chunks.pop((node_id, chunk_hash), None)

    def _check(self, node_id: str) -> None:
        if node_id in self.unavailable:
            raise OSError(f"{node_id} unavailable")


def make_manager(storage: MemoryStorage) -> RedundancyManager:
    return RedundancyManager(storage, ConsistentHashRing(NODE_URLS, 128), NODE_URLS)


def protection_for(data: bytes, placements, read_quorum: int = 2) -> ChunkProtection:
    protection = ChunkProtection(
        chunk_hash=hashlib.sha256(data).hexdigest(),
        strategy="replication",
        replication_factor=3,
        read_quorum=read_quorum,
        write_quorum=2,
        data_shards=4,
        parity_shards=2,
    )
    for result in placements:
        from metadata_service.models import ChunkPlacement

        protection.placements.append(
            ChunkPlacement(
                node_id=result.node_id,
                storage_hash=result.storage_hash,
                size=result.size,
                healthy=True,
            )
        )
    return protection


def test_replication_n3_write_w2_and_read_r2() -> None:
    storage = MemoryStorage()
    manager = make_manager(storage)
    data = b"quorum replication"
    chunk_hash = hashlib.sha256(data).hexdigest()
    placements = manager.write_replication(chunk_hash, data, 3, 2)
    assert len(placements) == 3
    assert len({placement.node_id for placement in placements}) == 3
    assert manager.read_replication(protection_for(data, placements)) == data


def test_write_and_read_quorum_failure() -> None:
    data = b"quorum failure"
    chunk_hash = hashlib.sha256(data).hexdigest()
    storage = MemoryStorage()
    manager = make_manager(storage)
    targets = manager.ring.get_nodes(chunk_hash, 3)
    storage.unavailable.update(targets[:2])
    with pytest.raises(QuorumError, match="write quorum failed"):
        manager.write_replication(chunk_hash, data, 3, 2)

    storage = MemoryStorage()
    manager = make_manager(storage)
    placements = manager.write_replication(chunk_hash, data, 3, 2)
    storage.unavailable.update([placements[0].node_id, placements[1].node_id])
    with pytest.raises(QuorumError, match="read quorum failed"):
        manager.read_replication(protection_for(data, placements))


def test_replica_integrity_validation() -> None:
    storage = MemoryStorage()
    manager = make_manager(storage)
    data = b"integrity"
    chunk_hash = hashlib.sha256(data).hexdigest()
    placements = manager.write_replication(chunk_hash, data, 3, 2)
    storage.chunks[placements[0].node_id, chunk_hash] = b"bit rot"
    protection = protection_for(data, placements)
    assert manager.read_replication(protection) == data
    assert protection.placements[0].healthy is False


def test_reed_solomon_4_plus_2_reconstruction() -> None:
    data = bytes(range(256)) * 41 + b"tail"
    shards = encode_shards(data, 4, 2)
    assert len(shards) == 6
    for missing in ([1], [0, 5]):
        available = [(index, shard) for index, shard in enumerate(shards) if index not in missing]
        rebuilt = decode_shards(
            [shard for _, shard in available],
            [index for index, _ in available],
            4,
            2,
            len(data),
        )
        assert rebuilt == data
    with pytest.raises(RedundancyError, match="at least 4"):
        decode_shards(shards[:3], [0, 1, 2], 4, 2, len(data))


@pytest.fixture()
def recovery_context():
    engine = create_engine(TEST_DATABASE_URL)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    storage = MemoryStorage()
    settings = MetadataSettings(
        database_url=TEST_DATABASE_URL,
        node_urls=NODE_URLS,
        inactive_after_seconds=10,
        repair_interval_seconds=0,
        scrub_interval_seconds=0,
    )
    with TestClient(create_app(settings, storage_client=storage, engine=engine)) as client:
        for node_id in NODE_URLS:
            client.post(
                "/heartbeats",
                json={"node_id": node_id, "free_space_bytes": 10**11, "chunk_count": 0},
            )
        yield client, storage, engine
    Base.metadata.drop_all(engine)


def test_per_bucket_erasure_policy_and_two_missing_shards(recovery_context) -> None:
    client, storage, engine = recovery_context
    policy = client.put(
        "/buckets/archive/redundancy",
        json={
            "strategy": "erasure",
            "replication_factor": 3,
            "read_quorum": 2,
            "write_quorum": 2,
            "data_shards": 4,
            "parity_shards": 2,
        },
    )
    assert policy.status_code == 200 and policy.json()["strategy"] == "erasure"
    content = b"real erasure-coded object" * 100
    response = client.post("/objects/archive/item", content=content)
    assert response.status_code == 201
    with Session(engine) as session:
        protection = session.scalar(
            select(ChunkProtection).options(selectinload(ChunkProtection.placements))
        )
        assert len(protection.placements) == 6
        storage.unavailable.update([protection.placements[0].node_id, protection.placements[1].node_id])
    assert client.get("/data/archive/item").content == content
    with Session(engine) as session:
        protection = session.scalar(
            select(ChunkProtection).options(selectinload(ChunkProtection.placements))
        )
        storage.unavailable.add(protection.placements[2].node_id)
    assert client.get("/data/archive/item").status_code == 503


def test_failed_node_replica_repair(recovery_context) -> None:
    client, storage, engine = recovery_context
    content = b"repair after heartbeat loss"
    assert client.post("/objects/replicated/item", content=content).status_code == 201
    with Session(engine) as session:
        protection = session.scalar(
            select(ChunkProtection).options(selectinload(ChunkProtection.placements))
        )
        failed_node = protection.placements[0].node_id
        old_nodes = {placement.node_id for placement in protection.placements}
        heartbeat = session.get(NodeHeartbeat, failed_node)
        heartbeat.last_heartbeat = datetime.now(timezone.utc) - timedelta(seconds=30)
        session.commit()
    result = client.post("/repairs/run")
    assert result.status_code == 200 and result.json()["repaired"] == 1
    with Session(engine) as session:
        protection = session.scalar(
            select(ChunkProtection).options(selectinload(ChunkProtection.placements))
        )
        new_nodes = {placement.node_id for placement in protection.placements}
        assert len(new_nodes) == 3
        assert failed_node not in new_nodes
        assert new_nodes != old_nodes


def test_scrubber_detects_and_repairs_bit_rot(recovery_context) -> None:
    client, storage, engine = recovery_context
    content = b"scrubber content"
    chunk_hash = hashlib.sha256(content).hexdigest()
    assert client.post("/objects/scrub/item", content=content).status_code == 201
    with Session(engine) as session:
        protection = session.scalar(
            select(ChunkProtection).options(selectinload(ChunkProtection.placements))
        )
        corrupt_node = protection.placements[0].node_id
    storage.chunks[corrupt_node, chunk_hash] = b"corrupted"
    result = client.post("/scrub/run")
    assert result.status_code == 200 and result.json()["repaired"] == 1
    assert storage.chunks[corrupt_node, chunk_hash] == content
    assert client.get("/repairs").json()[0]["event_type"] == "scrub"


def test_storage_overhead_and_repair_traffic_comparison() -> None:
    rows = comparison_rows()
    assert len(rows) == 4
    for workload in {row.workload for row in rows}:
        replication, erasure = [row for row in rows if row.workload == workload]
        assert replication.overhead_ratio == 3.0
        assert erasure.overhead_ratio >= 1.5
        assert erasure.overhead_ratio < replication.overhead_ratio
        assert replication.repair_bytes > erasure.repair_bytes
