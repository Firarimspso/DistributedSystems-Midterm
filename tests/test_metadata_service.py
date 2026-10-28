import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from metadata_service.app import MetadataSettings, create_app
from metadata_service.models import Base, ChunkRecord, NodeHeartbeat, ObjectVersion


TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+psycopg://storage:storage@localhost:5434/storage_metadata_test",
)


class FakeStorage:
    def __init__(self) -> None:
        self.chunks: dict[tuple[str, str], bytes] = {}
        self.delete_calls: list[tuple[str, str]] = []

    def exists(self, node_id: str, chunk_hash: str) -> bool:
        return (node_id, chunk_hash) in self.chunks

    def put(self, node_id: str, chunk_hash: str, data: bytes) -> None:
        self.chunks[node_id, chunk_hash] = data

    def get(self, node_id: str, chunk_hash: str) -> bytes:
        return self.chunks[node_id, chunk_hash]

    def delete(self, node_id: str, chunk_hash: str) -> None:
        self.delete_calls.append((node_id, chunk_hash))
        self.chunks.pop((node_id, chunk_hash), None)


@pytest.fixture()
def metadata_context():
    engine = create_engine(TEST_DATABASE_URL)
    try:
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
    except Exception as exc:
        pytest.fail(f"PostgreSQL integration database is unavailable: {exc}")
    storage = FakeStorage()
    settings = MetadataSettings(
        database_url=TEST_DATABASE_URL,
        node_urls={f"node{index}": f"http://node{index}:8000" for index in range(1, 5)},
        inactive_after_seconds=10,
        replication_factor=1,
        read_quorum=1,
        write_quorum=1,
        repair_interval_seconds=0,
        scrub_interval_seconds=0,
    )
    app = create_app(settings, storage_client=storage, engine=engine)
    with TestClient(app) as client:
        yield client, storage, engine
    Base.metadata.drop_all(engine)


def test_metadata_persists_in_postgresql(metadata_context) -> None:
    client, _, engine = metadata_context
    response = client.post("/objects/course/notes/week1.txt", content=b"metadata persistence")
    assert response.status_code == 201
    with Session(engine) as session:
        persisted = session.scalar(select(ObjectVersion))
        assert persisted is not None
        assert (persisted.bucket, persisted.object_key, persisted.size) == (
            "course",
            "notes/week1.txt",
            len(b"metadata persistence"),
        )


def test_deduplication_and_reference_count_changes(metadata_context) -> None:
    client, storage, engine = metadata_context
    content = b"same chunk content"
    first = client.post("/objects/bucket/object", content=content)
    second = client.post("/objects/bucket/object", content=content)
    assert first.status_code == second.status_code == 201
    assert len(storage.chunks) == 1
    with Session(engine) as session:
        record = session.scalar(select(ChunkRecord))
        assert record is not None and record.reference_count == 2

    assert client.delete("/objects/bucket/object?version=1").status_code == 204
    assert len(storage.chunks) == 1
    assert storage.delete_calls == []
    with Session(engine) as session:
        assert session.scalar(select(ChunkRecord)).reference_count == 1

    assert client.delete("/objects/bucket/object?version=2").status_code == 204
    assert storage.chunks == {}
    assert len(storage.delete_calls) == 1
    with Session(engine) as session:
        assert session.scalar(select(ChunkRecord)) is None


def test_repeated_chunk_occurrences_count_as_references(metadata_context) -> None:
    client, _, engine = metadata_context
    repeated = b"x" * (4 * 1024 * 1024)
    response = client.post("/objects/bucket/repeated", content=repeated + repeated)
    assert response.status_code == 201
    assert len(response.json()["chunk_hashes"]) == 2
    assert response.json()["chunk_hashes"][0] == response.json()["chunk_hashes"][1]
    with Session(engine) as session:
        assert session.scalar(select(ChunkRecord)).reference_count == 2


def test_heartbeat_reception_and_node_status(metadata_context) -> None:
    client, _, engine = metadata_context
    response = client.post(
        "/heartbeats",
        json={"node_id": "node1", "free_space_bytes": 108_000_000_000, "chunk_count": 7},
    )
    assert response.status_code == 200
    assert client.get("/nodes").json()[0]["status"] == "active"

    with Session(engine) as session:
        record = session.get(NodeHeartbeat, "node1")
        record.last_heartbeat = datetime.now(timezone.utc) - timedelta(seconds=30)
        session.commit()
    nodes = client.get("/nodes").json()
    assert nodes[0]["status"] == "inactive"
    assert nodes[0]["free_space_bytes"] == 108_000_000_000
    assert nodes[0]["chunk_count"] == 7


def test_metadata_prometheus_metrics(metadata_context) -> None:
    client, _, _ = metadata_context
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "storage_degraded_chunks" in response.text
    assert "storage_repairs_total" in response.text

