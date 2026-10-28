import hashlib
from pathlib import Path

from fastapi.testclient import TestClient

from storage_node.app import StorageSettings, create_app


def make_client(storage_path: Path) -> TestClient:
    settings = StorageSettings(
        node_id="test-node",
        storage_path=storage_path,
        metadata_url=None,
    )
    return TestClient(create_app(settings))


def upload(client: TestClient, content: bytes) -> str:
    chunk_hash = hashlib.sha256(content).hexdigest()
    response = client.put(f"/chunks/{chunk_hash}", content=content)
    assert response.status_code == 201
    return chunk_hash


def test_successful_chunk_upload(tmp_path: Path) -> None:
    content = b"distributed systems"
    chunk_hash = hashlib.sha256(content).hexdigest()
    with make_client(tmp_path) as client:
        response = client.put(f"/chunks/{chunk_hash}", content=content)
    assert response.status_code == 201
    assert response.json() == {"chunk_hash": chunk_hash, "size": len(content)}
    assert (tmp_path / chunk_hash).read_bytes() == content


def test_successful_chunk_retrieval(tmp_path: Path) -> None:
    content = b"retrieve this chunk"
    with make_client(tmp_path) as client:
        chunk_hash = upload(client, content)
        response = client.get(f"/chunks/{chunk_hash}")
    assert response.status_code == 200
    assert response.content == content
    assert response.headers["etag"] == f'"{chunk_hash}"'


def test_head_request(tmp_path: Path) -> None:
    content = b"chunk metadata"
    with make_client(tmp_path) as client:
        chunk_hash = upload(client, content)
        response = client.head(f"/chunks/{chunk_hash}")
    assert response.status_code == 200
    assert response.content == b""
    assert response.headers["content-length"] == str(len(content))


def test_chunk_deletion(tmp_path: Path) -> None:
    with make_client(tmp_path) as client:
        chunk_hash = upload(client, b"delete this chunk")
        response = client.delete(f"/chunks/{chunk_hash}")
        missing_response = client.get(f"/chunks/{chunk_hash}")
    assert response.status_code == 204
    assert missing_response.status_code == 404


def test_invalid_sha256_hash(tmp_path: Path) -> None:
    with make_client(tmp_path) as client:
        response = client.put("/chunks/not-a-sha256", content=b"content")
    assert response.status_code == 422
    assert "64 lowercase hexadecimal" in response.json()["detail"]


def test_hash_mismatch_on_upload(tmp_path: Path) -> None:
    wrong_hash = hashlib.sha256(b"different").hexdigest()
    with make_client(tmp_path) as client:
        response = client.put(f"/chunks/{wrong_hash}", content=b"actual content")
    assert response.status_code == 400
    assert "Content hash mismatch" in response.json()["detail"]


def test_corrupted_chunk_detection_on_get_and_head(tmp_path: Path) -> None:
    with make_client(tmp_path) as client:
        chunk_hash = upload(client, b"original content")
        (tmp_path / chunk_hash).write_bytes(b"corrupted content")
        get_response = client.get(f"/chunks/{chunk_hash}")
        head_response = client.head(f"/chunks/{chunk_hash}")
    assert get_response.status_code == 409
    assert "integrity check failed" in get_response.json()["detail"]
    assert head_response.status_code == 409
    assert head_response.content == b""
    assert head_response.headers["x-chunk-error"] == "integrity-check-failed"


def test_missing_chunk(tmp_path: Path) -> None:
    missing_hash = hashlib.sha256(b"missing").hexdigest()
    with make_client(tmp_path) as client:
        get_response = client.get(f"/chunks/{missing_hash}")
        head_response = client.head(f"/chunks/{missing_hash}")
        delete_response = client.delete(f"/chunks/{missing_hash}")
    assert get_response.status_code == 404
    assert head_response.status_code == 404
    assert delete_response.status_code == 404


def test_storage_node_prometheus_metrics(tmp_path: Path) -> None:
    with make_client(tmp_path) as client:
        upload(client, b"metrics chunk")
        response = client.get("/metrics")
    assert response.status_code == 200
    assert 'storage_node_disk_free_bytes{node_id="test-node"}' in response.text
    assert 'storage_node_chunks{node_id="test-node"} 1' in response.text

