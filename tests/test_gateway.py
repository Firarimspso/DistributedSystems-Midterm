import json
from datetime import datetime, timezone
from urllib.parse import unquote

import httpx
from fastapi.testclient import TestClient

from gateway.app import GatewaySettings, create_app


class FakeMetadataTransport:
    def __init__(self) -> None:
        self.buckets: set[str] = set()
        self.objects: dict[tuple[str, str], list[dict]] = {}

    async def close(self) -> None:
        pass

    async def request(self, method: str, path: str, **kwargs) -> httpx.Response:
        path = unquote(path)
        params = kwargs.get("params", {})
        if path == "/catalog/buckets" and method == "GET":
            return httpx.Response(200, json=[{"bucket": bucket} for bucket in sorted(self.buckets)])
        if path.startswith("/catalog/buckets/") and method == "POST":
            bucket = path.rsplit("/", 1)[1]
            if bucket in self.buckets:
                return httpx.Response(409, json={"detail": "Bucket already exists"})
            self.buckets.add(bucket)
            return httpx.Response(201, json={"name": bucket})
        if path.startswith("/catalog/objects/") and method == "GET":
            bucket = path.rsplit("/", 1)[1]
            rows = [version for (row_bucket, _), versions in self.objects.items()
                    if row_bucket == bucket for version in versions]
            if params.get("prefix"):
                rows = [row for row in rows if row["key"].startswith(params["prefix"])]
            if params.get("min_size") is not None:
                rows = [row for row in rows if row["size"] >= int(params["min_size"])]
            if params.get("max_size") is not None:
                rows = [row for row in rows if row["size"] <= int(params["max_size"])]
            if params.get("tags"):
                tags = json.loads(params["tags"])
                rows = [row for row in rows if all(row["tags"].get(k) == v for k, v in tags.items())]
            return httpx.Response(200, json=[{k: v for k, v in row.items() if k != "content"} for row in rows])
        if path == "/cluster/status":
            return httpx.Response(200, json={"nodes": [{"node_id": "node1", "status": "active",
                "free_space_bytes": 1000, "chunk_count": 2}], "total_free_space_bytes": 1000,
                "total_chunks": 2, "degraded_objects": []})
        if path == "/repairs":
            return httpx.Response(200, json=[{"id": 1, "event_type": "scrub", "status": "repaired",
                "strategy": "replication", "detail": "replica repaired", "bytes_transferred": 20}])
        if path.startswith("/objects/"):
            bucket, key = path[len("/objects/"):].split("/", 1)
            object_key = (bucket, key)
            if method == "POST":
                body = kwargs.get("content", b"")
                if hasattr(body, "__aiter__"):
                    body = b"".join([block async for block in body])
                versions = self.objects.setdefault(object_key, [])
                row = {"bucket": bucket, "key": key, "version": len(versions) + 1,
                       "size": len(body), "created_at": datetime.now(timezone.utc).isoformat(),
                       "chunk_hashes": [], "redundancy_strategy": "replication",
                       "tags": json.loads(kwargs.get("headers", {}).get("X-Object-Tags", "{}")),
                       "content": body}
                versions.append(row)
                return httpx.Response(201, json={k: v for k, v in row.items() if k != "content"})
            versions = self.objects.get(object_key, [])
            if not versions:
                return httpx.Response(404, json={"detail": "Object version not found"})
            version = int(params.get("version") or len(versions))
            row = next((item for item in versions if item["version"] == version), None)
            if row is None:
                return httpx.Response(404, json={"detail": "Object version not found"})
            if method == "GET":
                return httpx.Response(200, json={k: v for k, v in row.items() if k != "content"})
            if method == "DELETE":
                versions.remove(row)
                return httpx.Response(204)
        if path.startswith("/data/") and method == "GET":
            bucket, key = path[len("/data/"):].split("/", 1)
            versions = self.objects.get((bucket, key), [])
            if not versions:
                return httpx.Response(404, json={"detail": "Object version not found"})
            version = int(params.get("version") or len(versions))
            row = next(item for item in versions if item["version"] == version)
            return httpx.Response(200, content=row["content"])
        return httpx.Response(404, json={"detail": "Unknown metadata route"})


class FakeSearchTransport:
    def __init__(self) -> None:
        self.documents: list[dict] = []

    async def close(self) -> None:
        pass

    async def index_object(self, metadata: dict) -> None:
        self.documents.append(metadata)

    async def delete_object(self, bucket: str, key: str, version: int) -> None:
        self.documents = [row for row in self.documents
                          if (row["bucket"], row["key"], row["version"]) != (bucket, key, version)]

    async def search(self, **filters) -> list[dict]:
        rows = self.documents
        if filters.get("bucket"):
            rows = [row for row in rows if row["bucket"] == filters["bucket"]]
        if filters.get("prefix"):
            rows = [row for row in rows if row["key"].startswith(filters["prefix"])]
        return rows


def make_client(limit: int = 100) -> tuple[TestClient, FakeMetadataTransport]:
    transport = FakeMetadataTransport()
    settings = GatewaySettings(api_keys=("test-key",), rate_limit_requests=limit,
                               rate_limit_window_seconds=60)
    return TestClient(create_app(settings, transport, FakeSearchTransport())), transport


HEADERS = {"X-API-Key": "test-key"}


def test_authentication_required() -> None:
    client, _ = make_client()
    assert client.get("/buckets").status_code == 401
    assert client.get("/buckets", headers={"X-API-Key": "wrong"}).status_code == 401


def test_react_ui_assets_are_served() -> None:
    client, _ = make_client()
    page = client.get("/ui/")
    assert page.status_code == 200
    assert "react.production.min.js" in page.text
    script = client.get("/ui/app.js")
    assert script.status_code == 200
    assert "Distributed Object Storage" in script.text


def test_gateway_prometheus_metrics() -> None:
    client, _ = make_client()
    client.get("/health")
    metrics = client.get("/metrics")
    assert metrics.status_code == 200
    assert "gateway_requests_total" in metrics.text
    assert "gateway_request_latency_seconds_sum" in metrics.text


def test_create_and_list_buckets() -> None:
    client, _ = make_client()
    assert client.post("/buckets", headers=HEADERS, json={"name": "course-data"}).status_code == 201
    response = client.get("/buckets", headers=HEADERS)
    assert response.status_code == 200
    assert response.json()[0]["bucket"] == "course-data"


def test_stream_upload_download_metadata_and_delete() -> None:
    client, _ = make_client()
    client.post("/buckets", headers=HEADERS, json={"name": "course-data"})
    content = b"streamed-" + b"object-data" * 100
    headers = {**HEADERS, "X-Object-Tags": '{"course":"distributed-systems"}'}
    uploaded = client.put("/buckets/course-data/objects/notes/week1.bin", headers=headers,
                          content=iter([content[:20], content[20:]]))
    assert uploaded.status_code == 201 and uploaded.json()["size"] == len(content)
    downloaded = client.get("/buckets/course-data/objects/notes/week1.bin", headers=HEADERS)
    assert downloaded.content == content
    metadata = client.get("/buckets/course-data/objects/notes/week1.bin/metadata", headers=HEADERS)
    assert metadata.json()["tags"] == {"course": "distributed-systems"}
    assert client.delete("/buckets/course-data/objects/notes/week1.bin", headers=HEADERS).status_code == 204
    assert client.get("/buckets/course-data/objects/notes/week1.bin", headers=HEADERS).status_code == 404


def test_byte_ranges() -> None:
    client, _ = make_client()
    content = b"0123456789"
    client.put("/buckets/data-bucket/objects/value", headers=HEADERS, content=content)
    response = client.get("/buckets/data-bucket/objects/value", headers={**HEADERS, "Range": "bytes=2-5"})
    assert response.status_code == 206 and response.content == b"2345"
    assert response.headers["content-range"] == "bytes 2-5/10"
    assert client.get("/buckets/data-bucket/objects/value",
                      headers={**HEADERS, "Range": "bytes=-3"}).content == b"789"
    assert client.get("/buckets/data-bucket/objects/value",
                      headers={**HEADERS, "Range": "bytes=99-100"}).status_code == 416


def test_listing_and_search_filters() -> None:
    client, _ = make_client()
    client.put("/buckets/search-bucket/objects/docs/a.txt",
               headers={**HEADERS, "X-Object-Tags": '{"type":"text"}'}, content=b"abc")
    client.put("/buckets/search-bucket/objects/images/a.png",
               headers={**HEADERS, "X-Object-Tags": '{"type":"image"}'}, content=b"123456789")
    response = client.get("/buckets/search-bucket/objects", headers=HEADERS,
                          params={"prefix": "docs/", "max_size": 4, "tags": '{"type":"text"}'})
    assert response.status_code == 200
    assert [row["key"] for row in response.json()] == ["docs/a.txt"]


def test_cluster_status() -> None:
    client, _ = make_client()
    response = client.get("/cluster/status", headers=HEADERS)
    assert response.status_code == 200
    assert response.json()["nodes"][0]["chunk_count"] == 2
    assert response.json()["degraded_objects"] == []
    repairs = client.get("/cluster/repairs", headers=HEADERS)
    assert repairs.status_code == 200 and repairs.json()[0]["status"] == "repaired"


def test_elasticsearch_metadata_search() -> None:
    client, _ = make_client()
    client.put("/buckets/search-bucket/objects/docs/report.pdf", headers=HEADERS, content=b"report")
    client.put("/buckets/search-bucket/objects/images/chart.png", headers=HEADERS, content=b"chart")
    response = client.get("/search/objects", headers=HEADERS,
                          params={"bucket": "search-bucket", "prefix": "docs/"})
    assert response.status_code == 200
    assert [row["key"] for row in response.json()] == ["docs/report.pdf"]


def test_rate_limiting() -> None:
    client, _ = make_client(limit=2)
    assert client.get("/buckets", headers=HEADERS).status_code == 200
    assert client.get("/buckets", headers=HEADERS).status_code == 200
    response = client.get("/buckets", headers=HEADERS)
    assert response.status_code == 429
    assert response.headers["retry-after"] == "60"

