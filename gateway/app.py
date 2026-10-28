import os
import re
import time
import json
import logging
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Protocol
from pathlib import Path

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.staticfiles import StaticFiles
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field


BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
LOGGER = logging.getLogger("gateway")


@dataclass(frozen=True)
class GatewaySettings:
    metadata_url: str = "http://metadata:9000"
    api_keys: tuple[str, ...] = ("dev-secret-key",)
    rate_limit_requests: int = 100
    rate_limit_window_seconds: float = 60.0
    elasticsearch_url: str = "http://elasticsearch:9200"
    elasticsearch_index: str = "object-metadata"

    @classmethod
    def from_environment(cls) -> "GatewaySettings":
        return cls(
            metadata_url=os.getenv("METADATA_URL", "http://metadata:9000"),
            api_keys=tuple(key for key in os.getenv("API_KEYS", "dev-secret-key").split(",") if key),
            rate_limit_requests=int(os.getenv("RATE_LIMIT_REQUESTS", "100")),
            rate_limit_window_seconds=float(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60")),
            elasticsearch_url=os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200"),
            elasticsearch_index=os.getenv("ELASTICSEARCH_INDEX", "object-metadata"),
        )


class MetadataTransport(Protocol):
    async def request(self, method: str, path: str, **kwargs) -> httpx.Response: ...
    async def close(self) -> None: ...


class HTTPMetadataTransport:
    def __init__(self, base_url: str) -> None:
        self.client = httpx.AsyncClient(base_url=base_url, timeout=30.0)

    async def request(self, method: str, path: str, **kwargs) -> httpx.Response:
        return await self.client.request(method, path, **kwargs)

    async def close(self) -> None:
        await self.client.aclose()


class SearchTransport(Protocol):
    async def index_object(self, metadata: dict) -> None: ...
    async def delete_object(self, bucket: str, key: str, version: int) -> None: ...
    async def search(self, **filters) -> list[dict]: ...
    async def close(self) -> None: ...


class ElasticsearchTransport:
    def __init__(self, base_url: str, index: str) -> None:
        self.client = httpx.AsyncClient(base_url=base_url, timeout=3.0)
        self.index = index

    @staticmethod
    def document_id(bucket: str, key: str, version: int) -> str:
        import hashlib
        return hashlib.sha256(f"{bucket}\0{key}\0{version}".encode()).hexdigest()

    async def index_object(self, metadata: dict) -> None:
        document_id = self.document_id(metadata["bucket"], metadata["key"], metadata["version"])
        response = await self.client.put(f"/{self.index}/_doc/{document_id}", json=metadata)
        response.raise_for_status()

    async def delete_object(self, bucket: str, key: str, version: int) -> None:
        response = await self.client.delete(f"/{self.index}/_doc/{self.document_id(bucket, key, version)}")
        if response.status_code != 404:
            response.raise_for_status()

    async def search(self, **filters) -> list[dict]:
        clauses: list[dict] = []
        if filters.get("bucket"):
            clauses.append({"term": {"bucket.keyword": filters["bucket"]}})
        if filters.get("prefix"):
            clauses.append({"prefix": {"key.keyword": filters["prefix"]}})
        ranges = {key: value for key, value in {
            "gte": filters.get("min_size"), "lte": filters.get("max_size")}.items() if value is not None}
        if ranges:
            clauses.append({"range": {"size": ranges}})
        date_ranges = {key: value for key, value in {
            "gte": filters.get("created_after"), "lte": filters.get("created_before")}.items() if value}
        if date_ranges:
            clauses.append({"range": {"created_at": date_ranges}})
        for key, value in (filters.get("tags") or {}).items():
            clauses.append({"term": {f"tags.{key}.keyword": value}})
        query = {"query": {"bool": {"filter": clauses}}} if clauses else {"query": {"match_all": {}}}
        response = await self.client.post(f"/{self.index}/_search", json=query)
        response.raise_for_status()
        return [hit["_source"] for hit in response.json()["hits"]["hits"]]

    async def close(self) -> None:
        await self.client.aclose()


class BucketCreate(BaseModel):
    name: str = Field(min_length=3, max_length=63)


class RateLimiter:
    def __init__(self, limit: int, window: float) -> None:
        self.limit, self.window = limit, window
        self.requests: dict[str, deque[float]] = defaultdict(deque)

    def check(self, key: str) -> None:
        now = time.monotonic()
        history = self.requests[key]
        while history and history[0] <= now - self.window:
            history.popleft()
        if len(history) >= self.limit:
            raise HTTPException(status_code=429, detail="Rate limit exceeded", headers={"Retry-After": str(int(self.window))})
        history.append(now)


def parse_range(value: str, size: int) -> tuple[int, int]:
    if not value.startswith("bytes=") or "," in value:
        raise HTTPException(status_code=416, detail="Invalid byte range", headers={"Content-Range": f"bytes */{size}"})
    start_text, end_text = value[6:].split("-", 1)
    try:
        if not start_text:
            length = int(end_text)
            if length <= 0:
                raise ValueError
            start, end = max(0, size - length), size - 1
        else:
            start = int(start_text)
            end = min(int(end_text), size - 1) if end_text else size - 1
            if start < 0 or start >= size or end < start:
                raise ValueError
    except ValueError as exc:
        raise HTTPException(status_code=416, detail="Unsatisfiable byte range",
                            headers={"Content-Range": f"bytes */{size}"}) from exc
    return start, end


def create_app(settings: GatewaySettings | None = None,
               transport: MetadataTransport | None = None,
               search_client: SearchTransport | None = None) -> FastAPI:
    resolved = settings or GatewaySettings.from_environment()
    metadata = transport or HTTPMetadataTransport(resolved.metadata_url)
    search = search_client or ElasticsearchTransport(resolved.elasticsearch_url, resolved.elasticsearch_index)
    limiter = RateLimiter(resolved.rate_limit_requests, resolved.rate_limit_window_seconds)
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        await metadata.close()
        await search.close()

    app = FastAPI(title="Distributed Object Storage Gateway", lifespan=lifespan)
    metric_counts: dict[tuple[str, str, int], int] = defaultdict(int)
    metric_latency: dict[tuple[str, str], float] = defaultdict(float)

    @app.middleware("http")
    async def collect_metrics(request: Request, call_next):
        started = time.perf_counter()
        response = await call_next(request)
        route = request.url.path
        metric_counts[request.method, route, response.status_code] += 1
        metric_latency[request.method, route] += time.perf_counter() - started
        return response

    async def authorize(x_api_key: str | None = Header(None)) -> str:
        if x_api_key not in resolved.api_keys:
            raise HTTPException(status_code=401, detail="Invalid or missing API key")
        limiter.check(x_api_key)
        return x_api_key

    async def relay(response: httpx.Response) -> httpx.Response:
        if response.status_code >= 400:
            detail = response.text
            try:
                detail = response.json().get("detail", detail)
            except Exception:
                pass
            raise HTTPException(status_code=response.status_code, detail=detail)
        return response

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics() -> str:
        lines = ["# TYPE gateway_requests_total counter"]
        for (method, route, status), count in sorted(metric_counts.items()):
            lines.append(f'gateway_requests_total{{method="{method}",route="{route}",status="{status}"}} {count}')
        lines.append("# TYPE gateway_request_latency_seconds_sum counter")
        for (method, route), latency in sorted(metric_latency.items()):
            lines.append(f'gateway_request_latency_seconds_sum{{method="{method}",route="{route}"}} {latency}')
        return "\n".join(lines) + "\n"

    @app.post("/buckets", status_code=201, dependencies=[Depends(authorize)])
    async def create_bucket(payload: BucketCreate) -> dict:
        if not BUCKET_PATTERN.fullmatch(payload.name):
            raise HTTPException(status_code=422, detail="Invalid bucket name")
        return (await relay(await metadata.request("POST", f"/catalog/buckets/{payload.name}"))).json()

    @app.get("/buckets", dependencies=[Depends(authorize)])
    async def list_buckets() -> list[dict]:
        return (await relay(await metadata.request("GET", "/catalog/buckets"))).json()

    @app.put("/buckets/{bucket}/objects/{key:path}", status_code=201, dependencies=[Depends(authorize)])
    async def upload_object(bucket: str, key: str, request: Request,
                            chunking: str = Query("fixed", pattern="^(fixed|cdc)$"),
                            x_object_tags: str = Header("{}")) -> dict:
        response = await metadata.request("POST", f"/objects/{bucket}/{key}",
                                          params={"strategy": chunking}, content=request.stream(),
                                          headers={"X-Object-Tags": x_object_tags})
        result = (await relay(response)).json()
        try:
            await search.index_object(result)
        except httpx.HTTPError as exc:
            LOGGER.warning("Elasticsearch indexing failed: %s", exc)
        return result

    @app.get("/buckets/{bucket}/objects", dependencies=[Depends(authorize)])
    async def list_objects(bucket: str, prefix: str | None = None, min_size: int | None = None,
                           max_size: int | None = None, created_after: str | None = None,
                           created_before: str | None = None, tags: str | None = None) -> list[dict]:
        params = {key: value for key, value in locals().items() if key != "bucket" and value is not None}
        response = await metadata.request("GET", f"/catalog/objects/{bucket}", params=params)
        return (await relay(response)).json()

    @app.get("/buckets/{bucket}/objects/{key:path}/metadata", dependencies=[Depends(authorize)])
    async def object_metadata(bucket: str, key: str, version: int | None = None) -> dict:
        response = await metadata.request("GET", f"/objects/{bucket}/{key}",
                                          params={"version": version} if version else {})
        return (await relay(response)).json()

    @app.get("/buckets/{bucket}/objects/{key:path}", dependencies=[Depends(authorize)])
    async def download_object(bucket: str, key: str, version: int | None = None,
                              range_header: str | None = Header(None, alias="Range")) -> Response:
        upstream = await relay(await metadata.request("GET", f"/data/{bucket}/{key}",
                                                       params={"version": version} if version else {}))
        data = upstream.content
        headers = {"Accept-Ranges": "bytes"}
        if range_header:
            start, end = parse_range(range_header, len(data))
            headers["Content-Range"] = f"bytes {start}-{end}/{len(data)}"
            return Response(data[start:end + 1], status_code=206,
                            media_type="application/octet-stream", headers=headers)
        return Response(data, media_type="application/octet-stream", headers=headers)

    @app.delete("/buckets/{bucket}/objects/{key:path}", status_code=204, dependencies=[Depends(authorize)])
    async def delete_object(bucket: str, key: str, version: int | None = None) -> Response:
        if version is None:
            current = await relay(await metadata.request("GET", f"/objects/{bucket}/{key}"))
            version = current.json()["version"]
        await relay(await metadata.request("DELETE", f"/objects/{bucket}/{key}", params={"version": version}))
        try:
            await search.delete_object(bucket, key, version)
        except httpx.HTTPError as exc:
            LOGGER.warning("Elasticsearch deletion failed: %s", exc)
        return Response(status_code=204)

    @app.get("/cluster/status", dependencies=[Depends(authorize)])
    async def cluster_status() -> dict:
        return (await relay(await metadata.request("GET", "/cluster/status"))).json()

    @app.get("/cluster/repairs", dependencies=[Depends(authorize)])
    async def repair_progress() -> list[dict]:
        return (await relay(await metadata.request("GET", "/repairs"))).json()

    @app.get("/search/objects", dependencies=[Depends(authorize)])
    async def search_objects(bucket: str | None = None, prefix: str | None = None,
                             min_size: int | None = None, max_size: int | None = None,
                             created_after: str | None = None, created_before: str | None = None,
                             tags: str | None = None) -> list[dict]:
        try:
            parsed_tags = json.loads(tags) if tags else {}
            return await search.search(bucket=bucket, prefix=prefix, min_size=min_size,
                                       max_size=max_size, created_after=created_after,
                                       created_before=created_before, tags=parsed_tags)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=422, detail="tags must be a JSON object") from exc
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=503, detail="Elasticsearch is unavailable") from exc

    web_root = Path(__file__).resolve().parent.parent / "web"
    if web_root.is_dir():
        app.mount("/ui", StaticFiles(directory=web_root, html=True), name="ui")

    return app


app = create_app()

