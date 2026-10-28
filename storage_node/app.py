import asyncio
import hashlib
import logging
import os
import re
import shutil
import tempfile
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator

import httpx
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.responses import FileResponse
from fastapi.responses import PlainTextResponse


LOGGER = logging.getLogger("storage_node")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class StorageSettings:
    node_id: str
    storage_path: Path
    metadata_url: str | None
    heartbeat_interval_seconds: float = 5.0
    heartbeat_timeout_seconds: float = 2.0

    @classmethod
    def from_environment(cls) -> "StorageSettings":
        metadata_url = os.getenv("METADATA_URL", "http://metadata:9000/heartbeats").strip()
        return cls(
            node_id=os.getenv("NODE_ID", "storage-node"),
            storage_path=Path(os.getenv("STORAGE_PATH", "/data")),
            metadata_url=metadata_url or None,
        )


def validate_chunk_hash(chunk_hash: str) -> None:
    if not SHA256_PATTERN.fullmatch(chunk_hash):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="chunk_hash must be exactly 64 lowercase hexadecimal characters",
        )


def digest_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as chunk_file:
        for block in iter(lambda: chunk_file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def count_chunks(storage_path: Path) -> int:
    return sum(
        1
        for path in storage_path.iterdir()
        if path.is_file() and SHA256_PATTERN.fullmatch(path.name)
    )


async def send_heartbeats(settings: StorageSettings, stop_event: asyncio.Event) -> None:
    if not settings.metadata_url:
        return

    async with httpx.AsyncClient(timeout=settings.heartbeat_timeout_seconds) as client:
        while not stop_event.is_set():
            try:
                payload = {
                    "node_id": settings.node_id,
                    "free_space_bytes": shutil.disk_usage(settings.storage_path).free,
                    "chunk_count": count_chunks(settings.storage_path),
                }
                response = await client.post(settings.metadata_url, json=payload)
                response.raise_for_status()
            except (OSError, httpx.HTTPError) as exc:
                LOGGER.warning("Heartbeat to %s failed: %s", settings.metadata_url, exc)

            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=settings.heartbeat_interval_seconds
                )
            except TimeoutError:
                pass


def create_app(settings: StorageSettings | None = None) -> FastAPI:
    resolved_settings = settings or StorageSettings.from_environment()

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        resolved_settings.storage_path.mkdir(parents=True, exist_ok=True)
        stop_event = asyncio.Event()
        heartbeat_task = asyncio.create_task(send_heartbeats(resolved_settings, stop_event))
        try:
            yield
        finally:
            stop_event.set()
            await heartbeat_task

    app = FastAPI(title="Distributed Storage Node", lifespan=lifespan)
    app.state.settings = resolved_settings

    def checked_chunk_path(chunk_hash: str) -> Path:
        validate_chunk_hash(chunk_hash)
        path = resolved_settings.storage_path / chunk_hash
        if not path.is_file():
            raise HTTPException(status_code=404, detail="Chunk not found")
        try:
            actual_hash = digest_file(path)
        except OSError as exc:
            LOGGER.exception("Could not read chunk %s", chunk_hash)
            raise HTTPException(status_code=500, detail="Could not read chunk") from exc
        if actual_hash != chunk_hash:
            raise HTTPException(
                status_code=409,
                detail=f"Chunk integrity check failed: expected {chunk_hash}, got {actual_hash}",
                headers={"X-Chunk-Error": "integrity-check-failed"},
            )
        return path

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "node_id": resolved_settings.node_id}

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics() -> str:
        usage = shutil.disk_usage(resolved_settings.storage_path)
        return ("# TYPE storage_node_disk_free_bytes gauge\n"
                f'storage_node_disk_free_bytes{{node_id="{resolved_settings.node_id}"}} {usage.free}\n'
                "# TYPE storage_node_chunks gauge\n"
                f'storage_node_chunks{{node_id="{resolved_settings.node_id}"}} {count_chunks(resolved_settings.storage_path)}\n')

    @app.put("/chunks/{chunk_hash}", status_code=status.HTTP_201_CREATED)
    async def put_chunk(chunk_hash: str, request: Request) -> dict[str, int | str]:
        validate_chunk_hash(chunk_hash)
        content = await request.body()
        actual_hash = hashlib.sha256(content).hexdigest()
        if actual_hash != chunk_hash:
            raise HTTPException(
                status_code=400,
                detail=f"Content hash mismatch: expected {chunk_hash}, got {actual_hash}",
            )

        resolved_settings.storage_path.mkdir(parents=True, exist_ok=True)
        temporary_name: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=resolved_settings.storage_path, delete=False
            ) as temporary_file:
                temporary_name = temporary_file.name
                temporary_file.write(content)
                temporary_file.flush()
                os.fsync(temporary_file.fileno())
            os.replace(temporary_name, resolved_settings.storage_path / chunk_hash)
        except OSError as exc:
            if temporary_name:
                Path(temporary_name).unlink(missing_ok=True)
            LOGGER.exception("Could not store chunk %s", chunk_hash)
            raise HTTPException(status_code=500, detail="Could not store chunk") from exc

        return {"chunk_hash": chunk_hash, "size": len(content)}

    @app.get("/chunks/{chunk_hash}", response_class=FileResponse)
    async def get_chunk(chunk_hash: str) -> FileResponse:
        path = checked_chunk_path(chunk_hash)
        return FileResponse(
            path,
            media_type="application/octet-stream",
            headers={"ETag": f'"{chunk_hash}"'},
        )

    @app.head("/chunks/{chunk_hash}")
    async def head_chunk(chunk_hash: str) -> Response:
        path = checked_chunk_path(chunk_hash)
        return Response(
            headers={
                "Content-Length": str(path.stat().st_size),
                "Content-Type": "application/octet-stream",
                "ETag": f'"{chunk_hash}"',
            }
        )

    @app.delete("/chunks/{chunk_hash}", status_code=status.HTTP_204_NO_CONTENT)
    async def delete_chunk(chunk_hash: str) -> Response:
        validate_chunk_hash(chunk_hash)
        path = resolved_settings.storage_path / chunk_hash
        try:
            path.unlink()
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Chunk not found") from exc
        except OSError as exc:
            LOGGER.exception("Could not delete chunk %s", chunk_hash)
            raise HTTPException(status_code=500, detail="Could not delete chunk") from exc
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    return app


app = create_app()

