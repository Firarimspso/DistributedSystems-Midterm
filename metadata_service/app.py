import asyncio
import logging
import os
import json
from collections import Counter
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Iterator

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import Engine, create_engine, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload, sessionmaker

from metadata_service.chunking import content_defined_chunks, fixed_size_chunks
from metadata_service.hashing import ConsistentHashRing
from metadata_service.models import Base, BucketPolicy, ChunkPlacement, ChunkProtection, ChunkRecord, NodeHeartbeat, ObjectChunk, ObjectVersion, RepairEvent
from metadata_service.redundancy import QuorumError, RedundancyError, RedundancyManager, validate_erasure, validate_replication
from metadata_service.storage import HTTPStorageClient, StorageOperations


LOGGER = logging.getLogger("metadata_service")
DEFAULT_NODE_URLS = {f"node{i}": f"http://node{i}:8000" for i in range(1, 7)}


@dataclass(frozen=True)
class MetadataSettings:
    database_url: str
    node_urls: dict[str, str]
    virtual_nodes: int = 128
    inactive_after_seconds: float = 15.0
    replication_factor: int = 3
    read_quorum: int = 2
    write_quorum: int = 2
    data_shards: int = 4
    parity_shards: int = 2
    repair_interval_seconds: float = 10.0
    scrub_interval_seconds: float = 30.0

    @classmethod
    def from_environment(cls) -> "MetadataSettings":
        raw_nodes = os.getenv("STORAGE_NODES", ",".join(f"{n}={u}" for n, u in DEFAULT_NODE_URLS.items()))
        return cls(
            database_url=os.getenv("DATABASE_URL", "postgresql+psycopg://storage:storage@postgres:5432/storage_metadata"),
            node_urls=dict(item.split("=", 1) for item in raw_nodes.split(",") if item),
            replication_factor=int(os.getenv("REPLICATION_FACTOR", "3")),
            read_quorum=int(os.getenv("READ_QUORUM", "2")),
            write_quorum=int(os.getenv("WRITE_QUORUM", "2")),
            data_shards=int(os.getenv("DATA_SHARDS", "4")),
            parity_shards=int(os.getenv("PARITY_SHARDS", "2")),
            inactive_after_seconds=float(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "15")),
            repair_interval_seconds=float(os.getenv("REPAIR_INTERVAL_SECONDS", "10")),
            scrub_interval_seconds=float(os.getenv("SCRUB_INTERVAL_SECONDS", "30")),
        )


class HeartbeatRequest(BaseModel):
    node_id: str = Field(min_length=1, max_length=255)
    free_space_bytes: int = Field(ge=0)
    chunk_count: int = Field(ge=0)


class BucketPolicyRequest(BaseModel):
    strategy: str = Field(pattern="^(replication|erasure)$")
    replication_factor: int = 3
    read_quorum: int = 2
    write_quorum: int = 2
    data_shards: int = 4
    parity_shards: int = 2

    @model_validator(mode="after")
    def validate_policy(self) -> "BucketPolicyRequest":
        if self.strategy == "replication":
            validate_replication(self.replication_factor, self.read_quorum, self.write_quorum)
        elif self.data_shards <= 0 or self.parity_shards <= 0:
            raise ValueError("data_shards and parity_shards must be positive")
        return self


def serialize_object(obj: ObjectVersion) -> dict[str, object]:
    return {"bucket": obj.bucket, "key": obj.object_key, "version": obj.version, "size": obj.size,
            "created_at": obj.created_at.isoformat(), "redundancy_strategy": obj.redundancy_strategy,
            "chunk_hashes": [chunk.chunk_hash for chunk in obj.chunks], "tags": obj.tags or {}}


def serialize_policy(policy: BucketPolicy) -> dict[str, object]:
    return {"bucket": policy.bucket, "strategy": policy.strategy,
            "replication_factor": policy.replication_factor, "read_quorum": policy.read_quorum,
            "write_quorum": policy.write_quorum, "data_shards": policy.data_shards,
            "parity_shards": policy.parity_shards}


def create_app(settings: MetadataSettings | None = None, storage_client: StorageOperations | None = None,
               engine: Engine | None = None) -> FastAPI:
    resolved = settings or MetadataSettings.from_environment()
    validate_replication(resolved.replication_factor, resolved.read_quorum, resolved.write_quorum)
    db_engine = engine or create_engine(resolved.database_url, pool_pre_ping=True)
    session_factory = sessionmaker(db_engine, expire_on_commit=False)
    ring = ConsistentHashRing(resolved.node_urls, resolved.virtual_nodes)
    storage = storage_client or HTTPStorageClient(resolved.node_urls)
    redundancy = RedundancyManager(storage, ring, resolved.node_urls)

    def active_nodes(session: Session) -> set[str]:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=resolved.inactive_after_seconds)
        return set(session.scalars(select(NodeHeartbeat.node_id).where(NodeHeartbeat.last_heartbeat >= cutoff)))

    def run_maintenance(event_type: str) -> dict[str, int]:
        with session_factory() as session:
            return redundancy.repair_all(session, active_nodes(session), event_type)

    async def maintenance_loop(interval: float, event_type: str, stop: asyncio.Event) -> None:
        if interval <= 0:
            return
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                break
            except TimeoutError:
                try:
                    await asyncio.to_thread(run_maintenance, event_type)
                except Exception:
                    LOGGER.exception("%s loop failed", event_type)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        Base.metadata.create_all(db_engine)
        stop = asyncio.Event()
        tasks = [asyncio.create_task(maintenance_loop(resolved.repair_interval_seconds, "repair", stop)),
                 asyncio.create_task(maintenance_loop(resolved.scrub_interval_seconds, "scrub", stop))]
        try:
            yield
        finally:
            stop.set()
            await asyncio.gather(*tasks)
            close = getattr(storage, "close", None)
            if close:
                close()
            db_engine.dispose()

    app = FastAPI(title="Metadata, Redundancy, and Recovery Service", lifespan=lifespan)
    app.state.engine = db_engine
    app.state.ring = ring

    def get_session() -> Iterator[Session]:
        with session_factory() as session:
            yield session

    def default_policy(bucket: str) -> BucketPolicy:
        return BucketPolicy(bucket=bucket, strategy="replication", replication_factor=resolved.replication_factor,
                            read_quorum=resolved.read_quorum, write_quorum=resolved.write_quorum,
                            data_shards=resolved.data_shards, parity_shards=resolved.parity_shards)

    @app.get("/health")
    def health() -> dict[str, str]:
        with db_engine.connect() as connection:
            connection.execute(select(1))
        return {"status": "ok"}

    @app.post("/heartbeats")
    def heartbeat(payload: HeartbeatRequest, session: Session = Depends(get_session)) -> dict[str, str]:
        record = session.get(NodeHeartbeat, payload.node_id)
        if record is None:
            record = NodeHeartbeat(node_id=payload.node_id)
            session.add(record)
        record.free_space_bytes, record.chunk_count = payload.free_space_bytes, payload.chunk_count
        record.last_heartbeat = datetime.now(timezone.utc)
        session.commit()
        return {"status": "accepted"}

    @app.get("/nodes")
    def nodes(session: Session = Depends(get_session)) -> list[dict[str, object]]:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=resolved.inactive_after_seconds)
        records = session.scalars(select(NodeHeartbeat).order_by(NodeHeartbeat.node_id)).all()
        return [{"node_id": r.node_id, "free_space_bytes": r.free_space_bytes, "chunk_count": r.chunk_count,
                 "last_heartbeat": r.last_heartbeat.isoformat(),
                 "status": "active" if r.last_heartbeat >= cutoff else "inactive"} for r in records]

    @app.put("/buckets/{bucket}/redundancy")
    def set_bucket_policy(bucket: str, payload: BucketPolicyRequest,
                          session: Session = Depends(get_session)) -> dict[str, object]:
        if payload.strategy == "erasure":
            try:
                validate_erasure(payload.data_shards, payload.parity_shards, len(resolved.node_urls))
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
        policy = session.get(BucketPolicy, bucket)
        if policy is None:
            policy = BucketPolicy(bucket=bucket)
            session.add(policy)
        for field, value in payload.model_dump().items():
            setattr(policy, field, value)
        session.commit()
        return serialize_policy(policy)

    @app.get("/buckets/{bucket}/redundancy")
    def get_bucket_policy(bucket: str, session: Session = Depends(get_session)) -> dict[str, object]:
        return serialize_policy(session.get(BucketPolicy, bucket) or default_policy(bucket))

    @app.post("/catalog/buckets/{bucket}", status_code=201)
    def create_bucket(bucket: str, session: Session = Depends(get_session)) -> dict[str, str]:
        if session.get(BucketPolicy, bucket) is not None:
            raise HTTPException(status_code=409, detail="Bucket already exists")
        session.add(default_policy(bucket))
        session.commit()
        return {"name": bucket}

    @app.get("/catalog/buckets")
    def list_buckets(session: Session = Depends(get_session)) -> list[dict[str, object]]:
        policies = session.scalars(select(BucketPolicy).order_by(BucketPolicy.bucket)).all()
        return [serialize_policy(policy) for policy in policies]

    @app.get("/catalog/objects/{bucket}")
    def catalog_objects(bucket: str, prefix: str | None = None, min_size: int | None = None,
                        max_size: int | None = None, created_after: datetime | None = None,
                        created_before: datetime | None = None, tags: str | None = None,
                        session: Session = Depends(get_session)) -> list[dict[str, object]]:
        statement = select(ObjectVersion).where(ObjectVersion.bucket == bucket).options(
            selectinload(ObjectVersion.chunks)).order_by(ObjectVersion.object_key, ObjectVersion.version)
        if prefix is not None:
            statement = statement.where(ObjectVersion.object_key.startswith(prefix))
        if min_size is not None:
            statement = statement.where(ObjectVersion.size >= min_size)
        if max_size is not None:
            statement = statement.where(ObjectVersion.size <= max_size)
        if created_after is not None:
            statement = statement.where(ObjectVersion.created_at >= created_after)
        if created_before is not None:
            statement = statement.where(ObjectVersion.created_at <= created_before)
        required_tags = json.loads(tags) if tags else {}
        return [serialize_object(obj) for obj in session.scalars(statement).all()
                if all((obj.tags or {}).get(key) == value for key, value in required_tags.items())]

    @app.get("/cluster/status")
    def cluster_status(session: Session = Depends(get_session)) -> dict[str, object]:
        node_rows = nodes(session)
        active = {row["node_id"] for row in node_rows if row["status"] == "active"}
        protections = session.scalars(select(ChunkProtection).options(
            selectinload(ChunkProtection.placements), selectinload(ChunkProtection.chunk))).all()
        degraded_hashes = {p.chunk_hash for p in protections
            if sum(place.node_id in active and place.healthy for place in p.placements) <
               (p.replication_factor if p.strategy == "replication" else p.data_shards + p.parity_shards)}
        degraded_objects = session.scalars(select(ObjectVersion).options(
            selectinload(ObjectVersion.chunks))).all()
        return {"nodes": node_rows, "total_free_space_bytes": sum(row["free_space_bytes"] for row in node_rows),
                "total_chunks": sum(row["chunk_count"] for row in node_rows),
                "degraded_objects": [serialize_object(obj) for obj in degraded_objects
                    if any(chunk.chunk_hash in degraded_hashes for chunk in obj.chunks)]}

    @app.post("/objects/{bucket}/{key:path}", status_code=201)
    async def create_object(bucket: str, key: str, request: Request,
                            strategy: str = Query("fixed", pattern="^(fixed|cdc)$"),
                            session: Session = Depends(get_session)) -> dict[str, object]:
        data = await request.body()
        chunks = fixed_size_chunks(data) if strategy == "fixed" else content_defined_chunks(data)
        counts, unique = Counter(c.sha256 for c in chunks), {c.sha256: c for c in chunks}
        policy = session.get(BucketPolicy, bucket)
        if policy is None:
            policy = default_policy(bucket)
            session.add(policy)
            session.flush()
        if policy.strategy == "erasure":
            try:
                validate_erasure(policy.data_shards, policy.parity_shards, len(resolved.node_urls))
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
        protections = {p.chunk_hash: p for p in session.scalars(
            select(ChunkProtection).where(ChunkProtection.chunk_hash.in_(unique),
                                           ChunkProtection.strategy == policy.strategy)
            .options(selectinload(ChunkProtection.placements)).with_for_update())}
        physical: dict[str, list] = {}
        try:
            for chunk_hash, chunk in unique.items():
                if chunk_hash in protections:
                    continue
                physical[chunk_hash] = (
                    redundancy.write_replication(chunk_hash, chunk.data, policy.replication_factor, policy.write_quorum)
                    if policy.strategy == "replication" else
                    redundancy.write_erasure(chunk_hash, chunk.data, policy.data_shards, policy.parity_shards)
                )
        except (QuorumError, RedundancyError) as exc:
            session.rollback()
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        try:
            current = session.scalar(select(func.max(ObjectVersion.version)).where(
                ObjectVersion.bucket == bucket, ObjectVersion.object_key == key))
            try:
                object_tags = json.loads(request.headers.get("x-object-tags", "{}"))
            except json.JSONDecodeError as exc:
                raise HTTPException(status_code=422, detail="Invalid X-Object-Tags JSON") from exc
            obj = ObjectVersion(bucket=bucket, object_key=key, version=(current or 0) + 1,
                                size=len(data), redundancy_strategy=policy.strategy, tags=object_tags)
            session.add(obj)
            records = {r.chunk_hash: r for r in session.scalars(
                select(ChunkRecord).where(ChunkRecord.chunk_hash.in_(unique)).with_for_update())}
            for chunk_hash, chunk in unique.items():
                record = records.get(chunk_hash)
                if record is None:
                    record = ChunkRecord(chunk_hash=chunk_hash, size=len(chunk.data), reference_count=0)
                    session.add(record)
                elif record.size != len(chunk.data):
                    raise ValueError("SHA-256 collision with a different chunk size")
                record.reference_count += counts[chunk_hash]
                protection = protections.get(chunk_hash)
                if protection is None:
                    protection = ChunkProtection(chunk_hash=chunk_hash, strategy=policy.strategy, reference_count=0,
                        replication_factor=policy.replication_factor, read_quorum=policy.read_quorum,
                        write_quorum=policy.write_quorum, data_shards=policy.data_shards,
                        parity_shards=policy.parity_shards)
                    for result in physical[chunk_hash]:
                        protection.placements.append(ChunkPlacement(node_id=result.node_id,
                            storage_hash=result.storage_hash, shard_index=result.shard_index,
                            size=result.size, healthy=True))
                    session.add(protection)
                    protections[chunk_hash] = protection
                protection.reference_count += counts[chunk_hash]
            for position, chunk in enumerate(chunks):
                obj.chunks.append(ObjectChunk(position=position, chunk_hash=chunk.sha256))
            session.commit()
            session.refresh(obj)
            return serialize_object(obj)
        except (IntegrityError, ValueError) as exc:
            session.rollback()
            for results in physical.values():
                for result in results:
                    try:
                        storage.delete(result.node_id, result.storage_hash)
                    except Exception:
                        LOGGER.exception("Could not clean up %s", result.storage_hash)
            raise HTTPException(status_code=409, detail=f"Metadata update failed: {exc}") from exc

    def find_object(session: Session, bucket: str, key: str, version: int | None) -> ObjectVersion | None:
        statement = select(ObjectVersion).where(ObjectVersion.bucket == bucket, ObjectVersion.object_key == key).options(
            selectinload(ObjectVersion.chunks))
        statement = (statement.order_by(ObjectVersion.version.desc()).limit(1) if version is None
                     else statement.where(ObjectVersion.version == version))
        return session.scalar(statement)

    @app.get("/objects/{bucket}/{key:path}")
    def get_object_metadata(bucket: str, key: str, version: int | None = None,
                            session: Session = Depends(get_session)) -> dict[str, object]:
        obj = find_object(session, bucket, key, version)
        if obj is None:
            raise HTTPException(status_code=404, detail="Object version not found")
        return serialize_object(obj)

    @app.get("/data/{bucket}/{key:path}")
    def read_object(bucket: str, key: str, version: int | None = None,
                    session: Session = Depends(get_session)) -> Response:
        obj = find_object(session, bucket, key, version)
        if obj is None:
            raise HTTPException(status_code=404, detail="Object version not found")
        hashes = {c.chunk_hash for c in obj.chunks}
        protections = {p.chunk_hash: p for p in session.scalars(select(ChunkProtection).where(
            ChunkProtection.chunk_hash.in_(hashes), ChunkProtection.strategy == obj.redundancy_strategy).options(
            selectinload(ChunkProtection.placements), selectinload(ChunkProtection.chunk)))}
        try:
            chunk_data = {h: redundancy.read(protections[h], protections[h].chunk.size) for h in hashes}
            data = b"".join(chunk_data[c.chunk_hash] for c in obj.chunks)
            session.commit()
        except (QuorumError, RedundancyError, KeyError) as exc:
            session.commit()
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return Response(content=data, media_type="application/octet-stream")

    @app.delete("/objects/{bucket}/{key:path}", status_code=204)
    def delete_object(bucket: str, key: str, version: int,
                      session: Session = Depends(get_session)) -> Response:
        obj = find_object(session, bucket, key, version)
        if obj is None:
            raise HTTPException(status_code=404, detail="Object version not found")
        counts = Counter(c.chunk_hash for c in obj.chunks)
        records = {r.chunk_hash: r for r in session.scalars(select(ChunkRecord).where(
            ChunkRecord.chunk_hash.in_(counts)).with_for_update())}
        protections = {p.chunk_hash: p for p in session.scalars(select(ChunkProtection).where(
            ChunkProtection.chunk_hash.in_(counts), ChunkProtection.strategy == obj.redundancy_strategy).options(
            selectinload(ChunkProtection.placements)).with_for_update())}
        remove_protections, remove_records = [], []
        for chunk_hash, count in counts.items():
            record, protection = records[chunk_hash], protections[chunk_hash]
            if record.reference_count < count or protection.reference_count < count:
                raise HTTPException(status_code=409, detail="Invalid chunk reference count")
            record.reference_count -= count
            protection.reference_count -= count
            if protection.reference_count == 0:
                remove_protections.append(protection)
            if record.reference_count == 0:
                remove_records.append(record)
        try:
            for protection in remove_protections:
                for placement in protection.placements:
                    storage.delete(placement.node_id, placement.storage_hash)
        except Exception as exc:
            session.rollback()
            raise HTTPException(status_code=503, detail=f"Storage deletion failed: {exc}") from exc
        session.delete(obj)
        for item in remove_protections + remove_records:
            session.delete(item)
        session.commit()
        return Response(status_code=204)

    @app.post("/repairs/run")
    def run_repairs() -> dict[str, int]:
        return run_maintenance("repair")

    @app.post("/scrub/run")
    def run_scrub() -> dict[str, int]:
        return run_maintenance("scrub")

    @app.post("/maintenance/rebalance")
    def run_rebalance(session: Session = Depends(get_session)) -> dict[str, int | bool]:
        return redundancy.rebalance_all(session, active_nodes(session))

    @app.post("/maintenance/garbage-collection")
    def run_garbage_collection(session: Session = Depends(get_session)) -> dict[str, int]:
        protections = session.scalars(select(ChunkProtection).where(
            ChunkProtection.reference_count == 0).options(selectinload(ChunkProtection.placements))).all()
        deleted = 0
        for protection in protections:
            for placement in protection.placements:
                try:
                    storage.delete(placement.node_id, placement.storage_hash)
                except Exception:
                    continue
            session.delete(protection)
            deleted += 1
        records = session.scalars(select(ChunkRecord).where(ChunkRecord.reference_count == 0)).all()
        for record in records:
            session.delete(record)
        session.commit()
        return {"deleted_protections": deleted, "deleted_chunks": len(records)}

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics(session: Session = Depends(get_session)) -> str:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=resolved.inactive_after_seconds)
        active = set(session.scalars(select(NodeHeartbeat.node_id).where(NodeHeartbeat.last_heartbeat >= cutoff)))
        protections = session.scalars(select(ChunkProtection).options(
            selectinload(ChunkProtection.placements), selectinload(ChunkProtection.chunk))).all()
        degraded = sum(1 for protection in protections
            if sum(p.node_id in active and p.healthy for p in protection.placements) <
               (protection.replication_factor if protection.strategy == "replication"
                else protection.data_shards + protection.parity_shards))
        repairs = session.scalar(select(func.count(RepairEvent.id)).where(RepairEvent.status == "repaired")) or 0
        return ("# TYPE storage_degraded_chunks gauge\n"
                f"storage_degraded_chunks {degraded}\n"
                "# TYPE storage_repairs_total counter\n"
                f"storage_repairs_total {repairs}\n")

    @app.get("/repairs")
    def repairs(session: Session = Depends(get_session)) -> list[dict[str, object]]:
        events = session.scalars(select(RepairEvent).order_by(RepairEvent.id.desc()).limit(100)).all()
        return [{"id": e.id, "event_type": e.event_type, "status": e.status, "strategy": e.strategy,
                 "chunk_hash": e.chunk_hash, "node_id": e.node_id, "bytes_transferred": e.bytes_transferred,
                 "detail": e.detail, "created_at": e.created_at.isoformat()} for e in events]

    return app


app = create_app()
