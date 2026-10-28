from datetime import datetime, timezone

from sqlalchemy import JSON, BigInteger, Boolean, CheckConstraint, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class ObjectVersion(Base):
    __tablename__ = "object_versions"
    __table_args__ = (UniqueConstraint("bucket", "object_key", "version"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    bucket: Mapped[str] = mapped_column(String(255), index=True)
    object_key: Mapped[str] = mapped_column(String(1024), index=True)
    version: Mapped[int] = mapped_column(Integer)
    size: Mapped[int] = mapped_column(BigInteger)
    redundancy_strategy: Mapped[str] = mapped_column(String(32), default="replication")
    tags: Mapped[dict[str, str]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    chunks: Mapped[list["ObjectChunk"]] = relationship(
        back_populates="object_version",
        cascade="all, delete-orphan",
        order_by="ObjectChunk.position",
    )


class ChunkRecord(Base):
    __tablename__ = "chunks"
    __table_args__ = (CheckConstraint("reference_count >= 0"),)

    chunk_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    size: Mapped[int] = mapped_column(BigInteger)
    reference_count: Mapped[int] = mapped_column(Integer, default=0)
    protections: Mapped[list["ChunkProtection"]] = relationship(
        cascade="all, delete-orphan", back_populates="chunk"
    )


class ObjectChunk(Base):
    __tablename__ = "object_chunks"

    object_version_id: Mapped[int] = mapped_column(
        ForeignKey("object_versions.id", ondelete="CASCADE"), primary_key=True
    )
    position: Mapped[int] = mapped_column(Integer, primary_key=True)
    chunk_hash: Mapped[str] = mapped_column(
        ForeignKey("chunks.chunk_hash", ondelete="RESTRICT"), index=True
    )
    object_version: Mapped[ObjectVersion] = relationship(back_populates="chunks")


class ChunkProtection(Base):
    __tablename__ = "chunk_protections"
    __table_args__ = (
        UniqueConstraint("chunk_hash", "strategy"),
        CheckConstraint("reference_count >= 0"),
        CheckConstraint("replication_factor > 0"),
        CheckConstraint("read_quorum > 0"),
        CheckConstraint("write_quorum > 0"),
        CheckConstraint("data_shards > 0"),
        CheckConstraint("parity_shards > 0"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    chunk_hash: Mapped[str] = mapped_column(
        ForeignKey("chunks.chunk_hash", ondelete="CASCADE"), index=True
    )
    strategy: Mapped[str] = mapped_column(String(32))
    reference_count: Mapped[int] = mapped_column(Integer, default=0)
    replication_factor: Mapped[int] = mapped_column(Integer, default=3)
    read_quorum: Mapped[int] = mapped_column(Integer, default=2)
    write_quorum: Mapped[int] = mapped_column(Integer, default=2)
    data_shards: Mapped[int] = mapped_column(Integer, default=4)
    parity_shards: Mapped[int] = mapped_column(Integer, default=2)
    chunk: Mapped[ChunkRecord] = relationship(back_populates="protections")
    placements: Mapped[list["ChunkPlacement"]] = relationship(
        cascade="all, delete-orphan", back_populates="protection"
    )


class ChunkPlacement(Base):
    __tablename__ = "chunk_placements"

    protection_id: Mapped[int] = mapped_column(
        ForeignKey("chunk_protections.id", ondelete="CASCADE"), primary_key=True
    )
    node_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    storage_hash: Mapped[str] = mapped_column(String(64), index=True)
    shard_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    size: Mapped[int] = mapped_column(BigInteger)
    healthy: Mapped[bool] = mapped_column(Boolean, default=True)
    protection: Mapped[ChunkProtection] = relationship(back_populates="placements")


class BucketPolicy(Base):
    __tablename__ = "bucket_policies"
    __table_args__ = (
        CheckConstraint("replication_factor > 0"),
        CheckConstraint("read_quorum > 0"),
        CheckConstraint("write_quorum > 0"),
        CheckConstraint("data_shards > 0"),
        CheckConstraint("parity_shards > 0"),
    )

    bucket: Mapped[str] = mapped_column(String(255), primary_key=True)
    strategy: Mapped[str] = mapped_column(String(32), default="replication")
    replication_factor: Mapped[int] = mapped_column(Integer, default=3)
    read_quorum: Mapped[int] = mapped_column(Integer, default=2)
    write_quorum: Mapped[int] = mapped_column(Integer, default=2)
    data_shards: Mapped[int] = mapped_column(Integer, default=4)
    parity_shards: Mapped[int] = mapped_column(Integer, default=2)


class RepairEvent(Base):
    __tablename__ = "repair_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    event_type: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(32))
    strategy: Mapped[str] = mapped_column(String(32))
    chunk_hash: Mapped[str] = mapped_column(String(64), index=True)
    node_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    bytes_transferred: Mapped[int] = mapped_column(BigInteger, default=0)
    detail: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class NodeHeartbeat(Base):
    __tablename__ = "node_heartbeats"

    node_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    free_space_bytes: Mapped[int] = mapped_column(BigInteger)
    chunk_count: Mapped[int] = mapped_column(BigInteger)
    last_heartbeat: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

