import hashlib
from dataclasses import dataclass

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload
from zfec import Decoder, Encoder

from metadata_service.hashing import ConsistentHashRing
from metadata_service.models import ChunkPlacement, ChunkProtection, RepairEvent
from metadata_service.storage import StorageOperations


class RedundancyError(RuntimeError):
    pass


class QuorumError(RedundancyError):
    pass


@dataclass(frozen=True)
class PlacementResult:
    node_id: str
    storage_hash: str
    size: int
    shard_index: int | None = None


def validate_replication(replication_factor: int, read_quorum: int, write_quorum: int) -> None:
    if replication_factor <= 0:
        raise ValueError("replication_factor must be positive")
    if not 0 < write_quorum <= replication_factor:
        raise ValueError("write_quorum must satisfy 0 < W <= N")
    if not 0 < read_quorum <= replication_factor:
        raise ValueError("read_quorum must satisfy 0 < R <= N")
    if write_quorum + read_quorum <= replication_factor:
        raise ValueError("quorums must satisfy W + R > N")


def validate_erasure(data_shards: int, parity_shards: int, node_count: int) -> None:
    if data_shards <= 0 or parity_shards <= 0:
        raise ValueError("data_shards and parity_shards must be positive")
    if data_shards + parity_shards > node_count:
        raise ValueError("erasure coding requires one distinct node per shard")


def encode_shards(data: bytes, data_shards: int, parity_shards: int) -> list[bytes]:
    shard_size = max(1, (len(data) + data_shards - 1) // data_shards)
    padded = data + b"\0" * (shard_size * data_shards - len(data))
    blocks = [padded[index * shard_size : (index + 1) * shard_size] for index in range(data_shards)]
    return list(Encoder(data_shards, data_shards + parity_shards).encode(blocks))


def decode_shards(
    shards: list[bytes],
    shard_indexes: list[int],
    data_shards: int,
    parity_shards: int,
    original_size: int,
) -> bytes:
    if len(shards) < data_shards:
        raise RedundancyError(f"need at least {data_shards} valid shards")
    decoded = Decoder(data_shards, data_shards + parity_shards).decode(
        shards[:data_shards], shard_indexes[:data_shards]
    )
    return b"".join(decoded)[:original_size]


class RedundancyManager:
    def __init__(
        self,
        storage: StorageOperations,
        ring: ConsistentHashRing,
        node_urls: dict[str, str],
    ) -> None:
        self.storage = storage
        self.ring = ring
        self.node_urls = node_urls

    def write_replication(
        self,
        chunk_hash: str,
        data: bytes,
        replication_factor: int,
        write_quorum: int,
    ) -> list[PlacementResult]:
        targets = self.ring.get_nodes(chunk_hash, replication_factor)
        successes: list[PlacementResult] = []
        newly_written: list[str] = []
        for node_id in targets:
            try:
                existed = self.storage.exists(node_id, chunk_hash)
                if not existed:
                    self.storage.put(node_id, chunk_hash, data)
                    newly_written.append(node_id)
                successes.append(PlacementResult(node_id, chunk_hash, len(data)))
            except (httpx.HTTPError, OSError, KeyError):
                continue
        if len(successes) < write_quorum:
            for node_id in newly_written:
                try:
                    self.storage.delete(node_id, chunk_hash)
                except Exception:
                    pass
            raise QuorumError(
                f"write quorum failed: {len(successes)} successful replicas, W={write_quorum}"
            )
        return successes

    def read_replication(self, protection: ChunkProtection) -> bytes:
        valid: list[bytes] = []
        for placement in protection.placements:
            try:
                data = self.storage.get(placement.node_id, placement.storage_hash)
                if hashlib.sha256(data).hexdigest() != protection.chunk_hash:
                    placement.healthy = False
                    continue
                placement.healthy = True
                valid.append(data)
            except (httpx.HTTPError, OSError, KeyError):
                placement.healthy = False
            if len(valid) >= protection.read_quorum:
                return valid[0]
        raise QuorumError(
            f"read quorum failed: {len(valid)} valid replicas, R={protection.read_quorum}"
        )

    def write_erasure(
        self,
        chunk_hash: str,
        data: bytes,
        data_shards: int,
        parity_shards: int,
    ) -> list[PlacementResult]:
        total = data_shards + parity_shards
        targets = self.ring.get_nodes(chunk_hash, total)
        if len(targets) < total:
            raise RedundancyError(f"erasure coding requires {total} distinct storage nodes")
        shards = encode_shards(data, data_shards, parity_shards)
        successes: list[PlacementResult] = []
        try:
            for index, (node_id, shard) in enumerate(zip(targets, shards, strict=True)):
                shard_hash = hashlib.sha256(shard).hexdigest()
                if not self.storage.exists(node_id, shard_hash):
                    self.storage.put(node_id, shard_hash, shard)
                successes.append(PlacementResult(node_id, shard_hash, len(shard), index))
        except (httpx.HTTPError, OSError, KeyError) as exc:
            for placement in successes:
                try:
                    self.storage.delete(placement.node_id, placement.storage_hash)
                except Exception:
                    pass
            raise RedundancyError(f"erasure shard write failed: {exc}") from exc
        return successes

    def read_erasure(self, protection: ChunkProtection, original_size: int) -> bytes:
        valid: list[tuple[int, bytes]] = []
        for placement in protection.placements:
            try:
                shard = self.storage.get(placement.node_id, placement.storage_hash)
                if hashlib.sha256(shard).hexdigest() != placement.storage_hash:
                    placement.healthy = False
                    continue
                placement.healthy = True
                valid.append((placement.shard_index, shard))
            except (httpx.HTTPError, OSError, KeyError):
                placement.healthy = False
        valid.sort(key=lambda item: item[0])
        data = decode_shards(
            [item[1] for item in valid],
            [item[0] for item in valid],
            protection.data_shards,
            protection.parity_shards,
            original_size,
        )
        if hashlib.sha256(data).hexdigest() != protection.chunk_hash:
            raise RedundancyError("reconstructed chunk failed SHA-256 validation")
        return data

    def read(self, protection: ChunkProtection, original_size: int) -> bytes:
        if protection.strategy == "replication":
            return self.read_replication(protection)
        return self.read_erasure(protection, original_size)

    def repair_all(
        self,
        session: Session,
        active_nodes: set[str],
        event_type: str = "repair",
    ) -> dict[str, int]:
        protections = session.scalars(
            select(ChunkProtection).options(selectinload(ChunkProtection.placements))
        ).all()
        repaired = 0
        traffic = 0
        for protection in protections:
            try:
                if protection.strategy == "replication":
                    count, byte_count = self._repair_replication(session, protection, active_nodes, event_type)
                else:
                    count, byte_count = self._repair_erasure(session, protection, active_nodes, event_type)
                repaired += count
                traffic += byte_count
            except RedundancyError as exc:
                session.add(
                    RepairEvent(
                        event_type=event_type,
                        status="unrecoverable",
                        strategy=protection.strategy,
                        chunk_hash=protection.chunk_hash,
                        bytes_transferred=0,
                        detail=str(exc),
                    )
                )
        session.commit()
        return {"repaired": repaired, "bytes_transferred": traffic}

    def rebalance_all(self, session: Session, active_nodes: set[str]) -> dict[str, int | bool]:
        """Move replicated chunks to the targets of the current active-node ring."""
        if not active_nodes:
            raise RedundancyError("no active nodes available for rebalance")
        target_ring = ConsistentHashRing(active_nodes, self.ring.virtual_nodes)
        protections = session.scalars(select(ChunkProtection).where(
            ChunkProtection.strategy == "replication").options(
            selectinload(ChunkProtection.placements), selectinload(ChunkProtection.chunk))).all()
        moved = traffic = 0
        integrity = True
        for protection in protections:
            existing = {placement.node_id: placement for placement in protection.placements}
            source = None
            for placement in protection.placements:
                if placement.node_id in active_nodes:
                    candidate = self._valid_replica(protection, placement)
                    if candidate is not None:
                        source = candidate
                        break
            if source is None:
                integrity = False
                continue
            desired = set(target_ring.get_nodes(protection.chunk_hash, protection.replication_factor))
            for node_id in desired - existing.keys():
                self.storage.put(node_id, protection.chunk_hash, source)
                protection.placements.append(ChunkPlacement(
                    node_id=node_id, storage_hash=protection.chunk_hash,
                    size=len(source), healthy=True))
                moved += 1
                traffic += len(source)
                self._event(session, "rebalance", protection, node_id, len(source), "replica moved to scale-out target")
            for node_id in existing.keys() - desired:
                placement = existing[node_id]
                try:
                    self.storage.delete(node_id, placement.storage_hash)
                finally:
                    protection.placements.remove(placement)
        session.commit()
        return {"moved_chunks": moved, "bytes_transferred": traffic,
                "integrity_verified": integrity}

    def _valid_replica(self, protection: ChunkProtection, placement: ChunkPlacement) -> bytes | None:
        try:
            data = self.storage.get(placement.node_id, placement.storage_hash)
            if hashlib.sha256(data).hexdigest() == protection.chunk_hash:
                placement.healthy = True
                return data
        except (httpx.HTTPError, OSError, KeyError):
            pass
        placement.healthy = False
        return None

    def _event(
        self,
        session: Session,
        event_type: str,
        protection: ChunkProtection,
        node_id: str,
        byte_count: int,
        detail: str,
    ) -> None:
        session.add(
            RepairEvent(
                event_type=event_type,
                status="repaired",
                strategy=protection.strategy,
                chunk_hash=protection.chunk_hash,
                node_id=node_id,
                bytes_transferred=byte_count,
                detail=detail,
            )
        )

    def _repair_replication(
        self,
        session: Session,
        protection: ChunkProtection,
        active_nodes: set[str],
        event_type: str,
    ) -> tuple[int, int]:
        valid: dict[str, bytes] = {}
        corrupt_active: list[ChunkPlacement] = []
        for placement in list(protection.placements):
            if placement.node_id not in active_nodes:
                protection.placements.remove(placement)
                continue
            data = self._valid_replica(protection, placement)
            if data is not None:
                valid[placement.node_id] = data
            else:
                corrupt_active.append(placement)
        if not valid:
            raise RedundancyError("no healthy replica available")
        source = next(iter(valid.values()))
        repaired = 0
        traffic = 0
        for placement in corrupt_active:
            self.storage.put(placement.node_id, protection.chunk_hash, source)
            placement.storage_hash = protection.chunk_hash
            placement.healthy = True
            valid[placement.node_id] = source
            repaired += 1
            traffic += len(source) * 2
            self._event(session, event_type, protection, placement.node_id, len(source) * 2, "corrupt replica replaced")

        candidates = [node for node in self.ring.get_nodes(protection.chunk_hash, len(self.node_urls)) if node in active_nodes]
        for node_id in candidates:
            if len(valid) >= protection.replication_factor:
                break
            if node_id in valid:
                continue
            self.storage.put(node_id, protection.chunk_hash, source)
            protection.placements.append(
                ChunkPlacement(
                    node_id=node_id,
                    storage_hash=protection.chunk_hash,
                    size=len(source),
                    healthy=True,
                )
            )
            valid[node_id] = source
            repaired += 1
            traffic += len(source) * 2
            self._event(session, event_type, protection, node_id, len(source) * 2, "missing replica recreated")
        return repaired, traffic

    def _repair_erasure(
        self,
        session: Session,
        protection: ChunkProtection,
        active_nodes: set[str],
        event_type: str,
    ) -> tuple[int, int]:
        total = protection.data_shards + protection.parity_shards
        valid: dict[int, tuple[str, bytes]] = {}
        corrupt: dict[int, ChunkPlacement] = {}
        for placement in list(protection.placements):
            if placement.node_id not in active_nodes:
                protection.placements.remove(placement)
                continue
            try:
                shard = self.storage.get(placement.node_id, placement.storage_hash)
                if hashlib.sha256(shard).hexdigest() == placement.storage_hash:
                    placement.healthy = True
                    valid[placement.shard_index] = (placement.node_id, shard)
                    continue
            except (httpx.HTTPError, OSError, KeyError):
                pass
            placement.healthy = False
            corrupt[placement.shard_index] = placement
        if len(valid) < protection.data_shards:
            raise RedundancyError(f"only {len(valid)} valid shards; need {protection.data_shards}")
        indexes = sorted(valid)[: protection.data_shards]
        original = decode_shards(
            [valid[index][1] for index in indexes],
            indexes,
            protection.data_shards,
            protection.parity_shards,
            protection.chunk.size,
        )
        if hashlib.sha256(original).hexdigest() != protection.chunk_hash:
            raise RedundancyError("reconstructed chunk failed SHA-256 validation")
        all_shards = encode_shards(original, protection.data_shards, protection.parity_shards)
        used_nodes = {node_id for node_id, _ in valid.values()}
        candidates = [node for node in self.ring.get_nodes(protection.chunk_hash, len(self.node_urls)) if node in active_nodes and node not in used_nodes]
        repaired = 0
        traffic = 0
        for index in range(total):
            if index in valid:
                continue
            placement = corrupt.get(index)
            if placement is not None:
                node_id = placement.node_id
            elif candidates:
                node_id = candidates.pop(0)
                placement = ChunkPlacement(node_id=node_id)
                protection.placements.append(placement)
            else:
                continue
            shard = all_shards[index]
            shard_hash = hashlib.sha256(shard).hexdigest()
            self.storage.put(node_id, shard_hash, shard)
            placement.storage_hash = shard_hash
            placement.shard_index = index
            placement.size = len(shard)
            placement.healthy = True
            used_nodes.add(node_id)
            repaired += 1
            byte_count = protection.data_shards * len(shard) + len(shard)
            traffic += byte_count
            self._event(session, event_type, protection, node_id, byte_count, f"shard {index} reconstructed")
        return repaired, traffic

