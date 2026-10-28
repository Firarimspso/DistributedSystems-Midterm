import hashlib
from dataclasses import dataclass
from typing import Iterable


MIB = 1024 * 1024
DEFAULT_FIXED_SIZE = 4 * MIB


@dataclass(frozen=True)
class Chunk:
    data: bytes
    sha256: str


def _chunk(data: bytes) -> Chunk:
    return Chunk(data=data, sha256=hashlib.sha256(data).hexdigest())


def fixed_size_chunks(data: bytes, chunk_size: int = DEFAULT_FIXED_SIZE) -> list[Chunk]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    return [_chunk(data[offset : offset + chunk_size]) for offset in range(0, len(data), chunk_size)]


# Deterministic 64-bit gear table. FastCDC uses a gear hash whose state depends
# on byte values and position; deriving the table keeps the source compact and
# reproducible without pretending fixed boundaries are content-defined.
GEAR_TABLE = tuple(
    int.from_bytes(hashlib.sha256(bytes([value])).digest()[:8], "big")
    for value in range(256)
)
UINT64_MASK = (1 << 64) - 1


def content_defined_chunks(
    data: bytes,
    min_size: int = 1 * MIB,
    avg_size: int = 4 * MIB,
    max_size: int = 8 * MIB,
) -> list[Chunk]:
    """Split bytes with a FastCDC-style gear rolling hash.

    A boundary is selected after ``min_size`` when the low mask bits are zero,
    or forcibly at ``max_size``. The mask uses the nearest power-of-two average.
    """
    if not 0 < min_size <= avg_size <= max_size:
        raise ValueError("sizes must satisfy 0 < min_size <= avg_size <= max_size")
    if not data:
        return []

    mask_bits = max(1, round(avg_size.bit_length() - 1))
    boundary_mask = (1 << mask_bits) - 1
    chunks: list[Chunk] = []
    start = 0
    length = len(data)

    while start < length:
        maximum_end = min(start + max_size, length)
        scan = min(start + min_size, maximum_end)
        gear_hash = 0
        boundary = maximum_end
        while scan < maximum_end:
            gear_hash = ((gear_hash << 1) + GEAR_TABLE[data[scan]]) & UINT64_MASK
            scan += 1
            if gear_hash & boundary_mask == 0:
                boundary = scan
                break
        chunks.append(_chunk(data[start:boundary]))
        start = boundary

    return chunks


def reconstruct(chunks: Iterable[Chunk]) -> bytes:
    return b"".join(chunk.data for chunk in chunks)

