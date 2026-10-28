import random

from metadata_service.chunking import (
    content_defined_chunks,
    fixed_size_chunks,
    reconstruct,
)


def test_fixed_size_chunking_preserves_order_and_hashes() -> None:
    data = b"abcdefghij"
    chunks = fixed_size_chunks(data, chunk_size=4)
    assert [chunk.data for chunk in chunks] == [b"abcd", b"efgh", b"ij"]
    assert reconstruct(chunks) == data
    assert all(len(chunk.sha256) == 64 for chunk in chunks)


def test_content_defined_chunking_has_content_selected_boundaries() -> None:
    generator = random.Random(2026)
    data = bytes(generator.randrange(256) for _ in range(32_000))
    chunks = content_defined_chunks(data, min_size=256, avg_size=1024, max_size=4096)
    sizes = [len(chunk.data) for chunk in chunks]
    assert reconstruct(chunks) == data
    assert all(256 <= size <= 4096 for size in sizes[:-1])
    assert len(set(sizes[:-1])) > 1


def test_content_defined_chunking_improves_shifted_data_deduplication() -> None:
    generator = random.Random(44)
    original = bytes(generator.randrange(256) for _ in range(80_000))
    shifted = b"inserted-prefix" * 37 + original
    fixed_original = {chunk.sha256 for chunk in fixed_size_chunks(original, 1024)}
    fixed_shifted = {chunk.sha256 for chunk in fixed_size_chunks(shifted, 1024)}
    cdc_original = {
        chunk.sha256
        for chunk in content_defined_chunks(original, min_size=256, avg_size=1024, max_size=4096)
    }
    cdc_shifted = {
        chunk.sha256
        for chunk in content_defined_chunks(shifted, min_size=256, avg_size=1024, max_size=4096)
    }
    assert len(cdc_original & cdc_shifted) > len(fixed_original & fixed_shifted)

