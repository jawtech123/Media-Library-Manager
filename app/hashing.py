from __future__ import annotations
from pathlib import Path
from typing import Optional, Literal
import hashlib

try:
    import blake3  # type: ignore
except Exception:
    blake3 = None  # type: ignore

try:
    import xxhash  # type: ignore
except Exception:
    xxhash = None  # type: ignore

Algo = Literal["blake3", "xxhash64", "sha256"]


def _hash_stream(path: Path, algo: Algo, chunk_size: int = 4 * 1024 * 1024) -> str:
    if algo == "blake3" and blake3 is not None:
        h = blake3.blake3()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                h.update(chunk)
        return h.hexdigest()
    elif algo == "xxhash64" and xxhash is not None:
        h = xxhash.xxh64()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                h.update(chunk)
        return h.hexdigest()
    else:
        # fallback to sha256
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                h.update(chunk)
        return h.hexdigest()


def sample_hash(path: Path, algo: Algo, sample_size: int) -> str:
    size = path.stat().st_size
    if size == 0:
        return ""
    data = bytearray()
    with path.open("rb") as f:
        # first sample_size
        data.extend(f.read(min(sample_size, size)))
        if size > sample_size:
            # middle sample
            mid_pos = max(0, size // 2 - sample_size // 2)
            f.seek(mid_pos)
            data.extend(f.read(min(sample_size, size - mid_pos)))
        if size > 2 * sample_size:
            # tail sample
            f.seek(max(0, size - sample_size))
            data.extend(f.read(sample_size))
    if algo == "blake3" and blake3 is not None:
        return blake3.blake3(data).hexdigest()
    elif algo == "xxhash64" and xxhash is not None:
        return xxhash.xxh64(data).hexdigest()
    else:
        return hashlib.sha256(data).hexdigest()


def full_hash(path: Path, algo: Algo) -> str:
    return _hash_stream(path, algo)
