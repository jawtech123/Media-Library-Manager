from __future__ import annotations
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Callable

from .settings import Settings
from typing import Literal
from . import db as dbm
from .hashing import sample_hash, full_hash, Algo
from .metadata import has_ffprobe, probe_ffprobe


@dataclass
class ScanResult:
    files_processed: int = 0
    metadata_count: int = 0
    hashed_count: int = 0


from .scan_common import default_inode_key, classify_extension


def iter_media_files(roots: list[Path], settings: Settings) -> Iterable[Path]:
    # Iterate all files to classify them; follow symlinks if enabled
    for root in roots:
        for dirpath, dirnames, filenames in os.walk(root, followlinks=settings.follow_symlinks):
            for name in filenames:
                p = Path(dirpath) / name
                yield p


def process_file(conn, p: Path, settings: Settings, algo: Algo) -> tuple[int, bool, bool]: # returns (file_id, did_meta, did_hash)
    st = p.stat()
    inode = default_inode_key(st)

    # Check existing
    existing = dbm.get_file_row(conn, p)
    if existing is not None:
        file_id, size0, mtime0, ctime0, inode0 = existing
    else:
        file_id = None
        size0 = mtime0 = ctime0 = inode0 = None

    # Upsert file row (ensures record exists)
    category = classify_extension(p.suffix, settings)
    file_id = dbm.upsert_file(conn, p, st.st_size, st.st_mtime, st.st_ctime, inode, p.suffix, category)

    did_meta = False
    did_hash = False

    unchanged = (
        settings.skip_unchanged and size0 is not None and
        size0 == st.st_size and int(mtime0) == int(st.st_mtime) and str(inode0) == inode
    )

    # metadata (video only, new files only)
    is_new = existing is None
    if has_ffprobe() and is_new and category == "video":
        meta = probe_ffprobe(p)
        if meta:
            dbm.upsert_metadata(conn, file_id, meta)
            did_meta = True

    # hashing
    if not unchanged:
        s_hash = sample_hash(p, settings.hash_algo, settings.hash_sample_size) if settings.hash_sample_size else None
        f_hash = None
        if settings.do_full_hash:
            f_hash = full_hash(p, settings.hash_algo)
        dbm.upsert_hash(conn, file_id, settings.hash_algo, settings.hash_sample_size, s_hash, f_hash, time.time())
        did_hash = True

    return file_id, did_meta, did_hash


def scan(conn, roots: list[Path], settings: Settings, progress_cb: Callable[[ScanResult], None] | None = None) -> ScanResult:
    res = ScanResult()
    # Stream tasks instead of building a full list, to start work immediately and reduce memory
    with ThreadPoolExecutor(max_workers=settings.max_workers) as pool:
        futs: set = set()
        def submit_path(p: Path):
            futs.add(pool.submit(process_file, conn, p, settings, settings.hash_algo))
        for p in iter_media_files(roots, settings):
            submit_path(p)
            # Reap completed futures to keep the set small
            done = [f for f in futs if f.done()]
            for f in done:
                futs.remove(f)
                try:
                    _, did_meta, did_hash = f.result()
                    res.files_processed += 1
                    if did_meta:
                        res.metadata_count += 1
                    if did_hash:
                        res.hashed_count += 1
                    if progress_cb:
                        progress_cb(res)
                except Exception:
                    res.files_processed += 1
                    if progress_cb:
                        progress_cb(res)
        # Drain remaining
        for f in as_completed(list(futs)):
            try:
                _, did_meta, did_hash = f.result()
                res.files_processed += 1
                if did_meta:
                    res.metadata_count += 1
                if did_hash:
                    res.hashed_count += 1
                if progress_cb:
                    progress_cb(res)
            except Exception:
                res.files_processed += 1
                if progress_cb:
                    progress_cb(res)
    return res
