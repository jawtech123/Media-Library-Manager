#!/usr/bin/env python3
from __future__ import annotations
import argparse
import json
import gzip
import os
import time
import random
from pathlib import Path
import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn
import threading
import logging
from logging.handlers import RotatingFileHandler
import sqlite3

# Embedded agent cache (SQLite) so agent.py is self-contained
class _EmbeddedAgentCache:
    SCHEMA = """
    CREATE TABLE IF NOT EXISTS agent_index (
        path TEXT PRIMARY KEY,
        inode_key TEXT,
        size INTEGER,
        mtime REAL,
        ctime REAL,
        probed INTEGER DEFAULT 0,
        hashed INTEGER DEFAULT 0,
        hash_algo TEXT,
        hash_sample_size INTEGER,
        sample_hash TEXT,
        full_hash TEXT,
        last_seen REAL,
        last_hashed_at REAL
    );
    CREATE INDEX IF NOT EXISTS idx_agent_inode ON agent_index(inode_key);

    CREATE TABLE IF NOT EXISTS outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id TEXT,
        payload_json TEXT NOT NULL,
        created_at REAL
    );
    CREATE INDEX IF NOT EXISTS idx_outbox_created ON outbox(created_at);

    CREATE TABLE IF NOT EXISTS scan_progress (
        root TEXT NOT NULL,
        phase TEXT NOT NULL,
        last_path TEXT,
        updated_at REAL,
        PRIMARY KEY (root, phase)
    );
    """

    @staticmethod
    def connect(db_path: Path) -> sqlite3.Connection:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path), timeout=30, isolation_level=None, check_same_thread=False)
        conn.executescript(_EmbeddedAgentCache.SCHEMA)
        return conn

    @staticmethod
    def get(conn: sqlite3.Connection, path: Path):
        cur = conn.execute(
            "SELECT path, inode_key, size, mtime, ctime, probed, hashed, hash_algo, hash_sample_size, sample_hash, full_hash, last_seen, last_hashed_at FROM agent_index WHERE path=?",
            (str(path),),
        )
        return cur.fetchone()

    @staticmethod
    def upsert_seen(conn: sqlite3.Connection, path: Path, inode_key: str, size: int, mtime: float, ctime: float, ts: float) -> None:
        conn.execute(
            """
            INSERT INTO agent_index(path, inode_key, size, mtime, ctime, last_seen)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(path) DO UPDATE SET inode_key=excluded.inode_key, size=excluded.size, mtime=excluded.mtime, ctime=excluded.ctime, last_seen=excluded.last_seen
            """,
            (str(path), inode_key, size, mtime, ctime, ts),
        )

    @staticmethod
    def mark_probed(conn: sqlite3.Connection, path: Path) -> None:
        conn.execute("UPDATE agent_index SET probed=1 WHERE path=?", (str(path),))

    @staticmethod
    def save_hashes(conn: sqlite3.Connection, path: Path, algo: str, sample_size: int | None, sample_hash: str | None, full_hash: str | None, ts: float) -> None:
        conn.execute(
            """
            UPDATE agent_index
            SET hashed=1, hash_algo=?, hash_sample_size=?, sample_hash=?, full_hash=?, last_hashed_at=?
            WHERE path=?
            """,
            (algo, sample_size, sample_hash, full_hash, ts, str(path)),
        )

    @staticmethod
    def valid_probe_cached(row: tuple, current_inode_key: str) -> bool:
        if not row:
            return False
        inode_key = row[1]
        return str(inode_key) == str(current_inode_key)

    @staticmethod
    def valid_hash_cached(row: tuple, current_inode_key: str, algo: str, sample_size: int | None):
        if not row:
            return False, None
        inode_key = row[1]
        if str(inode_key) != str(current_inode_key):
            return False, None
        hashed = row[6]
        if not hashed:
            return False, None
        h_algo = row[7] or ""
        h_sample_size = row[8] or 0
        s_hash = row[9]
        f_hash = row[10]
        if h_algo != algo:
            return False, None
        if h_sample_size != (sample_size or 0):
            return False, None
        return True, {"algo": algo, "sample_size": sample_size, "sample_hash": s_hash, "full_hash": f_hash}

    # Outbox helpers
    @staticmethod
    def enqueue_outbox(conn: sqlite3.Connection, batch_id: str, payload_json: str, ts: float) -> int:
        cur = conn.execute(
            "INSERT INTO outbox(batch_id, payload_json, created_at) VALUES(?,?,?)",
            (batch_id, payload_json, ts),
        )
        return int(cur.lastrowid)

    @staticmethod
    def read_outbox(conn: sqlite3.Connection):
        cur = conn.execute("SELECT id, batch_id, payload_json, created_at FROM outbox ORDER BY created_at ASC")
        return list(cur.fetchall())

    @staticmethod
    def delete_outbox(conn: sqlite3.Connection, row_id: int) -> None:
        conn.execute("DELETE FROM outbox WHERE id=?", (row_id,))

    # Scan progress helpers
    @staticmethod
    def save_progress(conn: sqlite3.Connection, root: str, phase: str, last_path: str | None, ts: float) -> None:
        conn.execute(
            """
            INSERT INTO scan_progress(root, phase, last_path, updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(root, phase) DO UPDATE SET last_path=excluded.last_path, updated_at=excluded.updated_at
            """,
            (root, phase, last_path, ts),
        )

    @staticmethod
    def load_progress(conn: sqlite3.Connection, root: str, phase: str):
        cur = conn.execute("SELECT last_path FROM scan_progress WHERE root=? AND phase=?", (root, phase))
        row = cur.fetchone()
        return row[0] if row and row[0] else None

    @staticmethod
    def clear_progress(conn: sqlite3.Connection, root: str, phase: str) -> None:
        conn.execute("DELETE FROM scan_progress WHERE root=? AND phase=?", (root, phase))

# Use embedded cache helper namespace as 'ac'
ac = _EmbeddedAgentCache

# Shared scan stats
_last_scan_stats = {}
_agent_active = False
_server_base_global = None
_cache_path_global = None
_scan_total_all = 0
_scan_total_videos = 0
_scan_phase = 0
_scan_phase_name = "idle"
_scan_seen_videos = 0

# Helper: normalize server input
def normalize_server_ip(ip: str, default_port: int = 8765) -> str:
    ip = ip.strip()
    if not ip:
        raise ValueError("Empty server IP")
    if ip.startswith("http://") or ip.startswith("https://"):
        base = ip
    else:
        base = f"http://{ip}:{default_port}"
    return base.rstrip("/")

# Minimal agent: scans roots, computes metadata + hashes, posts batches to server
try:
    from app.hashing import sample_hash, full_hash
    from app.metadata import has_ffprobe, probe_ffprobe
    from app.scan_common import default_inode_key
except Exception:
    # Allow running standalone if app package not importable (copy minimal logic)
    from hashlib import sha256
    def sample_hash(path: Path, algo: str, sample_size: int) -> str:
        size = path.stat().st_size
        if size == 0:
            return ""
        data = bytearray()
        with path.open("rb") as f:
            data.extend(f.read(min(sample_size, size)))
            if size > sample_size:
                mid_pos = max(0, size // 2 - sample_size // 2)
                f.seek(mid_pos)
                data.extend(f.read(min(sample_size, size - mid_pos)))
            if size > 2 * sample_size:
                f.seek(max(0, size - sample_size))
                data.extend(f.read(sample_size))
        return sha256(data).hexdigest()
    def full_hash(path: Path, algo: str) -> str:
        import hashlib
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024*1024), b""):
                h.update(chunk)
        return h.hexdigest()
    def has_ffprobe() -> bool:
        from shutil import which
        return which("ffprobe") is not None
    def probe_ffprobe(path: Path) -> dict:
        import subprocess, json
        try:
            out = subprocess.check_output(["ffprobe","-v","error","-show_format","-show_streams","-print_format","json",str(path)], stderr=subprocess.STDOUT)
            data = json.loads(out.decode("utf-8","ignore"))
        except Exception:
            return {}
        fmt = data.get("format", {})
        duration = float(fmt.get("duration", 0)) if fmt.get("duration") else None
        container = fmt.get("format_name")
        bitrate = int(fmt.get("bit_rate")) if fmt.get("bit_rate") else None
        video_codec = None
        audio_codecs = []
        width = height = None
        for s in data.get("streams", []):
            if s.get("codec_type") == "video" and video_codec is None:
                video_codec = s.get("codec_name")
                width = s.get("width")
                height = s.get("height")
            elif s.get("codec_type") == "audio":
                if s.get("codec_name"):
                    audio_codecs.append(s.get("codec_name"))
        return {
            "duration": duration,
            "container": container,
            "video_codec": video_codec,
            "audio_codecs": audio_codecs,
            "width": width,
            "height": height,
            "bitrate": bitrate,
            "streams_json": json.dumps(data),
        }
    def default_inode_key(st):
        parts = [str(st.st_size), str(int(st.st_mtime)), str(int(getattr(st, "st_ctime", 0))), str(getattr(st, "st_ino", 0)), str(getattr(st, "st_dev", 0))]
        return ":".join(parts)


import fnmatch

def iter_media_files(roots, video_exts, image_exts, subtitle_exts, xml_exts, other_exts, follow_links=False, junk_patterns=None, junk_exclude_exts=None, mode: str = "batched"):
    video_set = set(e.lower() for e in video_exts)
    image_set = set(e.lower() for e in image_exts)
    sub_set = set(e.lower() for e in subtitle_exts)
    xml_set = set(e.lower() for e in xml_exts)
    other_set = set(e.lower() for e in other_exts)
    junk_patterns = junk_patterns or []
    junk_exclude_exts = set((junk_exclude_exts or []))
    for root in roots:
        dir_count = 0
        file_count = 0
        matched = 0
        for dirpath, dirnames, filenames in os.walk(root, followlinks=follow_links):
            dir_count += 1
            for name in filenames:
                file_count += 1
                p = Path(dirpath) / name
                ext = p.suffix.lower()
                # Junk detection first
                if ext not in junk_exclude_exts:
                    for pat in junk_patterns:
                        if fnmatch.fnmatch(name.lower(), pat.lower()):
                            # If extension is marked as 'other', treat it as other instead of junk
                            if ext in other_set:
                                yield ("other", p, pat)
                            else:
                                yield ("junk", p, pat)
                            break
                    else:
                        # Not junk; classify
                        if ext in video_set:
                            matched += 1
                            yield ("video", p, None)
                        elif ext in image_set:
                            yield ("image", p, None)
                        elif ext in sub_set:
                            yield ("subtitle", p, None)
                        elif ext in xml_set:
                            yield ("xml", p, None)
                        elif ext in other_set:
                            yield ("other", p, None)
                        else:
                            yield ("unknown", p, None)
                # If ext is in junk_exclude_exts, skip junk classification but still classify
                else:
                    if ext in video_set:
                        matched += 1
                        yield ("video", p, None)
                    elif ext in image_set:
                        yield ("image", p, None)
                    elif ext in sub_set:
                        yield ("subtitle", p, None)
                    elif ext in xml_set:
                        yield ("xml", p, None)
                    elif ext in other_set:
                        yield ("other", p, None)
                    else:
                        yield ("unknown", p, None)
        print(f"Scanned root {root}: {dir_count} dirs, {file_count} files, {matched} video matches")


_sent_bytes_total = 0

def post_batch(server_base: str, files: list[dict], cache=None, batch_id: str | None = None, use_gzip: bool = False) -> int:
    global _sent_bytes_total
    if batch_id is None:
        batch_id = f"b-{int(time.time()*1000)}-{random.randint(1000,9999)}"
    payload = {"batch_id": batch_id, "files": files}
    raw = json.dumps(payload).encode("utf-8")
    try:
        if use_gzip:
            comp = gzip.compress(raw)
            headers = {"Content-Type": "application/json", "Content-Encoding": "gzip"}
            r = requests.post(server_base + "/ingest/batch", data=comp, headers=headers, timeout=120)
        else:
            r = requests.post(server_base + "/ingest/batch", json=payload, timeout=120)
        if not r.ok:
            raise requests.HTTPError(f"{r.status_code} {r.reason}: {r.text}")
        jd = r.json()
        processed = int(jd.get("processed", 0))
        echoed = jd.get("batch_id")
        logging.getLogger("medialib.agent").info(
            f"Sent batch: id={echoed or batch_id}, items={len(files)}, raw={len(raw)}B, processed={processed}"
        )
        _sent_bytes_total += len(raw)
        # opportunistically drain any queued outbox items now that we're online
        if cache is not None:
            try:
                rows = ac.read_outbox(cache)
                for row_id, bid, payload_json, created_at in rows:
                    try:
                        jd2 = json.loads(payload_json)
                        files2 = jd2.get("files") or []
                        r2 = requests.post(server_base + "/ingest/batch", json={"batch_id": bid, "files": files2}, timeout=60)
                        if r2.ok:
                            ac.delete_outbox(cache, row_id)
                    except Exception:
                        pass
            except Exception:
                pass
        return processed
    except Exception as e:
        logging.getLogger("medialib.agent").error(f"Batch post failed, enqueuing locally: {e}")
        try:
            if cache is not None:
                ac.enqueue_outbox(cache, batch_id, json.dumps(payload), time.time())
        except Exception:
            pass
        return 0

# Lightweight browse server to help host pick remote roots

def start_browse_server(port: int = 8877):
    app = FastAPI(title="MediaLib Agent")

    @app.get("/agent/ping")
    async def ping():
        return {"ok": True}

    @app.get("/agent/stats")
    async def stats():
        # include active flag even if no stats yet
        from datetime import datetime
        out = dict(_last_scan_stats)
        out["active"] = _agent_active
        if not out:
            return {"ok": False, "message": "no stats yet", "active": _agent_active}
        return out

    @app.get("/agent/ls")
    async def ls(path: str = "/"):
        p = Path(path)
        if not p.exists():
            return JSONResponse({"error": "not found"}, status_code=404)
        dirs = []
        files = []
        try:
            for entry in sorted(p.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
                if entry.is_dir():
                    dirs.append({"name": entry.name, "path": str(entry)})
                else:
                    files.append({"name": entry.name, "path": str(entry)})
        except PermissionError:
            return JSONResponse({"error": "permission denied"}, status_code=403)
        return {"path": str(p), "dirs": dirs, "files": files}

    @app.post("/agent/clear_cache")
    async def clear_cache():
        try:
            from pathlib import Path as _P
            global _cache_path_global
            if _cache_path_global:
                p = _P(_cache_path_global)
                if p.exists():
                    p.unlink()
                return {"ok": True, "cleared": True}
            return {"ok": True, "cleared": False}
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    @app.get("/agent/cache_info")
    async def cache_info():
        import os, sqlite3, time as _t
        try:
            db_path = _cache_path_global or ""
            exists = bool(db_path and os.path.exists(db_path))
            size_bytes = os.path.getsize(db_path) if exists else 0
            size_mib = size_bytes / (1024*1024) if size_bytes else 0.0
            rows = {"agent_index": 0, "outbox": 0, "scan_progress": 0}
            last = {"last_seen": None, "last_hashed_at": None, "progress_updated_at": None}
            if exists:
                try:
                    conn = sqlite3.connect(db_path)
                    cur = conn.cursor()
                    for tbl in ["agent_index","outbox","scan_progress"]:
                        try:
                            cur.execute(f"SELECT COUNT(1) FROM {tbl}")
                            rows[tbl] = int(cur.fetchone()[0])
                        except Exception:
                            rows[tbl] = 0
                    try:
                        cur.execute("SELECT MAX(last_seen), MAX(last_hashed_at) FROM agent_index")
                        a,b = cur.fetchone()
                        last["last_seen"] = float(a) if a is not None else None
                        last["last_hashed_at"] = float(b) if b is not None else None
                    except Exception:
                        pass
                    try:
                        cur.execute("SELECT MAX(updated_at) FROM scan_progress")
                        c = cur.fetchone()[0]
                        last["progress_updated_at"] = float(c) if c is not None else None
                    except Exception:
                        pass
                    conn.close()
                except Exception:
                    pass
            return {"ok": True, "db_path": db_path, "exists": exists, "size_bytes": size_bytes, "size_mib": round(size_mib,2), "rows": rows, "last": last, "ts": int(_t.time())}
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    @app.post("/agent/compact_cache")
    async def compact_cache():
        import sqlite3
        try:
            if not _cache_path_global:
                return {"ok": False, "error": "no cache path"}
            conn = sqlite3.connect(_cache_path_global)
            conn.execute("VACUUM")
            conn.close()
            return {"ok": True}
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    @app.post("/agent/scan_now")
    async def scan_now():
        try:
            global _agent_active
            if _agent_active:
                return {"ok": True, "started": False, "message": "already running"}
            # run two-pass in background
            def _bg():
                try:
                    # fetch config fresh
                    cfg = fetch_config(_server_base_global)
                    roots = cfg.get("remote_roots", [])
                    if not roots:
                        return
                    # pass 1: hashes only
                    for r in roots:
                        scan_once([r], do_probe=False, do_hashes=True, only_kinds=None)
                    # pass 2: ffprobe for videos only
                    for r in roots:
                        scan_once([r], do_probe=True, do_hashes=False, only_kinds={"video"})
                except Exception:
                    pass
            threading.Thread(target=_bg, daemon=True).start()
            return {"ok": True, "started": True}
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    def _run():
        uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t


def _setup_logging():
    logger = logging.getLogger("medialib.agent")
    logger.setLevel(logging.INFO)
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(ch)
    # Rotating file handler
    try:
        log_dir = Path.home() / ".medialib"
        log_dir.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(log_dir / "agent.log", maxBytes=5*1024*1024, backupCount=3)
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(fh)
    except Exception:
        pass
    return logger


def main():
    ap = argparse.ArgumentParser(description="MediaLib Linux Agent")
    ap.add_argument("server_ip", help="Host app IP address (port defaults to 8765)")
    ap.add_argument("--clear-cache", action="store_true", help="Clear local agent cache (~/.medialib/agent_cache.db) on startup")
    args = ap.parse_args()

    log = _setup_logging()

    # Start browse server (for host-side remote browsing)
    start_browse_server(8877)

    # Normalize server base URL and check connectivity
    server_base = normalize_server_ip(args.server_ip, 8765)
    global _server_base_global
    _server_base_global = server_base
    try:
        requests.get(server_base + "/health", timeout=5).raise_for_status()
        log.info(f"Connected to host server at {server_base}")
    except Exception as e:
        log.error(f"Cannot reach host server at {server_base}: {e}")
        # Continue running to allow offline queueing; we will try to drain outbox later

    # Initialize local agent cache (works with app.agent_cache or local agent_cache module; falls back to no-cache)
    cache_path = Path.home() / ".medialib" / "agent_cache.db"
    global _cache_path_global
    _cache_path_global = str(cache_path)
    if args.clear_cache:
        try:
            if cache_path.exists():
                cache_path.unlink()
                log.info(f"Cleared agent cache at {cache_path}")
        except Exception as e:
            log.warning(f"Failed to clear agent cache: {e}")
    try:
        cache = ac.connect(cache_path)
    except Exception:
        cache = None

    # Drain local outbox first (if any)
    if cache is not None:
        try:
            rows = ac.read_outbox(cache)
        except Exception:
            rows = []
        if rows:
            log.info(f"Draining local outbox with {len(rows)} pending batch(es)")
        local_use_gzip = False
        for row_id, bid, payload_json, created_at in rows:
            try:
                jd = json.loads(payload_json)
                files = jd.get("files") or []
                processed = post_batch(server_base, files, cache=cache, batch_id=bid, use_gzip=local_use_gzip)
                if processed >= 0:
                    try:
                        ac.delete_outbox(cache, row_id)
                    except Exception:
                        pass
            except Exception as e:
                log.warning(f"Failed to resend outbox batch id={bid}: {e}")

    # Helper to fetch config safely
    def fetch_config(server_base: str) -> dict:
        try:
            return requests.get(server_base + "/ingest/config", timeout=30).json()
        except Exception:
            return {}

    # Fetch config from server
    cfg = fetch_config(server_base)
    roots = cfg.get("remote_roots", [])
    log.info(f"Fetched config: {len(roots)} roots, batch={cfg.get('batch_size')}, workers={cfg.get('agent_max_workers')}")
    algo = cfg.get("hash_algo", "xxhash64")
    sample = int(cfg.get("hash_sample_size", 4*1024*1024))
    do_full = bool(cfg.get("do_full_hash", False))
    batch_size = int(cfg.get("batch_size", 500))
    agent_workers = int(cfg.get("agent_max_workers", 4))
    use_gzip = bool(cfg.get("agent_gzip", False))
    adaptive = bool(cfg.get("agent_adaptive", True))
    off_start = int(cfg.get("agent_offpeak_start", 1))
    off_end = int(cfg.get("agent_offpeak_end", 6))
    walk_mode = str(cfg.get("walk_mode", "batched"))
    # no tokens used
    video_exts = tuple(cfg.get("media_extensions", [".mp4", ".mkv"]))
    image_exts = tuple(cfg.get("image_extensions", [".jpg", ".png"]))
    subtitle_exts = tuple(cfg.get("subtitle_extensions", [".srt", ".ass"]))
    xml_exts = tuple(cfg.get("xml_extensions", [".xml", ".nfo"]))
    other_exts = tuple(cfg.get("other_extensions", []))
    follow_links = bool(cfg.get("follow_symlinks", False))
    junk_patterns = cfg.get("junk_patterns", [])
    junk_exclude_exts = cfg.get("junk_exclude_extensions", [])

    # Wait for remote roots if none configured yet
    wait_secs = 0
    while not roots and wait_secs < 600:
        print("No remote roots configured on server. Waiting (agent will keep running)...")
        time.sleep(10)
        wait_secs += 10
        try:
            cfg = requests.get(server_base + "/ingest/config", timeout=30).json()
            roots = cfg.get("remote_roots", [])
        except Exception:
            pass
    if not roots:
        print("Still no remote roots after waiting. Continuing to allow offline queueing.")

    def scan_root_with_resume(root: str, phase: str, do_probe: bool, do_hashes: bool, only_kinds: set[str] | None) -> int:
        # Load last cursor for this root+phase
        last_path = None
        if cache is not None:
            try:
                last_path = ac.load_progress(cache, root, phase)
            except Exception:
                last_path = None
        # Scan this single root
        return scan_once([root], do_probe=do_probe, do_hashes=do_hashes, only_kinds=only_kinds, phase=phase, cursor=last_path)

    def scan_once(roots_list: list[str], do_probe: bool = True, do_hashes: bool = True, only_kinds: set[str] | None = None, phase: str | None = None, cursor: str | None = None) -> int:
        nonlocal log
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import time
        global _agent_active
        _agent_active = True
        processed_inner = 0  # uploaded
        batch: list[dict] = []
        count_seen = 0  # discovered
        batches_sent = 0
        start_ts = time.time()
        per_kind = {"video": 0, "image": 0, "subtitle": 0, "xml": 0, "other": 0, "unknown": 0, "junk": 0}
        total_bytes = 0
        errors = 0

        def build_item(kind, p: Path, pat):
            try:
                st = p.stat()
                if kind == "junk":
                    per_kind["junk"] += 1
                    return {
                        "kind": "junk",
                        "path": str(p),
                        "size": st.st_size,
                        "mtime": st.st_mtime,
                        "ctime": st.st_ctime,
                        "ext": p.suffix.lower(),
                        "reason": f"pattern: {pat}",
                    }
                inode = default_inode_key(st)
                item = {
                    "kind": kind,
                    "path": str(p),
                    "size": st.st_size,
                    "mtime": st.st_mtime,
                    "ctime": st.st_ctime,
                    "inode_key": inode,
                    "ext": p.suffix.lower(),
                }
                total_bytes_local = st.st_size
                # record seen in cache
                try:
                    ac.upsert_seen(cache, p, inode, st.st_size, st.st_mtime, st.st_ctime, time.time())
                except Exception:
                    pass
                # Optionally include only requested kinds
                if only_kinds is not None and kind not in only_kinds:
                    return None
                # Optional ffprobe pass with cache
                if do_probe and kind == "video" and has_ffprobe():
                    row = None
                    try:
                        row = ac.get(cache, p)
                    except Exception:
                        row = None
                    if row and ac.valid_probe_cached(row, inode):
                        # already probed for this inode; skip sending metadata again
                        pass
                    else:
                        try:
                            meta = probe_ffprobe(p)
                            if meta:
                                item["metadata"] = meta
                                try:
                                    ac.mark_probed(cache, p)
                                except Exception:
                                    pass
                        except Exception:
                            pass
                # Defer full hashes outside off-peak window
                do_full_effective = do_full
                try:
                    h = time.localtime().tm_hour
                    if not (off_start <= h < off_end):
                        do_full_effective = False
                except Exception:
                    pass
                # Optional hashes pass with cache reuse
                if do_hashes:
                    row = None
                    try:
                        row = ac.get(cache, p)
                    except Exception:
                        row = None
                    reused = False
                    if row:
                        ok, h = ac.valid_hash_cached(row, inode, algo, sample)
                        if ok and h:
                            item["hashes"] = h
                            reused = True
                    if not reused:
                        s_hash = sample if sample else None
                        if sample:
                            s_hash = sample_hash(p, algo, sample)
                        f_hash = full_hash(p, algo) if do_full_effective else None
                        h = {"algo": algo, "sample_size": sample, "sample_hash": s_hash, "full_hash": f_hash}
                        item["hashes"] = h
                        try:
                            ac.save_hashes(cache, p, algo, sample, s_hash, f_hash, time.time())
                        except Exception:
                            pass
                # Track stats
                per_kind[kind] = per_kind.get(kind, 0) + 1
                return item, total_bytes_local
            except Exception as e:
                try:
                    logging.getLogger("medialib.agent").error(f"build_item error for {kind} {p}: {e}")
                except Exception:
                    pass
                return None

        # Use high-ceiling executor, gate concurrency with a semaphore so we can adapt at runtime
        max_ceiling = max(agent_workers, 8)
        with ThreadPoolExecutor(max_workers=max_ceiling) as pool:
            permits = threading.Semaphore(agent_workers)
            current_limit = agent_workers
            last_adjust = time.time()
            futs = []
            last_post = time.time()
            # If resuming, we'll skip until we pass the cursor path
            skipping = bool(cursor)
            cursor_path = cursor or ""
            for kind, p, pat in iter_media_files(roots_list, video_exts, image_exts, subtitle_exts, xml_exts, other_exts, follow_links, junk_patterns, junk_exclude_exts, mode=walk_mode):
                sp = str(p)
                if skipping:
                    if sp <= cursor_path:
                        continue
                    else:
                        skipping = False
                if count_seen % 1000 == 0 and count_seen > 0:
                    log.info(f"Seen {count_seen} files... current batch {len(batch)}")
                count_seen += 1
                # Periodically persist cursor so we can resume
                if cache is not None and (count_seen % 500 == 0 or (len(batch) >= batch_size)):
                    try:
                        ac.save_progress(cache, roots_list[0], phase or ("hashes" if do_hashes and not do_probe else "ffprobe"), sp, time.time())
                    except Exception:
                        pass
                if adaptive:
                    permits.acquire()
                    futs.append(pool.submit(lambda k=kind, pp=p, pa=pat: (permits.release(), build_item(k, pp, pa))[-1]))
                else:
                    futs.append(pool.submit(build_item, kind, p, pat))
                # Reap frequently to keep memory bounded and feed batches
                if len(futs) >= agent_workers:
                    done = [f for f in futs if f.done()]
                    for f in done:
                        futs.remove(f)
                        try:
                            item = f.result()
                        except Exception as e:
                            log.error(f"Future error: {e}")
                            errors += 1
                            continue
                        if item:
                            # item may be (dict) or (dict, bytes)
                            if isinstance(item, tuple):
                                d, b = item
                                total_bytes += int(b or 0)
                                batch.append(d)
                            else:
                                batch.append(item)
                # Time-based flush to avoid waiting for full batches forever
                now = time.time()
                if batch and (len(batch) >= batch_size or (now - last_post) >= 2.0):
                    try:
                        processed_inner += post_batch(server_base, batch, cache=cache, use_gzip=use_gzip)
                        log.info(f"Posted batch ({len(batch)} items). Total uploaded: {processed_inner}")
                        batches_sent += 1
                        last_post = now
                    except Exception as e:
                        log.error(f"Batch post failed: {e}")
                    finally:
                        batch.clear()
            # Drain remaining futures
            for f in as_completed(futs):
                try:
                    item = f.result()
                except Exception as e:
                    log.error(f"Future error: {e}")
                    errors += 1
                    continue
                if item:
                    if isinstance(item, tuple):
                        d, b = item
                        total_bytes += int(b or 0)
                        # track videos seen during processing
                        try:
                            if (_scan_phase == 2) and (d.get("kind") == "video"):
                                globals()["_scan_seen_videos"] = int(globals().get("_scan_seen_videos", 0)) + 1
                        except Exception:
                            pass
                        batch.append(d)
                    else:
                        try:
                            if (_scan_phase == 2) and (item.get("kind") == "video"):
                                globals()["_scan_seen_videos"] = int(globals().get("_scan_seen_videos", 0)) + 1
                        except Exception:
                            pass
                        batch.append(item)
                    now = time.time()
                    if batch and (len(batch) >= batch_size or (now - last_post) >= 2.0):
                        # simple adaptive tuner: adjust permit limit every 2s
                        if adaptive and (now - last_adjust) >= 2.0:
                            backlog = len([f for f in futs if not f.done()])
                            # increase if backlog small and rate good
                            if backlog < current_limit // 2 and rate > 5.0 and current_limit < max_ceiling:
                                current_limit += 1
                                permits.release()
                            # decrease if backlog is huge or errors increase
                            if backlog > current_limit * 2 and current_limit > 1:
                                try:
                                    permits.acquire(blocking=False)
                                    current_limit -= 1
                                except Exception:
                                    pass
                            last_adjust = now
                        try:
                            processed_inner += post_batch(server_base, batch, cache=cache, use_gzip=use_gzip)
                            log.info(f"Posted batch ({len(batch)} items). Total uploaded: {processed_inner}")
                            batches_sent += 1
                            last_post = now
                        except Exception as e:
                            log.error(f"Batch post failed: {e}")
                        finally:
                            batch.clear()
                else:
                    errors += 1
        # Final flush
        if batch:
            try:
                processed_inner += post_batch(server_base, batch, cache=cache, use_gzip=use_gzip)
                log.info(f"Posted final batch ({len(batch)} items). Total uploaded: {processed_inner}")
                batches_sent += 1
            except Exception as e:
                log.error(f"Final batch post failed: {e}")
            finally:
                batch.clear()
        # Clear cursor for this root+phase on successful completion
        if cache is not None:
            try:
                ac.clear_progress(cache, roots_list[0], phase or ("hashes" if do_hashes and not do_probe else "ffprobe"))
            except Exception:
                pass
        _agent_active = False
        elapsed = max(0.0001, time.time() - start_ts)
        rate = processed_inner / elapsed
        mb = total_bytes / (1024*1024)
        stats = {
            "ts": int(time.time()),
            "roots": list(roots_list),
            "active": _agent_active,
            "elapsed": elapsed,
            "seen": count_seen,
            "uploaded": processed_inner,
            "batches": batches_sent,
            "rate_files_per_s": rate,
            "data_mib": mb,
            "kinds": per_kind,
            "workers": agent_workers,
            "batch_size": batch_size,
            "sent_bytes": _sent_bytes_total,
            "errors": errors,
            "total_all": int(_scan_total_all),
            "total_videos": int(_scan_total_videos),
            "phase": int(_scan_phase),
            "phase_name": str(_scan_phase_name),
            "seen_videos": int(_scan_seen_videos),
        }
        try:
            global _last_scan_stats
            _last_scan_stats = stats
        except Exception:
            pass
        print(f"Scan complete in {elapsed:.1f}s. Seen: {count_seen}, Uploaded: {processed_inner}, Batches: {batches_sent}, Rate: {rate:.1f} files/s, Data: {mb:.1f} MiB, Kinds: {per_kind}, Roots: {len(roots_list)}, Workers: {agent_workers}, Batch: {batch_size}")
        return processed_inner

    # Continuous loop: refresh config and rescan periodically
    while True:
        try:
            cfg = requests.get(server_base + "/ingest/config", timeout=30).json()
            roots = cfg.get("remote_roots", [])
            if not roots:
                print("No remote roots configured on server. Waiting...")
                time.sleep(10)
                continue
            log.info(f"Starting scan of {len(roots)} roots")
            total_processed = 0
            # Pre-count totals for better progress (all files and video files)
            try:
                def _count_all(rs):
                    import os
                    c_all = 0
                    c_vid = 0
                    exts_video = set([x.lower() for x in cfg.get("media_extensions", [])])
                    for r in rs:
                        for dp, dn, fn in os.walk(r, followlinks=bool(cfg.get("follow_symlinks", False))):
                            for name in fn:
                                c_all += 1
                                ext = ("." + name.rsplit(".",1)[-1]).lower() if "." in name else ""
                                if ext in exts_video:
                                    c_vid += 1
                    return c_all, c_vid
                total_all, total_vid = _count_all(roots)
                log.info(f"Pre-count: total files={total_all}, video files={total_vid}")
                global _scan_total_all, _scan_total_videos
                _scan_total_all, _scan_total_videos = total_all, total_vid
            except Exception:
                pass
            # Pass 1: hashes only, no ffprobe (all kinds)
            global _scan_phase, _scan_phase_name, _scan_seen_videos
            _scan_phase = 1
            _scan_phase_name = "hashes"
            _scan_seen_videos = 0
            for r in roots:
                log.info(f"Scanning root (pass1 no-probe): {r} (total_all={_scan_total_all})")
                processed = scan_root_with_resume(r, phase="hashes", do_probe=False, do_hashes=True, only_kinds=None)
                log.info(f"Root pass1 done: {r} uploaded {processed}")
                total_processed += processed
            # Pass 2: ffprobe for videos only (no hashes)
            _scan_phase = 2
            _scan_phase_name = "ffprobe"
            _scan_seen_videos = 0
            for r in roots:
                log.info(f"Scanning root (pass2 ffprobe): {r} (total_videos={_scan_total_videos})")
                processed = scan_root_with_resume(r, phase="ffprobe", do_probe=True, do_hashes=False, only_kinds={"video"})
                log.info(f"Root pass2 done: {r} uploaded {processed}")
                total_processed += processed
            # Sleep before next pass; shorter if we processed nothing
            sleep_secs = 300 if total_processed > 0 else 60
            log.info(f"Sleeping {sleep_secs}s before next pass...")
            time.sleep(sleep_secs)
        except KeyboardInterrupt:
            print("Agent stopped by user.")
            break
        except Exception as e:
            print(f"Agent loop error: {e}. Retrying in 15s...")
            time.sleep(15)

if __name__ == "__main__":
    main()
