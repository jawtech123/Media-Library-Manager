from __future__ import annotations
import threading
import time
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Request, HTTPException
import logging
from logging.handlers import RotatingFileHandler
from fastapi.responses import JSONResponse
import uvicorn
from pathlib import Path

from .settings import Settings
from . import db as dbm
from pathlib import Path

# Shared stats for GUI
_last_ingest_ts: Optional[float] = None
_last_ingest_count: int = 0
_server_thread: Optional[threading.Thread] = None
_server_should_stop = threading.Event()
_server_instance: Optional[object] = None


def get_ingest_stats() -> dict:
    return {
        "last_ingest_ts": _last_ingest_ts,
        "last_ingest_count": _last_ingest_count,
        "running": _server_thread is not None and _server_thread.is_alive(),
    }


def _setup_server_logging():
    logger = logging.getLogger("medialib.server")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(ch)
    try:
        log_dir = Path.home() / ".medialib"
        log_dir.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(log_dir / "server.log", maxBytes=5*1024*1024, backupCount=3)
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(fh)
    except Exception:
        pass
    return logger


def create_app(settings: Settings):
    app = FastAPI(title="MediaLib Ingestion API")
    conn = dbm.connect(settings.db_path)
    logger = _setup_server_logging()
    logger.info("Ingestion server started")

    @app.get("/ingest/config")
    async def ingest_config():
        # Provide agent configuration: remote roots and hashing/scan settings
        try:
            from .settings import Settings
            from .db import list_remote_roots
        except Exception:
            from .db import list_remote_roots
        cfg = {
            "remote_roots": list_remote_roots(conn),
            "media_extensions": list(getattr(settings, "media_extensions", [])),
            "image_extensions": list(getattr(settings, "image_extensions", [])),
            "subtitle_extensions": list(getattr(settings, "subtitle_extensions", [])),
            "xml_extensions": list(getattr(settings, "xml_extensions", [])),
            "other_extensions": list(getattr(settings, "other_extensions", [])), 
            "junk_patterns": list(getattr(settings, "junk_patterns", [])),
            "junk_exclude_extensions": list(getattr(settings, "junk_exclude_extensions", [])),
            "hash_algo": getattr(settings, "hash_algo", "xxhash64"),
            "hash_sample_size": getattr(settings, "hash_sample_size", 4*1024*1024),
            "do_full_hash": getattr(settings, "do_full_hash", False),
            "batch_size": getattr(settings, "agent_batch_size", 500),
            "agent_max_workers": getattr(settings, "agent_max_workers", 4),  # controls agent threading
            "agent_gzip": getattr(settings, "agent_gzip", False),
            "agent_adaptive": getattr(settings, "agent_adaptive", True),
            "agent_offpeak_start": getattr(settings, "agent_offpeak_start", 1),
            "agent_offpeak_end": getattr(settings, "agent_offpeak_end", 6),
            "walk_mode": getattr(settings, "walk_mode", "batched"),
            "follow_symlinks": getattr(settings, "follow_symlinks", False),
        }
        return cfg

    @app.get("/")
    async def root():
        return {"service": "MediaLib Ingestion API", "endpoints": ["/health", "/ingest/config", "/ingest/batch"]}

    @app.get("/health")
    async def health():
        return {"ok": True}

    @app.post("/ingest/batch")
    async def ingest_batch(request: Request):
        global _last_ingest_ts, _last_ingest_count
        # Parse possibly gzipped JSON
        if request.headers.get("Content-Encoding", "").lower() == "gzip":
            raw = await request.body()
            try:
                data = json.loads(gzip.decompress(raw).decode("utf-8"))
            except Exception:
                raise HTTPException(400, "invalid gzip json")
        else:
            try:
                data = await request.json()
            except Exception:
                raise HTTPException(400, "invalid json")
        batch_id = data.get("batch_id")
        files = data.get("files", [])
        if not isinstance(files, list):
            raise HTTPException(400, "files must be a list")
        processed = 0
        ts_now = time.time()
        try:
            logger.info(f"/ingest/batch received: id={batch_id} items={len(files)}")
        except Exception:
            pass
        for idx, item in enumerate(files):
            try:
                kind = item.get("kind", "media")
                path = item["path"]
                size = int(item["size"]) if item.get("size") is not None else 0
                mtime = float(item.get("mtime", ts_now))
                ctime = float(item.get("ctime", ts_now))
                inode_key = str(item.get("inode_key", ""))
                ext = str(item.get("ext", ""))
                if kind == "media":
                    # Backward-compat: classify on server if agent sent generic 'media'
                    from .settings import Settings as _S
                    s = _S()
                    def _classify(ext_: str) -> str:
                        e = (ext_ or "").lower()
                        if e in [x.lower() for x in getattr(s, "media_extensions", [])]:
                            return "video"
                        if e in [x.lower() for x in getattr(s, "image_extensions", [])]:
                            return "image"
                        if e in [x.lower() for x in getattr(s, "subtitle_extensions", [])]:
                            return "subtitle"
                        if e in [x.lower() for x in getattr(s, "xml_extensions", [])]:
                            return "xml"
                        if e in [x.lower() for x in getattr(s, "other_extensions", [])]:
                            return "other"
                        return "unknown"
                    category = _classify(ext)
                    file_id = dbm.upsert_file(conn, Path(path), size, mtime, ctime, inode_key, ext, category)
                    h = item.get("hashes") or {}
                    if h:
                        dbm.upsert_hash(conn, file_id, h.get("algo", "xxhash64"), h.get("sample_size"), h.get("sample_hash"), h.get("full_hash"), ts_now)
                    m = item.get("metadata") or {}
                    if m and isinstance(m, dict):
                        dbm.upsert_metadata(conn, file_id, m)
                elif kind in {"video", "image", "subtitle", "xml", "other", "unknown"}:
                    file_id = dbm.upsert_file(conn, Path(path), size, mtime, ctime, inode_key, ext, kind)
                    h = item.get("hashes") or {}
                    if h:
                        dbm.upsert_hash(conn, file_id, h.get("algo", "xxhash64"), h.get("sample_size"), h.get("sample_hash"), h.get("full_hash"), ts_now)
                    m = item.get("metadata") or {}
                    if m and isinstance(m, dict):
                        dbm.upsert_metadata(conn, file_id, m)
                else:
                    dbm.upsert_junk(conn, path, size, mtime, ext, item.get("reason"))
                processed += 1
            except Exception as e:
                try:
                    logger.error(f"/ingest/batch item error idx={idx} path={item.get('path')} err={e}")
                except Exception:
                    pass
                continue
        _last_ingest_ts = time.time()
        _last_ingest_count += processed
        try:
            logger.info(f"/ingest/batch processed={processed} id={batch_id}")
        except Exception:
            pass
        return {"processed": processed, "batch_id": batch_id}

    return app


from pathlib import Path

def _run_server(settings: Settings):
    global _server_instance
    app = create_app(settings)
    config = uvicorn.Config(app, host=getattr(settings, "ingest_host", "127.0.0.1"), port=getattr(settings, "ingest_port", 8765), log_level="info")
    server = uvicorn.Server(config)
    _server_instance = server
    server.run()
    _server_instance = None


def start_server(settings: Settings) -> None:
    global _server_thread
    if _server_thread and _server_thread.is_alive():
        return
    _server_thread = threading.Thread(target=_run_server, args=(settings,), daemon=True)
    _server_thread.start()


def stop_server() -> None:
    global _server_instance
    if _server_instance is not None:
        try:
            _server_instance.should_exit = True  # type: ignore[attr-defined]
        except Exception:
            pass
