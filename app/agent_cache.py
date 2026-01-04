from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Optional, Tuple

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
-- scan progress per root and phase (hashes|ffprobe)
CREATE TABLE IF NOT EXISTS scan_progress (
    root TEXT NOT NULL,
    phase TEXT NOT NULL,
    last_path TEXT,
    updated_at REAL,
    PRIMARY KEY (root, phase)
);
"""

def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30, isolation_level=None, check_same_thread=False)
    conn.executescript(SCHEMA)
    return conn


def get(conn: sqlite3.Connection, path: Path) -> Optional[tuple]:
    cur = conn.execute("SELECT path, inode_key, size, mtime, ctime, probed, hashed, hash_algo, hash_sample_size, sample_hash, full_hash, last_seen, last_hashed_at FROM agent_index WHERE path=?", (str(path),))
    return cur.fetchone()


def upsert_seen(conn: sqlite3.Connection, path: Path, inode_key: str, size: int, mtime: float, ctime: float, ts: float) -> None:
    conn.execute(
        """
        INSERT INTO agent_index(path, inode_key, size, mtime, ctime, last_seen)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(path) DO UPDATE SET inode_key=excluded.inode_key, size=excluded.size, mtime=excluded.mtime, ctime=excluded.ctime, last_seen=excluded.last_seen
        """,
        (str(path), inode_key, size, mtime, ctime, ts),
    )


def mark_probed(conn: sqlite3.Connection, path: Path) -> None:
    conn.execute("UPDATE agent_index SET probed=1 WHERE path=?", (str(path),))


def save_hashes(conn: sqlite3.Connection, path: Path, algo: str, sample_size: int | None, sample_hash: str | None, full_hash: str | None, ts: float) -> None:
    conn.execute(
        """
        UPDATE agent_index
        SET hashed=1, hash_algo=?, hash_sample_size=?, sample_hash=?, full_hash=?, last_hashed_at=?
        WHERE path=?
        """,
        (algo, sample_size, sample_hash, full_hash, ts, str(path)),
    )


def valid_probe_cached(row: tuple, current_inode_key: str) -> bool:
    if not row:
        return False
    _, inode_key, *_rest = row
    return str(inode_key) == str(current_inode_key)


def valid_hash_cached(row: tuple, current_inode_key: str, algo: str, sample_size: int | None) -> Tuple[bool, Optional[dict]]:
    if not row:
        return False, None
    _, inode_key, *_pre, probed, hashed, h_algo, h_sample_size, s_hash, f_hash, *_post = row
    if str(inode_key) != str(current_inode_key):
        return False, None
    if not hashed:
        return False, None
    if (h_algo or "") != algo:
        return False, None
    if (h_sample_size or 0) != (sample_size or 0):
        return False, None
    return True, {"algo": algo, "sample_size": sample_size, "sample_hash": s_hash, "full_hash": f_hash}

# Outbox helpers

def enqueue_outbox(conn: sqlite3.Connection, batch_id: str, payload_json: str, ts: float) -> int:
    cur = conn.execute(
        "INSERT INTO outbox(batch_id, payload_json, created_at) VALUES(?,?,?)",
        (batch_id, payload_json, ts),
    )
    return int(cur.lastrowid)


def read_outbox(conn: sqlite3.Connection) -> list[tuple[int, str, str, float]]:
    cur = conn.execute("SELECT id, batch_id, payload_json, created_at FROM outbox ORDER BY created_at ASC")
    return list(cur.fetchall())


def delete_outbox(conn: sqlite3.Connection, row_id: int) -> None:
    conn.execute("DELETE FROM outbox WHERE id=?", (row_id,))

# Scan progress helpers

def save_progress(conn: sqlite3.Connection, root: str, phase: str, last_path: str | None, ts: float) -> None:
    conn.execute(
        """
        INSERT INTO scan_progress(root, phase, last_path, updated_at)
        VALUES(?,?,?,?)
        ON CONFLICT(root, phase) DO UPDATE SET last_path=excluded.last_path, updated_at=excluded.updated_at
        """,
        (root, phase, last_path, ts),
    )


def load_progress(conn: sqlite3.Connection, root: str, phase: str) -> str | None:
    cur = conn.execute("SELECT last_path FROM scan_progress WHERE root=? AND phase=?", (root, phase))
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def clear_progress(conn: sqlite3.Connection, root: str, phase: str) -> None:
    conn.execute("DELETE FROM scan_progress WHERE root=? AND phase=?", (root, phase))
