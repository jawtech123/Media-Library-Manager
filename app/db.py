from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Iterable, Optional

SCHEMA = r"""
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS roots (
    id INTEGER PRIMARY KEY,
    path TEXT UNIQUE NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    path TEXT UNIQUE NOT NULL,
    size INTEGER NOT NULL,
    mtime REAL NOT NULL,
    ctime REAL NOT NULL,
    inode_key TEXT,
    ext TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT 'unknown'
);

CREATE TABLE IF NOT EXISTS remote_roots (
    id INTEGER PRIMARY KEY,
    path TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS hashes (
    file_id INTEGER PRIMARY KEY,
    algo TEXT NOT NULL,
    sample_size INTEGER,
    sample_hash TEXT,
    full_hash TEXT,
    last_hashed_at REAL,
    FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS media_metadata (
    file_id INTEGER PRIMARY KEY,
    duration REAL,
    container TEXT,
    video_codec TEXT,
    audio_codecs TEXT,
    width INTEGER,
    height INTEGER,
    bitrate INTEGER,
    streams_json TEXT,
    FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS operations_log (
    id INTEGER PRIMARY KEY,
    ts REAL NOT NULL,
    op_type TEXT NOT NULL,
    before_path TEXT,
    after_path TEXT,
    details_json TEXT,
    success INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS junk_candidates (
    id INTEGER PRIMARY KEY,
    path TEXT UNIQUE NOT NULL,
    size INTEGER,
    mtime REAL,
    ext TEXT,
    reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_files_ext ON files(ext);
CREATE INDEX IF NOT EXISTS idx_files_size ON files(size);
CREATE INDEX IF NOT EXISTS idx_hashes_fullhash ON hashes(full_hash);
CREATE INDEX IF NOT EXISTS idx_roots_enabled ON roots(enabled);
CREATE INDEX IF NOT EXISTS idx_remote_roots_path ON remote_roots(path);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=60, isolation_level=None, check_same_thread=False)
    # Use executescript to run multiple DDL statements safely in one call (avoids 'one statement at a time' error)
    conn.executescript(SCHEMA)
    conn.execute("PRAGMA foreign_keys=ON;")
    # Backward-compatible migration: ensure 'category' column exists on files
    try:
        cur = conn.execute("PRAGMA table_info(files)")
        cols = [r[1] for r in cur.fetchall()]
        if "category" not in cols:
            conn.execute("ALTER TABLE files ADD COLUMN category TEXT NOT NULL DEFAULT 'unknown'")
        # Ensure index exists regardless of whether column pre-existed
        conn.execute("CREATE INDEX IF NOT EXISTS idx_files_category ON files(category)")
    except Exception:
        pass
    return conn


def upsert_file(conn: sqlite3.Connection, path: Path, size: int, mtime: float, ctime: float, inode_key: str, ext: str, category: str = "unknown") -> int:
    conn.execute(
        """
        INSERT INTO files(path,size,mtime,ctime,inode_key,ext,category)
        VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(path) DO UPDATE SET size=excluded.size, mtime=excluded.mtime, ctime=excluded.ctime, inode_key=excluded.inode_key, ext=excluded.ext, category=excluded.category
        """,
        (str(path), size, mtime, ctime, inode_key, ext.lower(), category),
    )
    cur = conn.execute("SELECT id FROM files WHERE path=?", (str(path),))
    return int(cur.fetchone()[0])


def get_file_row(conn: sqlite3.Connection, path: Path) -> tuple | None:
    cur = conn.execute("SELECT id, size, mtime, ctime, inode_key FROM files WHERE path=?", (str(path),))
    return cur.fetchone()


def get_hash_row(conn: sqlite3.Connection, file_id: int) -> tuple | None:
    cur = conn.execute("SELECT algo, sample_size, sample_hash, full_hash, last_hashed_at FROM hashes WHERE file_id=?", (file_id,))
    return cur.fetchone()


def upsert_hash(conn: sqlite3.Connection, file_id: int, algo: str, sample_size: int | None, sample_hash: str | None, full_hash: str | None, ts: float) -> None:
    conn.execute(
        """
        INSERT INTO hashes(file_id, algo, sample_size, sample_hash, full_hash, last_hashed_at)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(file_id) DO UPDATE SET algo=excluded.algo, sample_size=excluded.sample_size, sample_hash=excluded.sample_hash, full_hash=excluded.full_hash, last_hashed_at=excluded.last_hashed_at
        """,
        (file_id, algo, sample_size, sample_hash, full_hash, ts),
    )


def upsert_metadata(conn: sqlite3.Connection, file_id: int, meta: dict) -> None:
    conn.execute(
        """
        INSERT INTO media_metadata(file_id, duration, container, video_codec, audio_codecs, width, height, bitrate, streams_json)
        VALUES(?,?,?,?,?,?,?,?,json(?))
        ON CONFLICT(file_id) DO UPDATE SET duration=excluded.duration, container=excluded.container, video_codec=excluded.video_codec, audio_codecs=excluded.audio_codecs, width=excluded.width, height=excluded.height, bitrate=excluded.bitrate, streams_json=excluded.streams_json
        """,
        (
            file_id,
            meta.get("duration"),
            meta.get("container"),
            meta.get("video_codec"),
            ",".join(meta.get("audio_codecs", [])) if meta.get("audio_codecs") else None,
            meta.get("width"),
            meta.get("height"),
            meta.get("bitrate"),
            meta.get("streams_json", "{}"),
        ),
    )


def list_remote_roots(conn: sqlite3.Connection) -> list[str]:
    return [r[0] for r in conn.execute("SELECT path FROM remote_roots ORDER BY path").fetchall()]


def add_remote_root(conn: sqlite3.Connection, path: str) -> None:
    # Store raw POSIX path string from Linux agent; do not normalize on Windows
    conn.execute(
        """
        INSERT INTO remote_roots(path)
        VALUES(?)
        ON CONFLICT(path) DO NOTHING
        """,
        (path,),
    )


def remove_remote_root(conn: sqlite3.Connection, path: str) -> None:
    conn.execute("DELETE FROM remote_roots WHERE path=?", (path,))


def list_roots(conn: sqlite3.Connection) -> list[tuple[str, int]]:
    cur = conn.execute("SELECT path, enabled FROM roots ORDER BY path")
    return cur.fetchall()


def add_root(conn: sqlite3.Connection, path: Path, enabled: bool = True) -> None:
    conn.execute(
        """
        INSERT INTO roots(path, enabled)
        VALUES(?, ?)
        ON CONFLICT(path) DO UPDATE SET enabled=excluded.enabled
        """,
        (str(path), 1 if enabled else 0),
    )


def set_root_enabled(conn: sqlite3.Connection, path: Path, enabled: bool) -> None:
    conn.execute("UPDATE roots SET enabled=? WHERE path=?", (1 if enabled else 0, str(path)))


def remove_root(conn: sqlite3.Connection, path: Path) -> None:
    conn.execute("DELETE FROM roots WHERE path=?", (str(path),))


def clear_root(conn: sqlite3.Connection, path: Path) -> None:
    like = str(path).rstrip("/\\") + "%"
    # delete files under path; cascades remove metadata/hashes via foreign keys
    conn.execute("DELETE FROM files WHERE path LIKE ?", (like,))
    # also delete junk candidates under the same path prefix
    try:
        conn.execute("DELETE FROM junk_candidates WHERE path LIKE ?", (like,))
    except Exception:
        pass


def enabled_roots(conn: sqlite3.Connection) -> list[Path]:
    return [Path(r[0]) for r in conn.execute("SELECT path FROM roots WHERE enabled=1 ORDER BY path").fetchall()]


def count_files_under_root(conn: sqlite3.Connection, path: Path) -> int:
    like = str(path).rstrip("/\\") + "%"
    cur = conn.execute("SELECT COUNT(*) FROM files WHERE path LIKE ?", (like,))
    return int(cur.fetchone()[0])


def clear_all_library(conn: sqlite3.Connection) -> None:
    # Remove all indexed files (cascades remove metadata/hashes via foreign keys)
    conn.execute("DELETE FROM files")
    # Also clear junk candidates, since they relate to the same library lifecycle
    try:
        conn.execute("DELETE FROM junk_candidates")
    except Exception:
        pass


def clear_all_junk(conn: sqlite3.Connection) -> None:
    # Explicitly clear all junk candidates only
    conn.execute("DELETE FROM junk_candidates")


def clear_junk_under_root(conn: sqlite3.Connection, path: Path) -> None:
    like = str(path).rstrip("/\\") + "%"
    conn.execute("DELETE FROM junk_candidates WHERE path LIKE ?", (like,))


def count_all_files(conn: sqlite3.Connection) -> int:
    cur = conn.execute("SELECT COUNT(*) FROM files")
    return int(cur.fetchone()[0])


def fetch_duplicates_by_fullhash(conn: sqlite3.Connection) -> list[tuple[int, str, int]]:
    cur = conn.execute(
        """
        SELECT f.id, f.path, f.size
        FROM files f
        JOIN hashes h ON h.file_id = f.id
        WHERE h.full_hash IN (
            SELECT full_hash FROM hashes WHERE full_hash IS NOT NULL GROUP BY full_hash HAVING COUNT(*) > 1
        )
        ORDER BY h.full_hash, f.size DESC
        """
    )
    return cur.fetchall()


def fetch_duplicate_rows(conn: sqlite3.Connection, include_suspected: bool = True) -> list[tuple]:
    if include_suspected:
        sql = (
            """
            WITH dup_groups AS (
                SELECT CASE WHEN h.full_hash IS NOT NULL THEN 'F:'||h.full_hash ELSE 'S:'||h.sample_hash||':'||f.size END AS gkey
                FROM files f
                JOIN hashes h ON h.file_id = f.id
                WHERE (h.full_hash IS NOT NULL) OR (h.sample_hash IS NOT NULL)
                GROUP BY gkey
                HAVING COUNT(*) > 1
            )
            SELECT CASE WHEN h.full_hash IS NOT NULL THEN 'F:'||h.full_hash ELSE 'S:'||h.sample_hash||':'||f.size END AS group_key,
                   f.path, f.size, f.mtime, f.ext,
                   m.duration, m.container, m.video_codec, m.audio_codecs, m.width, m.height, m.bitrate,
                   h.sample_hash, h.full_hash
            FROM files f
            JOIN hashes h ON h.file_id = f.id
            LEFT JOIN media_metadata m ON m.file_id = f.id
            WHERE (h.full_hash IS NOT NULL OR h.sample_hash IS NOT NULL)
              AND (CASE WHEN h.full_hash IS NOT NULL THEN 'F:'||h.full_hash ELSE 'S:'||h.sample_hash||':'||f.size END) IN (SELECT gkey FROM dup_groups)
            ORDER BY group_key, f.path
            """
        )
    else:
        sql = (
            """
            SELECT 'F:'||h.full_hash as group_key,
                   f.path, f.size, f.mtime, f.ext,
                   m.duration, m.container, m.video_codec, m.audio_codecs, m.width, m.height, m.bitrate,
                   h.sample_hash, h.full_hash
            FROM files f
            JOIN hashes h ON h.file_id = f.id
            LEFT JOIN media_metadata m ON m.file_id = f.id
            WHERE h.full_hash IN (
                SELECT full_hash FROM hashes WHERE full_hash IS NOT NULL GROUP BY full_hash HAVING COUNT(*) > 1
            )
            ORDER BY h.full_hash, f.path
            """
        )
    cur = conn.execute(sql)
    return cur.fetchall()


def upsert_junk(conn: sqlite3.Connection, path: str, size: int | None, mtime: float | None, ext: str | None, reason: str | None) -> None:
    conn.execute(
        """
        INSERT INTO junk_candidates(path,size,mtime,ext,reason)
        VALUES(?,?,?,?,?)
        ON CONFLICT(path) DO UPDATE SET size=excluded.size, mtime=excluded.mtime, ext=excluded.ext, reason=excluded.reason
        """,
        (path, size, mtime, ext, reason),
    )


def list_junk(conn: sqlite3.Connection) -> list[tuple]:
    cur = conn.execute("SELECT path, size, mtime, ext, reason FROM junk_candidates ORDER BY path")
    return cur.fetchall()


def delete_junk_entry(conn: sqlite3.Connection, path: str) -> None:
    conn.execute("DELETE FROM junk_candidates WHERE path=?", (path,))


def list_unknown_extensions(conn: sqlite3.Connection) -> list[tuple[str, int, str]]:
    # Return ext, count, sample_path for unknown-category files
    sql = (
        """
        SELECT f.ext, COUNT(*) as cnt, MIN(f.path) as sample_path
        FROM files f
        WHERE COALESCE(f.category,'unknown') = 'unknown'
        GROUP BY f.ext
        ORDER BY cnt DESC, f.ext
        """
    )
    cur = conn.execute(sql)
    return [(r[0], int(r[1]), r[2]) for r in cur.fetchall()]


def set_category_for_extension(conn: sqlite3.Connection, ext: str, category: str) -> None:
    conn.execute("UPDATE files SET category=? WHERE LOWER(ext)=LOWER(?)", (category, ext))


def fetch_library_rows(conn: sqlite3.Connection, limit: int | None = None, offset: int | None = None) -> list[tuple]:
    sql = (
        """
        SELECT f.path, f.size, f.mtime, f.ext, f.category,
               m.duration, m.container, m.video_codec, m.audio_codecs, m.width, m.height, m.bitrate,
               h.sample_hash, h.full_hash
        FROM files f
        LEFT JOIN media_metadata m ON m.file_id = f.id
        LEFT JOIN hashes h ON h.file_id = f.id
        ORDER BY f.path
        """
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
        if offset is not None:
            sql += f" OFFSET {int(offset)}"
    cur = conn.execute(sql)
    return cur.fetchall()


def log_operation(conn: sqlite3.Connection, op_type: str, before_path: str | None, after_path: str | None, details_json: str | None, success: bool) -> None:
    import time
    conn.execute(
        """
        INSERT INTO operations_log(ts, op_type, before_path, after_path, details_json, success)
        VALUES(?,?,?,?,?,?)
        """,
        (time.time(), op_type, before_path, after_path, details_json, 1 if success else 0),
    )


def delete_file_entry(conn: sqlite3.Connection, path: str) -> None:
    conn.execute("DELETE FROM files WHERE path=?", (path,))
