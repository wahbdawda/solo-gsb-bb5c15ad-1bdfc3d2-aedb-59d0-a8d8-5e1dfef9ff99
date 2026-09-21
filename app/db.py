"""SQLite persistence layer.

The database is the source of truth for session/chunk metadata; chunk payloads
live as files on disk next to it. Both reside under DATA_DIR, so a restart (or a
new container with the same volume) picks up unfinished sessions transparently.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id           TEXT PRIMARY KEY,
    filename     TEXT NOT NULL,
    total_size   INTEGER NOT NULL,
    chunk_size   INTEGER NOT NULL,
    total_chunks INTEGER NOT NULL,
    file_sha256  TEXT,               -- optional overall digest, filled in at merge
    status       TEXT NOT NULL DEFAULT 'uploading'
                 CHECK (status IN ('uploading', 'completed')),
    final_path   TEXT,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chunks (
    session_id  TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    size        INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (session_id, chunk_index)
);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def connect() -> sqlite3.Connection:
    """Open a short-lived connection. SQLite handles concurrency via WAL."""
    conn = sqlite3.connect(config.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    with connect() as conn:
        conn.executescript(SCHEMA)


def get_session(conn: sqlite3.Connection, session_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()


def received_indexes(conn: sqlite3.Connection, session_id: str) -> set[int]:
    rows = conn.execute(
        "SELECT chunk_index FROM chunks WHERE session_id = ?", (session_id,)
    ).fetchall()
    return {row["chunk_index"] for row in rows}


def missing_indexes(conn: sqlite3.Connection, session: sqlite3.Row) -> list[int]:
    have = received_indexes(conn, session["id"])
    return [i for i in range(session["total_chunks"]) if i not in have]


def session_payload(conn: sqlite3.Connection, session: sqlite3.Row) -> dict:
    """Public representation of a session, including missing chunk indexes."""
    missing = missing_indexes(conn, session)
    total = session["total_chunks"]
    return {
        "session_id": session["id"],
        "filename": session["filename"],
        "status": session["status"],
        "total_size": session["total_size"],
        "chunk_size": session["chunk_size"],
        "total_chunks": total,
        "received_count": total - len(missing),
        "missing_chunks": missing,
        "file_sha256": session["file_sha256"],
        "final_path": session["final_path"],
        "created_at": session["created_at"],
        "updated_at": session["updated_at"],
    }
