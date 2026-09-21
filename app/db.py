"""SQLite persistence layer.

The database is the source of truth for session/chunk metadata; chunk payloads
live as files on disk next to it. Both reside under DATA_DIR, so a restart (or a
new container with the same volume) picks up unfinished sessions transparently.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id                   TEXT PRIMARY KEY,
    filename             TEXT NOT NULL,
    total_size           INTEGER NOT NULL,
    chunk_size           INTEGER NOT NULL,
    total_chunks         INTEGER NOT NULL,
    file_sha256          TEXT,   -- optional overall digest, filled in at merge
    status               TEXT NOT NULL DEFAULT 'uploading'
                         CHECK (status IN ('uploading', 'completed', 'expired')),
    final_path           TEXT,
    idle_timeout_seconds INTEGER NOT NULL,
    last_activity        TEXT NOT NULL,
    expired_at           TEXT,
    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL
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


def parse_ts(value: str) -> datetime:
    """Parse a timestamp emitted by :func:`utcnow` as an aware UTC datetime."""
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def lease_deadline(last_activity: str, idle_timeout_seconds: int) -> datetime:
    """The instant at which a lease on `last_activity` times out."""
    return parse_ts(last_activity) + timedelta(seconds=idle_timeout_seconds)


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
        # Databases created before the lease feature carry an older sessions
        # schema (no lease columns, status CHECK without 'expired'). Rebuild
        # that one table in place so existing rows keep their leases with the
        # backwards-compatible default timeout.
        create_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='sessions'"
        ).fetchone()[0]
        legacy = "idle_timeout_seconds" not in create_sql

    if legacy:
        _migrate_legacy_sessions()


def _migrate_legacy_sessions() -> None:
    """Upgrade a pre-lease sessions table without losing rows."""
    now = utcnow()
    timeout = config.LEGACY_IDLE_TIMEOUT_SECONDS
    # DDL inside a transaction requires foreign_keys=OFF (SQLite limitation);
    # use a dedicated short-lived autocommit connection.
    conn = sqlite3.connect(config.DB_PATH, timeout=30)
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        # Keep the existing chunks table's REFERENCES pointing at "sessions"
        # when we temporarily rename it; otherwise modern SQLite rewrites the
        # reference to sessions_legacy, leaving a dangling FK after the drop.
        conn.execute("PRAGMA legacy_alter_table=ON")
        conn.execute("BEGIN")
        conn.execute("ALTER TABLE sessions RENAME TO sessions_legacy")
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT INTO sessions (id, filename, total_size, chunk_size, "
            "total_chunks, file_sha256, status, final_path, "
            "idle_timeout_seconds, last_activity, expired_at, "
            "created_at, updated_at) "
            "SELECT id, filename, total_size, chunk_size, total_chunks, "
            "file_sha256, status, final_path, ?, ?, NULL, created_at, updated_at "
            "FROM sessions_legacy",
            (timeout, now),
        )
        conn.execute("DROP TABLE sessions_legacy")
        conn.execute("COMMIT")
        conn.execute("PRAGMA legacy_alter_table=OFF")
        conn.execute("PRAGMA foreign_keys=ON")
        # Validate the rebuilt schema: dangling FKs would show up here.
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise RuntimeError(f"schema migration left FK violations: {violations}")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass  # nothing in flight
        raise
    finally:
        conn.close()


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
    total = session["total_chunks"]
    # Expired sessions retain no chunks and cannot be resumed, so progress
    # counters are meaningless: report zero chunks and an empty missing list
    # rather than the whole index range.
    if session["status"] == "expired":
        missing: list[int] = []
        received = 0
    else:
        missing = missing_indexes(conn, session)
        received = total - len(missing)
    payload = {
        "session_id": session["id"],
        "filename": session["filename"],
        "status": session["status"],
        "total_size": session["total_size"],
        "chunk_size": session["chunk_size"],
        "total_chunks": total,
        "received_count": received,
        "missing_chunks": missing,
        "file_sha256": session["file_sha256"],
        "final_path": session["final_path"],
        "idle_timeout_seconds": session["idle_timeout_seconds"],
        "last_activity": session["last_activity"],
        "expired_at": session["expired_at"],
        "created_at": session["created_at"],
        "updated_at": session["updated_at"],
    }
    # Only a live lease has a meaningful deadline; expired sessions carry the
    # moment they were reaped in `expired_at` instead.
    if session["status"] == "uploading":
        payload["expires_at"] = lease_deadline(
            session["last_activity"], session["idle_timeout_seconds"]
        ).isoformat(timespec="milliseconds")
    else:
        payload["expires_at"] = None
    return payload
