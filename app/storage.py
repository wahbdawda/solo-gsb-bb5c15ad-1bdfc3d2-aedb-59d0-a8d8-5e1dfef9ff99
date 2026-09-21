"""On-disk layout, merge logic and lease reaping for chunk payloads."""

from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
import sqlite3
import threading
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

from . import config, db

logger = logging.getLogger(__name__)

_COPY_BUFFER = 1024 * 1024  # 1 MiB streaming buffer for merge/verify

# Per-session locks guarding the merge. The lease reaper takes the same lock
# (non-blocking) before expiring a session, so a session can never be marked
# expired while its chunks are being merged.
_merge_locks: dict[str, threading.Lock] = {}
_merge_locks_guard = threading.Lock()


def merge_lock(session_id: str) -> threading.Lock:
    with _merge_locks_guard:
        return _merge_locks.setdefault(session_id, threading.Lock())


def sanitize_filename(name: str) -> str:
    """Reduce a client-supplied filename to a safe path component."""
    base = os.path.basename(name.strip().replace(" ", "_"))
    return re.sub(r"[^A-Za-z0-9._-]", "_", base).strip("._") or "file"


def session_dir(session_id: str) -> Path:
    return config.sessions_dir() / session_id


def chunks_dir(session_id: str) -> Path:
    return session_dir(session_id) / "chunks"


def chunk_path(session_id: str, index: int) -> Path:
    return chunks_dir(session_id) / f"{index:08d}.part"


def expected_chunk_size(session: sqlite3.Row, index: int) -> int:
    """All chunks are `chunk_size` bytes except the last, which takes the remainder."""
    if index == session["total_chunks"] - 1:
        return session["total_size"] - session["chunk_size"] * (session["total_chunks"] - 1)
    return session["chunk_size"]


def final_path_for(session: sqlite3.Row) -> Path:
    name = f"{session['id']}_{sanitize_filename(session['filename'])}"
    return config.files_dir() / name


def iter_file_range(path: Path, start: int, end: int) -> Iterator[bytes]:
    """Yield the inclusive byte range [start, end] of `path` in bounded blocks.

    Reads are capped at _COPY_BUFFER, so even a huge range is streamed
    without ever loading the whole file into memory.
    """
    remaining = end - start + 1
    with open(path, "rb") as fh:
        fh.seek(start)
        while remaining > 0:
            block = fh.read(min(_COPY_BUFFER, remaining))
            if not block:
                break  # file shrank underneath us; stop cleanly
            remaining -= len(block)
            yield block


def reconcile() -> None:
    """Drop chunk rows whose payload file vanished (crash between DB and disk).

    Runs at startup so a restarted service reports exactly the chunks that are
    physically present; clients simply re-upload the rest. Only still-uploading
    sessions are reconciled — completed and expired sessions are left untouched.
    """
    with db.connect() as conn:
        sessions = conn.execute(
            "SELECT * FROM sessions WHERE status = 'uploading'"
        ).fetchall()
        for session in sessions:
            rows = conn.execute(
                "SELECT chunk_index, size FROM chunks WHERE session_id = ?",
                (session["id"],),
            ).fetchall()
            for row in rows:
                path = chunk_path(session["id"], row["chunk_index"])
                if not path.is_file() or path.stat().st_size != row["size"]:
                    conn.execute(
                        "DELETE FROM chunks WHERE session_id = ? AND chunk_index = ?",
                        (session["id"], row["chunk_index"]),
                    )


class SessionExpired(Exception):
    """Raised when an operation targets a session reaped by the lease reaper."""

    def __init__(self, expired_at: str):
        super().__init__("upload session has expired")
        self.expired_at = expired_at


def expire_due_sessions(now: datetime | None = None) -> list[str]:
    """Reap uploading sessions whose idle lease has elapsed.

    A session is expired when it is still ``uploading`` and
    ``last_activity + idle_timeout_seconds`` is in the past. For each one the
    status is flipped to ``expired`` (and ``expired_at`` stamped) inside a
    conditional UPDATE, its chunk rows and files are removed, and the session
    directory is deleted. Completed sessions are never touched, and deleting
    (DELETE) removes the row entirely so a missing session is simply skipped.

    The flip is atomic per session, so a chunk upload racing with the reaper
    cannot resurrect an expired session or write chunk rows that outlive it.
    """
    now = now or datetime.now(timezone.utc)
    expired_ids: list[str] = []
    with db.connect() as conn:
        candidates = conn.execute(
            "SELECT * FROM sessions WHERE status = 'uploading'"
        ).fetchall()
        for session in candidates:
            deadline = db.lease_deadline(
                session["last_activity"], session["idle_timeout_seconds"]
            )
            if deadline > now:
                continue
            sid = session["id"]
            # Never expire a session whose merge is in flight; it is about to
            # become 'completed' and its activity is very much alive.
            if not merge_lock(sid).acquire(blocking=False):
                continue
            try:
                expired_at = db.utcnow()
                cur = conn.execute(
                    "UPDATE sessions SET status = 'expired', expired_at = ?, "
                    "updated_at = ? WHERE id = ? AND status = 'uploading'",
                    (expired_at, expired_at, sid),
                )
                if cur.rowcount == 0:
                    # Completed, deleted or reaped by another worker meanwhile.
                    continue
                conn.execute("DELETE FROM chunks WHERE session_id = ?", (sid,))
            finally:
                merge_lock(sid).release()
            expired_ids.append(sid)
            logger.info(
                "upload session %s expired (idle timeout %ss); removing chunks",
                sid,
                session["idle_timeout_seconds"],
            )

    # Disk cleanup happens after the transaction commits; a crash here leaves
    # only harmless orphan files for a session that is already 'expired'.
    for sid in expired_ids:
        shutil.rmtree(session_dir(sid), ignore_errors=True)
    return expired_ids


class MergeError(Exception):
    """Raised when chunks are not mergeable; carries the refreshed missing list."""

    def __init__(self, message: str, missing: list[int]):
        super().__init__(message)
        self.missing = missing


def merge_session(session_id: str) -> dict:
    """Verify and atomically merge all chunks into the final file.

    Every chunk's SHA-256 is re-verified against the DB record while streaming
    to the target; the merged file appears via os.replace() (atomic on POSIX),
    so observers only ever see a complete file or nothing.
    """
    with db.connect() as conn:
        session = db.get_session(conn, session_id)
        if session is None:
            raise KeyError(session_id)
        if session["status"] == "completed":
            return db.session_payload(conn, session)
        if session["status"] == "expired":
            raise SessionExpired(session["expired_at"])
        missing = db.missing_indexes(conn, session)
        if missing:
            raise MergeError(f"{len(missing)} chunk(s) still missing", missing)
        rows = conn.execute(
            "SELECT chunk_index, size, sha256 FROM chunks "
            "WHERE session_id = ? ORDER BY chunk_index",
            (session_id,),
        ).fetchall()

    final_path = final_path_for(session)
    tmp_path = final_path.with_name(final_path.name + ".tmp")
    overall = hashlib.sha256()
    corrupt: list[int] = []
    try:
        with open(tmp_path, "wb") as out:
            for row in rows:
                path = chunk_path(session_id, row["chunk_index"])
                if not path.is_file():
                    corrupt.append(row["chunk_index"])
                    continue
                hasher = hashlib.sha256()
                with open(path, "rb") as part:
                    while True:
                        buf = part.read(_COPY_BUFFER)
                        if not buf:
                            break
                        hasher.update(buf)
                        overall.update(buf)
                        out.write(buf)
                if hasher.hexdigest() != row["sha256"]:
                    corrupt.append(row["chunk_index"])
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise

    if corrupt:
        # Payload diverged from its recorded digest (e.g. disk corruption):
        # forget those chunks so the client can re-upload them.
        tmp_path.unlink(missing_ok=True)
        with db.connect() as conn:
            for index in corrupt:
                conn.execute(
                    "DELETE FROM chunks WHERE session_id = ? AND chunk_index = ?",
                    (session_id, index),
                )
                chunk_path(session_id, index).unlink(missing_ok=True)
            session = db.get_session(conn, session_id)
            missing = db.missing_indexes(conn, session)
        raise MergeError(
            f"{len(corrupt)} chunk(s) failed re-verification and were discarded",
            missing,
        )

    digest = overall.hexdigest()
    declared = session["file_sha256"]
    if declared and declared.lower() != digest:
        tmp_path.unlink(missing_ok=True)
        raise MergeError(
            f"merged file sha256 {digest} does not match declared {declared}",
            missing=[],
        )

    os.replace(tmp_path, final_path)  # atomic publish
    shutil.rmtree(session_dir(session_id), ignore_errors=True)

    with db.connect() as conn:
        cur = conn.execute(
            "UPDATE sessions SET status = 'completed', final_path = ?, "
            "file_sha256 = ?, updated_at = ? "
            "WHERE id = ? AND status = 'uploading'",
            (str(final_path), digest, db.utcnow(), session_id),
        )
        if cur.rowcount == 0:
            # The session vanished (deleted) or was reaped while we merged.
            # The merge lock normally prevents the latter, but do not publish a
            # file that no longer has a session row to own it.
            try:
                final_path.unlink(missing_ok=True)
            except OSError:
                pass
            current = db.get_session(conn, session_id)
            if current is not None and current["status"] == "expired":
                raise SessionExpired(current["expired_at"])
            raise KeyError(session_id)
        conn.execute("DELETE FROM chunks WHERE session_id = ?", (session_id,))
        session = db.get_session(conn, session_id)
        return db.session_payload(conn, session)
