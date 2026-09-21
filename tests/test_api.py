"""End-to-end tests for the chunked upload service.

Run with:  DATA_DIR=$(mktemp -d) pytest tests/
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import tempfile
import time
from datetime import datetime, timedelta, timezone

# Point storage at a throwaway directory BEFORE importing the app.
_TMP = tempfile.mkdtemp(prefix="chunked-upload-test-")
os.environ["DATA_DIR"] = _TMP

from fastapi.testclient import TestClient  # noqa: E402

from app import config, db, storage  # noqa: E402
from app.main import app  # noqa: E402


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _make_file(chunk_size: int, total_chunks: int, last_size: int) -> tuple[bytes, list[bytes]]:
    chunks = []
    for i in range(total_chunks):
        size = last_size if i == total_chunks - 1 else chunk_size
        chunks.append(bytes(f"chunk-{i:05d}-".encode()) + os.urandom(size - 12))
    return b"".join(chunks), chunks


def _create(client: TestClient, data: bytes, chunk_size: int, **extra) -> dict:
    resp = client.post(
        "/uploads",
        json={
            "filename": "sample.bin",
            "total_size": len(data),
            "chunk_size": chunk_size,
            **extra,
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def _put(client: TestClient, sid: str, index: int, payload: bytes, digest: str | None = None):
    return client.put(
        f"/uploads/{sid}/chunks/{index}",
        content=payload,
        headers={"X-Chunk-SHA256": digest or _sha256(payload)},
    )


def test_full_flow_out_of_order_with_retry():
    with TestClient(app) as client:
        chunk_size = 1024
        data, chunks = _make_file(chunk_size, total_chunks=5, last_size=500)
        session = _create(client, data, chunk_size, file_sha256=_sha256(data))
        sid = session["session_id"]
        assert session["total_chunks"] == 5
        assert session["missing_chunks"] == [0, 1, 2, 3, 4]

        # Corrupted payload must be rejected and not persisted.
        bad = _put(client, sid, 0, chunks[0], digest="0" * 64)
        assert bad.status_code == 400
        assert client.get(f"/uploads/{sid}").json()["missing_chunks"] == [0, 1, 2, 3, 4]

        # Out-of-order upload: 3, 1, 4 ...
        for i in (3, 1, 4):
            resp = _put(client, sid, i, chunks[i])
            assert resp.status_code == 200, resp.text
        state = client.get(f"/uploads/{sid}").json()
        assert state["missing_chunks"] == [0, 2]
        assert state["received_count"] == 3

        # Retry of an already-received chunk is idempotent.
        assert _put(client, sid, 3, chunks[3]).status_code == 200
        assert client.get(f"/uploads/{sid}").json()["received_count"] == 3

        # Cannot merge while chunks are missing.
        early = client.post(f"/uploads/{sid}/complete")
        assert early.status_code == 409
        assert early.json()["detail"]["missing_chunks"] == [0, 2]

        # Finish the rest and merge.
        assert _put(client, sid, 0, chunks[0]).status_code == 200
        assert _put(client, sid, 2, chunks[2]).status_code == 200
        done = client.post(f"/uploads/{sid}/complete")
        assert done.status_code == 200, done.text
        body = done.json()
        assert body["status"] == "completed"
        assert body["file_sha256"] == _sha256(data)

        final_path = body["final_path"]
        assert os.path.isfile(final_path)
        with open(final_path, "rb") as fh:
            assert fh.read() == data

        # Complete is idempotent; further chunk uploads are refused.
        assert client.post(f"/uploads/{sid}/complete").status_code == 200
        assert _put(client, sid, 0, chunks[0]).status_code == 409


def test_restart_resumes_unfinished_session():
    chunk_size = 512
    data, chunks = _make_file(chunk_size, total_chunks=3, last_size=512)

    with TestClient(app) as client:
        sid = _create(client, data, chunk_size)["session_id"]
        assert _put(client, sid, 0, chunks[0]).status_code == 200
        assert _put(client, sid, 2, chunks[2]).status_code == 200

    # "Restart": a brand-new app instance over the same DATA_DIR.
    with TestClient(app) as client:
        state = client.get(f"/uploads/{sid}").json()
        assert state["status"] == "uploading"
        assert state["missing_chunks"] == [1]

        assert _put(client, sid, 1, chunks[1]).status_code == 200
        done = client.post(f"/uploads/{sid}/complete")
        assert done.status_code == 200, done.text
        with open(done.json()["final_path"], "rb") as fh:
            assert fh.read() == data


def test_validation_errors():
    with TestClient(app) as client:
        data, chunks = _make_file(256, total_chunks=2, last_size=100)
        sid = _create(client, data, 256)["session_id"]

        # Index out of range.
        assert _put(client, sid, 2, chunks[0]).status_code == 400
        # Missing digest.
        assert client.put(f"/uploads/{sid}/chunks/0", content=chunks[0]).status_code == 400
        # Wrong size.
        assert _put(client, sid, 0, chunks[0][:-1]).status_code == 400
        # Unknown session.
        assert client.get("/uploads/nope").status_code == 404
        # Bad overall digest must fail the merge with 422.
        wrong = _create(client, data, 256, file_sha256="f" * 64)
        wsid = wrong["session_id"]
        assert _put(client, wsid, 0, chunks[0]).status_code == 200
        assert _put(client, wsid, 1, chunks[1]).status_code == 200
        assert client.post(f"/uploads/{wsid}/complete").status_code == 422


def test_abort_session():
    with TestClient(app) as client:
        data, chunks = _make_file(256, total_chunks=2, last_size=256)
        sid = _create(client, data, 256)["session_id"]
        assert _put(client, sid, 0, chunks[0]).status_code == 200
        assert client.delete(f"/uploads/{sid}").status_code == 204
        assert client.get(f"/uploads/{sid}").status_code == 404


# ---------------------------------------------------------------- downloads


def _completed(client: TestClient, data: bytes, chunk_size: int) -> dict:
    """Upload every chunk of `data`, merge, and return the completed payload."""
    session = _create(client, data, chunk_size, file_sha256=_sha256(data))
    sid = session["session_id"]
    for index, offset in enumerate(range(0, len(data), chunk_size)):
        assert _put(client, sid, index, data[offset : offset + chunk_size]).status_code == 200
    done = client.post(f"/uploads/{sid}/complete")
    assert done.status_code == 200, done.text
    return done.json()


def test_download_full_file_with_etag():
    with TestClient(app) as client:
        data = os.urandom(5000)
        done = _completed(client, data, 1024)
        sid = done["session_id"]
        etag = f'"{_sha256(data)}"'

        resp = client.get(f"/uploads/{sid}/content")
        assert resp.status_code == 200
        assert resp.content == data
        assert resp.headers["Accept-Ranges"] == "bytes"
        assert resp.headers["Content-Length"] == str(len(data))
        assert resp.headers["ETag"] == etag  # strong validator: "<sha256>"
        assert "Content-Range" not in resp.headers

        # If-None-Match hit -> 304 with no body; weak form matches too.
        for header in (etag, f"W/{etag}", f'"{"0" * 64}", {etag}', "*"):
            cached = client.get(
                f"/uploads/{sid}/content", headers={"If-None-Match": header}
            )
            assert cached.status_code == 304, header
            assert cached.headers["ETag"] == etag
            assert cached.content == b""

        # A stale validator gets the full file again.
        stale = client.get(
            f"/uploads/{sid}/content", headers={"If-None-Match": f'"{"0" * 64}"'}
        )
        assert stale.status_code == 200
        assert stale.content == data


def test_download_single_ranges():
    with TestClient(app) as client:
        data = bytes(range(256)) * 20  # 5120 deterministic bytes
        done = _completed(client, data, 1024)
        sid = done["session_id"]
        size = len(data)
        url = f"/uploads/{sid}/content"

        # bytes=start-end
        resp = client.get(url, headers={"Range": "bytes=100-199"})
        assert resp.status_code == 206
        assert resp.content == data[100:200]
        assert resp.headers["Content-Range"] == f"bytes 100-199/{size}"
        assert resp.headers["Content-Length"] == "100"
        assert resp.headers["Accept-Ranges"] == "bytes"
        assert resp.headers["ETag"] == f'"{_sha256(data)}"'

        # bytes=start-  (open-ended)
        resp = client.get(url, headers={"Range": f"bytes={size - 10}-"})
        assert resp.status_code == 206
        assert resp.content == data[-10:]
        assert resp.headers["Content-Range"] == f"bytes {size - 10}-{size - 1}/{size}"

        # bytes=-suffix
        resp = client.get(url, headers={"Range": "bytes=-25"})
        assert resp.status_code == 206
        assert resp.content == data[-25:]
        assert resp.headers["Content-Range"] == f"bytes {size - 25}-{size - 1}/{size}"

        # Suffix longer than the file selects the whole file (still 206).
        resp = client.get(url, headers={"Range": "bytes=-999999"})
        assert resp.status_code == 206
        assert resp.content == data
        assert resp.headers["Content-Range"] == f"bytes 0-{size - 1}/{size}"

        # End beyond EOF is clamped.
        resp = client.get(url, headers={"Range": f"bytes=0-{size + 100}"})
        assert resp.status_code == 206
        assert resp.content == data
        assert resp.headers["Content-Range"] == f"bytes 0-{size - 1}/{size}"


def test_download_range_errors_are_416():
    with TestClient(app) as client:
        data = os.urandom(1000)
        done = _completed(client, data, 256)
        sid = done["session_id"]
        url = f"/uploads/{sid}/content"

        bad_ranges = [
            "bytes=0-1,3-4",      # multiple ranges unsupported
            "bytes=abc",          # garbage
            "bytes=",             # empty spec
            "bytes=-",            # neither bound
            "items=0-1",          # unknown unit
            "bytes=5-2",          # start > end
            f"bytes={len(data)}-",   # starts at EOF
            f"bytes={len(data)}-{len(data) + 9}",  # fully out of bounds
            "bytes=-0",           # zero-length suffix
        ]
        for bad in bad_ranges:
            resp = client.get(url, headers={"Range": bad})
            assert resp.status_code == 416, bad
            assert resp.headers["Content-Range"] == f"bytes */{len(data)}"
            assert resp.headers["Accept-Ranges"] == "bytes"


def test_download_if_range():
    with TestClient(app) as client:
        data = os.urandom(2048)
        done = _completed(client, data, 1024)
        sid = done["session_id"]
        url = f"/uploads/{sid}/content"
        etag = f'"{_sha256(data)}"'

        # Matching If-Range -> the range is honoured.
        resp = client.get(url, headers={"Range": "bytes=0-9", "If-Range": etag})
        assert resp.status_code == 206
        assert resp.content == data[:10]

        # Mismatched / weak / date-valued If-Range -> Range ignored, full 200.
        for if_range in (f'"{"0" * 64}"', f"W/{etag}", "Wed, 21 Oct 2015 07:28:00 GMT"):
            resp = client.get(url, headers={"Range": "bytes=0-9", "If-Range": if_range})
            assert resp.status_code == 200, if_range
            assert resp.content == data
            assert "Content-Range" not in resp.headers


def test_download_session_state_errors():
    with TestClient(app) as client:
        # Unknown session -> 404.
        assert client.get("/uploads/nope/content").status_code == 404

        # Unfinished session -> 409 (both before and during upload).
        data, chunks = _make_file(256, total_chunks=2, last_size=256)
        sid = _create(client, data, 256)["session_id"]
        assert client.get(f"/uploads/{sid}/content").status_code == 409
        assert _put(client, sid, 0, chunks[0]).status_code == 200
        assert client.get(f"/uploads/{sid}/content").status_code == 409


# ------------------------------------------------------------- lease / expiry


def _backdate(sid: str, seconds: float) -> str:
    """Force a session's last_activity `seconds` into the past and return it."""
    stale = (
        datetime.now(timezone.utc) - timedelta(seconds=seconds)
    ).isoformat(timespec="milliseconds")
    with db.connect() as conn:
        conn.execute(
            "UPDATE sessions SET last_activity = ? WHERE id = ?", (stale, sid)
        )
    return stale


def test_create_session_lease_defaults_and_bounds():
    with TestClient(app) as client:
        data, _ = _make_file(256, total_chunks=1, last_size=256)

        # The configured default timeout is applied when omitted.
        resp = _create(client, data, 256)
        assert resp["idle_timeout_seconds"] == config.DEFAULT_IDLE_TIMEOUT_SECONDS
        assert resp["last_activity"] == resp["updated_at"]
        assert resp["expired_at"] is None
        expected = (
            db.parse_ts(resp["last_activity"])
            + timedelta(seconds=config.DEFAULT_IDLE_TIMEOUT_SECONDS)
        ).isoformat(timespec="milliseconds")
        assert resp["expires_at"] == expected

        # Explicit timeout is accepted and reflected in the deadline.
        resp = _create(client, data, 256, idle_timeout_seconds=60)
        assert resp["idle_timeout_seconds"] == 60

        # Out-of-range timeouts are rejected (422).
        for bad in (0, -5, config.MAX_IDLE_TIMEOUT_SECONDS + 1):
            r = client.post(
                "/uploads",
                json={
                    "filename": "sample.bin",
                    "total_size": len(data),
                    "chunk_size": 256,
                    "idle_timeout_seconds": bad,
                },
            )
            assert r.status_code == 422, bad


def test_status_query_refreshes_lease():
    with TestClient(app) as client:
        data, _ = _make_file(256, total_chunks=1, last_size=256)
        sid = _create(client, data, 256, idle_timeout_seconds=60)["session_id"]

        original = client.get(f"/uploads/{sid}").json()["last_activity"]
        _backdate(sid, 59)  # one second before the deadline
        refreshed = client.get(f"/uploads/{sid}").json()
        assert refreshed["status"] == "uploading"
        assert refreshed["last_activity"] > original
        assert db.parse_ts(refreshed["expires_at"]) > datetime.now(timezone.utc)

        # A query in the nick of time keeps the session alive through reaping.
        _backdate(sid, 59)
        storage.expire_due_sessions()
        assert client.get(f"/uploads/{sid}").json()["status"] == "uploading"


def test_chunk_upload_refreshes_lease():
    with TestClient(app) as client:
        data, chunks = _make_file(256, total_chunks=2, last_size=256)
        sid = _create(client, data, 256, idle_timeout_seconds=60)["session_id"]

        _put(client, sid, 0, chunks[0])
        first = client.get(f"/uploads/{sid}").json()["last_activity"]
        _backdate(sid, 59)
        stale_before_put = client.get(f"/uploads/{sid}")  # query itself refreshes
        assert stale_before_put.json()["status"] == "uploading"

        # Make it stale again, then revive it with a successful chunk upload.
        _backdate(sid, 59)
        resp = _put(client, sid, 1, chunks[1])
        assert resp.status_code == 200, resp.text
        after = client.get(f"/uploads/{sid}").json()
        assert after["status"] == "uploading"
        assert after["last_activity"] > first
        storage.expire_due_sessions()
        assert client.get(f"/uploads/{sid}").json()["status"] == "uploading"


def test_reaper_expires_idle_session_and_removes_chunks():
    with TestClient(app) as client:
        data, chunks = _make_file(256, total_chunks=2, last_size=256)
        sid = _create(client, data, 256, idle_timeout_seconds=60)["session_id"]
        assert _put(client, sid, 0, chunks[0]).status_code == 200
        chunk_file = storage.chunk_path(sid, 0)
        assert chunk_file.is_file()

        # Still within the lease: nothing happens.
        _backdate(sid, 30)
        assert storage.expire_due_sessions() == []
        assert client.get(f"/uploads/{sid}").json()["status"] == "uploading"

        # Past the deadline: reaped.
        _backdate(sid, 61)
        assert storage.expire_due_sessions() == [sid]
        # Idempotent: a second sweep has nothing to do.
        assert storage.expire_due_sessions() == []

        state = client.get(f"/uploads/{sid}").json()
        assert state["status"] == "expired"
        assert state["expired_at"] is not None
        assert state["expires_at"] is None
        assert state["missing_chunks"] == []  # all chunk rows deleted
        assert state["received_count"] == 0

        # Chunk files and session directory are gone.
        assert not chunk_file.exists()
        assert not storage.session_dir(sid).exists()

        # Expired sessions are excluded from the resumable listing.
        listed = {s["session_id"] for s in client.get("/uploads").json()["sessions"]}
        assert sid not in listed

        # Uploads and complete return 410 Gone with expired_at.
        bad_put = _put(client, sid, 1, chunks[1])
        assert bad_put.status_code == 410
        assert bad_put.json()["detail"]["expired_at"] == state["expired_at"]
        bad_complete = client.post(f"/uploads/{sid}/complete")
        assert bad_complete.status_code == 410
        assert bad_complete.json()["detail"]["expired_at"] == state["expired_at"]
        assert client.get(f"/uploads/{sid}/content").status_code == 410


def test_reaper_leaves_completed_sessions_untouched():
    with TestClient(app) as client:
        data, chunks = _make_file(256, total_chunks=2, last_size=256)
        sid = _create(client, data, 256, idle_timeout_seconds=60)["session_id"]
        _put(client, sid, 0, chunks[0])
        _put(client, sid, 1, chunks[1])
        done = client.post(f"/uploads/{sid}/complete")
        assert done.status_code == 200
        final_path = done.json()["final_path"]

        _backdate(sid, 10**6)  # absurdly old activity must not matter
        assert storage.expire_due_sessions() == []
        body = client.get(f"/uploads/{sid}").json()
        assert body["status"] == "completed"
        assert body["expired_at"] is None
        assert os.path.isfile(final_path)
        # Completed file still downloadable.
        assert client.get(f"/uploads/{sid}/content").status_code == 200


def test_reaper_leaves_deleted_sessions_alone():
    with TestClient(app) as client:
        data, chunks = _make_file(256, total_chunks=2, last_size=256)
        sid = _create(client, data, 256, idle_timeout_seconds=60)["session_id"]
        _put(client, sid, 0, chunks[0])
        assert client.delete(f"/uploads/{sid}").status_code == 204

        _backdate(sid, 10**6)
        assert storage.expire_due_sessions() == []
        assert client.get(f"/uploads/{sid}").status_code == 404
        assert not storage.session_dir(sid).exists()


def test_startup_recovery_expires_stale_sessions(monkeypatch):
    data, chunks = _make_file(256, total_chunks=2, last_size=256)

    with TestClient(app) as client:
        keep = _create(client, data, 256, idle_timeout_seconds=60)["session_id"]
        stale = _create(client, data, 256, idle_timeout_seconds=60)["session_id"]
        assert _put(client, stale, 0, chunks[0]).status_code == 200
        chunk_file = storage.chunk_path(stale, 0)

        _backdate(stale, 61)
        with db.connect() as conn:
            assert db.get_session(conn, stale)["status"] == "uploading"

    # Simulate downtime beyond the lease, then boot again: the lifespan
    # recovery sweep must expire the stale session before requests are served.
    with TestClient(app) as client:
        body = client.get(f"/uploads/{stale}").json()
        assert body["status"] == "expired"
        assert body["expired_at"] is not None
        assert not chunk_file.exists()

        # The fresh session survives the same recovery sweep.
        assert client.get(f"/uploads/{keep}").json()["status"] == "uploading"


def test_background_reaper_sweep(monkeypatch):
    monkeypatch.setattr(config, "REAPER_INTERVAL_SECONDS", 0.05)
    data, chunks = _make_file(256, total_chunks=1, last_size=256)
    with TestClient(app) as client:
        sid = _create(client, data, 256, idle_timeout_seconds=60)["session_id"]
        assert _put(client, sid, 0, chunks[0]).status_code == 200
        assert storage.chunk_path(sid, 0).is_file()

        _backdate(sid, 61)
        deadline = time.time() + 3
        while time.time() < deadline:
            with db.connect() as conn:
                row = db.get_session(conn, sid)
            if row["status"] == "expired":
                break
            time.sleep(0.02)
        assert row["status"] == "expired"
        assert row["expired_at"] is not None
        assert not storage.chunk_path(sid, 0).exists()


def test_legacy_schema_migration(monkeypatch):
    """A database written by the pre-lease version migrates without data loss."""
    legacy_dir = tempfile.mkdtemp(prefix="chunked-upload-legacy-")
    db_path = os.path.join(legacy_dir, "uploads.db")
    now = "2026-01-01T00:00:00.000+00:00"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY, filename TEXT NOT NULL,
            total_size INTEGER NOT NULL, chunk_size INTEGER NOT NULL,
            total_chunks INTEGER NOT NULL, file_sha256 TEXT,
            status TEXT NOT NULL DEFAULT 'uploading'
                CHECK (status IN ('uploading', 'completed')),
            final_path TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE chunks (
            session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
            chunk_index INTEGER NOT NULL, size INTEGER NOT NULL,
            sha256 TEXT NOT NULL, received_at TEXT NOT NULL,
            PRIMARY KEY (session_id, chunk_index)
        );
        """
    )
    conn.execute(
        "INSERT INTO sessions (id, filename, total_size, chunk_size, "
        "total_chunks, file_sha256, status, final_path, created_at, updated_at) "
        "VALUES ('legacy1', 'old.bin', 512, 256, 2, NULL, 'uploading', NULL, ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO chunks (session_id, chunk_index, size, sha256, received_at) "
        "VALUES ('legacy1', 0, 256, ?, ?)",
        ("a" * 64, now),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(config, "DATA_DIR", legacy_dir)
    monkeypatch.setattr(config, "DB_PATH", os.path.join(legacy_dir, "uploads.db"))
    os.makedirs(os.path.join(legacy_dir, "sessions"), exist_ok=True)
    os.makedirs(os.path.join(legacy_dir, "files"), exist_ok=True)

    db.init_db()
    with db.connect() as conn:
        row = db.get_session(conn, "legacy1")
        assert row["idle_timeout_seconds"] == config.LEGACY_IDLE_TIMEOUT_SECONDS
        assert row["last_activity"] is not None
        assert row["status"] == "uploading"
        # Chunk rows survive the table rebuild.
        assert db.received_indexes(conn, "legacy1") == {0}
        # The rebuild must not leave the chunks FK dangling: new rows work,
        # inserts for unknown sessions are rejected, and deletes cascade.
        conn.execute(
            "INSERT INTO chunks (session_id, chunk_index, size, sha256, received_at) "
            "VALUES ('legacy1', 1, 256, ?, ?)",
            ("b" * 64, now),
        )
        assert db.received_indexes(conn, "legacy1") == {0, 1}
        try:
            conn.execute(
                "INSERT INTO chunks (session_id, chunk_index, size, sha256, received_at) "
                "VALUES ('ghost', 0, 1, ?, ?)",
                ("c" * 64, now),
            )
            raise AssertionError("foreign key not enforced after migration")
        except sqlite3.IntegrityError:
            pass
        conn.execute("DELETE FROM sessions WHERE id = 'legacy1'")
        assert conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] == 0
