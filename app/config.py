"""Runtime configuration, read from environment variables."""

from __future__ import annotations

import os
from pathlib import Path

# Root directory for all persistent state (SQLite DB, chunk files, merged files).
DATA_DIR = Path(os.environ.get("DATA_DIR", "./data")).resolve()

# Subdirectory layout inside DATA_DIR.
SESSIONS_DIRNAME = "sessions"  # one subdir per upload session, holds .part chunks
FILES_DIRNAME = "files"        # merged, completed files
DB_FILENAME = "uploads.db"

# Port the container/uvicorn listens on (documentational; uvicorn flag wins).
PORT = int(os.environ.get("PORT", "8000"))

# Upper bound for a single chunk (64 MiB) to avoid pathological allocations.
MAX_CHUNK_SIZE = 64 * 1024 * 1024

# Upload-session lease: an "uploading" session expires after this many seconds
# without a successful chunk upload or status query. The client may request a
# different value at session creation, bounded by [1, MAX_IDLE_TIMEOUT_SECONDS].
MAX_IDLE_TIMEOUT_SECONDS = 7 * 24 * 3600  # hard ceiling: 604800 (one week)
DEFAULT_IDLE_TIMEOUT_SECONDS = max(
    1,
    min(int(os.environ.get("DEFAULT_IDLE_TIMEOUT_SECONDS", "86400")), MAX_IDLE_TIMEOUT_SECONDS),
)

# How often the background reaper scans for expired sessions (seconds). Set to
# 0 to disable the background scan (the startup recovery scan still runs).
REAPER_INTERVAL_SECONDS = float(os.environ.get("REAPER_INTERVAL_SECONDS", "300"))

# Backwards-compatible default lease used when migrating pre-existing databases
# that predate the lease columns.
LEGACY_IDLE_TIMEOUT_SECONDS = 86400

DB_PATH = DATA_DIR / DB_FILENAME


def sessions_dir() -> Path:
    return DATA_DIR / SESSIONS_DIRNAME


def files_dir() -> Path:
    return DATA_DIR / FILES_DIRNAME


def ensure_dirs() -> None:
    """Create the storage layout. Called at startup; safe to call repeatedly."""
    sessions_dir().mkdir(parents=True, exist_ok=True)
    files_dir().mkdir(parents=True, exist_ok=True)
