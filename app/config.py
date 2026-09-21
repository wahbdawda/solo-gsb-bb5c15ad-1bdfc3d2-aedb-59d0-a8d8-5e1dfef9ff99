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

DB_PATH = DATA_DIR / DB_FILENAME


def sessions_dir() -> Path:
    return DATA_DIR / SESSIONS_DIRNAME


def files_dir() -> Path:
    return DATA_DIR / FILES_DIRNAME


def ensure_dirs() -> None:
    """Create the storage layout. Called at startup; safe to call repeatedly."""
    sessions_dir().mkdir(parents=True, exist_ok=True)
    files_dir().mkdir(parents=True, exist_ok=True)
