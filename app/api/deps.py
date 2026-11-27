"""Shared FastAPI dependencies for pyscrap v3 (Neon PostgreSQL + DuckDB)."""

from __future__ import annotations

import hmac
import os
import threading
from typing import Annotated, Any, Iterator

from fastapi import Depends, Header, HTTPException

from app.core.neon_sync import load_dotenv_into_env

load_dotenv_into_env()

# Cap concurrent DuckDB S3 reads on OCI ARM A1 (2GB RAM budget).
DUCKDB_SEMAPHORE = threading.BoundedSemaphore(int(os.environ.get("API_DUCKDB_CONCURRENCY", "2")))


def get_neon_conn() -> Iterator[Any]:
    """Yield one pooled Neon PostgreSQL connection per request."""
    import psycopg
    from psycopg.rows import dict_row

    dsn = os.environ.get("NEON_DATABASE_URL")
    if not dsn:
        raise HTTPException(500, "NEON_DATABASE_URL is not configured")
    with psycopg.connect(dsn, connect_timeout=10, row_factory=dict_row) as conn:
        yield conn


NeonConnDep = Annotated[Any, Depends(get_neon_conn)]


def verify_admin_api_key(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    authorization: str | None = Header(default=None),
) -> None:
    """Enforce API-key auth whenever ``ADMIN_API_KEY`` is configured."""
    expected = os.environ.get("ADMIN_API_KEY")
    if not expected:
        return
    provided = x_api_key
    if not provided and authorization and authorization.lower().startswith("bearer "):
        provided = authorization[7:].strip()
    if not provided or not hmac.compare_digest(provided, expected):
        raise HTTPException(401, "Invalid or missing API key")
