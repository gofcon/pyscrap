"""Health check endpoint (Neon DB connectivity + configured S3 targets)."""

from __future__ import annotations

from fastapi import APIRouter

from app.api.deps import NeonConnDep
from app.object_storage import configured_targets

router = APIRouter(tags=["health"])


@router.get("/health")
def health_check(conn: NeonConnDep) -> dict[str, object]:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 AS ok")
        cur.fetchone()
    return {
        "status": "ok",
        "version": "v3",
        "neon": "ok",
        "s3_targets": configured_targets(),
    }
