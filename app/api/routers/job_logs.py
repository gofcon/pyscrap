"""Read-only Neon ``job_log`` execution history router."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Response

from app.api.deps import NeonConnDep

router = APIRouter(prefix="/job-logs", tags=["job_log"])


@router.get("")
def list_job_logs(
    conn: NeonConnDep,
    response: Response,
    job_type: str | None = Query(default=None, pattern="^(API|ETL)$"),
    job_id: str | None = None,
    api_id: str | None = None,
    target_date: str | None = Query(default=None, pattern=r"^\d{8}$"),
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> list[dict[str, Any]]:
    where, params = [], []
    if job_type:
        where.append("job_type = %s")
        params.append(job_type)
    if job_id:
        where.append("job_id = %s")
        params.append(job_id)
    if api_id:
        where.append("api_id = %s")
        params.append(api_id)
    if target_date:
        where.append("target_date = %s")
        params.append(target_date)
    if status:
        where.append("status = %s")
        params.append(status)
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS cnt FROM job_log {clause}", params)
        response.headers["X-Total-Count"] = str(cur.fetchone()["cnt"])
        cur.execute(
            f"SELECT * FROM job_log {clause} ORDER BY executed_at DESC, id DESC LIMIT %s OFFSET %s",
            [*params, limit, offset],
        )
        return list(cur.fetchall())
