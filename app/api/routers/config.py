"""Neon DB control-plane CRUD routers for pyscrap v3.

Covers:
- ``/api-mst``       (validates ``spec_json`` via Pydantic ``ApiSpec`` + exposes JSON Schema & ``api_mst_hist``)
- ``/api-jobs``      (validates ``params_json`` / ``save_mode`` via ``ScrapeJobSpec``)
- ``/etl-jobs``      (validates ``output_layer`` / ``target_name`` / ``sql_text`` via ``EtlJobSpec``)
- ``/host-policies`` (per-host pacing & cooldown policies)
- ``/schedule``      (read-only ``v_schedule_job`` merged timeline)
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import ValidationError

from app.api.deps import NeonConnDep
from app.v3.spec import ApiSpec, EtlJobSpec, ScrapeJobSpec, export_spec_json_schema

router = APIRouter()


def _validate_or_422(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except ValidationError as exc:
        raise HTTPException(422, exc.errors()) from exc


# ---------------------------------------------------------------------------
# /api-mst
# ---------------------------------------------------------------------------

@router.get("/api-mst/schema", tags=["api_mst"])
def get_api_spec_schema() -> dict[str, Any]:
    """Return JSON Schema for ``api_mst.spec_json`` (for admin UI auto-forms)."""
    return export_spec_json_schema()


@router.get("/api-mst", tags=["api_mst"])
def list_api_mst(
    conn: NeonConnDep,
    response: Response,
    source_name: str | None = None,
    domain: str | None = None,
    status: str | None = None,
    q: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[dict[str, Any]]:
    where, params = [], []
    if source_name:
        where.append("source_name = %s")
        params.append(source_name)
    if domain:
        where.append("domain = %s")
        params.append(domain)
    if status:
        where.append("status = %s")
        params.append(status)
    if q:
        where.append("(api_id ILIKE %s OR api_name ILIKE %s)")
        like = f"%{q}%"
        params.extend([like, like])
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS cnt FROM api_mst {clause}", params)
        response.headers["X-Total-Count"] = str(cur.fetchone()["cnt"])
        cur.execute(
            f"SELECT * FROM api_mst {clause} ORDER BY updated_at DESC, api_id ASC LIMIT %s OFFSET %s",
            [*params, limit, offset],
        )
        return list(cur.fetchall())


@router.get("/api-mst/{api_id}", tags=["api_mst"])
def get_api_mst(api_id: str, conn: NeonConnDep) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM api_mst WHERE api_id = %s", (api_id,))
        row = cur.fetchone()
    if row is None:
        raise HTTPException(404, f"api_mst '{api_id}' not found")
    return dict(row)


@router.get("/api-mst/{api_id}/history", tags=["api_mst"])
def get_api_mst_history(
    api_id: str,
    conn: NeonConnDep,
    limit: int = Query(default=20, ge=1, le=200),
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT hist_id, api_id, action, changed_by, changed_at, old_row "
            "FROM api_mst_hist WHERE api_id = %s ORDER BY changed_at DESC LIMIT %s",
            (api_id, limit),
        )
        return list(cur.fetchall())


@router.post("/api-mst", status_code=201, tags=["api_mst"])
def create_api_mst(body: dict[str, Any], conn: NeonConnDep) -> dict[str, Any]:
    required = ("api_id", "api_name", "source_name", "protocol", "sink_type", "spec_json")
    missing = [k for k in required if not body.get(k)]
    if missing:
        raise HTTPException(422, f"missing required field(s): {missing}")
    validated = _validate_or_422(ApiSpec.model_validate, body["spec_json"])
    spec_str = json.dumps(validated.model_dump(mode="json", by_alias=True, exclude_none=True), ensure_ascii=False)
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM api_mst WHERE api_id = %s", (body["api_id"],))
        if cur.fetchone():
            raise HTTPException(409, f"api_mst '{body['api_id']}' already exists")
        cur.execute(
            """
            INSERT INTO api_mst (
                api_id, api_name, source_name, domain, protocol,
                sink_type, status, schema_version, spec_json, note
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            RETURNING *
            """,
            (
                body["api_id"],
                body["api_name"],
                body["source_name"],
                body.get("domain"),
                body["protocol"],
                body["sink_type"],
                body.get("status", "DRAFT"),
                int(body.get("schema_version", 1)),
                spec_str,
                body.get("note"),
            ),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row)


@router.patch("/api-mst/{api_id}", tags=["api_mst"])
def update_api_mst(api_id: str, body: dict[str, Any], conn: NeonConnDep) -> dict[str, Any]:
    allowed = {"api_name", "source_name", "domain", "protocol", "sink_type", "status", "schema_version", "spec_json", "note"}
    unknown = set(body) - allowed
    if unknown:
        raise HTTPException(400, f"unknown field(s): {sorted(unknown)}")
    if not body:
        raise HTTPException(400, "empty update body")

    sets, params = [], []
    for k, v in body.items():
        if k == "spec_json":
            validated = _validate_or_422(ApiSpec.model_validate, v)
            sets.append("spec_json = %s::jsonb")
            params.append(json.dumps(validated.model_dump(mode="json", by_alias=True, exclude_none=True), ensure_ascii=False))
        else:
            sets.append(f"{k} = %s")
            params.append(v)
    sets.append("updated_at = now()")
    params.append(api_id)

    with conn.cursor() as cur:
        cur.execute(f"UPDATE api_mst SET {', '.join(sets)} WHERE api_id = %s RETURNING *", params)
        row = cur.fetchone()
    if row is None:
        raise HTTPException(404, f"api_mst '{api_id}' not found")
    conn.commit()
    return dict(row)


@router.delete("/api-mst/{api_id}", status_code=204, tags=["api_mst"])
def delete_api_mst(api_id: str, conn: NeonConnDep) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM api_mst WHERE api_id = %s", (api_id,))
        if cur.rowcount == 0:
            raise HTTPException(404, f"api_mst '{api_id}' not found")
    conn.commit()


# ---------------------------------------------------------------------------
# /api-jobs
# ---------------------------------------------------------------------------

@router.get("/api-jobs", tags=["api_job"])
def list_api_jobs(
    conn: NeonConnDep,
    response: Response,
    schedule_tag: str | None = None,
    api_id: str | None = None,
    is_active: bool | None = None,
    q: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[dict[str, Any]]:
    where, params = [], []
    if schedule_tag:
        where.append("schedule_tag = %s")
        params.append(schedule_tag)
    if api_id:
        where.append("api_id = %s")
        params.append(api_id)
    if is_active is not None:
        where.append("is_active = %s")
        params.append(is_active)
    if q:
        where.append("job_id ILIKE %s")
        params.append(f"%{q}%")
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS cnt FROM api_job {clause}", params)
        response.headers["X-Total-Count"] = str(cur.fetchone()["cnt"])
        cur.execute(
            f"SELECT * FROM api_job {clause} ORDER BY schedule_tag, run_order, job_id LIMIT %s OFFSET %s",
            [*params, limit, offset],
        )
        return list(cur.fetchall())


@router.get("/api-jobs/{job_id}", tags=["api_job"])
def get_api_job(job_id: str, conn: NeonConnDep) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM api_job WHERE job_id = %s", (job_id,))
        row = cur.fetchone()
    if row is None:
        raise HTTPException(404, f"api_job '{job_id}' not found")
    return dict(row)


@router.post("/api-jobs", status_code=201, tags=["api_job"])
def create_api_job(body: dict[str, Any], conn: NeonConnDep) -> dict[str, Any]:
    if not body.get("job_id") or not body.get("api_id"):
        raise HTTPException(422, "job_id and api_id are required")
    spec = _validate_or_422(
        ScrapeJobSpec.model_validate,
        {"params": body.get("params_json") or {}, "save_mode": body.get("save_mode", "overwrite")},
    )
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM api_job WHERE job_id = %s", (body["job_id"],))
        if cur.fetchone():
            raise HTTPException(409, f"api_job '{body['job_id']}' already exists")
        cur.execute(
            """
            INSERT INTO api_job (
                job_id, api_id, schedule_tag, run_order, is_active,
                snapshot_unit, save_mode, params_json, note
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            RETURNING *
            """,
            (
                body["job_id"],
                body["api_id"],
                body.get("schedule_tag", "daily"),
                int(body.get("run_order", 100)),
                bool(body.get("is_active", True)),
                body.get("snapshot_unit"),
                spec.save_mode,
                json.dumps(spec.params, ensure_ascii=False),
                body.get("note"),
            ),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row)


@router.patch("/api-jobs/{job_id}", tags=["api_job"])
def update_api_job(job_id: str, body: dict[str, Any], conn: NeonConnDep) -> dict[str, Any]:
    allowed = {"api_id", "schedule_tag", "run_order", "is_active", "snapshot_unit", "save_mode", "params_json", "note"}
    unknown = set(body) - allowed
    if unknown:
        raise HTTPException(400, f"unknown field(s): {sorted(unknown)}")
    if not body:
        raise HTTPException(400, "empty update body")
    if "params_json" in body or "save_mode" in body:
        _validate_or_422(
            ScrapeJobSpec.model_validate,
            {"params": body.get("params_json") or {}, "save_mode": body.get("save_mode", "overwrite")},
        )
    sets, params = [], []
    for k, v in body.items():
        if k == "params_json":
            sets.append("params_json = %s::jsonb")
            params.append(json.dumps(v, ensure_ascii=False))
        else:
            sets.append(f"{k} = %s")
            params.append(v)
    sets.append("updated_at = now()")
    params.append(job_id)
    with conn.cursor() as cur:
        cur.execute(f"UPDATE api_job SET {', '.join(sets)} WHERE job_id = %s RETURNING *", params)
        row = cur.fetchone()
    if row is None:
        raise HTTPException(404, f"api_job '{job_id}' not found")
    conn.commit()
    return dict(row)


@router.delete("/api-jobs/{job_id}", status_code=204, tags=["api_job"])
def delete_api_job(job_id: str, conn: NeonConnDep) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM api_job WHERE job_id = %s", (job_id,))
        if cur.rowcount == 0:
            raise HTTPException(404, f"api_job '{job_id}' not found")
    conn.commit()


# ---------------------------------------------------------------------------
# /etl-jobs
# ---------------------------------------------------------------------------

@router.get("/etl-jobs", tags=["etl_job"])
def list_etl_jobs(
    conn: NeonConnDep,
    response: Response,
    schedule_tag: str | None = None,
    output_layer: str | None = None,
    is_active: bool | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[dict[str, Any]]:
    where, params = [], []
    if schedule_tag:
        where.append("schedule_tag = %s")
        params.append(schedule_tag)
    if output_layer:
        where.append("output_layer = %s")
        params.append(output_layer)
    if is_active is not None:
        where.append("is_active = %s")
        params.append(is_active)
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS cnt FROM etl_job {clause}", params)
        response.headers["X-Total-Count"] = str(cur.fetchone()["cnt"])
        cur.execute(
            f"SELECT * FROM etl_job {clause} ORDER BY schedule_tag, run_order, job_id LIMIT %s OFFSET %s",
            [*params, limit, offset],
        )
        return list(cur.fetchall())


@router.get("/etl-jobs/{job_id}", tags=["etl_job"])
def get_etl_job(job_id: str, conn: NeonConnDep) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM etl_job WHERE job_id = %s", (job_id,))
        row = cur.fetchone()
    if row is None:
        raise HTTPException(404, f"etl_job '{job_id}' not found")
    return dict(row)


@router.post("/etl-jobs", status_code=201, tags=["etl_job"])
def create_etl_job(body: dict[str, Any], conn: NeonConnDep) -> dict[str, Any]:
    spec = _validate_or_422(
        EtlJobSpec.model_validate,
        {
            "job_id": body.get("job_id", ""),
            "sql_text": body.get("sql_text", ""),
            "output_layer": body.get("output_layer", "daily"),
            "target_name": body.get("target_name", ""),
            "targets": body.get("targets") or ["oci"],
            "memory_limit": body.get("memory_limit", "1GB"),
            "threads": body.get("threads", 2),
            "min_rows": body.get("min_rows", 0),
        },
    )
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM etl_job WHERE job_id = %s", (spec.job_id,))
        if cur.fetchone():
            raise HTTPException(409, f"etl_job '{spec.job_id}' already exists")
        cur.execute(
            """
            INSERT INTO etl_job (
                job_id, schedule_tag, run_order, is_active,
                output_layer, target_name, targets, memory_limit,
                threads, min_rows, sql_text, note
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                spec.job_id,
                body.get("schedule_tag", "daily_close"),
                int(body.get("run_order", 200)),
                bool(body.get("is_active", True)),
                spec.output_layer,
                spec.target_name,
                spec.targets,
                spec.memory_limit,
                spec.threads,
                spec.min_rows,
                spec.sql_text,
                body.get("note"),
            ),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row)


@router.patch("/etl-jobs/{job_id}", tags=["etl_job"])
def update_etl_job(job_id: str, body: dict[str, Any], conn: NeonConnDep) -> dict[str, Any]:
    allowed = {
        "schedule_tag", "run_order", "is_active", "output_layer", "target_name",
        "targets", "memory_limit", "threads", "min_rows", "sql_text", "note",
    }
    unknown = set(body) - allowed
    if unknown:
        raise HTTPException(400, f"unknown field(s): {sorted(unknown)}")
    if not body:
        raise HTTPException(400, "empty update body")

    with conn.cursor() as cur:
        cur.execute("SELECT * FROM etl_job WHERE job_id = %s", (job_id,))
        existing = cur.fetchone()
    if existing is None:
        raise HTTPException(404, f"etl_job '{job_id}' not found")

    merged = {**existing, **body}
    _validate_or_422(
        EtlJobSpec.model_validate,
        {
            "job_id": job_id,
            "sql_text": merged["sql_text"],
            "output_layer": merged["output_layer"],
            "target_name": merged["target_name"],
            "targets": list(merged["targets"]),
            "memory_limit": merged["memory_limit"],
            "threads": merged["threads"],
            "min_rows": merged["min_rows"],
        },
    )
    sets = [f"{k} = %s" for k in body]
    sets.append("updated_at = now()")
    params = [*body.values(), job_id]
    with conn.cursor() as cur:
        cur.execute(f"UPDATE etl_job SET {', '.join(sets)} WHERE job_id = %s RETURNING *", params)
        row = cur.fetchone()
    conn.commit()
    return dict(row)


@router.delete("/etl-jobs/{job_id}", status_code=204, tags=["etl_job"])
def delete_etl_job(job_id: str, conn: NeonConnDep) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM etl_job WHERE job_id = %s", (job_id,))
        if cur.rowcount == 0:
            raise HTTPException(404, f"etl_job '{job_id}' not found")
    conn.commit()


# ---------------------------------------------------------------------------
# /host-policies & /schedule
# ---------------------------------------------------------------------------

@router.get("/host-policies", tags=["host_policy"])
def list_host_policies(conn: NeonConnDep) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM host_policy ORDER BY host_suffix")
        return list(cur.fetchall())


@router.put("/host-policies/{host_suffix}", tags=["host_policy"])
def upsert_host_policy(host_suffix: str, body: dict[str, Any], conn: NeonConnDep) -> dict[str, Any]:
    min_iv = float(body.get("min_interval_sec", 0.5))
    cooldown = int(body.get("cooldown_sec", 600))
    if min_iv < 0 or cooldown < 0:
        raise HTTPException(422, "min_interval_sec and cooldown_sec must be >= 0")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO host_policy (host_suffix, min_interval_sec, cooldown_sec, note)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (host_suffix) DO UPDATE SET
                min_interval_sec = EXCLUDED.min_interval_sec,
                cooldown_sec = EXCLUDED.cooldown_sec,
                note = EXCLUDED.note,
                updated_at = now()
            RETURNING *
            """,
            (host_suffix, min_iv, cooldown, body.get("note")),
        )
        row = cur.fetchone()
    conn.commit()
    return dict(row)


@router.delete("/host-policies/{host_suffix}", status_code=204, tags=["host_policy"])
def delete_host_policy(host_suffix: str, conn: NeonConnDep) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM host_policy WHERE host_suffix = %s", (host_suffix,))
        if cur.rowcount == 0:
            raise HTTPException(404, f"host_policy '{host_suffix}' not found")
    conn.commit()


@router.get("/schedule", tags=["schedule"])
def list_schedule(
    conn: NeonConnDep,
    schedule_tag: str | None = None,
    active_only: bool = True,
) -> list[dict[str, Any]]:
    where, params = [], []
    if schedule_tag:
        where.append("schedule_tag = %s")
        params.append(schedule_tag)
    if active_only:
        where.append("is_active = TRUE")
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM v_schedule_job {clause} ORDER BY schedule_tag, run_order, job_type, job_id",
            params,
        )
        return list(cur.fetchall())
