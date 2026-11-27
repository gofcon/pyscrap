"""v3 job action endpoints (sync-config, dry-run, run-job, run-cycle, flush-log)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.errors import ConfigError, SourceRefused
from app.core.batch_cache import DailyConfigSnapshot, LocalRunLedger
from app.pipeline.executor import dry_run_api, execute_job, execute_schedule_cycle, now_kst
from app.core.neon_sync import mirror_user_tables_to_parquet, pull_daily_snapshot

router = APIRouter(prefix="/jobs", tags=["actions"])


@router.post("/sync-config")
def sync_config(mirror_user_parquet: bool = Query(default=False)) -> dict[str, Any]:
    """Pull daily config snapshot from Neon DB (and optionally mirror manual tables to v3/snapshot/)."""
    snap, path = pull_daily_snapshot()
    mirrored = mirror_user_tables_to_parquet(snap) if mirror_user_parquet else {}
    return {
        "snapshot_path": str(path),
        "api_mst": len(snap.apis),
        "api_job": len(snap.api_jobs),
        "etl_job": len(snap.etl_jobs),
        "host_policy": len(snap.host_policies),
        "mirrored": mirrored,
    }


@router.post("/dry-run/{api_id}")
def dry_run(api_id: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    """Fetch 1 request, cleanse, and evaluate contract without uploading or logging."""
    payload = body or {}
    snap = DailyConfigSnapshot.load()
    try:
        res = dry_run_api(
            snap,
            api_id,
            payload.get("params") or {},
            sample_limit=int(payload.get("sample_limit", 5)),
        )
    except ConfigError as exc:
        raise HTTPException(404 if "No api_mst row" in str(exc) else 400, str(exc)) from exc
    return {
        "api_id": res.api_id,
        "raw_count": res.raw_count,
        "cleaned_count": res.cleaned_count,
        "contract_ok": res.contract.ok,
        "contract_action": res.contract.action,
        "violations": res.contract.violations,
        "columns": res.columns,
        "sample_rows": res.sample_rows,
    }


@router.post("/run/{job_id}")
def run_one_job(
    job_id: str,
    job_type: str | None = Query(default=None, pattern="^(?i)(api|etl)$"),
    target_date: str | None = Query(default=None, pattern=r"^\d{8}$"),
    limit: int | None = Query(default=None, ge=1),
) -> dict[str, Any]:
    """Execute one ``api_job`` or ``etl_job`` from the local snapshot."""
    snap = DailyConfigSnapshot.load()
    try:
        return execute_job(snap, job_id, job_type=job_type, target_date=target_date, limit=limit)
    except ConfigError as exc:
        raise HTTPException(404 if "No " in str(exc) else 400, str(exc)) from exc
    except SourceRefused as exc:
        raise HTTPException(503, str(exc)) from exc


@router.post("/run-cycle/{schedule_tag}")
def run_cycle(
    schedule_tag: str,
    target_date: str | None = Query(default=None, pattern=r"^\d{8}$"),
) -> list[dict[str, Any]]:
    """Run every active ``api_job`` + ``etl_job`` for ``schedule_tag`` ordered by ``run_order``."""
    snap = DailyConfigSnapshot.load()
    try:
        return execute_schedule_cycle(snap, schedule_tag, target_date=target_date)
    except SourceRefused as exc:
        raise HTTPException(503, str(exc)) from exc


@router.post("/flush-log")
def flush_log(target_date: str | None = Query(default=None, pattern=r"^\d{8}$")) -> dict[str, Any]:
    """Bulk-push the local daily ledger into Neon ``job_log``."""
    day = target_date or now_kst().strftime("%Y%m%d")
    count = LocalRunLedger(day).flush_to_neon()
    return {"target_date": day, "flushed_rows": count}
