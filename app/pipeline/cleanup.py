"""Retention cleanup helpers for v3 S3 ``raw_clean/`` partitions and Neon ``job_log``.

All destructive operations default to ``dry_run=True`` so operators always preview
what will be deleted before passing ``--apply``.
"""

from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

from app.storage.object_storage import (
    delete_keys_from_targets,
    list_keys_with_prefix,
    v3_prefix,
)

KST = timezone(timedelta(hours=9))
_DT_SEGMENT_RE = re.compile(r"/dt=(\d{8})/")
_YM_SEGMENT_RE = re.compile(r"/ym=(\d{6})/")


def extract_dt_from_key(key: str) -> str | None:
    """Extract ``YYYYMMDD`` from a Hive-partitioned key containing ``/dt=YYYYMMDD/``."""
    m = _DT_SEGMENT_RE.search(key)
    return m.group(1) if m else None


def extract_ym_from_key(key: str) -> str | None:
    """Extract ``YYYYMM`` from a Hive-partitioned key containing ``/ym=YYYYMM/``."""
    m = _YM_SEGMENT_RE.search(key)
    return m.group(1) if m else None


def purge_raw_clean(
    *,
    keep_days: int = 30,
    target_name: str | None = None,
    ref_date: date | None = None,
    targets: Iterable[str] = ("oci",),
    dry_run: bool = True,
    skip_unconfigured: bool = True,
) -> dict[str, Any]:
    """Delete ``v3/raw_clean/<target>/dt=YYYYMMDD/*`` older than ``keep_days``.

    Keeps objects whose ``dt=YYYYMMDD`` is ``>= cutoff`` (i.e. ``ref_date - keep_days``).
    Objects without a ``/dt=YYYYMMDD/`` partition segment are never touched.
    """
    if keep_days < 0:
        raise ValueError(f"keep_days must be >= 0, got {keep_days}")
    base_date = ref_date or datetime.now(KST).date()
    cutoff = (base_date - timedelta(days=keep_days)).strftime("%Y%m%d")

    pfx = v3_prefix()
    scan_prefix = f"{pfx}/raw_clean/{target_name.strip('/')}/" if target_name else f"{pfx}/raw_clean/"

    matched_by_target: dict[str, list[str]] = {}
    deleted_counts: dict[str, int] = {}

    for storage_target in targets:
        try:
            all_keys = list_keys_with_prefix(scan_prefix, target=storage_target)
        except KeyError:
            if skip_unconfigured:
                continue
            raise
        expired = [
            k for k in all_keys
            if (dt := extract_dt_from_key(k)) is not None and dt < cutoff
        ]
        matched_by_target[storage_target] = expired
        if expired and not dry_run:
            res = delete_keys_from_targets(
                expired,
                targets=(storage_target,),
                skip_unconfigured=skip_unconfigured,
            )
            deleted_counts[storage_target] = res.get(storage_target, 0)
        else:
            deleted_counts[storage_target] = 0

    return {
        "scan_prefix": scan_prefix,
        "cutoff_date": cutoff,
        "dry_run": dry_run,
        "matched": {t: len(ks) for t, ks in matched_by_target.items()},
        "deleted": deleted_counts,
        "sample_keys": [k for ks in matched_by_target.values() for k in ks][:10],
    }


def purge_old_job_logs(
    *,
    keep_days: int = 90,
    dry_run: bool = True,
    dsn: str | None = None,
) -> dict[str, Any]:
    """Delete Neon ``job_log`` rows older than ``keep_days`` days."""
    if keep_days < 1:
        raise ValueError(f"keep_days must be >= 1, got {keep_days}")
    import psycopg

    conn_dsn = dsn or os.environ["NEON_DATABASE_URL"]
    with psycopg.connect(conn_dsn, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            if dry_run:
                cur.execute(
                    "SELECT COUNT(*) FROM job_log WHERE executed_at < now() - (%s || ' days')::interval",
                    (keep_days,),
                )
                count = int(cur.fetchone()[0])
            else:
                cur.execute(
                    "DELETE FROM job_log WHERE executed_at < now() - (%s || ' days')::interval",
                    (keep_days,),
                )
                count = int(cur.rowcount or 0)
        if not dry_run:
            conn.commit()
    return {"keep_days": keep_days, "dry_run": dry_run, "rows": count}


def _shift_ym(ref: date, months_back: int) -> str:
    idx = ref.year * 12 + (ref.month - 1) - months_back
    return f"{idx // 12:04d}{idx % 12 + 1:02d}"


def verify_monthly_matches_daily(
    target_name: str,
    ym: str,
    *,
    oci_bucket: str | None = None,
    prefix: str | None = None,
    memory_limit: str = "512MB",
) -> tuple[bool, dict[str, Any]]:
    """Verify ``v3/monthly/<target>/ym=<ym>/<ym>.parquet`` matches ``DISTINCT`` rows
    of ``v3/daily/<target>/ym=<ym>/**/*.parquet`` using ``COUNT(*)`` + ``BIT_XOR(HASH(*))``."""
    import duckdb
    from app.storage.object_storage import bucket_name
    from app.pipeline.duckdb_runner import build_duckdb_config, register_s3_secrets

    bkt = oci_bucket or bucket_name("oci")
    pfx = prefix or v3_prefix()
    daily_uri = f"s3://{bkt}/{pfx}/daily/{target_name}/ym={ym}/**/*.parquet"
    monthly_uri = f"s3://{bkt}/{pfx}/monthly/{target_name}/ym={ym}/{ym}.parquet"

    con = duckdb.connect(":memory:", config=build_duckdb_config(memory_limit=memory_limit, threads=2))
    try:
        register_s3_secrets(con, targets=["oci"])
        m_cnt, m_hash = con.execute(
            f"SELECT COUNT(*), BIT_XOR(HASH(*)) FROM read_parquet('{monthly_uri}')"
        ).fetchone()
        d_cnt, d_hash = con.execute(
            f"SELECT COUNT(*), BIT_XOR(HASH(*)) FROM ("
            f"  SELECT DISTINCT * FROM read_parquet('{daily_uri}', union_by_name=true)"
            f")"
        ).fetchone()
    finally:
        con.close()

    matched = (int(m_cnt) > 0) and (int(m_cnt) == int(d_cnt)) and (m_hash == d_hash)
    return matched, {
        "ym": ym,
        "monthly_rows": int(m_cnt),
        "daily_distinct_rows": int(d_cnt),
        "hash_match": m_hash == d_hash,
    }


def purge_verified_daily(
    target_name: str,
    *,
    keep_months: int = 3,
    ref_date: date | None = None,
    targets: Iterable[str] = ("oci",),
    dry_run: bool = True,
    skip_unconfigured: bool = True,
) -> dict[str, Any]:
    """Delete ``v3/daily/<target>/ym=<ym>/`` ONLY when ``ym < cutoff_ym`` AND
    ``v3/monthly/<target>/ym=<ym>/monthly.parquet`` matches 100% by COUNT + HASH."""
    if keep_months < 1:
        raise ValueError(f"keep_months must be >= 1, got {keep_months}")
    base_date = ref_date or datetime.now(KST).date()
    cutoff_ym = _shift_ym(base_date, keep_months)

    pfx = v3_prefix()
    daily_root = f"{pfx}/daily/{target_name.strip('/')}/"
    all_daily = list_keys_with_prefix(daily_root, target="oci")

    by_ym: dict[str, list[str]] = {}
    for k in all_daily:
        ym = extract_ym_from_key(k)
        if ym and ym < cutoff_ym:
            by_ym.setdefault(ym, []).append(k)

    verified_months: list[dict[str, Any]] = []
    skipped_months: list[dict[str, Any]] = []
    deleted_total = 0

    for ym in sorted(by_ym.keys()):
        try:
            ok, detail = verify_monthly_matches_daily(target_name, ym, prefix=pfx)
        except Exception as exc:
            skipped_months.append({"ym": ym, "reason": str(exc)})
            continue
        if not ok:
            skipped_months.append({**detail, "reason": "count_or_hash_mismatch"})
            continue
        verified_months.append({**detail, "daily_files": len(by_ym[ym])})
        if not dry_run:
            res = delete_keys_from_targets(by_ym[ym], targets=targets, skip_unconfigured=skip_unconfigured)
            deleted_total += res.get("oci", 0)

    return {
        "target_name": target_name,
        "cutoff_ym": cutoff_ym,
        "dry_run": dry_run,
        "verified_months": verified_months,
        "skipped_months": skipped_months,
        "deleted_files": deleted_total,
    }
