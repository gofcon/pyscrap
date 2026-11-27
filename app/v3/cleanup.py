"""Retention cleanup helpers for v3 S3 ``raw_clean/`` partitions and Neon ``job_log``.

All destructive operations default to ``dry_run=True`` so operators always preview
what will be deleted before passing ``--apply``.
"""

from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

from app.object_storage import (
    delete_keys_from_targets,
    list_keys_with_prefix,
    v3_prefix,
)

KST = timezone(timedelta(hours=9))
_DT_SEGMENT_RE = re.compile(r"/dt=(\d{8})/")


def extract_dt_from_key(key: str) -> str | None:
    """Extract ``YYYYMMDD`` from a Hive-partitioned key containing ``/dt=YYYYMMDD/``."""
    m = _DT_SEGMENT_RE.search(key)
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
