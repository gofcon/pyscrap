"""Single-connection Neon DB snapshot puller (Scale-to-Zero friendly).

Called once pre-market (``python3 -m app.v3.cli sync-config``) to pull all
active ``api_mst``, ``api_job``, ``etl_job``, ``host_policy``, and manual
``user_*`` tables from Neon into ``data/cache/config_snapshot.json`` so the
entire day's batches run without waking Neon again until the end-of-day
``job_log`` flush.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.v3.batch_cache import DailyConfigSnapshot

_MANUAL_USER_TABLES = (
    "user_etf",
    "mst_code",
    "user_code_map",
    "krx_index_mst",
    "mst_index",
    "meta_fuopt_info",
)


def load_dotenv_into_env(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip()
        if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
            v = v[1:-1]
        os.environ.setdefault(k, v)


def _fetchall_dicts(cur, sql: str) -> list[dict[str, Any]]:
    cur.execute(sql)
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def pull_daily_snapshot(
    dsn: str | None = None,
    out_path: Path | None = None,
) -> tuple[DailyConfigSnapshot, Path]:
    """Pull config + manual tables from Neon in ONE connection and save locally."""
    import psycopg

    conn_dsn = dsn or os.environ["NEON_DATABASE_URL"]
    apis: dict[str, dict[str, Any]] = {}
    api_jobs: dict[str, dict[str, Any]] = {}
    etl_jobs: dict[str, dict[str, Any]] = {}
    host_policies: dict[str, dict[str, Any]] = {}
    user_tables: dict[str, list[dict[str, Any]]] = {}

    with psycopg.connect(conn_dsn, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            for row in _fetchall_dicts(
                cur,
                "SELECT api_id, api_name, source_name, domain, protocol, sink_type, "
                "status, schema_version, spec_json, note "
                "FROM api_mst WHERE status <> 'PAUSED' ORDER BY api_id",
            ):
                apis[row["api_id"]] = row

            for row in _fetchall_dicts(
                cur,
                "SELECT job_id, api_id, schedule_tag, run_order, "
                "is_active, snapshot_unit, save_mode, params_json, note "
                "FROM api_job ORDER BY schedule_tag, run_order, job_id",
            ):
                api_jobs[row["job_id"]] = row

            for row in _fetchall_dicts(
                cur,
                "SELECT job_id, schedule_tag, run_order, is_active, "
                "output_layer, target_name, targets, memory_limit, "
                "threads, min_rows, sql_text, note "
                "FROM etl_job ORDER BY schedule_tag, run_order, job_id",
            ):
                etl_jobs[row["job_id"]] = row

            for row in _fetchall_dicts(
                cur,
                "SELECT host_suffix, min_interval_sec, cooldown_sec, note "
                "FROM host_policy ORDER BY host_suffix",
            ):
                host_policies[row["host_suffix"]] = row

            for tbl in _MANUAL_USER_TABLES:
                user_tables[tbl] = _fetchall_dicts(cur, f"SELECT * FROM {tbl}")

    snapshot = DailyConfigSnapshot(
        generated_at=datetime.now(timezone.utc).isoformat(),
        apis=apis,
        host_policies=host_policies,
        api_jobs=api_jobs,
        etl_jobs=etl_jobs,
        user_tables=user_tables,
    )
    saved = snapshot.save(out_path)
    return snapshot, saved


def mirror_user_tables_to_parquet(
    snapshot: DailyConfigSnapshot,
    targets: tuple[str, ...] = ("oci",),
    *,
    skip_unconfigured_targets: bool = True,
) -> dict[str, dict[str, str]]:
    """Publish manual ``user_*`` tables from snapshot to ``v3/snapshot/<table_name>.parquet``."""
    from app.v3.parquet_sink import publish_parquet_batch

    published: dict[str, dict[str, str]] = {}
    for table, rows in snapshot.user_tables.items():
        if not rows:
            continue
        res = publish_parquet_batch(
            rows,
            target_name=table,
            target_date="",
            layer="snapshot",
            targets=targets,
            skip_unconfigured_targets=skip_unconfigured_targets,
        )
        published[table] = res.uploaded_uris
    return published
