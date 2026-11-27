"""Fixed DuckDB post-processing runner tuned for OCI ARM A1 (2GB RAM budget).

All business SQL (daily merge, monthly sorted compaction, financial master
refresh with Neon ``user_*`` overrides, mart aggregation) lives in config
(``etl_job`` / local snapshot), keeping this Python runner frozen.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from app.storage.object_storage import (
    S3TargetConfig,
    configured_targets,
    get_target_config,
    resolve_etl_object_key,
    upload_file_to_targets,
)
from app.core.spec import DuckDBJobSpec

_DEFAULT_DUCKDB_TMP = Path(os.environ.get("V3_DUCKDB_TMP_DIR", "data/tmp/duckdb"))


@dataclass(frozen=True)
class DuckDBJobResult:
    job_id: str
    object_key: str
    row_count: int
    byte_size: int
    uploaded_uris: dict[str, str]


def is_tmpfs_path(path: Path) -> bool:
    """Return True if ``path`` resolves onto a RAM-backed ``tmpfs`` mount on Linux."""
    mounts = Path("/proc/mounts")
    if not mounts.exists():
        return False
    resolved = str(path.resolve())
    best_mount = ""
    best_fstype = ""
    for line in mounts.read_text(encoding="utf-8", errors="ignore").splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        mount_point, fstype = parts[1], parts[2]
        if (resolved == mount_point or resolved.startswith(mount_point.rstrip("/") + "/")) and len(mount_point) > len(best_mount):
            best_mount = mount_point
            best_fstype = fstype
    return best_fstype == "tmpfs"


def build_duckdb_config(
    *,
    memory_limit: str = "512MB",
    threads: int = 2,
    temp_directory: Path | None = None,
    allow_external_access: bool = True,
) -> dict[str, object]:
    """Build DuckDB connection config respecting the 2GB OCI A1 RAM budget."""
    spill_dir = (temp_directory or _DEFAULT_DUCKDB_TMP).resolve()
    spill_dir.mkdir(parents=True, exist_ok=True)
    if is_tmpfs_path(spill_dir):
        raise ValueError(
            f"DuckDB temp_directory {spill_dir} is on a RAM-backed tmpfs mount; "
            f"use a block-disk path such as data/tmp/duckdb to prevent OOM"
        )
    return {
        "memory_limit": memory_limit,
        "threads": threads,
        "temp_directory": str(spill_dir),
        "preserve_insertion_order": False,
        "enable_external_access": allow_external_access,
    }


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def register_s3_secrets(con, targets: Iterable[str] | None = None) -> list[str]:
    """Register S3 credentials for configured targets (e.g. ``oci``, ``r2``)."""
    names = list(targets) if targets is not None else configured_targets()
    if not names:
        return []
    try:
        con.execute("LOAD httpfs;")
    except Exception:
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
    registered: list[str] = []
    for name in names:
        cfg: S3TargetConfig = get_target_config(name)
        scope = f"s3://{cfg.bucket}"
        con.execute(
            f"CREATE OR REPLACE SECRET {cfg.name}_s3 ("
            f"TYPE S3, "
            f"KEY_ID {_sql_quote(cfg.access_key)}, "
            f"SECRET {_sql_quote(cfg.secret_key)}, "
            f"REGION {_sql_quote(cfg.region)}, "
            f"ENDPOINT {_sql_quote(cfg.endpoint_host)}, "
            f"SCOPE {_sql_quote(scope)}, "
            f"URL_STYLE 'path', "
            f"USE_SSL {'true' if cfg.use_ssl else 'false'}"
            f");"
        )
        registered.append(cfg.name)
    return registered


def run_duckdb_job(
    spec: DuckDBJobSpec,
    *,
    sql_params: dict[str, str] | None = None,
    read_targets: Iterable[str] | None = None,
    temp_directory: Path | None = None,
    delete_local: bool = True,
    skip_unconfigured_targets: bool = False,
) -> DuckDBJobResult:
    """Execute one configured DuckDB SQL job, write ZSTD Parquet locally, and upload."""
    import duckdb

    params = sql_params or {}
    object_key = resolve_etl_object_key(
        spec.output_layer,
        spec.target_name,
        target_date=params.get("TARGET_DATE", ""),
        target_ym=params.get("TARGET_YM", ""),
        prefix=spec.prefix,
    )
    query = spec.sql_text
    for key, value in params.items():
        query = query.replace(f"${{{key}}}", value)

    out_dir = (temp_directory or _DEFAULT_DUCKDB_TMP).parent / "duckdb_out"
    out_path = out_dir / object_key
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cfg = build_duckdb_config(
        memory_limit=spec.memory_limit,
        threads=spec.threads,
        temp_directory=temp_directory,
        allow_external_access=True,
    )
    con = duckdb.connect(":memory:", config=cfg)
    try:
        register_s3_secrets(con, targets=read_targets)
        dst = _sql_quote(str(out_path))
        con.execute(f"CREATE TEMP TABLE _job_out AS ({query})")
        row_count = int(con.execute("SELECT COUNT(*) FROM _job_out").fetchone()[0])
        if row_count < spec.min_rows:
            raise ValueError(
                f"etl_job {spec.job_id!r} produced {row_count} row(s), "
                f"below min_rows={spec.min_rows}"
            )
        con.execute(f"COPY _job_out TO {dst} (FORMAT PARQUET, COMPRESSION 'ZSTD')")
    finally:
        con.close()

    byte_size = out_path.stat().st_size
    uploaded = upload_file_to_targets(
        out_path,
        object_key,
        targets=spec.targets,
        delete_local=delete_local,
        skip_unconfigured=skip_unconfigured_targets,
    )

    return DuckDBJobResult(
        job_id=spec.job_id,
        object_key=object_key,
        row_count=row_count,
        byte_size=byte_size,
        uploaded_uris=uploaded,
    )
