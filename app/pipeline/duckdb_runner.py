"""Fixed DuckDB post-processing runner tuned for OCI ARM A1 (2GB RAM budget).

All business SQL (daily merge, monthly sorted compaction, financial master
refresh with Neon ``user_*`` overrides, mart aggregation) lives in config
(``etl_job`` / local snapshot), keeping this Python runner frozen.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

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


_VIEW_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_SINGLE_QUOTED_RE = re.compile(r"'(?:''|[^'])*'")
_LINE_COMMENT_RE = re.compile(r"--[^\n]*")


def _strip_sql_literals(sql: str) -> str:
    """Remove single-quoted string literals and line comments before identifier scan."""
    no_comments = _LINE_COMMENT_RE.sub(" ", sql)
    return _SINGLE_QUOTED_RE.sub(" ", no_comments)


def _substitute_sql_params(sql: str, params: dict[str, str]) -> str:
    out = sql
    for key, value in params.items():
        out = out.replace(f"${{{key}}}", value)
    return out


def register_duckdb_views(
    con,
    query: str,
    duckdb_views: dict[str, dict[str, Any]] | None,
    *,
    sql_params: dict[str, str] | None = None,
) -> list[str]:
    """Bind referenced declarative ``duckdb_view`` rows as TEMP VIEWs on ``con``.

    - Scans ``query`` (excluding string literals & comments) for word-boundary
      matches against active ``duckdb_views`` keys.
    - Transitively resolves view-on-view dependencies in topological order.
    - Falls back to ``fallback_sql`` when primary view binding fails (e.g. no
      Parquet files exist yet on S3).
    """
    if not duckdb_views:
        return []
    params = sql_params or {}
    active: dict[str, dict[str, Any]] = {
        name: row
        for name, row in duckdb_views.items()
        if _VIEW_NAME_RE.match(name) and row.get("is_active", True) and row.get("sql_text")
    }
    if not active:
        return []

    patterns = {name: re.compile(rf"(?<!\w){re.escape(name)}(?!\w)", re.IGNORECASE) for name in active}

    def _referenced_in(sql_text: str, exclude: str | None = None) -> list[str]:
        cleaned = _strip_sql_literals(sql_text)
        return [
            name
            for name, pat in patterns.items()
            if name != exclude and pat.search(cleaned)
        ]

    ordered: list[str] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def _visit(name: str) -> None:
        if name in visited or name in visiting:
            return
        visiting.add(name)
        for dep in _referenced_in(active[name]["sql_text"], exclude=name):
            _visit(dep)
        visiting.remove(name)
        visited.add(name)
        ordered.append(name)

    for root_name in _referenced_in(query):
        _visit(root_name)

    registered: list[str] = []
    for name in ordered:
        row = active[name]
        view_sql = _substitute_sql_params(row["sql_text"], params)
        fallback_sql = row.get("fallback_sql")
        try:
            con.execute(f"CREATE OR REPLACE TEMP VIEW {name} AS ({view_sql})")
        except Exception:
            if not fallback_sql:
                raise
            fb = _substitute_sql_params(fallback_sql, params)
            con.execute(f"CREATE OR REPLACE TEMP VIEW {name} AS ({fb})")
        registered.append(name)
    return registered


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
    duckdb_views: dict[str, dict[str, Any]] | None = None,
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
    query = _substitute_sql_params(spec.sql_text, params)

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
        register_duckdb_views(con, query, duckdb_views, sql_params=params)
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
