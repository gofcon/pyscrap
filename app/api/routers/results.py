"""Read-only DuckDB Parquet query router over ``v3/`` S3 layers.

Protects the 2GB OCI ARM A1 VM with ``DUCKDB_SEMAPHORE`` (max 2 concurrent
queries) and ``memory_limit='256MB'`` per request.
"""

from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.api.deps import DUCKDB_SEMAPHORE
from app.storage.object_storage import bucket_name, get_client, v3_prefix
from app.pipeline.duckdb_runner import build_duckdb_config, register_s3_secrets

router = APIRouter(prefix="/results", tags=["results"])
_TARGET_RE = re.compile(r"^[a-z0-9_]+$")
_READABLE_LAYERS = ("snapshot", "daily", "monthly", "raw_clean")


def build_result_parquet_uri(
    layer: str,
    target_name: str,
    *,
    oci_bucket: str,
    ym: str | None = None,
    dt: str | None = None,
    prefix: str | None = None,
) -> str:
    if layer not in _READABLE_LAYERS:
        raise ValueError(f"unsupported layer {layer!r}; expected one of {_READABLE_LAYERS}")
    if not _TARGET_RE.match(target_name):
        raise ValueError(f"invalid target_name {target_name!r}")
    pfx = prefix or v3_prefix()
    if layer == "snapshot":
        return f"s3://{oci_bucket}/{pfx}/snapshot/{target_name}.parquet"
    if layer == "raw_clean":
        if dt:
            return f"s3://{oci_bucket}/{pfx}/raw_clean/{target_name}/dt={dt}/*.parquet"
        return f"s3://{oci_bucket}/{pfx}/raw_clean/{target_name}/**/*.parquet"
    if layer == "daily":
        if dt:
            use_ym = ym or dt[:6]
            return f"s3://{oci_bucket}/{pfx}/daily/{target_name}/ym={use_ym}/dt={dt}/*.parquet"
        if ym:
            return f"s3://{oci_bucket}/{pfx}/daily/{target_name}/ym={ym}/**/*.parquet"
        return f"s3://{oci_bucket}/{pfx}/daily/{target_name}/**/*.parquet"
    # monthly
    if ym:
        return f"s3://{oci_bucket}/{pfx}/monthly/{target_name}/ym={ym}/*.parquet"
    return f"s3://{oci_bucket}/{pfx}/monthly/{target_name}/**/*.parquet"


@router.get("/targets")
def list_result_targets(
    layer: str = Query(default="snapshot", pattern="^(snapshot|daily|monthly|raw_clean)$"),
    storage: str = Query(default="oci"),
) -> list[str]:
    """List available target names under ``v3/<layer>/``."""
    bkt = bucket_name(storage)
    pfx = v3_prefix()
    base = f"{pfx}/{layer}/"
    client = get_client(storage)
    paginator = client.get_paginator("list_objects_v2")
    names: set[str] = set()
    if layer == "snapshot":
        for page in paginator.paginate(Bucket=bkt, Prefix=base):
            for obj in page.get("Contents", []):
                rel = obj["Key"][len(base):]
                if rel.endswith(".parquet") and "/" not in rel:
                    names.add(rel[:-8])
    else:
        for page in paginator.paginate(Bucket=bkt, Prefix=base, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                rel = cp["Prefix"][len(base):].strip("/")
                if rel:
                    names.add(rel)
    return sorted(names)


@router.get("/{layer}/{target_name}")
def query_parquet_results(
    layer: str,
    target_name: str,
    ym: str | None = Query(default=None, pattern=r"^\d{6}$"),
    dt: str | None = Query(default=None, pattern=r"^\d{8}$"),
    storage: str = Query(default="oci"),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """Read a page of rows from a ``v3/<layer>/<target_name>`` Parquet dataset via DuckDB."""
    import duckdb

    bkt = bucket_name(storage)
    try:
        uri = build_result_parquet_uri(layer, target_name, oci_bucket=bkt, ym=ym, dt=dt)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc

    hive = ", hive_partitioning=true" if layer != "snapshot" else ""
    with DUCKDB_SEMAPHORE:
        con = duckdb.connect(":memory:", config=build_duckdb_config(memory_limit="256MB", threads=2))
        try:
            register_s3_secrets(con, targets=[storage])
            cur = con.execute(
                f"SELECT * FROM read_parquet('{uri}', union_by_name=true{hive}) LIMIT {int(limit)} OFFSET {int(offset)}"
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        except Exception as exc:
            if "No files found" in str(exc) or "HTTP" in str(exc):
                raise HTTPException(404, f"No Parquet data found for {layer}/{target_name}") from exc
            raise HTTPException(500, str(exc)) from exc
        finally:
            con.close()
    return {"layer": layer, "target_name": target_name, "uri": uri, "count": len(rows), "rows": rows}
