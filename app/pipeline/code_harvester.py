"""DuckDB-powered source code harvester (v3 equivalent of Oracle ``sp_src_code_sync``).

Scans ``v3/raw_clean/<target>/**/*.parquet`` in OCI S3 (within the 2GB OCI A1 RAM
budget), discovers low-cardinality VARCHAR code columns (``2 <= distinct <= max_distinct``),
excludes date-like and engine-bookkeeping columns using the exact v2 rules from
``scripts/sql/sp_src_code_sync.sql``, merges with any existing ``v3/ref/src_code.parquet``
to preserve ``first_seen``, and publishes updated ``v3/ref/src_code.parquet``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from app.storage.object_storage import bucket_name, get_client, v3_prefix
from app.pipeline.duckdb_runner import DuckDBJobResult, build_duckdb_config, register_s3_secrets
from app.storage.parquet_sink import publish_parquet_batch

_EXCLUDED_COLUMNS = frozenset({
    "api_id",
    "job_id",
    "target_date",
    "dt",
    "ym",
    "description",
    "note",
    "block_index",
    "label",
    "html",
})
_DATE_SUFFIX_3 = ("_dd", "_dt", "_at")
_DATE_SUFFIX_4 = ("date", "_ymd", "yymm")


def infer_source_name(target_name: str) -> str:
    upper = target_name.upper()
    if upper.startswith("KRX") or upper.startswith("ISIN"):
        return "KRX"
    if upper.startswith("KSD"):
        return "KSD"
    if upper.startswith("KIS") or upper.startswith("FO_"):
        return "KIS"
    if upper.startswith("DART"):
        return "DART"
    return "AMC"


def is_candidate_code_column(col_name: str, col_type: str) -> bool:
    """Match ``sp_src_code_sync`` VARCHAR / non-date / non-bookkeeping filters."""
    low = col_name.lower()
    if not col_type.upper().startswith("VARCHAR"):
        return False
    if low in _EXCLUDED_COLUMNS:
        return False
    if low.endswith(_DATE_SUFFIX_3) or low.endswith(_DATE_SUFFIX_4):
        return False
    return True


def _discover_raw_clean_targets(oci_bucket: str, prefix: str) -> list[str]:
    client = get_client("oci")
    base = f"{prefix}/raw_clean/"
    paginator = client.get_paginator("list_objects_v2")
    targets: set[str] = set()
    for page in paginator.paginate(Bucket=oci_bucket, Prefix=base, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            rel = cp["Prefix"][len(base):].strip("/")
            if rel:
                targets.add(rel)
    return sorted(targets)


def _load_existing_src_code(con, oci_bucket: str, prefix: str) -> dict[tuple[str, str, str], dict[str, Any]]:
    uri = f"s3://{oci_bucket}/{prefix}/snapshot/src_code.parquet"
    try:
        rows = con.execute(
            f"SELECT table_name, column_name, code_value, source_name, "
            f"row_cnt, distinct_cnt, first_seen, last_seen "
            f"FROM read_parquet('{uri}')"
        ).fetchall()
    except Exception:
        return {}
    existing: dict[tuple[str, str, str], dict[str, Any]] = {}
    for r in rows:
        key = (str(r[0]), str(r[1]), str(r[2]))
        existing[key] = {
            "table_name": key[0],
            "column_name": key[1],
            "code_value": key[2],
            "source_name": r[3],
            "row_cnt": int(r[4]) if r[4] is not None else 0,
            "distinct_cnt": int(r[5]) if r[5] is not None else 0,
            "first_seen": str(r[6]) if r[6] is not None else None,
            "last_seen": str(r[7]) if r[7] is not None else None,
        }
    return existing


def harvest_src_codes(
    *,
    max_distinct: int = 40,
    only_targets: Iterable[str] | None = None,
    targets: Iterable[str] = ("oci",),
    memory_limit: str = "512MB",
    threads: int = 2,
    temp_directory: Path | None = None,
    skip_unconfigured_targets: bool = True,
) -> DuckDBJobResult:
    """Harvest low-cardinality string code vocabularies into ``v3/ref/src_code.parquet``."""
    import duckdb

    oci_bkt = bucket_name("oci")
    pfx = v3_prefix()
    scan_targets = sorted(only_targets) if only_targets else _discover_raw_clean_targets(oci_bkt, pfx)
    now_iso = datetime.now(timezone.utc).isoformat()

    con = duckdb.connect(
        ":memory:",
        config=build_duckdb_config(
            memory_limit=memory_limit,
            threads=threads,
            temp_directory=temp_directory,
        ),
    )
    try:
        register_s3_secrets(con, targets=["oci"])
        merged = _load_existing_src_code(con, oci_bkt, pfx)

        for tbl in scan_targets:
            uri = f"s3://{oci_bkt}/{pfx}/raw_clean/{tbl}/**/*.parquet"
            try:
                desc = con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{uri}', union_by_name=true)"
                ).fetchall()
            except Exception:
                continue

            src_name = infer_source_name(tbl)
            cand_cols = [
                col_name
                for col_name, col_type, *_ in desc
                if is_candidate_code_column(col_name, col_type)
            ]
            for col in cand_cols:
                qcol = '"' + col.replace('"', '""') + '"'
                try:
                    rows = con.execute(
                        f"SELECT code_value, row_cnt, grp_cnt FROM ("
                        f"  SELECT {qcol} AS code_value, COUNT(*) AS row_cnt, "
                        f"         COUNT(*) OVER () AS grp_cnt "
                        f"  FROM read_parquet('{uri}', union_by_name=true) "
                        f"  WHERE {qcol} IS NOT NULL AND LENGTH({qcol}) <= 400 "
                        f"  GROUP BY {qcol}"
                        f") WHERE grp_cnt BETWEEN 2 AND {int(max_distinct)}"
                    ).fetchall()
                except Exception:
                    continue
                for code_val, row_cnt, grp_cnt in rows:
                    key = (tbl.upper(), col.upper(), str(code_val))
                    prev = merged.get(key)
                    merged[key] = {
                        "table_name": key[0],
                        "column_name": key[1],
                        "code_value": key[2],
                        "source_name": prev["source_name"] if prev and prev.get("source_name") else src_name,
                        "row_cnt": int(row_cnt),
                        "distinct_cnt": int(grp_cnt),
                        "first_seen": prev["first_seen"] if prev and prev.get("first_seen") else now_iso,
                        "last_seen": now_iso,
                    }
    finally:
        con.close()

    out_rows = sorted(merged.values(), key=lambda r: (r["table_name"], r["column_name"], r["code_value"]))
    if not out_rows:
        return DuckDBJobResult(
            job_id="HARVEST_SRC_CODE",
            object_key=f"{pfx}/snapshot/src_code.parquet",
            row_count=0,
            byte_size=0,
            uploaded_uris={},
        )

    pub = publish_parquet_batch(
        out_rows,
        target_name="src_code",
        target_date="",
        layer="snapshot",
        targets=targets,
        prefix=pfx,
        skip_unconfigured_targets=skip_unconfigured_targets,
    )
    return DuckDBJobResult(
        job_id="HARVEST_SRC_CODE",
        object_key=pub.object_key,
        row_count=pub.row_count,
        byte_size=pub.byte_size,
        uploaded_uris=pub.uploaded_uris,
    )
