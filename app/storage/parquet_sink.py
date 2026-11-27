"""Local ZSTD Parquet writer + multi-bucket ``v3/`` S3 uploader.

Designed for OCI ARM A1 (2GB RAM):
- Small/medium batches write directly via PyArrow or DuckDB to a local block-
  disk temp path (``data/tmp/parquet``), never ``/tmp`` tmpfs.
- Large batches stream line-by-line to a local NDJSON file and convert to ZSTD
  Parquet with DuckDB's memory-capped out-of-core reader so Python heap never
  doubles.
- Uploads the single finished Parquet file to all configured S3 targets
  (e.g. ``["oci"]`` or ``["oci", "r2"]``) under the ``v3/`` key prefix.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from app.storage.object_storage import build_v3_key, upload_file_to_targets

_DEFAULT_TMP_DIR = Path(os.environ.get("V3_TMP_DIR", "data/tmp"))
_STREAM_THRESHOLD_ROWS = 20_000


@dataclass(frozen=True)
class ParquetWriteResult:
    object_key: str
    row_count: int
    byte_size: int
    uploaded_uris: dict[str, str]
    local_path: Path | None = None


def write_records_to_parquet(
    records: list[dict[str, Any]],
    out_path: Path,
    *,
    compression: str = "zstd",
    memory_limit: str = "512MB",
) -> int:
    """Write ``records`` to ``out_path`` as a ZSTD Parquet file.

    Uses DuckDB NDJSON streaming for large datasets to stay well within the
    2GB OCI A1 RAM budget, and PyArrow (falling back to DuckDB) for smaller
    batches. Returns the written file size in bytes.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not records:
        raise ValueError("cannot write Parquet from empty records list")

    if len(records) >= _STREAM_THRESHOLD_ROWS:
        return _write_via_duckdb_ndjson(records, out_path, compression=compression, memory_limit=memory_limit)

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        return _write_via_duckdb_ndjson(records, out_path, compression=compression, memory_limit=memory_limit)

    try:
        table = pa.Table.from_pylist(records)
        pq.write_table(table, out_path, compression=compression)
        return out_path.stat().st_size
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError, TypeError, ValueError):
        return _write_via_duckdb_ndjson(records, out_path, compression=compression, memory_limit=memory_limit)


def _write_via_duckdb_ndjson(
    records: list[dict[str, Any]],
    out_path: Path,
    *,
    compression: str = "zstd",
    memory_limit: str = "512MB",
) -> int:
    import duckdb

    ndjson_path = out_path.with_suffix(".ndjson")
    duckdb_tmp = _DEFAULT_TMP_DIR / "duckdb"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)
    try:
        with ndjson_path.open("w", encoding="utf-8") as fh:
            for row in records:
                fh.write(json.dumps(row, ensure_ascii=False, default=str))
                fh.write("\n")
        con = duckdb.connect(
            ":memory:",
            config={
                "memory_limit": memory_limit,
                "threads": 2,
                "temp_directory": str(duckdb_tmp),
                "preserve_insertion_order": False,
            },
        )
        try:
            src = str(ndjson_path).replace("'", "''")
            dst = str(out_path).replace("'", "''")
            con.execute(
                f"COPY (SELECT * FROM read_json_auto('{src}', format='newline_delimited')) "
                f"TO '{dst}' (FORMAT PARQUET, COMPRESSION '{compression.upper()}')"
            )
        finally:
            con.close()
    finally:
        ndjson_path.unlink(missing_ok=True)
    return out_path.stat().st_size


def publish_parquet_batch(
    records: list[dict[str, Any]],
    *,
    target_name: str,
    target_date: str,
    layer: str = "raw_clean",
    batch_name: str | None = None,
    targets: Iterable[str] = ("oci",),
    prefix: str | None = None,
    delete_local: bool = True,
    skip_unconfigured_targets: bool = False,
) -> ParquetWriteResult:
    """Write a cleansed batch to Parquet and upload it under ``v3/<layer>/...``.

    Hive partition layout:
    - ``raw_clean``: ``v3/raw_clean/<target>/dt=YYYYMMDD/<batch>.parquet``
    - ``snapshot``:  ``v3/snapshot/<target>.parquet``
    - ``daily``:     ``v3/daily/<target>/ym=YYYYMM/dt=YYYYMMDD/<batch>.parquet``
    - ``monthly``:   ``v3/monthly/<target>/ym=YYYYMM/<batch>.parquet``
    """
    filename = f"{batch_name or uuid.uuid4().hex[:12]}.parquet"
    if layer == "raw_clean":
        key = build_v3_key("raw_clean", target_name, f"dt={target_date}", filename, prefix=prefix)
    elif layer == "snapshot":
        key = build_v3_key("snapshot", f"{target_name}.parquet", prefix=prefix)
    elif layer == "daily":
        ym = target_date[:6]
        key = build_v3_key("daily", target_name, f"ym={ym}", f"dt={target_date}", filename, prefix=prefix)
    elif layer == "monthly":
        ym = target_date[:6]
        key = build_v3_key("monthly", target_name, f"ym={ym}", filename, prefix=prefix)
    else:
        raise ValueError(f"unsupported parquet layer {layer!r}")

    local_path = _DEFAULT_TMP_DIR / "parquet" / key
    byte_size = write_records_to_parquet(records, local_path)
    uploaded = upload_file_to_targets(
        local_path,
        key,
        targets=targets,
        delete_local=delete_local,
        skip_unconfigured=skip_unconfigured_targets,
    )
    return ParquetWriteResult(
        object_key=key,
        row_count=len(records),
        byte_size=byte_size,
        uploaded_uris=uploaded,
        local_path=None if delete_local else local_path,
    )
