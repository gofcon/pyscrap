"""Turns scraped rows into Parquet objects in OCI Object Storage.

One Parquet object per job, uploaded as {table_name}/{job_id}.parquet, so an
Oracle external table can point a wildcard file_uri_list at table_name/* and
pick up every job's file as one logical table without merging files by hand.

Rows aren't sent to Parquet the moment they're parsed, though -- they're
buffered locally as CSV first (append_csv_rows), and only turned into a
Parquet object by finalize_export(). For an ordinary one-shot job these
happen back-to-back in the same run_job call (see
app.services.execution), so nothing changes externally. The buffering
matters for a *repeated* job (a snapshot poll re-executing the same job_id
every few minutes, see RESERVED_NOW_KEY in app.services.job_builder): each
execution only has a row or two of new data, and job_id -- hence the Parquet
key -- stays the same run after run, so writing Parquet immediately would
overwrite the file down to just the latest execution's rows every time
instead of accumulating history. Buffering to CSV and finalizing on a
separate schedule (e.g. once a day) fixes that: run_job appends to the
CSV every run, and finalize_export is called later to turn the day's
accumulated CSV into one real Parquet batch.
"""

from __future__ import annotations

import csv
import inspect
import io
import os
import typing
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
from sqlmodel import SQLModel

from app.db import models as _models
from app.object_storage import bucket_name, get_client as get_object_storage_client

# Tables that are the engine's own bookkeeping (config/job/log), not a
# scraped-result table a "table_name" in output_tables_json could ever name
# -- excluded from the auto-discovery below. Deliberately small and stable:
# these four are fixed by the engine's own design, unlike result tables,
# which grow every time a new scraper's typed output table is added.
#
# Note this exclusion is still needed alongside the job_id rule in
# _discover_table_registry: api_job is keyed *by* job_id and api_job_log
# references it, so both would otherwise qualify as result tables.
_BOOKKEEPING_TABLES = {"api_mst", "api_job_builder", "api_job", "api_job_log"}


def _discover_table_registry() -> dict[str, type[SQLModel]]:
    """table_name -> SQLModel class, for every ``table=True`` model defined
    in app.db.models except the engine's own bookkeeping tables. Two kinds
    of target model are supported (see
    app.scrapers.dynamic.DynamicApiScraper.run_and_save):
    - "generic" (has a result_json field, e.g. ApiRst): the whole record
      dict is stored as-is, no per-site code needed.
    - "typed" (no result_json field, e.g. KisFutoptChart): the record dict
      is spread as constructor kwargs, so its keys must line up with real
      columns (extra keys are silently dropped by pydantic's default
      extra="ignore").

    Auto-discovered (via each class's __tablename__) instead of a
    hand-maintained dict, so adding a new result table (e.g. a future
    KisFutoptPrice) to models.py is the *only* step needed -- there's no
    second place to remember to register it.

    A ``job_id`` column is what marks a model as one of these: it is the
    column every write path here depends on (run_and_save stamps it,
    _clear_previous_results deletes by it, the Parquet key is derived from
    it), so a table without one cannot be a scrape target in the first
    place. models.py also holds reference tables -- expiry calendars,
    instrument masters, daily index series -- that are keyed on their own
    data and maintained DB-side by procedures rather than by this engine;
    requiring job_id keeps those out, so pointing an output_tables_json at
    one fails loudly in run_and_save ("unknown target table") instead of
    half-working."""
    registry: dict[str, type[SQLModel]] = {}
    for obj in vars(_models).values():
        if not (inspect.isclass(obj) and issubclass(obj, SQLModel) and obj is not SQLModel):
            continue
        table_name = getattr(obj, "__tablename__", None)
        if table_name and table_name not in _BOOKKEEPING_TABLES and "job_id" in obj.model_fields:
            registry[table_name] = obj
    return registry


# table_name (as used in ApiMst.output_tables_json) -> the SQLModel class it
# saves to -- see _discover_table_registry.
TABLE_REGISTRY: dict[str, type[SQLModel]] = _discover_table_registry()

_LOCAL_EXPORT_DIR = Path(os.environ.get("EXPORT_DATA_DIR", "data/exports"))


def _csv_path(table_name: str, job_id: str) -> Path:
    return _LOCAL_EXPORT_DIR / table_name / f"{job_id}.csv"


def append_csv_rows(table_name: str, job_id: str, rows: list[dict[str, Any]]) -> None:
    """Buffer typed rows locally as CSV instead of exporting to Parquet
    immediately -- see module docstring for why."""
    if not rows:
        return
    path = _csv_path(table_name, job_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def _unwrap_optional(annotation: Any) -> Any:
    if typing.get_origin(annotation) is typing.Union:
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _cast_csv_row(row: dict[str, str], model_cls: type[SQLModel] | None) -> dict[str, Any]:
    """CSV round-trips every value as a string; cast each field back to its
    real type using the model's own annotations before handing rows to
    pyarrow -- otherwise every Parquet column would come out as string
    (the exact bug model_validate was introduced to avoid in run_and_save,
    just re-appearing at the CSV boundary instead)."""
    casted: dict[str, Any] = {}
    for name, value in row.items():
        if value == "":
            casted[name] = None
            continue
        field = model_cls.model_fields.get(name) if model_cls else None
        base = _unwrap_optional(field.annotation) if field else str
        if base is float:
            casted[name] = float(value)
        elif base is int:
            casted[name] = int(value)
        else:
            casted[name] = value
    return casted


def _write_parquet(table_name: str, job_id: str, rows: list[dict[str, Any]]) -> str | None:
    if not rows:
        return None

    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buf)
    buf.seek(0)

    key = f"{table_name}/{job_id}.parquet"
    get_object_storage_client().put_object(Bucket=bucket_name(), Key=key, Body=buf.getvalue())
    # job_id alone (not api_id) identifies the upload in this log line --
    # _build_job_id always prefixes job_id with its api_id (see
    # app.services.job_builder), so that context isn't actually lost.
    logger.info("uploaded {} row(s) to oci://{}/{}", len(rows), bucket_name(), key)
    return key


def finalize_export(csv_path: Path, delete_csv: bool = True) -> str | None:
    """Convert one locally-buffered CSV file into a Parquet object and
    upload it (overwriting any previous version at that key), then remove
    the local CSV. No-op (returns None) if ``csv_path`` doesn't exist.

    ``table_name``/``job_id`` (needed for the model lookup and the
    Parquet's S3 key) are derived from the path itself -- it's always
    ``{table_name}/{job_id}.csv`` (see ``_csv_path``/``append_csv_rows``) --
    since the file's own location on disk is already the single source of
    truth for both; the caller doesn't need to separately track or re-pass
    them alongside the path."""
    if not csv_path.exists():
        return None

    table_name = csv_path.parent.name
    job_id = csv_path.stem

    model_cls = TABLE_REGISTRY.get(table_name)
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        rows = [_cast_csv_row(row, model_cls) for row in csv.DictReader(f)]

    key = _write_parquet(table_name, job_id, rows)

    if delete_csv:
        csv_path.unlink()
    return key


def finalize_pending_exports() -> dict[str, str]:
    """Finalize every CSV currently sitting in the local export directory
    (see append_csv_rows / _LOCAL_EXPORT_DIR) -- i.e. whatever's actually
    still pending, read directly off the filesystem. No DB query at all:
    run_job no longer finalizes immediately on success (that step was
    pulled out, the same way build_jobs_from_builder was pulled out of
    run_cycle into generate_jobs -- see app.services.execution), so a
    one-shot job's CSV now sits here for a while too, same as a
    repeated/snapshot job's (RESERVED_NOW_KEY -- see
    app.services.job_builder) still-accumulating one -- there's no DB-side
    "which kind is this" distinction left to make, so the filesystem is
    simply the source of truth for "what needs finalizing" (this also
    means a job that crashed mid-run before it could finalize gets swept up
    here too, as a side benefit).

    Meant to run once, as the very last step of the day's batch -- after
    every cycle's generate-jobs + run-cycle has already run (register it as
    its own systemd timer/service, scheduled after the last one) -- since
    that schedule is fixed and known ahead of time, there's no need for
    this app to detect "time to finalize" itself.

    Returns {job_id: uploaded_object_key}."""
    results: dict[str, str] = {}
    if not _LOCAL_EXPORT_DIR.exists():
        return results

    for table_dir in _LOCAL_EXPORT_DIR.iterdir():
        if not table_dir.is_dir():
            continue
        for csv_file in table_dir.glob("*.csv"):
            job_id = csv_file.stem
            key = finalize_export(csv_file)
            if key:
                results[job_id] = key
                logger.info("finalize_pending_exports: {} -> {}", job_id, key)

    return results
