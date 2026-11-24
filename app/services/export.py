"""Local staging for scrape output: buffered rows and downloaded documents.

Typed rows are appended to a CSV per job as they are parsed (append_csv_rows),
giving a local trail of what a run produced. Turning those into Parquet used
to happen here; it now happens in the database instead -- sp_export_parquet
reads the result table for a given day and has DBMS_CLOUD.EXPORT_DATA write
the objects directly. Exporting from the table rather than from whatever
buffers happened to be staged is what makes it idempotent and lets it reach
rows that never passed through a buffer at all, such as a backfill.

What still belongs here is what the database cannot hold: a scrape whose
result is a document -- a PDF, an archive -- rather than rows. Those are
staged whole (stage_file) and uploaded unchanged (upload_file), which is all
finalize_pending_exports does now.

The row buffers are kept on a retention window instead of being consumed
(purge_csv_buffers). Nothing deletes them any more now that Parquet comes
from the tables, and a 3-minute poll leaves one file per instrument per day,
so without a window they only grow -- 61k files and 820MB accumulated from a
single backfill.
"""

from __future__ import annotations

import csv
import inspect
import io
import os
import typing
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

from loguru import logger
from app.db.models.base import ResultTable
from sqlmodel import SQLModel

from app.db import models as _models
from app.object_storage import bucket_name, get_client as get_object_storage_client

def _discover_table_registry() -> dict[str, type[SQLModel]]:
    """table_name -> SQLModel class, for every result table in app.db.models.

    결과 표란 ``app.db.models.base.ResultTable`` 을 상속한 것이다. 두 종류가
    있고 둘 다 지원된다(app.scrapers.dynamic.DynamicApiScraper.run_and_save):

    - "generic" (result_json 필드가 있다, 예: ApiRst): 레코드 dict 를 통째로
      담는다. 소스별 코드가 필요 없다.
    - "typed" (result_json 이 없다, 예: KisFutoptChart): 레코드 dict 를 생성자
      인자로 펴므로 키가 실제 컬럼과 맞아야 한다(없는 키는 pydantic 의
      extra="ignore" 가 조용히 버린다).

    ``__tablename__`` 으로 자동 발견하므로, 새 결과 표를 더하는 일은 모델을
    쓰는 것 하나로 끝난다 -- 등록할 두 번째 자리가 없다.

    v1 은 "job_id 컬럼이 있으면 결과 표" 로 찾았다. 지금 데이터에서는 맞지만
    우연히 맞는 것이었다: api_job 은 job_id 로 키가 잡히고 api_job_log 는
    그것을 참조해서 둘 다 걸렸으므로, 부기 표 이름 넷을 하드코딩해 빼야 했다.
    그리고 job_id 가 없는 수집 상태 표(ksd_bond_no_detail)는 그 규칙으로
    분류할 자리가 아예 없었다.

    마커로 말하면 그 둘이 사라진다. ReferenceTable 과 StateTable 은 상속이
    다르므로 저절로 빠지고, 부기 표는 아무 마커도 달지 않는다 -- 그래서
    record_map_json 이 그런 표를 가리키면 run_and_save 가 "unknown target
    table" 로 크게 실패한다. 반쯤 동작하지 않는다."""
    registry: dict[str, type[SQLModel]] = {}
    for obj in vars(_models).values():
        if not (inspect.isclass(obj) and issubclass(obj, ResultTable)
                and obj is not ResultTable):
            continue
        table_name = getattr(obj, "__tablename__", None)
        if table_name:
            registry[table_name] = obj
    return registry


# table_name (as used in ApiMst.record_map_json) -> the SQLModel class it
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


def stage_file(group: str, filename: str, content: bytes) -> Path:
    """Write a downloaded document to the local staging area and return its
    path.

    ``group`` plays the part table_name plays for rows: it becomes the first
    segment of the object key, so related documents land together in the
    bucket the same way a table's Parquet files do. Written whole rather than
    appended -- unlike a CSV, a document has no partial state worth
    accumulating; a re-run replaces it."""
    path = _LOCAL_EXPORT_DIR / group / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def upload_file(path: Path, delete_local: bool = True) -> str:
    """Upload one staged document unchanged and remove the local copy.

    The object key mirrors the staged layout ({group}/{filename}), the same
    convention _write_parquet follows, so a bucket listing reads the same way
    for documents as for exported tables."""
    key = f"{path.parent.name}/{path.name}"
    get_object_storage_client().put_object(Bucket=bucket_name(), Key=key, Body=path.read_bytes())
    logger.info("uploaded {} bytes to oci://{}/{}", path.stat().st_size, bucket_name(), key)
    if delete_local:
        path.unlink()
    return key


# A job_id carries the trading day it covers for the tables that have one
# (KIS_FUTOPT_CHART_C01608C42_20260813_154500_60). Retention goes by that
# rather than by file age: a backfill writes decade-old data today, and
# treating those buffers as fresh would defeat the window entirely.
_DATE_IN_NAME_RE = re.compile(r"(?<!\d)(20\d{6})(?!\d)")


def _buffer_date(csv_path: Path) -> date:
    """Which day's data a buffer holds -- read from the job_id when it says,
    otherwise from the file's own age."""
    for token in _DATE_IN_NAME_RE.findall(csv_path.stem):
        try:
            return datetime.strptime(token, "%Y%m%d").date()
        except ValueError:
            continue
    return datetime.fromtimestamp(csv_path.stat().st_mtime).date()


def purge_csv_buffers(keep_from: date) -> tuple[int, int]:
    """Delete buffered CSVs holding data from before ``keep_from``.

    Returns (files removed, bytes freed). The caller decides the boundary --
    it is a market question (which maturity is still worth keeping a local
    trail for), not a filesystem one."""
    removed = freed = 0
    if not _LOCAL_EXPORT_DIR.exists():
        return removed, freed

    for group_dir in _LOCAL_EXPORT_DIR.iterdir():
        if not group_dir.is_dir():
            continue
        for csv_file in group_dir.glob("*.csv"):
            if _buffer_date(csv_file) >= keep_from:
                continue
            freed += csv_file.stat().st_size
            csv_file.unlink()
            removed += 1
    if removed:
        logger.info("purged {} buffered CSV(s) older than {}, freeing {:.1f} MB",
                    removed, keep_from, freed / 1024 / 1024)
    return removed, freed


def finalize_pending_exports() -> dict[str, str]:
    """Upload every staged document and clear it from the staging area.

    Reads the filesystem rather than the database: a document has no row to
    ask about, so what is on disk is the whole of what is pending. That also
    means one left behind by a crashed run is picked up on the next pass
    without any bookkeeping.

    Buffered CSVs are deliberately left where they are. They are a local
    record of what a run produced, not a source for the bucket -- Parquet is
    exported from the tables themselves now (see scripts/sql/
    sp_export_parquet.sql), so consuming the buffers here would delete a trail
    that nothing else keeps while adding nothing the export does not already
    cover.

    Meant to run as a late step of the day's batch, after the cycles that
    might have downloaded something.

    Returns {file_name: uploaded_object_key}."""
    results: dict[str, str] = {}
    if not _LOCAL_EXPORT_DIR.exists():
        return results

    for group_dir in sorted(_LOCAL_EXPORT_DIR.iterdir()):
        if not group_dir.is_dir():
            continue
        for staged in sorted(group_dir.iterdir()):
            if not staged.is_file() or staged.suffix == ".csv":
                continue
            key = upload_file(staged)
            results[staged.name] = key
            logger.info("finalize_pending_exports: {} -> {}", staged.name, key)

    return results
