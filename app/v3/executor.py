"""v3 Job Executor: runs ``api_job`` (API scrape) and ``etl_job`` (DuckDB ETL)
from ``DailyConfigSnapshot``.

Zero Neon DB connection during collection — reads config from the daily local
snapshot, expands ``api_job`` parameter axes (dates, literals, or DuckDB queries
over ``v3/ref/*.parquet``), cleanses + validates records, uploads ONE bundled
ZSTD Parquet per job under ``v3/raw_clean/``, executes ``etl_job`` SQL
post-processing, and logs all outcomes to ``LocalRunLedger`` (``job_log``).
"""

from __future__ import annotations

import calendar
import itertools
import re
import time
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from typing import Any

from app.errors import ConfigError, SourceRefused, classify
from app.object_storage import bucket_name
from app.v3.batch_cache import DailyConfigSnapshot, LocalRunLedger, RunLedgerEntry
from app.v3.cleanse import ContractResult, apply_cleanse, evaluate_contract
from app.v3.duckdb_runner import build_duckdb_config, register_s3_secrets, run_duckdb_job
from app.v3.http_fetcher import collect_http_records
from app.v3.parquet_sink import publish_parquet_batch
from app.v3.spec import ApiSpec, EtlJobSpec, ScrapeJobSpec

KST = timezone(timedelta(hours=9))
_CYCLE_RE = re.compile(r"^(\d+)([mhdw])(?:_\w+)?$")
_REF_PARQUET_TABLES = (
    "mst_etf",
    "mst_fuopt",
    "mst_bond",
    "mst_stock",
    "meta_maturity",
    "stock_index_his",
    "user_etf",
)


def now_kst() -> datetime:
    return datetime.now(KST)


def _add_months(d: date, months: int) -> date:
    idx = d.month - 1 + months
    y, m = d.year + idx // 12, idx % 12 + 1
    return date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def _last_bday(d: date) -> date:
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    return prev


def resolve_date_keyword(keyword: str, fmt: str | None = None, *, ref_date: date | None = None) -> str:
    today = ref_date or now_kst().date()
    table = {
        "today": (today, "%Y%m%d"),
        "yesterday": (today - timedelta(days=1), "%Y%m%d"),
        "last_month": (_add_months(today, -1), "%Y%m%d"),
        "last_eom": (date(today.year, today.month, 1) - timedelta(days=1), "%Y%m%d"),
        "last_yymm": (_add_months(today, -1), "%Y%m"),
        "last_bday": (_last_bday(today), "%Y%m%d"),
    }
    if keyword not in table:
        raise ConfigError(f"Unknown date keyword {keyword!r}")
    resolved, default_fmt = table[keyword]
    return resolved.strftime(fmt or default_fmt)


def resolve_snapshot_now(snapshot_unit: str | None = None, fmt: str = "%Y%m%d%H%M%S") -> str:
    dt = now_kst()
    if snapshot_unit:
        m = _CYCLE_RE.match(snapshot_unit.strip())
        if m:
            n, unit = int(m.group(1)), m.group(2)
            if unit == "m":
                dt = dt.replace(minute=dt.minute - (dt.minute % n), second=0, microsecond=0)
            elif unit == "h":
                dt = dt.replace(hour=dt.hour - (dt.hour % n), minute=0, second=0, microsecond=0)
            elif unit == "d":
                dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt.strftime(fmt)


def _run_duckdb_axis_query(query: str, oci_bucket: str) -> list[tuple[Any, ...]]:
    """Run a parameter axis SQL query via DuckDB against ``v3/ref/*.parquet``."""
    import duckdb

    sql = query.replace("${OCI_BUCKET}", oci_bucket)
    con = duckdb.connect(":memory:", config=build_duckdb_config(memory_limit="256MB", threads=2))
    try:
        register_s3_secrets(con, targets=["oci"])
        for name in _REF_PARQUET_TABLES:
            con.execute(
                f"CREATE OR REPLACE TEMP VIEW {name} AS "
                f"SELECT * FROM read_parquet('s3://{oci_bucket}/v3/ref/{name}.parquet')"
            )
        return con.execute(sql).fetchall()
    finally:
        con.close()


def expand_scrape_params(
    params_spec: dict[str, Any],
    *,
    ref_date: date | None = None,
    oci_bucket: str | None = None,
) -> list[dict[str, Any]]:
    """Expand ``ScrapeJobSpec.params`` into a list of concrete param dicts."""
    if not params_spec:
        return [{}]

    axes: list[list[dict[str, Any]]] = []
    for raw_key, spec in params_spec.items():
        keys = [k.strip() for k in raw_key.split(",")]
        if isinstance(spec, dict) and ("parquet" in spec or "db" in spec):
            q = spec.get("parquet") or spec["db"]
            bkt = oci_bucket or bucket_name("oci")
            rows = _run_duckdb_axis_query(q, bkt)
            if not rows:
                raise ConfigError(f"axis query for {raw_key!r} returned 0 rows")
            axes.append([dict(zip(keys, r)) for r in rows])
        elif isinstance(spec, dict) and "date" in spec:
            if len(keys) != 1:
                raise ConfigError(f"'date' spec requires single key, got {raw_key!r}")
            axes.append([{keys[0]: resolve_date_keyword(spec["date"], spec.get("format"), ref_date=ref_date)}])
        else:
            if len(keys) != 1:
                raise ConfigError(f"literal value requires single key, got {raw_key!r}")
            axes.append([{keys[0]: spec}])

    combos: list[dict[str, Any]] = []
    for parts in itertools.product(*axes):
        merged: dict[str, Any] = {}
        for part in parts:
            merged.update(part)
        combos.append(merged)
    return combos


def _partition_key(spec: ApiSpec, params: dict[str, Any]) -> str:
    keys = spec.engine.sink.job_key_params if spec.engine.sink else []
    if not keys:
        return "ALL"
    return "_".join(str(params.get(k, "")) for k in keys)


def _stamp_key_params(spec: ApiSpec, params: dict[str, Any], snapshot_unit: str | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    sink = spec.engine.sink
    if not sink:
        return out
    for param_key, col_name in sink.row_stamp.items():
        if param_key in params:
            out[col_name] = params[param_key]
    if sink.snapshot_column:
        out[sink.snapshot_column] = resolve_snapshot_now(snapshot_unit)
    return out


@dataclass(frozen=True)
class DryRunResult:
    api_id: str
    raw_count: int
    cleaned_count: int
    contract: ContractResult
    columns: list[str]
    sample_rows: list[dict[str, Any]]


def dry_run_api(
    snapshot: DailyConfigSnapshot,
    api_id: str,
    params: dict[str, Any] | None = None,
    *,
    sample_limit: int = 5,
) -> DryRunResult:
    """Fetch 1 request, cleanse, and evaluate contract WITHOUT uploading or logging."""
    api_row = snapshot.apis.get(api_id)
    if api_row is None:
        raise ConfigError(f"No api_mst row for {api_id!r} in local snapshot")
    if (api_row.get("protocol") or "HTTP").upper() != "HTTP":
        raise ConfigError(f"{api_id}: v3 dry-run currently supports protocol='HTTP' only")

    spec = ApiSpec.model_validate(api_row["spec_json"])
    raw = collect_http_records(api_id, spec, params or {}, host_policies=snapshot.host_policies)
    key_params = _stamp_key_params(spec, params or {}, snapshot_unit=None)
    cleaned = apply_cleanse(raw, spec.engine.cleanse, key_params=key_params, meta={"api_id": api_id})
    contract = evaluate_contract(cleaned, spec.engine.contract)
    cols = list(cleaned[0].keys()) if cleaned else []
    return DryRunResult(
        api_id=api_id,
        raw_count=len(raw),
        cleaned_count=len(cleaned),
        contract=contract,
        columns=cols,
        sample_rows=cleaned[:sample_limit],
    )


def execute_etl_job(
    snapshot: DailyConfigSnapshot,
    job_id: str,
    *,
    target_date: str | None = None,
    ledger: LocalRunLedger | None = None,
    skip_unconfigured_targets: bool = True,
) -> dict[str, Any]:
    """Execute one ``etl_job`` (DuckDB SQL -> Parquet -> S3) from the daily snapshot."""
    job_row = snapshot.etl_jobs.get(job_id)
    if job_row is None:
        raise ConfigError(f"No etl_job row for {job_id!r} in local snapshot")

    day = target_date or now_kst().strftime("%Y%m%d")
    run_ledger = ledger or LocalRunLedger(day)
    spec = EtlJobSpec.model_validate({
        "job_id": job_id,
        "sql_text": job_row["sql_text"],
        "output_layer": job_row.get("output_layer", "curated"),
        "output_rel_path": job_row["output_rel_path"],
        "targets": list(job_row.get("targets") or ["oci"]),
        "memory_limit": job_row.get("memory_limit", "1GB"),
        "threads": job_row.get("threads", 2),
        "min_rows": job_row.get("min_rows", 0),
    })
    started = time.monotonic()
    try:
        res = run_duckdb_job(
            spec,
            sql_params={
                "OCI_BUCKET": bucket_name("oci"),
                "TARGET_DATE": day,
                "TARGET_YM": day[:6],
            },
            skip_unconfigured_targets=skip_unconfigured_targets,
        )
        run_ledger.append(RunLedgerEntry(
            job_type="ETL",
            job_id=job_id,
            api_id=None,
            partition_key=day,
            target_date=day,
            status="SUCCESS",
            record_count=res.row_count,
            duration_ms=int((time.monotonic() - started) * 1000),
            output_uris=res.uploaded_uris,
        ))
        return {"job_id": job_id, "job_type": "ETL", "rows": res.row_count, "uris": res.uploaded_uris}
    except Exception as exc:
        err = classify(exc)
        run_ledger.append(RunLedgerEntry(
            job_type="ETL",
            job_id=job_id,
            api_id=None,
            partition_key=day,
            target_date=day,
            status=err.log_status,
            error_code=type(err).__name__,
            error_message=str(err)[:4000],
            duration_ms=int((time.monotonic() - started) * 1000),
        ))
        raise


def execute_api_job(
    snapshot: DailyConfigSnapshot,
    job_id: str,
    *,
    target_date: str | None = None,
    limit: int | None = None,
    ledger: LocalRunLedger | None = None,
    skip_unconfigured_targets: bool = True,
) -> dict[str, Any]:
    """Execute one ``api_job`` (HTTP scrape -> Parquet -> S3) from the daily snapshot."""
    job_row = snapshot.api_jobs.get(job_id)
    if job_row is None:
        raise ConfigError(f"No api_job row for {job_id!r} in local snapshot")

    day = target_date or now_kst().strftime("%Y%m%d")
    run_ledger = ledger or LocalRunLedger(day)

    api_id = job_row["api_id"]
    api_row = snapshot.apis.get(api_id)
    if api_row is None:
        raise ConfigError(f"{job_id}: references missing api_id={api_id!r}")
    if (api_row.get("protocol") or "HTTP").upper() != "HTTP":
        raise ConfigError(f"{job_id}: protocol={api_row.get('protocol')!r} not yet supported in v3 HTTP runner")

    api_spec = ApiSpec.model_validate(api_row["spec_json"])
    if api_spec.engine.sink is None:
        raise ConfigError(f"{api_id}: sink spec is required for api_job")

    scrape_spec = ScrapeJobSpec.model_validate({
        "params": job_row.get("params_json") or {},
        "save_mode": job_row.get("save_mode") or "overwrite",
    })
    ref_day = datetime.strptime(day, "%Y%m%d").date()
    param_sets = expand_scrape_params(scrape_spec.params, ref_date=ref_day)
    if limit is not None:
        param_sets = param_sets[:limit]

    already_done = run_ledger.succeeded_partitions(job_id, job_type="API")
    bundled_rows: list[dict[str, Any]] = []
    pending_success: list[RunLedgerEntry] = []
    succeeded = failed = skipped = 0

    for params in param_sets:
        pkey = _partition_key(api_spec, params)
        if not api_spec.engine.sink.is_repeated and (pkey, day) in already_done:
            skipped += 1
            continue
        started = time.monotonic()
        try:
            raw = collect_http_records(api_id, api_spec, params, host_policies=snapshot.host_policies)
            key_params = _stamp_key_params(api_spec, params, job_row.get("snapshot_unit"))
            cleaned = apply_cleanse(
                raw,
                api_spec.engine.cleanse,
                key_params=key_params,
                meta={"api_id": api_id, "job_id": job_id, "target_date": day},
            )
            contract = evaluate_contract(cleaned, api_spec.engine.contract)
            duration_ms = int((time.monotonic() - started) * 1000)

            if contract.action == "fail":
                raise ConfigError(f"{api_id}: contract failed: {'; '.join(contract.violations)}")
            if contract.action in ("pending", "keep_previous"):
                run_ledger.append(RunLedgerEntry(
                    job_type="API",
                    job_id=job_id,
                    api_id=api_id,
                    partition_key=pkey,
                    target_date=day,
                    status="EMPTY",
                    record_count=0,
                    duration_ms=duration_ms,
                ))
                continue

            bundled_rows.extend(cleaned)
            succeeded += 1
            pending_success.append(RunLedgerEntry(
                job_type="API",
                job_id=job_id,
                api_id=api_id,
                partition_key=pkey,
                target_date=day,
                status="SUCCESS",
                record_count=len(cleaned),
                duration_ms=duration_ms,
            ))
        except SourceRefused as exc:
            run_ledger.append(RunLedgerEntry(
                job_type="API",
                job_id=job_id,
                api_id=api_id,
                partition_key=pkey,
                target_date=day,
                status="BLOCKED",
                error_code="SourceRefused",
                error_message=str(exc)[:4000],
                duration_ms=int((time.monotonic() - started) * 1000),
            ))
            raise
        except Exception as exc:
            err = classify(exc)
            failed += 1
            run_ledger.append(RunLedgerEntry(
                job_type="API",
                job_id=job_id,
                api_id=api_id,
                partition_key=pkey,
                target_date=day,
                status=err.log_status,
                error_code=type(err).__name__,
                error_message=str(err)[:4000],
                duration_ms=int((time.monotonic() - started) * 1000),
            ))

    uploaded_uris: dict[str, str] = {}
    if bundled_rows:
        sink = api_spec.engine.sink
        batch_name = f"{job_id.lower()}_{now_kst().strftime('%H%M%S')}"
        try:
            res = publish_parquet_batch(
                bundled_rows,
                target_name=sink.target_name,
                target_date=day,
                layer=sink.layer,
                batch_name=batch_name,
                targets=sink.targets,
                prefix=sink.prefix,
                skip_unconfigured_targets=skip_unconfigured_targets,
            )
            uploaded_uris = res.uploaded_uris
        except Exception as exc:
            err = classify(exc)
            for entry in pending_success:
                run_ledger.append(RunLedgerEntry(
                    job_type="API",
                    job_id=entry.job_id,
                    api_id=entry.api_id,
                    partition_key=entry.partition_key,
                    target_date=entry.target_date,
                    status=err.log_status,
                    error_code=type(err).__name__,
                    error_message=f"publish failed: {err}"[:4000],
                    record_count=entry.record_count,
                    duration_ms=entry.duration_ms,
                ))
            raise
        for entry in pending_success:
            run_ledger.append(replace(entry, output_uris=dict(uploaded_uris)))

    return {
        "job_id": job_id,
        "job_type": "API",
        "api_id": api_id,
        "partitions_succeeded": succeeded,
        "partitions_failed": failed,
        "partitions_skipped": skipped,
        "rows": len(bundled_rows),
        "uris": uploaded_uris,
    }


def execute_job(
    snapshot: DailyConfigSnapshot,
    job_id: str,
    *,
    job_type: str | None = None,
    target_date: str | None = None,
    limit: int | None = None,
    ledger: LocalRunLedger | None = None,
    skip_unconfigured_targets: bool = True,
) -> dict[str, Any]:
    """Dispatch one job by ``job_id`` across ``api_job`` and ``etl_job``."""
    kind = job_type.upper() if job_type else None
    if kind == "API":
        return execute_api_job(
            snapshot, job_id, target_date=target_date, limit=limit,
            ledger=ledger, skip_unconfigured_targets=skip_unconfigured_targets,
        )
    if kind == "ETL":
        return execute_etl_job(
            snapshot, job_id, target_date=target_date,
            ledger=ledger, skip_unconfigured_targets=skip_unconfigured_targets,
        )
    if kind is not None:
        raise ConfigError(f"Unknown job_type={job_type!r}; expected 'API' or 'ETL'")

    in_api = job_id in snapshot.api_jobs
    in_etl = job_id in snapshot.etl_jobs
    if in_api and in_etl:
        raise ConfigError(f"job_id {job_id!r} exists in both api_job and etl_job; pass job_type='API' or 'ETL'")
    if in_api:
        return execute_api_job(
            snapshot, job_id, target_date=target_date, limit=limit,
            ledger=ledger, skip_unconfigured_targets=skip_unconfigured_targets,
        )
    if in_etl:
        return execute_etl_job(
            snapshot, job_id, target_date=target_date,
            ledger=ledger, skip_unconfigured_targets=skip_unconfigured_targets,
        )
    raise ConfigError(f"No api_job or etl_job row for {job_id!r} in local snapshot")


def execute_schedule_cycle(
    snapshot: DailyConfigSnapshot,
    schedule_tag: str,
    *,
    target_date: str | None = None,
    ledger: LocalRunLedger | None = None,
) -> list[dict[str, Any]]:
    """Run all active ``api_job`` + ``etl_job`` rows for ``schedule_tag`` ordered by ``run_order``."""
    items: list[tuple[int, int, str, str]] = []
    for row in snapshot.api_jobs.values():
        if row.get("is_active") and row.get("schedule_tag") == schedule_tag:
            items.append((int(row.get("run_order", 100)), 0, row["job_id"], "API"))
    for row in snapshot.etl_jobs.values():
        if row.get("is_active") and row.get("schedule_tag") == schedule_tag:
            items.append((int(row.get("run_order", 200)), 1, row["job_id"], "ETL"))
    items.sort()

    results: list[dict[str, Any]] = []
    for _, _, jid, jtype in items:
        results.append(execute_job(snapshot, jid, job_type=jtype, target_date=target_date, ledger=ledger))
    return results
