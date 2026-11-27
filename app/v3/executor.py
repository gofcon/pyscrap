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

from app.errors import ConfigError, SessionExpired, SourceRefused, classify
from app.object_storage import bucket_name
from app.v3.batch_cache import DailyConfigSnapshot, LocalRunLedger, RunLedgerEntry
from app.v3.binary_sink import publish_binary_payload
from app.v3.cleanse import ContractResult, apply_cleanse, evaluate_contract
from app.v3.duckdb_runner import build_duckdb_config, register_s3_secrets, run_duckdb_job
from app.v3.http_fetcher import (
    collect_http_bundle,
    collect_http_records,
    fetch_http_binary,
    http_state_key,
    run_http_session_login,
)
from app.v3.parquet_sink import publish_parquet_batch
from app.v3.session_store import clear_session_state, save_session_tokens
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
    "v_k2i_atm",
    "user_etf",
    "krx_index_mst",
    "mst_index",
    "meta_fuopt_info",
    "ksd_bond_no_detail",
    "src_code",
)
# Raw-clean tables referenced by axis queries (e.g. NOT EXISTS checks).
# When no files exist yet in v3/raw_clean/<name>/, bind an empty typed fallback view.
_RAW_CLEAN_AXIS_FALLBACKS: dict[str, str] = {
    "ksd_bond_info": "SELECT CAST(NULL AS VARCHAR) AS isin WHERE FALSE",
    "isin_bond_info": "SELECT CAST(NULL AS VARCHAR) AS isin WHERE FALSE",
    "ksd_kacd_code": "SELECT CAST(NULL AS VARCHAR) AS codevalue, CAST(NULL AS INTEGER) AS code_depth WHERE FALSE",
}


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


def _run_duckdb_axis_query(
    query: str,
    oci_bucket: str,
    *,
    target_date: str | None = None,
) -> list[tuple[Any, ...]]:
    """Run a parameter axis SQL query via DuckDB against ``v3/snapshot/*.parquet``
    (and optional ``v3/raw_clean/<tbl>/**/*.parquet`` with empty-table fallback)."""
    import duckdb

    day = target_date or now_kst().strftime("%Y%m%d")
    sql = (
        query.replace("${OCI_BUCKET}", oci_bucket)
        .replace("${TARGET_DATE}", day)
        .replace("${TARGET_YM}", day[:6])
    )
    con = duckdb.connect(":memory:", config=build_duckdb_config(memory_limit="256MB", threads=2))
    try:
        register_s3_secrets(con, targets=["oci"])
        sql_lower = sql.lower()
        for name in _REF_PARQUET_TABLES:
            if name in sql_lower:
                con.execute(
                    f"CREATE OR REPLACE TEMP VIEW {name} AS "
                    f"SELECT * FROM read_parquet('s3://{oci_bucket}/v3/snapshot/{name}.parquet')"
                )
        for name, fallback_sql in _RAW_CLEAN_AXIS_FALLBACKS.items():
            if name in sql_lower:
                try:
                    con.execute(
                        f"CREATE OR REPLACE TEMP VIEW {name} AS "
                        f"SELECT * FROM read_parquet("
                        f"'s3://{oci_bucket}/v3/raw_clean/{name}/**/*.parquet', union_by_name=true)"
                    )
                except Exception:
                    con.execute(f"CREATE OR REPLACE TEMP VIEW {name} AS {fallback_sql}")
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

    day_str = (ref_date or now_kst().date()).strftime("%Y%m%d")
    axes: list[list[dict[str, Any]]] = []
    for raw_key, spec in params_spec.items():
        keys = [k.strip() for k in raw_key.split(",")]
        if isinstance(spec, dict) and ("parquet" in spec or "db" in spec):
            q = spec.get("parquet") or spec["db"]
            bkt = oci_bucket or bucket_name("oci")
            rows = _run_duckdb_axis_query(q, bkt, target_date=day_str)
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


def _run_session_login(
    snapshot: DailyConfigSnapshot,
    login_api_id: str,
    params: dict[str, Any] | None = None,
) -> None:
    """Execute a ``sink_type='SESSION'`` login API (HTTP or BROWSER) and refresh cookies."""
    login_row = snapshot.apis.get(login_api_id)
    if login_row is None:
        raise ConfigError(f"session.login references missing api_id={login_api_id!r}")
    login_spec = ApiSpec.model_validate(login_row["spec_json"])
    proto = (login_row.get("protocol") or "HTTP").upper()
    clear_session_state(http_state_key(login_api_id, login_spec))
    if proto == "BROWSER":
        from app.v3.browser_runner import collect_browser_bundle

        collect_browser_bundle(
            login_api_id, login_spec, params or {}, host_policies=snapshot.host_policies
        )
    else:
        run_http_session_login(
            login_api_id, login_spec, params or {}, host_policies=snapshot.host_policies
        )


def _fetch_partition_data(
    snapshot: DailyConfigSnapshot,
    api_id: str,
    protocol: str,
    sink_type: str,
    api_spec: ApiSpec,
    params: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]], list[tuple[str, bytes]]]:
    """Fetch one partition (HTTP or BROWSER, PARQUET or BINARY) with 1 auto re-login retry."""
    def _once():
        if protocol == "BROWSER":
            from app.v3.browser_runner import collect_browser_bundle

            return collect_browser_bundle(
                api_id, api_spec, params, host_policies=snapshot.host_policies
            )
        if sink_type == "BINARY":
            fname, blob = fetch_http_binary(
                api_id, api_spec, params, host_policies=snapshot.host_policies
            )
            return [], {}, [(fname, blob)]
        if api_spec.engine.extract.extra_selectors:
            primary, extras = collect_http_bundle(
                api_id, api_spec, params, host_policies=snapshot.host_policies
            )
            return primary, extras, []
        primary = collect_http_records(
            api_id, api_spec, params, host_policies=snapshot.host_policies
        )
        return primary, {}, []

    try:
        return _once()
    except SessionExpired:
        login_id = api_spec.engine.session.login
        if not login_id:
            raise
        _run_session_login(snapshot, login_id)
        return _once()


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
    proto = (api_row.get("protocol") or "HTTP").upper()
    sink_type = (api_row.get("sink_type") or "PARQUET").upper()
    if proto not in ("HTTP", "BROWSER"):
        raise ConfigError(f"{api_id}: unsupported protocol={proto!r}")

    spec = ApiSpec.model_validate(api_row["spec_json"])
    if sink_type == "SESSION":
        _run_session_login(snapshot, api_id, params or {})
        return DryRunResult(
            api_id=api_id,
            raw_count=0,
            cleaned_count=0,
            contract=ContractResult(ok=True, action="commit"),
            columns=[],
            sample_rows=[],
        )

    raw, _, downloads = _fetch_partition_data(snapshot, api_id, proto, sink_type, spec, params or {})
    if sink_type == "BINARY":
        meta_rows = [{"file_name": fn, "size_bytes": len(blob)} for fn, blob in downloads]
        return DryRunResult(
            api_id=api_id,
            raw_count=len(meta_rows),
            cleaned_count=len(meta_rows),
            contract=ContractResult(ok=bool(meta_rows), action="commit" if meta_rows else "fail"),
            columns=["file_name", "size_bytes"],
            sample_rows=meta_rows[:sample_limit],
        )

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
        "output_layer": job_row.get("output_layer", "daily"),
        "target_name": job_row["target_name"],
        "targets": list(job_row.get("targets") or ["oci"]),
        "memory_limit": job_row.get("memory_limit", "1GB"),
        "threads": job_row.get("threads", 2),
        "min_rows": job_row.get("min_rows", 0),
    })
    started = time.monotonic()
    try:
        if spec.sql_text.strip().lower().startswith("-- builtin:harvest_src_code"):
            from app.v3.code_harvester import harvest_src_codes

            res = harvest_src_codes(
                targets=spec.targets,
                memory_limit=spec.memory_limit,
                threads=spec.threads,
                skip_unconfigured_targets=skip_unconfigured_targets,
            )
        else:
            res = run_duckdb_job(
                spec,
                sql_params={
                    "OCI_BUCKET": bucket_name("oci"),
                    "TARGET_DATE": day,
                    "TARGET_YM": day[:6],
                    "LEDGER_DIR": str(run_ledger.base_dir),
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
        return {
            "job_id": job_id,
            "job_type": "ETL",
            "rows": res.row_count,
            "uris": res.uploaded_uris,
        }
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
    protocol = (api_row.get("protocol") or "HTTP").upper()
    sink_type = (api_row.get("sink_type") or "PARQUET").upper()
    if protocol not in ("HTTP", "BROWSER"):
        raise ConfigError(f"{job_id}: unsupported protocol={protocol!r}")

    api_spec = ApiSpec.model_validate(api_row["spec_json"])
    if sink_type == "SESSION":
        started = time.monotonic()
        try:
            _run_session_login(snapshot, api_id)
            run_ledger.append(RunLedgerEntry(
                job_type="API",
                job_id=job_id,
                api_id=api_id,
                partition_key="SESSION",
                target_date=day,
                status="SUCCESS",
                record_count=0,
                duration_ms=int((time.monotonic() - started) * 1000),
            ))
            return {
                "job_id": job_id,
                "job_type": "API",
                "api_id": api_id,
                "sink_type": "SESSION",
                "partitions_succeeded": 1,
                "partitions_failed": 0,
                "partitions_skipped": 0,
                "rows": 0,
                "uris": {},
            }
        except Exception as exc:
            err = classify(exc)
            run_ledger.append(RunLedgerEntry(
                job_type="API",
                job_id=job_id,
                api_id=api_id,
                partition_key="SESSION",
                target_date=day,
                status=err.log_status,
                error_code=type(err).__name__,
                error_message=str(err)[:4000],
                duration_ms=int((time.monotonic() - started) * 1000),
            ))
            raise

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
    extra_bundled: dict[str, list[dict[str, Any]]] = {}
    pending_success: list[RunLedgerEntry] = []
    persisted_tokens: list[str] = []
    binary_uris: dict[str, str] = {}
    succeeded = failed = skipped = 0
    persist_map = api_spec.engine.extract.persist_env or {}

    for params in param_sets:
        pkey = _partition_key(api_spec, params)
        if not api_spec.engine.sink.is_repeated and (pkey, day) in already_done:
            skipped += 1
            continue
        started = time.monotonic()
        try:
            raw, extra_raw, downloads = _fetch_partition_data(
                snapshot, api_id, protocol, sink_type, api_spec, params
            )

            if sink_type == "BINARY":
                sink = api_spec.engine.sink
                bin_records: list[dict[str, Any]] = []
                last_bin_uris: dict[str, str] = {}
                for fname, blob in downloads:
                    recs, uris = publish_binary_payload(
                        blob,
                        staging=sink.staging or {},
                        params=params,
                        fallback_name=fname,
                        targets=sink.targets,
                        prefix=sink.prefix,
                        skip_unconfigured_targets=skip_unconfigured_targets,
                    )
                    bin_records.extend(recs)
                    last_bin_uris = uris
                duration_ms = int((time.monotonic() - started) * 1000)
                if not bin_records:
                    raise ConfigError(f"{api_id}: binary download produced 0 files")
                key_params = _stamp_key_params(api_spec, params, job_row.get("snapshot_unit"))
                meta = {"api_id": api_id, "job_id": job_id, "target_date": day}
                for rec in bin_records:
                    rec.update(key_params)
                    rec.update(meta)
                binary_uris = last_bin_uris
                succeeded += 1
                run_ledger.append(RunLedgerEntry(
                    job_type="API",
                    job_id=job_id,
                    api_id=api_id,
                    partition_key=pkey,
                    target_date=day,
                    status="SUCCESS",
                    record_count=len(bin_records),
                    duration_ms=duration_ms,
                    output_uris=last_bin_uris,
                ))
                bundled_rows.extend(bin_records)
                continue

            if persist_map and raw:
                token_updates = {
                    env_key: raw[0].get(field_name)
                    for field_name, env_key in persist_map.items()
                    if raw[0].get(field_name) is not None
                }
                if token_updates:
                    written = save_session_tokens(token_updates)
                    persisted_tokens.extend(sorted(written.keys()))

            key_params = _stamp_key_params(api_spec, params, job_row.get("snapshot_unit"))
            meta = {"api_id": api_id, "job_id": job_id, "target_date": day}
            cleaned = apply_cleanse(
                raw,
                api_spec.engine.cleanse,
                key_params=key_params,
                meta=meta,
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
            for extra_target, e_rows in extra_raw.items():
                if e_rows:
                    e_cleaned = apply_cleanse(e_rows, api_spec.engine.cleanse, key_params=key_params, meta=meta)
                    extra_bundled.setdefault(extra_target, []).extend(e_cleaned)
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

    uploaded_uris: dict[str, str] = dict(binary_uris)
    extra_uris: dict[str, dict[str, str]] = {}
    if bundled_rows and sink_type != "BINARY":
        sink = api_spec.engine.sink
        if not sink.is_repeated and scrape_spec.save_mode == "overwrite":
            # Deterministic filename so re-running the same day replaces in-place
            # instead of leaving duplicate batch files under dt=YYYYMMDD/.
            batch_name = job_id.lower()
        else:
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
            for extra_target, e_rows in extra_bundled.items():
                if not e_rows:
                    continue
                e_res = publish_parquet_batch(
                    e_rows,
                    target_name=extra_target,
                    target_date=day,
                    layer=sink.layer,
                    batch_name=batch_name,
                    targets=sink.targets,
                    prefix=sink.prefix,
                    skip_unconfigured_targets=skip_unconfigured_targets,
                )
                extra_uris[extra_target] = e_res.uploaded_uris
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

    result: dict[str, Any] = {
        "job_id": job_id,
        "job_type": "API",
        "api_id": api_id,
        "partitions_succeeded": succeeded,
        "partitions_failed": failed,
        "partitions_skipped": skipped,
        "rows": len(bundled_rows),
        "uris": uploaded_uris,
    }
    if extra_uris:
        result["extra_uris"] = extra_uris
    if persisted_tokens:
        result["persisted_tokens"] = sorted(set(persisted_tokens))
    return result


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
