"""Migrate existing Oracle v2 config + manual reference tables to Neon v3
(and optionally export derived financial masters to OCI bucket ``v3/ref/*.parquet``).

Read-only against Oracle v2 (never mutates Oracle). Default mode is dry-run:
validates every transformed ``api_mst`` row through ``app.v3.spec.ApiSpec`` and
prints exact counts before touching Neon or S3.

Usage::

    python scripts/migrate_v2_to_v3.py                        # dry-run (validate only)
    python scripts/migrate_v2_to_v3.py --apply                # upsert into Neon DB
    python scripts/migrate_v2_to_v3.py --export-ref-parquet   # export mst_* -> v3/ref/*.parquet
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.v3.spec import ApiSpec, ScrapeJobSpec

# Migrated from app.scrapers.dynamic._HOST_INTERVALS into Neon host_policy
DEFAULT_HOST_POLICIES: list[tuple[str, float, int, str]] = [
    ("seibro.or.kr", 0.5, 600, "KSD SEIBRO 웹화면 (2 req/s)"),
    ("isin.krx.co.kr", 0.5, 600, "KRX 표준코드 화면"),
    ("data-dbg.krx.co.kr", 0.3, 600, "KRX 오픈API (일 10,000건 한도 보호)"),
    ("data.krx.co.kr", 1.0, 600, "KRX 데이터마켓 (1분 115건 차단 실측 반영)"),
    ("samsungfund.com", 0.5, 600, "삼성자산운용 KODEX"),
    ("aceetf.co.kr", 0.5, 600, "한국투자신탁운용 ACE"),
    ("soletf.co.kr", 0.5, 600, "신한자산운용 SOL"),
    ("plusetf.co.kr", 0.5, 600, "한화자산운용 PLUS"),
    ("riseetf.co.kr", 0.5, 600, "KB자산운용 RISE"),
    ("hanaroetf.com", 0.5, 600, "NH-Amundi HANARO"),
    ("kiwoometf.com", 0.5, 600, "키움자산운용"),
    ("timeetf.co.kr", 0.5, 600, "타임폴리오자산운용"),
    ("investments.miraeasset.com", 0.5, 600, "미래에셋자산운용 TIGER"),
]

# Tables whose v2 ORM inherited MdcRecordBase (comma numbers & '-' -> NULL)
_MDC_TABLES = frozenset({
    "krx_index_daily", "krx_etf_list", "krx_etf_pdf", "krx_deriv_expyy",
})

# Tables/APIs whose v2 ORM used EtfPdfDateMixin (std_dt digits only)
_ETF_PDF_DATE_APIS = frozenset({
    "ACE_ETF_PDF", "TIGER_ETF_PDF", "KB_ETF_PDF", "NH_ETF_PDF",
    "KIWOOM_ETF_PDF", "TIME_ETF_PDF", "SOL_ETF_PDF", "PLUS_ETF_PDF",
})

# Hand-maintained tables copied 1:1 from Oracle v2 -> Neon public schema
MANUAL_TABLES: dict[str, tuple[list[str], list[str]]] = {
    "user_etf": (
        ["isu_cd", "amc", "amc_etf_cd", "description", "updated_at"],
        ["isu_cd"],
    ),
    "mst_code": (
        ["code_group", "code", "code_nm", "parent_code", "sort_order", "description", "updated_at"],
        ["code_group", "code"],
    ),
    "user_code_map": (
        ["table_name", "column_name", "code_value", "code_group", "code", "note", "updated_at"],
        ["table_name", "column_name", "code_value"],
    ),
    "krx_index_mst": (
        [
            "ind_tp_cd", "idx_ind_cd", "idx_nm", "idx_eng_nm", "mid_clss_cd",
            "bas_tm", "annc_tm", "bas_idx", "calc_cycle", "calc_tm",
            "compst_isu_cnt", "mv_id", "description", "updated_at",
        ],
        ["ind_tp_cd", "idx_ind_cd"],
    ),
    "mst_index": (
        [
            "mv_id", "idx_nm", "idx_eng_nm", "bas_tm", "annc_tm",
            "bas_idx", "kis_short_cd", "description", "updated_at",
        ],
        ["mv_id"],
    ),
    "meta_fuopt_info": (
        [
            "prod_type", "ul_code", "ul_nm", "krx_idx_nm", "mv_id",
            "cont_mult", "code_key", "exp_dow", "exp_week",
            "krx_prod_ids", "kis_info_types", "description", "updated_at",
        ],
        ["prod_type", "ul_code"],
    ),
}

# Derived financial masters & auxiliary state exported from Oracle v2 -> OCI bucket v3/ref/*.parquet
DERIVED_REF_TABLES: list[str] = [
    "mst_etf",
    "mst_fuopt",
    "mst_bond",
    "mst_stock",
    "meta_maturity",
    "stock_index_his",
    "user_etf",
    "krx_index_mst",
    "mst_index",
    "meta_fuopt_info",
    "ksd_bond_no_detail",
    "src_code",
]


def _load_dotenv_file(path: Path = Path(".env")) -> None:
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


def _as_json(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return val
    if hasattr(val, "read"):
        val = val.read()
    if isinstance(val, (bytes, bytearray)):
        val = val.decode("utf-8")
    if isinstance(val, str):
        return json.loads(val)
    return val


def _as_str(val: Any) -> str | None:
    if val is None:
        return None
    if hasattr(val, "read"):
        val = val.read()
    return str(val)


def convert_api_mst_row(row: dict[str, Any]) -> dict[str, Any]:
    """Convert one Oracle v2 ``api_mst`` dict into a validated Neon v3 row."""
    api_id = str(row["api_id"])
    protocol = (row.get("protocol") or "HTTP").upper()
    response_type = (row.get("response_type") or "auto").lower()
    record_map = _as_json(row.get("record_map_json")) or {}
    options = _as_json(row.get("options_json")) or {}
    parse_json = _as_json(row.get("parse_json")) or {}
    session_json = _as_json(row.get("session_json")) or {}
    staging_json = _as_json(row.get("staging_json")) or {}
    merge_fields = _as_json(row.get("merge_fields_json")) or {}
    persist_env = _as_json(row.get("persist_env_json")) or {}
    pagination = _as_json(row.get("pagination_json"))
    job_key_params = _as_json(row.get("job_key_params")) or []
    row_stamp = _as_json(row.get("row_stamp_json")) or {}
    is_repeated = bool(row.get("is_repeated"))
    snapshot_col = row.get("snapshot_column")

    # Determine sink_type & primary selector/target
    if response_type == "session":
        sink_type = "SESSION"
    elif response_type == "binary":
        sink_type = "BINARY"
    else:
        sink_type = "PARQUET"

    primary_selector = next(iter(record_map.keys()), ".")
    primary_table = next(iter(record_map.values()), api_id.lower())
    target_name = api_id.lower() if primary_table == "api_rst" else str(primary_table)

    # Derive v3 mechanical cleanse rules from v2 model conventions
    wildcard_filters: list[str] = ["strip", "blank_to_null"]
    if primary_table in _MDC_TABLES:
        wildcard_filters.extend(["dash_to_null", "strip_comma"])
    rules: dict[str, list[str]] = {"*": wildcard_filters}
    if api_id in _ETF_PDF_DATE_APIS:
        rules["std_dt"] = ["digits_only"]

    # Map v2 empty options -> v3 ContractSpec.empty_policy
    if options.get("empty_keeps_previous"):
        empty_policy = "keep_previous"
    elif options.get("empty_is_pending"):
        empty_policy = "pending"
    else:
        empty_policy = "succeed"

    # Extract merge_fields for primary selector (if dict-of-dicts)
    flat_merge: dict[str, str] = {}
    if isinstance(merge_fields, dict):
        sub = merge_fields.get(primary_selector)
        if isinstance(sub, dict):
            flat_merge = {str(k): str(v) for k, v in sub.items()}

    engine_dict: dict[str, Any] = {
        "request": {
            "method": row.get("method"),
            "url": row["api_url"],
            "headers": _as_json(row.get("header_json")) or {},
            "payload_type": row.get("payload_type"),
            "payload_json": _as_json(row.get("payload_json")),
            "payload_xml": _as_str(row.get("payload_xml")),
            "behavior": _as_json(row.get("behavior_json")),
        },
        "extract": {
            "response_type": response_type if response_type != "" else "auto",
            "selector": primary_selector,
            "parse": parse_json,
            "merge_fields": flat_merge,
            "persist_env": persist_env,
        },
        "cleanse": {
            "lowercase_keys": True,
            "rules": rules,
        },
        "contract": {
            "empty_policy": empty_policy,
        },
        "session": {k: v for k, v in session_json.items()
                    if k in ("login", "logged_out", "blocked", "expect", "state")},
        "pacing": {
            k: float(options[k])
            for k in ("min_interval_sec", "page_delay_sec", "timeout_sec")
            if options.get(k) is not None
        },
    }

    if pagination:
        engine_dict["pagination"] = pagination

    if sink_type != "SESSION":
        engine_dict["sink"] = {
            "target_name": target_name,
            "layer": "binary" if sink_type == "BINARY" else "raw_clean",
            "prefix": "v3",
            "targets": ["oci"],
            "mode": "append_snapshot" if is_repeated and snapshot_col else "replace_partition",
            "row_stamp": row_stamp,
            "job_key_params": job_key_params,
            "is_repeated": is_repeated,
            "snapshot_column": snapshot_col,
            "staging": staging_json,
        }

    ext_dict: dict[str, Any] = {
        "migrated_from": "v2",
        "v2_record_map": record_map,
    }
    if len(record_map) > 1:
        ext_dict["multi_selector_map"] = record_map

    validated = ApiSpec.model_validate({"engine": engine_dict, "ext": ext_dict})

    return {
        "api_id": api_id,
        "api_name": row["api_name"],
        "source_name": row["source_name"],
        "domain": row.get("domain"),
        "protocol": protocol,
        "sink_type": sink_type,
        "status": "ACTIVE",
        "schema_version": 1,
        "spec_json": validated.model_dump(mode="json", by_alias=True, exclude_none=True),
        "note": row.get("note"),
    }


def convert_builder_row(row: dict[str, Any]) -> dict[str, Any]:
    """Convert one Oracle v2 ``api_job_builder`` row into a Neon v3 ``api_job``."""
    params = _as_json(row.get("cartesian_param_json")) or {}
    save_mode = row.get("save_mode") or "overwrite"
    spec = ScrapeJobSpec.model_validate({"params": params, "save_mode": save_mode})
    return {
        "job_id": str(row["build_id"]),
        "api_id": str(row["api_id"]),
        "schedule_tag": row.get("schedule_tag") or "daily",
        "run_order": 100,
        "is_active": bool(row.get("is_active")),
        "snapshot_unit": row.get("snapshot_unit"),
        "save_mode": spec.save_mode,
        "params_json": spec.params,
        "note": row.get("note"),
    }


def _connect_oracle():
    import oracledb

    kwargs: dict[str, Any] = {
        "user": os.environ["ORACLE_USER"],
        "password": os.environ["ORACLE_PASSWORD"],
        "dsn": os.environ["ORACLE_DSN"],
    }
    wallet_dir = os.environ.get("ORACLE_WALLET_DIR")
    if wallet_dir:
        kwargs["config_dir"] = wallet_dir
        kwargs["wallet_location"] = wallet_dir
        kwargs["wallet_password"] = os.environ.get("ORACLE_WALLET_PASSWORD")
    return oracledb.connect(**kwargs)


def _fetch_oracle_dicts(conn, sql: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _upsert_neon(neon_dsn: str, apis: list[dict], jobs: list[dict], manual_data: dict[str, list[dict]]) -> None:
    import psycopg

    with psycopg.connect(neon_dsn, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            # 1. api_mst
            cur.executemany(
                """
                INSERT INTO api_mst (
                    api_id, api_name, source_name, domain, protocol,
                    sink_type, status, schema_version, spec_json, note
                ) VALUES (
                    %(api_id)s, %(api_name)s, %(source_name)s, %(domain)s, %(protocol)s,
                    %(sink_type)s, %(status)s, %(schema_version)s, %(spec_json)s::jsonb, %(note)s
                )
                ON CONFLICT (api_id) DO UPDATE SET
                    api_name = EXCLUDED.api_name,
                    source_name = EXCLUDED.source_name,
                    domain = EXCLUDED.domain,
                    protocol = EXCLUDED.protocol,
                    sink_type = EXCLUDED.sink_type,
                    spec_json = EXCLUDED.spec_json,
                    note = EXCLUDED.note,
                    updated_at = now()
                """,
                [{**r, "spec_json": json.dumps(r["spec_json"], ensure_ascii=False)} for r in apis],
            )
            # 2. api_job
            cur.executemany(
                """
                INSERT INTO api_job (
                    job_id, api_id, schedule_tag, run_order,
                    is_active, snapshot_unit, save_mode, params_json, note
                ) VALUES (
                    %(job_id)s, %(api_id)s, %(schedule_tag)s, %(run_order)s,
                    %(is_active)s, %(snapshot_unit)s, %(save_mode)s, %(params_json)s::jsonb, %(note)s
                )
                ON CONFLICT (job_id) DO UPDATE SET
                    api_id = EXCLUDED.api_id,
                    schedule_tag = EXCLUDED.schedule_tag,
                    is_active = EXCLUDED.is_active,
                    snapshot_unit = EXCLUDED.snapshot_unit,
                    save_mode = EXCLUDED.save_mode,
                    params_json = EXCLUDED.params_json,
                    note = EXCLUDED.note,
                    updated_at = now()
                """,
                [{**r, "params_json": json.dumps(r["params_json"], ensure_ascii=False)} for r in jobs],
            )
            # 3. host_policy
            cur.executemany(
                """
                INSERT INTO host_policy (host_suffix, min_interval_sec, cooldown_sec, note)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (host_suffix) DO UPDATE SET
                    min_interval_sec = EXCLUDED.min_interval_sec,
                    cooldown_sec = EXCLUDED.cooldown_sec,
                    note = EXCLUDED.note,
                    updated_at = now()
                """,
                DEFAULT_HOST_POLICIES,
            )
            # 4. manual ref tables
            for table, (cols, pk_cols) in MANUAL_TABLES.items():
                rows = manual_data.get(table, [])
                if not rows:
                    continue
                col_list = ", ".join(cols)
                val_list = ", ".join(f"%({c})s" for c in cols)
                pk_list = ", ".join(pk_cols)
                upd_cols = [c for c in cols if c not in pk_cols]
                upd_list = ", ".join(f"{c} = EXCLUDED.{c}" for c in upd_cols)
                sql = (
                    f"INSERT INTO {table} ({col_list}) VALUES ({val_list}) "
                    f"ON CONFLICT ({pk_list}) DO UPDATE SET {upd_list}"
                )
                cur.executemany(sql, rows)
        conn.commit()


def _export_ref_parquets(ora_conn, targets: tuple[str, ...] = ("oci",)) -> None:
    from app.v3.parquet_sink import publish_parquet_batch

    for table in DERIVED_REF_TABLES:
        try:
            rows = _fetch_oracle_dicts(ora_conn, f"SELECT * FROM {table}")
        except Exception as exc:
            print(f"  [SKIP] {table}: {exc}")
            continue
        if not rows:
            print(f"  [EMPTY] {table}: 0 rows, skipped")
            continue
        res = publish_parquet_batch(
            rows,
            target_name=table,
            target_date="",
            layer="snapshot",
            targets=targets,
        )
        print(f"  [EXPORTED] {table}: {res.row_count:,} rows -> {res.uploaded_uris}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate Oracle v2 config & manual data to Neon v3")
    parser.add_argument("--apply", action="store_true", help="Write validated rows into Neon DB")
    parser.add_argument(
        "--export-ref-parquet",
        action="store_true",
        help="Also export derived financial masters (mst_*) from Oracle v2 -> OCI v3/ref/*.parquet",
    )
    args = parser.parse_args()

    _load_dotenv_file()

    print("Connecting to Oracle v2 (read-only)...")
    ora = _connect_oracle()
    try:
        raw_apis = _fetch_oracle_dicts(ora, "SELECT * FROM api_mst ORDER BY api_id")
        raw_builders = _fetch_oracle_dicts(ora, "SELECT * FROM api_job_builder ORDER BY build_id")

        apis = [convert_api_mst_row(r) for r in raw_apis]
        jobs = [convert_builder_row(r) for r in raw_builders]

        manual_data: dict[str, list[dict[str, Any]]] = {}
        for table, (cols, _) in MANUAL_TABLES.items():
            try:
                manual_data[table] = _fetch_oracle_dicts(ora, f"SELECT {', '.join(cols)} FROM {table}")
            except Exception as exc:
                print(f"  [WARN] manual table {table} skipped: {exc}")
                manual_data[table] = []

        print("\n=== Migration Summary (validated via Pydantic ApiSpec / ScrapeJobSpec) ===")
        print(f"  api_mst        : {len(apis):>5} rows")
        print(f"  api_job        : {len(jobs):>5} rows (active={sum(1 for j in jobs if j['is_active'])})")
        print(f"  host_policy    : {len(DEFAULT_HOST_POLICIES):>5} rows")
        for table, rows in manual_data.items():
            print(f"  {table:<15}: {len(rows):>5} rows")

        if args.apply:
            neon_dsn = os.environ["NEON_DATABASE_URL"]
            print("\nApplying upsert to Neon DB...")
            _upsert_neon(neon_dsn, apis, jobs, manual_data)
            print("Neon DB migration complete.")
        else:
            print("\nDry-run only. Pass --apply to write to Neon DB.")

        if args.export_ref_parquet:
            print("\nExporting derived financial masters to v3/ref/*.parquet...")
            _export_ref_parquets(ora)
    finally:
        ora.close()


if __name__ == "__main__":
    main()
