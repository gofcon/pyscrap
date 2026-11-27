"""Unit tests for v3 multi-bucket S3 (v3/), spec, cleanse/contract, DuckDB config, and batch cache."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pydantic import ValidationError

from app.object_storage import (
    build_v3_key,
    configured_targets,
    get_target_config,
    upload_bytes_to_targets,
    v3_prefix,
)
from app.v3.batch_cache import DailyConfigSnapshot, LocalRunLedger, RunLedgerEntry
from app.v3.cleanse import apply_cleanse, evaluate_contract
from app.v3.duckdb_runner import build_duckdb_config
from app.v3.spec import ApiSpec, CleanseSpec, ContractSpec, DuckDBJobSpec, export_spec_json_schema


class TestObjectStorageV3(unittest.TestCase):
    def test_build_v3_key_default_prefix(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("V3_BUCKET_PREFIX", None)
            self.assertEqual(v3_prefix(), "v3")
            self.assertEqual(
                build_v3_key("monthly", "kis_futopt_chart", "ym=202611", "monthly.parquet"),
                "v3/monthly/kis_futopt_chart/ym=202611/monthly.parquet",
            )
            self.assertEqual(
                build_v3_key("snapshot", "mst_etf.parquet"),
                "v3/snapshot/mst_etf.parquet",
            )

    def test_build_v3_key_rejects_traversal_and_invalid_layer(self):
        with self.assertRaises(ValueError):
            build_v3_key("daily", "../v2_secret", "a.parquet")
        with self.assertRaises(ValueError):
            build_v3_key("unknown_layer", "a.parquet")

    def test_multi_target_oci_and_r2_addition(self):
        env = {
            "OCI_NAMESPACE": "ns1",
            "OCI_REGION": "ap-seoul-1",
            "OCI_BUCKET_NAME": "oci-bkt",
            "OCI_ACCESS_KEY": "oci-ak",
            "OCI_SECRET_KEY": "oci-sk",
            "S3_R2_ENDPOINT_URL": "https://acct123.r2.cloudflarestorage.com",
            "S3_R2_BUCKET": "r2-bkt",
            "S3_R2_ACCESS_KEY": "r2-ak",
            "S3_R2_SECRET_KEY": "r2-sk",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(configured_targets(), ["oci", "r2"])
            oci = get_target_config("oci")
            r2 = get_target_config("r2")
            self.assertEqual(oci.endpoint_host, "ns1.compat.objectstorage.ap-seoul-1.oraclecloud.com")
            self.assertEqual(r2.endpoint_host, "acct123.r2.cloudflarestorage.com")
            self.assertEqual(r2.region, "auto")

            fake_clients: dict[str, mock.MagicMock] = {}

            def _fake_client(target: str = "oci"):
                fake_clients[target] = mock.MagicMock()
                return fake_clients[target]

            with mock.patch("app.object_storage.get_client", side_effect=_fake_client):
                uris = upload_bytes_to_targets(
                    b"PAR1...",
                    "v3/mart/etf_summary/data.parquet",
                    targets=("oci", "r2"),
                )
            self.assertEqual(
                uris,
                {
                    "oci": "s3://oci-bkt/v3/mart/etf_summary/data.parquet",
                    "r2": "s3://r2-bkt/v3/mart/etf_summary/data.parquet",
                },
            )
            fake_clients["oci"].put_object.assert_called_once()
            fake_clients["r2"].put_object.assert_called_once()

    def test_skip_unconfigured_r2_in_stage1(self):
        env = {
            "OCI_NAMESPACE": "ns1",
            "OCI_REGION": "ap-seoul-1",
            "OCI_BUCKET_NAME": "oci-bkt",
            "OCI_ACCESS_KEY": "oci-ak",
            "OCI_SECRET_KEY": "oci-sk",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(configured_targets(), ["oci"])
            with mock.patch("app.object_storage.get_client", return_value=mock.MagicMock()):
                uris = upload_bytes_to_targets(
                    b"data",
                    "v3/ref/mst_etf.parquet",
                    targets=("oci", "r2"),
                    skip_unconfigured=True,
                )
            self.assertEqual(uris, {"oci": "s3://oci-bkt/v3/ref/mst_etf.parquet"})


class TestV3Spec(unittest.TestCase):
    def test_strict_engine_rejects_typos_and_ext_is_free(self):
        spec = ApiSpec.model_validate({
            "engine": {
                "request": {"method": "GET", "url": "https://example.com/api"},
                "pagination": {"mode": "cursor", "param": "HHMM", "from": "output2[].stck_cntg_hour"},
                "sink": {"target_name": "ace_etf_pdf", "targets": ["oci", "r2"]},
            },
            "ext": {"owner": "etf_ops", "retention_days": 365, "custom_flag": True},
        })
        self.assertEqual(spec.engine.sink.prefix, "v3")
        self.assertEqual(spec.engine.sink.targets, ["oci", "r2"])
        self.assertEqual(spec.ext["owner"], "etf_ops")

        with self.assertRaises(ValidationError):
            ApiSpec.model_validate({
                "engine": {
                    "request": {"method": "GET", "url": "https://example.com/api"},
                    "unknown_typo_key": True,
                }
            })

    def test_snapshot_column_requires_repeated(self):
        with self.assertRaises(ValidationError):
            ApiSpec.model_validate({
                "engine": {
                    "request": {"url": "https://example.com"},
                    "sink": {"target_name": "x", "snapshot_column": "snap_at", "is_repeated": False},
                }
            })

    def test_json_schema_export_and_duckdb_job_spec(self):
        schema = export_spec_json_schema()
        self.assertIn("engine", schema["properties"])
        self.assertIn("ext", schema["properties"])
        job = DuckDBJobSpec(
            job_id="compact_chart",
            sql="SELECT 1",
            output_layer="monthly",
            target_name="kis_futopt_chart",
            targets=["oci", "r2"],
        )
        self.assertEqual(job.prefix, "v3")
        self.assertEqual(job.memory_limit, "1GB")
        with self.assertRaises(ValidationError):
            DuckDBJobSpec(
                job_id="bad_target",
                sql="SELECT 1",
                output_layer="snapshot",
                target_name="../escape",
            )


class TestCleanseAndContract(unittest.TestCase):
    def test_cleanse_filters_and_precedence(self):
        spec = CleanseSpec(
            lowercase_keys=True,
            rename={"std_DT": "std_dt", "jm_KSC_CD": "isin"},
            rules={
                "*": ["strip", "blank_to_null", "dash_to_null", "strip_comma"],
                "std_dt": ["digits_only"],
                "val_am": ["to_float"],
                "qty": ["to_int"],
            },
        )
        raw = [
            {
                "std_DT": "2026-11-27",
                "jm_KSC_CD": " KR7069500007 ",
                "isu_nm": "코스피200 C 202609 1,000.0",
                "val_am": "1,234,567.50",
                "qty": "1,000",
                "empty_col": "   ",
                "dash_col": "-",
            }
        ]
        out = apply_cleanse(
            raw,
            spec,
            key_params={"fundcd": "2ETF01", "std_dt": "20260101"},
            meta={"api_id": "ACE_ETF_PDF"},
        )
        self.assertEqual(len(out), 1)
        row = out[0]
        # Source std_DT overrides key_params std_dt and gets digits_only applied
        self.assertEqual(row["std_dt"], "20261127")
        self.assertEqual(row["fundcd"], "2ETF01")
        self.assertEqual(row["api_id"], "ACE_ETF_PDF")
        self.assertEqual(row["isin"], "KR7069500007")
        # Comma inside instrument name is preserved; numeric comma is stripped
        self.assertEqual(row["isu_nm"], "코스피200 C 202609 1,000.0")
        self.assertEqual(row["val_am"], 1234567.5)
        self.assertEqual(row["qty"], 1000)
        self.assertIsNone(row["empty_col"])
        self.assertIsNone(row["dash_col"])

    def test_contract_policies(self):
        self.assertEqual(evaluate_contract([], ContractSpec(empty_policy="pending")).action, "pending")
        self.assertEqual(evaluate_contract([], ContractSpec(empty_policy="keep_previous")).action, "keep_previous")
        self.assertEqual(evaluate_contract([], ContractSpec(empty_policy="fail")).action, "fail")
        self.assertEqual(evaluate_contract([], ContractSpec(min_rows=1)).action, "fail")

        bad_rows = [
            {"isin": "KR1", "msg": "데이터가 없습니다."},
            {"isin": "KR1", "msg": "ok"},
        ]
        res = evaluate_contract(
            bad_rows,
            ContractSpec(
                min_rows=1,
                not_null_fields=["isin"],
                forbidden_values=["데이터가 없습니다."],
                unique_keys=["isin"],
            ),
        )
        self.assertFalse(res.ok)
        self.assertEqual(res.action, "fail")
        self.assertEqual(len(res.violations), 2)


class TestDuckDBConfigAndBatchCache(unittest.TestCase):
    def test_duckdb_config_rejects_tmpfs(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "duckdb"
            with mock.patch("app.v3.duckdb_runner.is_tmpfs_path", return_value=True):
                with self.assertRaises(ValueError):
                    build_duckdb_config(temp_directory=p)
            with mock.patch("app.v3.duckdb_runner.is_tmpfs_path", return_value=False):
                cfg = build_duckdb_config(memory_limit="1GB", threads=2, temp_directory=p)
                self.assertEqual(cfg["memory_limit"], "1GB")
                self.assertEqual(cfg["threads"], 2)
                self.assertFalse(cfg["preserve_insertion_order"])

    def test_daily_snapshot_and_local_ledger_roundtrip(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            snap = DailyConfigSnapshot(
                generated_at="2026-11-27T06:30:00+09:00",
                apis={"ACE_ETF_PDF": {"status": "ACTIVE"}},
                host_policies={"aceetf.co.kr": {"min_interval_sec": 0.5}},
                user_tables={"user_etf": [{"isin": "KR7069500007"}]},
            )
            saved = snap.save(root / "config_snapshot.json")
            loaded = DailyConfigSnapshot.load(saved)
            self.assertEqual(loaded.apis, snap.apis)
            self.assertEqual(loaded.user_tables, snap.user_tables)

            ledger = LocalRunLedger("20261127", base_dir=root / "ledger")
            ledger.append(RunLedgerEntry(
                job_id="KIS_FUTOPT_PRICE_3M_CALL",
                api_id="KIS_FUTOPT_PRICE",
                partition_key="A01612",
                target_date="20261127",
                status="SUCCESS",
                record_count=120,
                output_uris={"oci": "s3://b/v3/raw_clean/kis_futopt_price/dt=20261127/0903.parquet"},
            ))
            ledger.append(RunLedgerEntry(
                job_id="KIS_FUTOPT_PRICE_3M_CALL",
                api_id="KIS_FUTOPT_PRICE",
                partition_key="A01613",
                target_date="20261127",
                status="FAILED",
                error_code="SourceUnavailable",
            ))
            self.assertEqual(len(ledger.read_all()), 2)
            self.assertEqual(
                ledger.succeeded_partitions("KIS_FUTOPT_PRICE_3M_CALL"),
                {("A01612", "20261127")},
            )


class TestV2ToV3MigrationConverters(unittest.TestCase):
    def test_convert_api_mst_and_builder_rows(self):
        from scripts.migrate_v2_to_v3 import convert_api_mst_row, convert_builder_row

        v2_api = {
            "api_id": "ACE_ETF_PDF",
            "api_name": "ACE ETF 구성종목",
            "source_name": "AMC",
            "domain": "ETF",
            "protocol": "HTTP",
            "method": "GET",
            "api_url": "https://papi.aceetf.co.kr/api/funds/:FUNDCD/pdf?stdDt=:STD_DT",
            "header_json": {"Accept": "application/json"},
            "payload_type": None,
            "payload_json": None,
            "payload_xml": None,
            "behavior_json": None,
            "response_type": None,
            "parse_json": None,
            "session_json": None,
            "staging_json": None,
            "options_json": {"empty_keeps_previous": True, "timeout_sec": 10},
            "persist_env_json": None,
            "record_map_json": {"data": "api_rst"},
            "merge_fields_json": None,
            "job_key_params": ["FUNDCD", "STD_DT"],
            "row_stamp_json": {"FUNDCD": "fundcd", "STD_DT": "std_dt"},
            "is_repeated": False,
            "snapshot_column": None,
            "pagination_json": None,
            "note": "ACE PDF",
        }
        v3_api = convert_api_mst_row(v2_api)
        self.assertEqual(v3_api["sink_type"], "PARQUET")
        self.assertEqual(v3_api["spec_json"]["engine"]["sink"]["prefix"], "v3")
        self.assertEqual(v3_api["spec_json"]["engine"]["sink"]["targets"], ["oci"])
        self.assertEqual(v3_api["spec_json"]["engine"]["sink"]["target_name"], "ace_etf_pdf")
        self.assertEqual(v3_api["spec_json"]["engine"]["contract"]["empty_policy"], "keep_previous")
        self.assertEqual(v3_api["spec_json"]["engine"]["cleanse"]["rules"]["std_dt"], ["digits_only"])

        v2_builder = {
            "build_id": "ACE_ETF_PDF_DAILY",
            "api_id": "ACE_ETF_PDF",
            "cartesian_param_json": {
                "FUNDCD": {"db": "SELECT amc_etf_cd FROM mst_etf WHERE amc='한국투자신탁운용'"},
                "STD_DT": {"date": "today"},
            },
            "is_active": True,
            "save_mode": "overwrite",
            "schedule_tag": "daily_batch2",
            "snapshot_unit": None,
            "note": "daily ace",
        }
        v3_job = convert_builder_row(v2_builder)
        self.assertEqual(v3_job["job_id"], "ACE_ETF_PDF_DAILY")
        self.assertEqual(v3_job["api_id"], "ACE_ETF_PDF")
        self.assertEqual(v3_job["save_mode"], "overwrite")
        self.assertIn("FUNDCD", v3_job["params_json"])


class TestHttpFetcherAndExecutor(unittest.TestCase):
    def test_host_interval_longest_suffix_and_floor(self):
        from app.v3.http_fetcher import interval_for_host

        policies = {
            "krx.co.kr": {"min_interval_sec": 0.3},
            "data.krx.co.kr": {"min_interval_sec": 1.0},
        }
        self.assertEqual(interval_for_host("data.krx.co.kr", policies), 1.0)
        self.assertEqual(interval_for_host("sub.krx.co.kr", policies), 0.3)
        self.assertEqual(interval_for_host("data.krx.co.kr", policies, api_min=0.2), 1.0)
        self.assertEqual(interval_for_host("data.krx.co.kr", policies, api_min=1.5), 1.5)

    def test_secret_placeholder_guard_and_response_parsers(self):
        import httpx
        from app.v3.http_fetcher import parse_response, resolve_secret_placeholders

        with self.assertRaises(PermissionError):
            resolve_secret_placeholders("Bearer ${NEON_DATABASE_URL}")
        with self.assertRaises(PermissionError):
            resolve_secret_placeholders("${S3_R2_SECRET_KEY}")

        spec = ApiSpec.model_validate({
            "engine": {
                "request": {"url": "https://example.com"},
                "extract": {
                    "response_type": "json",
                    "selector": "data.items",
                    "merge_fields": {"meta.dt": "std_dt"},
                },
            }
        })
        resp = httpx.Response(200, json={"meta": {"dt": "20261127"}, "data": {"items": [{"code": "A"}]}})
        self.assertEqual(parse_response(resp, spec), [{"code": "A", "std_dt": "20261127"}])

        # Scalar string must never split into characters
        bad = httpx.Response(200, json={"data": {"items": "ERROR"}})
        self.assertEqual(parse_response(bad, spec), [])

    def test_expand_scrape_params_dates_and_literals(self):
        from datetime import date
        from app.v3.executor import expand_scrape_params

        combos = expand_scrape_params(
            {"DT": {"date": "last_bday"}, "MKT": "STK"},
            ref_date=date(2026, 11, 29),  # Sunday -> Friday 20261127
        )
        self.assertEqual(combos, [{"DT": "20261127", "MKT": "STK"}])

    def test_effective_pagination_detects_implicit_page_marker(self):
        from app.v3.http_fetcher import _effective_pagination

        url_spec = ApiSpec.model_validate({
            "engine": {"request": {"url": "https://example.com/list?page=:PAGE&size=20"}}
        })
        pag = _effective_pagination(url_spec)
        self.assertIsNotNone(pag)
        self.assertEqual(pag.mode, "page")
        self.assertEqual(pag.param, "PAGE")
        self.assertEqual(pag.start, 1)

        no_pag_spec = ApiSpec.model_validate({
            "engine": {"request": {"url": "https://example.com/list?dt=:STD_DT"}}
        })
        self.assertIsNone(_effective_pagination(no_pag_spec))

    def test_parquet_writer_falls_back_to_duckdb_on_mixed_types(self):
        import duckdb
        from app.v3.parquet_sink import write_records_to_parquet

        rows = [
            {"code": "A", "val": 100, "meta": {"tag": None}},
            {"code": "B", "val": "N/A", "meta": {"tag": "x"}},
        ]
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "mixed.parquet"
            size = write_records_to_parquet(rows, out)
            self.assertGreater(size, 0)
            con = duckdb.connect(":memory:")
            try:
                cnt = con.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
                self.assertEqual(cnt, 2)
            finally:
                con.close()

    def test_execute_api_job_records_output_uris_and_uses_target_date(self):
        from app.v3.executor import execute_api_job
        from app.v3.parquet_sink import ParquetWriteResult

        snap = DailyConfigSnapshot(
            generated_at="2026-11-27T06:30:00+09:00",
            apis={
                "SAMPLE_API": {
                    "api_id": "SAMPLE_API",
                    "protocol": "HTTP",
                    "spec_json": {
                        "engine": {
                            "request": {"url": "https://example.com?dt=:STD_DT"},
                            "sink": {
                                "target_name": "sample_tbl",
                                "job_key_params": ["STD_DT"],
                                "row_stamp": {"STD_DT": "std_dt"},
                            },
                        }
                    },
                }
            },
            api_jobs={
                "SAMPLE_JOB": {
                    "job_id": "SAMPLE_JOB",
                    "api_id": "SAMPLE_API",
                    "schedule_tag": "daily_batch2",
                    "run_order": 100,
                    "is_active": True,
                    "save_mode": "overwrite",
                    "params_json": {"STD_DT": {"date": "today"}},
                }
            },
            etl_jobs={
                "SAMPLE_ETL": {
                    "job_id": "SAMPLE_ETL",
                    "schedule_tag": "daily_batch2",
                    "run_order": 200,
                    "is_active": True,
                    "output_layer": "snapshot",
                    "target_name": "sample_ref",
                    "targets": ["oci"],
                    "memory_limit": "512MB",
                    "threads": 2,
                    "min_rows": 1,
                    "sql_text": "SELECT 1 AS x",
                }
            },
        )
        seen_params: list[dict] = []

        def _fake_collect(api_id, spec, params, **kw):
            seen_params.append(dict(params))
            return [{"isin": "KR1"}]

        fake_pub = ParquetWriteResult(
            object_key="v3/raw_clean/sample_tbl/dt=20261125/batch.parquet",
            row_count=1,
            byte_size=123,
            uploaded_uris={"oci": "s3://bkt/v3/raw_clean/sample_tbl/dt=20261125/batch.parquet"},
        )
        with tempfile.TemporaryDirectory() as td:
            ledger = LocalRunLedger("20261125", base_dir=Path(td))
            with mock.patch("app.v3.executor.collect_http_records", side_effect=_fake_collect), \
                 mock.patch("app.v3.executor.publish_parquet_batch", return_value=fake_pub):
                res = execute_api_job(snap, "SAMPLE_JOB", target_date="20261125", ledger=ledger)
            self.assertEqual(seen_params, [{"STD_DT": "20261125"}])
            self.assertEqual(res["job_type"], "API")
            self.assertEqual(res["rows"], 1)
            entries = ledger.read_all()
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].job_type, "API")
            self.assertEqual(entries[0].status, "SUCCESS")
            self.assertEqual(
                entries[0].output_uris,
                {"oci": "s3://bkt/v3/raw_clean/sample_tbl/dt=20261125/batch.parquet"},
            )

    def test_execute_schedule_cycle_orders_api_job_before_etl_job(self):
        from app.v3.duckdb_runner import DuckDBJobResult
        from app.v3.executor import execute_schedule_cycle
        from app.v3.parquet_sink import ParquetWriteResult

        snap = DailyConfigSnapshot(
            generated_at="2026-11-27T06:30:00+09:00",
            apis={
                "A1": {
                    "api_id": "A1",
                    "protocol": "HTTP",
                    "spec_json": {
                        "engine": {
                            "request": {"url": "https://example.com"},
                            "sink": {"target_name": "a1"},
                        }
                    },
                }
            },
            api_jobs={
                "JOB_API": {
                    "job_id": "JOB_API",
                    "api_id": "A1",
                    "schedule_tag": "daily_batch2",
                    "run_order": 100,
                    "is_active": True,
                    "save_mode": "overwrite",
                    "params_json": {},
                }
            },
            etl_jobs={
                "JOB_ETL": {
                    "job_id": "JOB_ETL",
                    "schedule_tag": "daily_batch2",
                    "run_order": 200,
                    "is_active": True,
                    "output_layer": "snapshot",
                    "target_name": "a1_ref",
                    "targets": ["oci"],
                    "memory_limit": "512MB",
                    "threads": 2,
                    "min_rows": 1,
                    "sql_text": "SELECT 1 AS id",
                }
            },
        )
        fake_pub = ParquetWriteResult(
            object_key="v3/raw_clean/a1/dt=20261127/batch.parquet",
            row_count=1,
            byte_size=10,
            uploaded_uris={"oci": "s3://bkt/v3/raw_clean/a1/dt=20261127/batch.parquet"},
        )
        fake_etl = DuckDBJobResult(
            job_id="JOB_ETL",
            object_key="v3/snapshot/a1_ref.parquet",
            row_count=5,
            byte_size=20,
            uploaded_uris={"oci": "s3://bkt/v3/snapshot/a1_ref.parquet"},
        )
        with tempfile.TemporaryDirectory() as td:
            ledger = LocalRunLedger("20261127", base_dir=Path(td))
            with mock.patch("app.v3.executor.collect_http_records", return_value=[{"x": 1}]), \
                 mock.patch("app.v3.executor.publish_parquet_batch", return_value=fake_pub), \
                 mock.patch("app.v3.executor.bucket_name", return_value="bkt"), \
                 mock.patch("app.v3.executor.run_duckdb_job", return_value=fake_etl):
                results = execute_schedule_cycle(snap, "daily_batch2", target_date="20261127", ledger=ledger)
            self.assertEqual([(r["job_type"], r["job_id"]) for r in results], [("API", "JOB_API"), ("ETL", "JOB_ETL")])
            entries = ledger.read_all()
            self.assertEqual([(e.job_type, e.job_id, e.status) for e in entries], [
                ("API", "JOB_API", "SUCCESS"),
                ("ETL", "JOB_ETL", "SUCCESS"),
            ])

    def test_duckdb_runner_resolves_canonical_snapshot_daily_monthly_paths(self):
        from app.object_storage import resolve_etl_object_key
        from app.v3.duckdb_runner import run_duckdb_job

        self.assertEqual(
            resolve_etl_object_key("snapshot", "mst_etf"),
            "v3/snapshot/mst_etf.parquet",
        )
        self.assertEqual(
            resolve_etl_object_key("daily", "test_tbl", target_date="20261127"),
            "v3/daily/test_tbl/ym=202611/dt=20261127/20261127.parquet",
        )
        self.assertEqual(
            resolve_etl_object_key("monthly", "test_tbl", target_ym="202611"),
            "v3/monthly/test_tbl/ym=202611/202611.parquet",
        )

        spec = DuckDBJobSpec(
            job_id="CURATE_TEST",
            sql_text="SELECT '${TARGET_DATE}' AS dt, 1 AS v",
            output_layer="daily",
            target_name="test_tbl",
            targets=["oci"],
            memory_limit="256MB",
            threads=1,
            min_rows=1,
        )
        with tempfile.TemporaryDirectory() as td:
            with mock.patch("app.v3.duckdb_runner.is_tmpfs_path", return_value=False), \
                 mock.patch("app.v3.duckdb_runner.register_s3_secrets", return_value=[]), \
                 mock.patch(
                     "app.v3.duckdb_runner.upload_file_to_targets",
                     side_effect=lambda p, key, **kw: {"oci": f"s3://bkt/{key}"},
                 ):
                res = run_duckdb_job(
                    spec,
                    sql_params={"TARGET_DATE": "20261127", "TARGET_YM": "202611"},
                    temp_directory=Path(td) / "duckdb",
                )
            self.assertEqual(res.object_key, "v3/daily/test_tbl/ym=202611/dt=20261127/20261127.parquet")
            self.assertEqual(res.row_count, 1)

    def test_code_harvester_column_filters_and_builtin_dispatch(self):
        from app.v3.code_harvester import infer_source_name, is_candidate_code_column
        from app.v3.duckdb_runner import DuckDBJobResult
        from app.v3.executor import execute_etl_job

        self.assertTrue(is_candidate_code_column("mkt_div", "VARCHAR"))
        self.assertFalse(is_candidate_code_column("bas_dd", "VARCHAR"))
        self.assertFalse(is_candidate_code_column("std_dt", "VARCHAR"))
        self.assertFalse(is_candidate_code_column("trade_at", "VARCHAR"))
        self.assertFalse(is_candidate_code_column("target_date", "VARCHAR"))
        self.assertFalse(is_candidate_code_column("api_id", "VARCHAR"))
        self.assertFalse(is_candidate_code_column("val_am", "DOUBLE"))
        self.assertEqual(infer_source_name("krx_etf_list"), "KRX")
        self.assertEqual(infer_source_name("ksd_bond_info"), "KSD")
        self.assertEqual(infer_source_name("fo_idx_code_mst"), "KIS")
        self.assertEqual(infer_source_name("ace_etf_list"), "AMC")

        snap = DailyConfigSnapshot(
            generated_at="2026-11-27T06:30:00+09:00",
            etl_jobs={
                "HARVEST_SRC_CODE": {
                    "job_id": "HARVEST_SRC_CODE",
                    "schedule_tag": "daily_batch2",
                    "run_order": 250,
                    "is_active": True,
                    "output_layer": "snapshot",
                    "target_name": "src_code",
                    "targets": ["oci"],
                    "memory_limit": "512MB",
                    "threads": 2,
                    "min_rows": 0,
                    "sql_text": "-- builtin:harvest_src_code",
                }
            },
        )
        fake_res = DuckDBJobResult(
            job_id="HARVEST_SRC_CODE",
            object_key="v3/snapshot/src_code.parquet",
            row_count=42,
            byte_size=1024,
            uploaded_uris={"oci": "s3://bkt/v3/snapshot/src_code.parquet"},
        )
        with tempfile.TemporaryDirectory() as td:
            ledger = LocalRunLedger("20261127", base_dir=Path(td))
            with mock.patch("app.v3.code_harvester.harvest_src_codes", return_value=fake_res):
                out = execute_etl_job(snap, "HARVEST_SRC_CODE", target_date="20261127", ledger=ledger)
            self.assertEqual(out["rows"], 42)
            entries = ledger.read_all()
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].job_type, "ETL")
            self.assertEqual(entries[0].job_id, "HARVEST_SRC_CODE")

    def test_cleanup_prefix_safety_and_purge_verified_daily(self):
        from datetime import date
        from app.object_storage import _validate_v3_cleanup_prefix
        from app.v3.cleanup import purge_verified_daily

        self.assertEqual(
            _validate_v3_cleanup_prefix("v3/daily/kis_futopt_chart/ym=202607/"),
            "v3/daily/kis_futopt_chart/ym=202607/",
        )
        for bad in ("v3/", "v3/daily/", "v2/daily/foo/", "v3/daily/../v2/foo"):
            with self.assertRaises(ValueError):
                _validate_v3_cleanup_prefix(bad)

        daily_keys = [
            "v3/daily/kis_futopt_chart/ym=202606/dt=20260601/data.parquet",
            "v3/daily/kis_futopt_chart/ym=202607/dt=20260701/data.parquet",
            "v3/daily/kis_futopt_chart/ym=202611/dt=20261127/data.parquet",
        ]

        def _fake_verify(target, ym, **kw):
            if ym == "202607":
                return True, {"ym": ym, "monthly_rows": 100, "daily_distinct_rows": 100, "hash_match": True}
            return False, {"ym": ym, "monthly_rows": 99, "daily_distinct_rows": 100, "hash_match": False}

        with mock.patch("app.v3.cleanup.list_keys_with_prefix", return_value=daily_keys), \
             mock.patch("app.v3.cleanup.verify_monthly_matches_daily", side_effect=_fake_verify), \
             mock.patch("app.v3.cleanup.delete_keys_from_targets", return_value={"oci": 1}) as del_mock:
            dry = purge_verified_daily("kis_futopt_chart", keep_months=3, ref_date=date(2026, 11, 27), dry_run=True)
            self.assertEqual([m["ym"] for m in dry["verified_months"]], ["202607"])
            self.assertEqual([m["ym"] for m in dry["skipped_months"]], ["202606"])
            self.assertEqual(dry["deleted_files"], 0)
            del_mock.assert_not_called()

            applied = purge_verified_daily("kis_futopt_chart", keep_months=3, ref_date=date(2026, 11, 27), dry_run=False)
            self.assertEqual(applied["deleted_files"], 1)
            del_mock.assert_called_once_with(
                ["v3/daily/kis_futopt_chart/ym=202607/dt=20260701/data.parquet"],
                targets=("oci",),
                skip_unconfigured=True,
            )

    def test_purge_raw_clean_and_iter_target_dates(self):
        from datetime import date
        from app.v3.cleanup import extract_dt_from_key, purge_raw_clean
        from app.v3.cli import iter_target_dates

        self.assertEqual(extract_dt_from_key("v3/raw_clean/ace_etf_pdf/dt=20261015/a.parquet"), "20261015")
        self.assertIsNone(extract_dt_from_key("v3/ref/mst_etf.parquet"))

        keys = [
            "v3/raw_clean/ace_etf_pdf/dt=20261001/old.parquet",
            "v3/raw_clean/ace_etf_pdf/dt=20261120/recent.parquet",
        ]
        with mock.patch("app.v3.cleanup.list_keys_with_prefix", return_value=keys), \
             mock.patch("app.v3.cleanup.delete_keys_from_targets", return_value={"oci": 1}) as del_mock:
            dry = purge_raw_clean(keep_days=30, ref_date=date(2026, 11, 27), dry_run=True)
            self.assertEqual(dry["matched"], {"oci": 1})
            self.assertEqual(dry["deleted"], {"oci": 0})
            del_mock.assert_not_called()

            applied = purge_raw_clean(keep_days=30, ref_date=date(2026, 11, 27), dry_run=False)
            self.assertEqual(applied["deleted"], {"oci": 1})
            del_mock.assert_called_once()

        # 2026-11-26 (Thu) .. 2026-11-30 (Mon): skips Sat 28 & Sun 29 by default
        self.assertEqual(
            iter_target_dates(date_from="20261126", date_to="20261130"),
            ["20261126", "20261127", "20261130"],
        )
        self.assertEqual(
            iter_target_dates(date_from="20261126", date_to="20261130", include_weekends=True),
            ["20261126", "20261127", "20261128", "20261129", "20261130"],
        )
        with self.assertRaises(ValueError):
            iter_target_dates(target_date="20261127", date_from="20261126", date_to="20261127")
        with self.assertRaises(ValueError):
            iter_target_dates(date_from="20261130", date_to="20261126")

    def test_session_store_and_persist_env_and_multi_selector(self):
        import httpx
        from app.v3.executor import execute_api_job
        from app.v3.http_fetcher import resolve_secret_placeholders
        from app.v3.parquet_sink import ParquetWriteResult
        from app.v3.session_store import get_session_token, save_session_tokens

        with tempfile.TemporaryDirectory() as td:
            store = Path(td) / "session_store.json"
            with self.assertRaises(PermissionError):
                save_session_tokens({"OCI_SECRET_KEY": "bad"}, path=store)
            with self.assertRaises(PermissionError):
                save_session_tokens({"NEON_DATABASE_URL": "bad"}, path=store)

            save_session_tokens({"KIS_ACCESS_TOKEN": "tok-123"}, path=store)
            self.assertEqual(get_session_token("KIS_ACCESS_TOKEN", path=store), "tok-123")
            with mock.patch("app.v3.http_fetcher.get_session_token", side_effect=lambda k: get_session_token(k, path=store)):
                self.assertEqual(resolve_secret_placeholders("Bearer ${KIS_ACCESS_TOKEN}"), "Bearer tok-123")

            # Multi-selector + persist_env end-to-end via execute_api_job
            snap = DailyConfigSnapshot(
                generated_at="2026-11-27T06:30:00+09:00",
                apis={
                    "KIS_ETF_PDF": {
                        "api_id": "KIS_ETF_PDF",
                        "protocol": "HTTP",
                        "spec_json": {
                            "engine": {
                                "request": {"url": "https://example.com/etf"},
                                "extract": {
                                    "response_type": "json",
                                    "selector": "output1",
                                    "extra_selectors": {"output2": "kis_etf_pdf"},
                                    "persist_env": {"next_tok": "KIS_TEST_TOKEN"},
                                },
                                "sink": {"target_name": "kis_etf"},
                            }
                        },
                    }
                },
                api_jobs={
                    "KIS_ETF_PDF_JOB": {
                        "job_id": "KIS_ETF_PDF_JOB",
                        "api_id": "KIS_ETF_PDF",
                        "schedule_tag": "daily_batch2",
                        "run_order": 100,
                        "is_active": True,
                        "save_mode": "overwrite",
                        "params_json": {},
                    }
                },
            )
            fake_resp = httpx.Response(
                200,
                json={
                    "output1": {"etf_cd": "069500", "next_tok": "fresh-tok"},
                    "output2": [{"comp_cd": "005930"}, {"comp_cd": "000660"}],
                },
            )
            published_targets: list[tuple[str, int]] = []

            def _fake_pub(rows, *, target_name, target_date, layer, batch_name, **kw):
                published_targets.append((target_name, len(rows)))
                key = f"v3/{layer}/{target_name}/dt={target_date}/{batch_name}.parquet"
                return ParquetWriteResult(object_key=key, row_count=len(rows), byte_size=10, uploaded_uris={"oci": f"s3://bkt/{key}"})

            ledger = LocalRunLedger("20261127", base_dir=Path(td) / "ledger")
            with mock.patch("app.v3.http_fetcher.fetch_once", return_value=fake_resp), \
                 mock.patch("app.v3.executor.publish_parquet_batch", side_effect=_fake_pub), \
                 mock.patch("app.v3.executor.save_session_tokens", side_effect=lambda u: save_session_tokens(u, path=store)):
                res = execute_api_job(snap, "KIS_ETF_PDF_JOB", target_date="20261127", ledger=ledger)

            self.assertEqual(published_targets, [("kis_etf", 1), ("kis_etf_pdf", 2)])
            self.assertIn("kis_etf_pdf", res["extra_uris"])
            self.assertEqual(res["persisted_tokens"], ["KIS_TEST_TOKEN"])
            self.assertEqual(get_session_token("KIS_TEST_TOKEN", path=store), "fresh-tok")

    def test_v2_converter_multi_selector_and_duckdb_axis_sql(self):
        from scripts.migrate_v2_to_v3 import convert_api_mst_row, convert_axis_sql_to_duckdb

        v2_chart = {
            "api_id": "KIS_FUTOPT_CHART",
            "api_name": "선물옵션 분봉",
            "source_name": "KIS",
            "domain": "DERIV",
            "protocol": "HTTP",
            "method": "GET",
            "api_url": "https://example.com/chart",
            "record_map_json": {"output1": "api_rst", "output2": "kis_futopt_chart"},
            "job_key_params": ["SHORT_CODE", "DATE"],
            "is_repeated": False,
        }
        v3_chart = convert_api_mst_row(v2_chart)
        self.assertEqual(v3_chart["spec_json"]["engine"]["extract"]["selector"], "output2")
        self.assertEqual(
            v3_chart["spec_json"]["engine"]["extract"]["extra_selectors"],
            {"output1": "kis_futopt_chart_meta"},
        )
        self.assertEqual(v3_chart["spec_json"]["engine"]["sink"]["target_name"], "kis_futopt_chart")

        self.assertIn(
            "VALUES ('KRDRVFUK2I'), ('KRDRVOPK2I')",
            convert_axis_sql_to_duckdb("SELECT c, c FROM (SELECT 'KRDRVFUK2I' c FROM dual UNION ALL SELECT 'KRDRVOPK2I' c FROM dual)"),
        )
        self.assertIn(
            "LIMIT 800",
            convert_axis_sql_to_duckdb("SELECT isin FROM (SELECT m.isin FROM mst_bond m ORDER BY m.first_seen DESC) WHERE ROWNUM <= 800"),
        )
        self.assertIn(
            "CAST(strptime('${TARGET_DATE}', '%Y%m%d') AS DATE)",
            convert_axis_sql_to_duckdb("SELECT * FROM mst_fuopt WHERE mat_date >= TRUNC(CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Seoul' AS DATE))"),
        )

    def test_binary_sink_unzip_and_zip_slip_guard(self):
        import io
        import zipfile
        from app.v3.binary_sink import publish_binary_payload
        from app.v3.executor import execute_api_job

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("../../escaped/doc1.xml", b"<doc>1</doc>")
            zf.writestr("nested/doc2.xml", b"<doc>2</doc>")
        zip_bytes = buf.getvalue()

        uploaded: list[tuple[str, int]] = []

        def _fake_upload(data, key, **kw):
            uploaded.append((key, len(data)))
            return {"oci": f"s3://bkt/{key}"}

        with mock.patch("app.v3.binary_sink.upload_bytes_to_targets", side_effect=_fake_upload):
            recs, last_uris = publish_binary_payload(
                zip_bytes,
                staging={"group": "dart_docs", "name": ":RCEPT_NO.zip", "unzip": True},
                params={"RCEPT_NO": "2026112700001"},
            )
        self.assertEqual([r["object_key"] for r in recs], [
            "v3/binary/dart_docs/doc1.xml",
            "v3/binary/dart_docs/doc2.xml",
        ])
        self.assertEqual(last_uris, {"oci": "s3://bkt/v3/binary/dart_docs/doc2.xml"})

        # End-to-end BINARY execute_api_job
        snap = DailyConfigSnapshot(
            generated_at="2026-11-27T06:30:00+09:00",
            apis={
                "DART_DOCUMENT": {
                    "api_id": "DART_DOCUMENT",
                    "protocol": "HTTP",
                    "sink_type": "BINARY",
                    "spec_json": {
                        "engine": {
                            "request": {"url": "https://opendart.fss.or.kr/api/document.xml?rcept_no=:RCEPT_NO"},
                            "extract": {"response_type": "binary"},
                            "sink": {
                                "target_name": "dart_document",
                                "layer": "binary",
                                "job_key_params": ["RCEPT_NO"],
                                "staging": {"group": "dart_docs", "name": ":RCEPT_NO.zip", "unzip": True},
                            },
                        }
                    },
                }
            },
            api_jobs={
                "DART_DOC_JOB": {
                    "job_id": "DART_DOC_JOB",
                    "api_id": "DART_DOCUMENT",
                    "is_active": True,
                    "params_json": {"RCEPT_NO": "2026112700001"},
                }
            },
        )
        with tempfile.TemporaryDirectory() as td:
            ledger = LocalRunLedger("20261127", base_dir=Path(td))
            with mock.patch("app.v3.executor.fetch_http_binary", return_value=("2026112700001.zip", zip_bytes)), \
                 mock.patch("app.v3.binary_sink.upload_bytes_to_targets", side_effect=_fake_upload):
                res = execute_api_job(snap, "DART_DOC_JOB", target_date="20261127", ledger=ledger)
            self.assertEqual(res["rows"], 2)
            entries = ledger.read_all()
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].status, "SUCCESS")
            self.assertEqual(entries[0].record_count, 2)

    def test_session_login_auto_retry_and_browser_dispatch(self):
        from app.errors import SessionExpired
        from app.v3.executor import execute_api_job
        from app.v3.parquet_sink import ParquetWriteResult
        from app.v3.session_store import clear_session_state, load_http_cookies, save_http_cookies

        # 1. Cookie store roundtrip
        with tempfile.TemporaryDirectory() as td:
            sd = Path(td)
            save_http_cookies("data.krx.co.kr", [{"name": "JSESSIONID", "value": "abc"}], state_dir=sd)
            loaded = load_http_cookies("data.krx.co.kr", state_dir=sd)
            self.assertEqual(loaded[0]["value"], "abc")
            clear_session_state("data.krx.co.kr", state_dir=sd)
            self.assertEqual(load_http_cookies("data.krx.co.kr", state_dir=sd), [])

        # 2. SessionExpired auto-triggers session.login once and retries
        snap = DailyConfigSnapshot(
            generated_at="2026-11-27T06:30:00+09:00",
            apis={
                "KRX_DATA_LOGIN": {
                    "api_id": "KRX_DATA_LOGIN",
                    "protocol": "HTTP",
                    "sink_type": "SESSION",
                    "spec_json": {
                        "engine": {
                            "request": {"method": "POST", "url": "https://data.krx.co.kr/login"},
                            "extract": {"response_type": "session"},
                            "session": {"expect": "CD001"},
                        }
                    },
                },
                "KRX_ETF_LIST": {
                    "api_id": "KRX_ETF_LIST",
                    "protocol": "HTTP",
                    "sink_type": "PARQUET",
                    "spec_json": {
                        "engine": {
                            "request": {"method": "POST", "url": "https://data.krx.co.kr/etf"},
                            "session": {"login": "KRX_DATA_LOGIN", "logged_out": "LOGOUT"},
                            "sink": {"target_name": "krx_etf_list"},
                        }
                    },
                },
                "KRX_ISIN_SRCH": {
                    "api_id": "KRX_ISIN_SRCH",
                    "protocol": "BROWSER",
                    "sink_type": "PARQUET",
                    "spec_json": {
                        "engine": {
                            "request": {"url": "https://isin.krx.co.kr/srch"},
                            "extract": {"response_type": "dom", "parse": {"rows": "tr"}},
                            "sink": {"target_name": "krx_isin_srch"},
                        }
                    },
                },
            },
            api_jobs={
                "KRX_LOGIN_JOB": {
                    "job_id": "KRX_LOGIN_JOB",
                    "api_id": "KRX_DATA_LOGIN",
                    "is_active": True,
                    "params_json": {},
                },
                "KRX_ETF_JOB": {
                    "job_id": "KRX_ETF_JOB",
                    "api_id": "KRX_ETF_LIST",
                    "is_active": True,
                    "params_json": {},
                },
                "KRX_BROWSER_JOB": {
                    "job_id": "KRX_BROWSER_JOB",
                    "api_id": "KRX_ISIN_SRCH",
                    "is_active": True,
                    "params_json": {},
                },
            },
        )
        attempts = {"n": 0}

        def _flaky_collect(api_id, spec, params, **kw):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise SessionExpired("LOGOUT")
            return [{"isu_cd": "KR1"}]

        fake_pub = ParquetWriteResult(
            object_key="v3/raw_clean/krx_etf_list/dt=20261127/krx_etf_job.parquet",
            row_count=1,
            byte_size=10,
            uploaded_uris={"oci": "s3://bkt/v3/raw_clean/krx_etf_list/dt=20261127/krx_etf_job.parquet"},
        )
        with tempfile.TemporaryDirectory() as td:
            ledger = LocalRunLedger("20261127", base_dir=Path(td))
            with mock.patch("app.v3.executor.run_http_session_login") as login_mock, \
                 mock.patch("app.v3.executor.collect_http_records", side_effect=_flaky_collect), \
                 mock.patch("app.v3.executor.publish_parquet_batch", return_value=fake_pub):
                # Explicit SESSION job succeeds
                sres = execute_api_job(snap, "KRX_LOGIN_JOB", target_date="20261127", ledger=ledger)
                self.assertEqual(sres["sink_type"], "SESSION")
                # Dependent job recovers from SessionExpired via auto re-login
                eres = execute_api_job(snap, "KRX_ETF_JOB", target_date="20261127", ledger=ledger)
                self.assertEqual(eres["rows"], 1)
                self.assertEqual(login_mock.call_count, 2)

            # 3. BROWSER job dispatches to browser_runner.collect_browser_bundle
            with mock.patch(
                "app.v3.browser_runner.collect_browser_bundle",
                return_value=([{"isin": "KR7069500007"}], {}, []),
            ) as br_mock, mock.patch("app.v3.executor.publish_parquet_batch", return_value=fake_pub):
                bres = execute_api_job(snap, "KRX_BROWSER_JOB", target_date="20261127", ledger=ledger)
                self.assertEqual(bres["rows"], 1)
                br_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
