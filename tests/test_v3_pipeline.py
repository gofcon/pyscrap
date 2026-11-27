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
                build_v3_key("curated", "kis_futopt_chart", "ym=202611", "monthly.parquet"),
                "v3/curated/kis_futopt_chart/ym=202611/monthly.parquet",
            )
            self.assertEqual(
                build_v3_key("ref", "mst_etf.parquet"),
                "v3/ref/mst_etf.parquet",
            )

    def test_build_v3_key_rejects_traversal_and_invalid_layer(self):
        with self.assertRaises(ValueError):
            build_v3_key("curated", "../v2_secret", "a.parquet")
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
            output_layer="curated",
            output_rel_path="kis_futopt_chart/ym=202611/monthly.parquet",
            targets=["oci", "r2"],
        )
        self.assertEqual(job.prefix, "v3")
        self.assertEqual(job.memory_limit, "1GB")


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
        self.assertEqual(v3_job["job_type"], "SCRAPE")
        self.assertEqual(v3_job["api_id"], "ACE_ETF_PDF")
        self.assertEqual(v3_job["job_spec_json"]["save_mode"], "overwrite")


if __name__ == "__main__":
    unittest.main()
