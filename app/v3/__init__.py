"""pyscrap v3 pipeline components (Neon DB + Multi-S3 Parquet + DuckDB)."""

from app.v3.batch_cache import DailyConfigSnapshot, LocalRunLedger, RunLedgerEntry
from app.v3.cleanse import ContractResult, apply_cleanse, evaluate_contract
from app.v3.spec import (
    ApiSpec,
    CleanseSpec,
    ContractSpec,
    DuckDBJobSpec,
    EngineSpec,
    SinkSpec,
    export_spec_json_schema,
)

__all__ = [
    "ApiSpec",
    "CleanseSpec",
    "ContractResult",
    "ContractSpec",
    "DailyConfigSnapshot",
    "DuckDBJobSpec",
    "EngineSpec",
    "LocalRunLedger",
    "RunLedgerEntry",
    "SinkSpec",
    "apply_cleanse",
    "evaluate_contract",
    "export_spec_json_schema",
]
