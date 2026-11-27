"""Ingestion layer: HTTP fetcher, Playwright browser runner, and mechanical cleansing."""

from app.ingest.cleanse import ContractResult, apply_cleanse, evaluate_contract

__all__ = ["ContractResult", "apply_cleanse", "evaluate_contract"]
