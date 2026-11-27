"""Strict Pydantic v2 schemas for v3 ``ops.api_mst.spec_json`` and ``ops.duckdb_job``.

``spec_json`` is split into two top-level namespaces:
- ``engine`` (strict, ``extra="forbid"``): the frozen set of primitives the
  Python engine understands. Typos fail validation immediately.
- ``ext`` (free-form ``dict[str, Any]``): ignored by Python so DB/SQL operators
  can attach custom metadata (owner, retention, reporting flags, SQL hints)
  without ever changing Python code or table DDL.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RequestSpec(_StrictModel):
    method: Literal["GET", "POST"] | None = None
    url: str = Field(min_length=1)
    headers: dict[str, Any] = Field(default_factory=dict)
    payload_type: Literal["json", "data", "content"] | None = None
    payload_json: dict[str, Any] | None = None
    payload_xml: str | None = None
    behavior: list[dict[str, Any]] | None = None


class PagePagination(_StrictModel):
    mode: Literal["page"] = "page"
    param: str = "PAGE"
    start: int = Field(default=1, ge=1)
    max_pages: int = Field(default=100, ge=1)


class CursorPagination(_StrictModel):
    mode: Literal["cursor"] = "cursor"
    param: str
    from_path: str = Field(alias="from")
    pick: Literal["min", "max"] = "min"
    min_records: int | None = Field(default=None, ge=0)
    max_pages: int = Field(default=100, ge=1)

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class TokenPagination(_StrictModel):
    mode: Literal["token"] = "token"
    params: dict[str, str]
    continue_when: dict[str, Any] | None = None
    max_pages: int = Field(default=100, ge=1)


class BrowserPagination(_StrictModel):
    mode: Literal["click", "scroll"]
    selector: str | None = None
    wait_for: str | None = None
    max_pages: int = Field(default=20, ge=1)


PaginationSpec = Annotated[
    PagePagination | CursorPagination | TokenPagination | BrowserPagination,
    Field(discriminator="mode"),
]


class ExtractSpec(_StrictModel):
    response_type: Literal[
        "auto", "json", "xml", "delimited", "zip_delimited",
        "html_table", "html_block", "xlsx", "xls", "binary",
        "dom", "xhr", "session",
    ] = "auto"
    selector: str = "."
    parse: dict[str, Any] = Field(default_factory=dict)
    merge_fields: dict[str, str] = Field(default_factory=dict)
    persist_env: dict[str, str] = Field(default_factory=dict)


CleanseFilterName = Literal[
    "strip",
    "blank_to_null",
    "dash_to_null",
    "strip_comma",
    "digits_only",
    "to_float",
    "to_int",
    "to_str",
    "lower",
    "upper",
]


class CleanseSpec(_StrictModel):
    """Config-driven mechanical cleansing replacing per-vendor ORM mixins."""

    lowercase_keys: bool = True
    rename: dict[str, str] = Field(default_factory=dict)
    rules: dict[str, list[CleanseFilterName]] = Field(default_factory=dict)
    keep_columns: list[str] | None = None
    drop_columns: list[str] = Field(default_factory=list)


class ContractSpec(_StrictModel):
    """Pre-sink data quality checks preventing silent 200-OK garbage loads."""

    empty_policy: Literal["succeed", "pending", "keep_previous", "fail"] = "succeed"
    min_rows: int = Field(default=0, ge=0)
    not_null_fields: list[str] = Field(default_factory=list)
    forbidden_values: list[str] = Field(default_factory=list)
    unique_keys: list[str] = Field(default_factory=list)


class SessionSpec(_StrictModel):
    login: str | None = None
    logged_out: str | None = None
    blocked: str | None = None
    expect: str | None = None
    state: str | None = None


class PacingSpec(_StrictModel):
    min_interval_sec: float | None = Field(default=None, ge=0)
    page_delay_sec: float | None = Field(default=None, ge=0)
    timeout_sec: float | None = Field(default=None, gt=0)


class SinkSpec(_StrictModel):
    """Where cleansed Parquet (or binary) artifacts land under ``v3/``.

    ``targets`` supports multi-bucket publishing (e.g. ``["oci"]`` for canonical
    storage, ``["oci", "r2"]`` to also publish into Cloudflare R2 for Web UI).
    """

    target_name: str = Field(min_length=1)
    layer: Literal["raw_clean", "curated", "ref", "mart", "binary"] = "raw_clean"
    prefix: str = "v3"
    targets: list[str] = Field(default_factory=lambda: ["oci"], min_length=1)
    mode: Literal["replace_partition", "append_snapshot", "upsert"] = "replace_partition"
    row_stamp: dict[str, str] = Field(default_factory=dict)
    job_key_params: list[str] = Field(default_factory=list)
    is_repeated: bool = False
    snapshot_column: str | None = None
    staging: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_snapshot(self) -> "SinkSpec":
        if self.snapshot_column and not self.is_repeated:
            raise ValueError("snapshot_column requires is_repeated=True")
        return self


class EngineSpec(_StrictModel):
    request: RequestSpec
    pagination: PaginationSpec | None = None
    extract: ExtractSpec = Field(default_factory=ExtractSpec)
    cleanse: CleanseSpec = Field(default_factory=CleanseSpec)
    contract: ContractSpec = Field(default_factory=ContractSpec)
    session: SessionSpec = Field(default_factory=SessionSpec)
    pacing: PacingSpec = Field(default_factory=PacingSpec)
    sink: SinkSpec | None = None


class ApiSpec(_StrictModel):
    """Root schema for ``ops.api_mst.spec_json``."""

    engine: EngineSpec
    ext: dict[str, Any] = Field(default_factory=dict)


class DuckDBJobSpec(_StrictModel):
    """Config for a DuckDB SQL post-processing / compaction / master job."""

    job_name: str = Field(min_length=1)
    sql: str = Field(min_length=1)
    output_layer: Literal["curated", "ref", "mart"] = "curated"
    output_rel_path: str = Field(min_length=1)
    prefix: str = "v3"
    targets: list[str] = Field(default_factory=lambda: ["oci"], min_length=1)
    memory_limit: str = "1GB"
    threads: int = Field(default=2, ge=1, le=8)
    min_rows: int = Field(default=0, ge=0)


def export_spec_json_schema() -> dict[str, Any]:
    """Generate JSON Schema for DB/API validation and admin form rendering."""
    return ApiSpec.model_json_schema()
