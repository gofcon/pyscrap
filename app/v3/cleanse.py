"""Pure-function mechanical cleansing and pre-sink data contract checks.

Replaces per-vendor ORM mixins (``VendorRecordBase``, ``MdcRecordBase``,
``EtfPdfDateMixin``) with composable, config-driven filters so adding a new
site or adapting to a vendor formatting quirk needs zero Python code changes.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Literal

from app.v3.spec import CleanseSpec, ContractSpec

_NUMERIC_COMMA_RE = re.compile(r"^-?[\d,]+(?:\.\d+)?$")
_NON_DIGIT_RE = re.compile(r"\D")


def _f_strip(value: Any) -> Any:
    return value.strip() if isinstance(value, str) else value


def _f_blank_to_null(value: Any) -> Any:
    if isinstance(value, str) and not value.strip():
        return None
    return value


def _f_dash_to_null(value: Any) -> Any:
    if isinstance(value, str) and value.strip() == "-":
        return None
    return value


def _f_strip_comma(value: Any) -> Any:
    if isinstance(value, str):
        s = value.strip()
        if "," in s and _NUMERIC_COMMA_RE.match(s):
            return s.replace(",", "")
    return value


def _f_digits_only(value: Any) -> Any:
    if isinstance(value, str):
        digits = _NON_DIGIT_RE.sub("", value)
        return digits or None
    return value


def _f_to_float(value: Any) -> Any:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip().replace(",", "")
        if not s or s == "-":
            return None
        return float(s)
    return value


def _f_to_int(value: Any) -> Any:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        s = value.strip().replace(",", "")
        if not s or s == "-":
            return None
        return int(float(s))
    return value


def _f_to_str(value: Any) -> Any:
    return None if value is None else str(value)


def _f_lower(value: Any) -> Any:
    return value.lower() if isinstance(value, str) else value


def _f_upper(value: Any) -> Any:
    return value.upper() if isinstance(value, str) else value


_FILTERS: dict[str, Callable[[Any], Any]] = {
    "strip": _f_strip,
    "blank_to_null": _f_blank_to_null,
    "dash_to_null": _f_dash_to_null,
    "strip_comma": _f_strip_comma,
    "digits_only": _f_digits_only,
    "to_float": _f_to_float,
    "to_int": _f_to_int,
    "to_str": _f_to_str,
    "lower": _f_lower,
    "upper": _f_upper,
}


def apply_cleanse(
    records: list[dict[str, Any]],
    spec: CleanseSpec,
    *,
    key_params: dict[str, Any] | None = None,
    meta: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Apply key folding, renaming, column filters, and metadata stamping.

    Merge order matches v2 invariant: ``meta`` -> ``key_params`` -> ``record``,
    so a genuine field from the upstream response wins over stamped job params.
    """
    rename_map = spec.rename
    if spec.lowercase_keys:
        rename_map = {k.lower(): v.lower() for k, v in spec.rename.items()}

    wildcard_rules = spec.rules.get("*", [])
    col_rules = {
        (k.lower() if spec.lowercase_keys else k): v
        for k, v in spec.rules.items()
        if k != "*"
    }
    keep_set = (
        {c.lower() if spec.lowercase_keys else c for c in spec.keep_columns}
        if spec.keep_columns is not None
        else None
    )
    drop_set = {c.lower() if spec.lowercase_keys else c for c in spec.drop_columns}

    cleaned: list[dict[str, Any]] = []
    for raw in records:
        merged: dict[str, Any] = {}
        for source in (meta or {}, key_params or {}, raw):
            for k, v in source.items():
                key = str(k).lower() if spec.lowercase_keys else str(k)
                key = rename_map.get(key, key)
                merged[key] = v

        out: dict[str, Any] = {}
        for k, v in merged.items():
            if keep_set is not None and k not in keep_set:
                continue
            if k in drop_set:
                continue
            for fname in wildcard_rules:
                v = _FILTERS[fname](v)
            for fname in col_rules.get(k, ()):
                v = _FILTERS[fname](v)
            out[k] = v
        cleaned.append(out)
    return cleaned


@dataclass(frozen=True)
class ContractResult:
    action: Literal["commit", "pending", "keep_previous", "fail"]
    ok: bool
    row_count: int
    violations: list[str] = field(default_factory=list)


def evaluate_contract(
    records: list[dict[str, Any]],
    contract: ContractSpec,
) -> ContractResult:
    """Validate cleansed records against pre-sink quality rules."""
    count = len(records)
    if count == 0:
        if contract.empty_policy == "pending":
            return ContractResult(action="pending", ok=True, row_count=0)
        if contract.empty_policy == "keep_previous":
            return ContractResult(action="keep_previous", ok=True, row_count=0)
        if contract.empty_policy == "fail" or contract.min_rows > 0:
            return ContractResult(
                action="fail",
                ok=False,
                row_count=0,
                violations=[f"empty result (min_rows={contract.min_rows}, empty_policy={contract.empty_policy})"],
            )
        return ContractResult(action="commit", ok=True, row_count=0)

    violations: list[str] = []
    if count < contract.min_rows:
        violations.append(f"row_count={count} < min_rows={contract.min_rows}")

    for field_name in contract.not_null_fields:
        nulls = sum(1 for r in records if r.get(field_name) is None)
        if nulls:
            violations.append(f"field {field_name!r} has {nulls}/{count} null value(s)")

    if contract.forbidden_values:
        forbidden = set(contract.forbidden_values)
        for idx, rec in enumerate(records):
            for k, v in rec.items():
                if isinstance(v, str) and v.strip() in forbidden:
                    violations.append(f"row {idx} field {k!r} matched forbidden value {v!r}")
                    break
            if len(violations) >= 10:
                break

    if contract.unique_keys:
        seen: set[tuple[Any, ...]] = set()
        dups = 0
        for rec in records:
            key = tuple(rec.get(k) for k in contract.unique_keys)
            if key in seen:
                dups += 1
            else:
                seen.add(key)
        if dups:
            violations.append(f"unique_keys {contract.unique_keys} has {dups} duplicate row(s)")

    if violations:
        return ContractResult(action="fail", ok=False, row_count=count, violations=violations)
    return ContractResult(action="commit", ok=True, row_count=count)
