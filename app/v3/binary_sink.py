"""Safe binary/ZIP staging and ``v3/binary/`` multi-target S3 uploader.

Handles ``sink_type='BINARY'`` downloads from both HTTP (e.g. ``DART_DOCUMENT``)
and Playwright browser download steps, with Zip Slip neutralization and SHA-256
metadata records returned for optional catalog Parquet logging.
"""

from __future__ import annotations

import hashlib
import io
import zipfile
from pathlib import Path
from typing import Any, Iterable

from app.object_storage import build_v3_key, upload_bytes_to_targets
from app.v3.http_fetcher import substitute_placeholders


def _safe_filename(name: str, fallback: str = "download.bin") -> str:
    """Strip directory components and reject path traversal (Zip Slip guard)."""
    cleaned = Path(name.replace("\\", "/")).name.strip()
    if not cleaned or cleaned in (".", ".."):
        return fallback
    return cleaned


def publish_binary_payload(
    content: bytes,
    *,
    staging: dict[str, Any],
    params: dict[str, Any],
    fallback_name: str = "download.bin",
    targets: Iterable[str] = ("oci",),
    prefix: str | None = None,
    skip_unconfigured_targets: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Upload raw or unzipped binary bytes to ``v3/binary/<group>/<filename>``.

    Returns ``(metadata_records, last_uploaded_uris)``.
    """
    group = _safe_filename(str(staging.get("group") or "misc"), fallback="misc")
    raw_name = str(staging.get("name") or fallback_name)
    resolved_name = _safe_filename(substitute_placeholders(raw_name, params), fallback=_safe_filename(fallback_name))
    unzip = bool(staging.get("unzip", False))

    items: list[tuple[str, bytes]] = []
    if unzip and zipfile.is_zipfile(io.BytesIO(content)):
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                member_name = _safe_filename(info.filename, fallback="")
                if not member_name:
                    continue
                items.append((member_name, zf.read(info.filename)))
    if not items:
        items = [(resolved_name, content)]

    records: list[dict[str, Any]] = []
    last_uris: dict[str, str] = {}
    for fname, body in items:
        key = build_v3_key("binary", group, fname, prefix=prefix)
        uris = upload_bytes_to_targets(
            body,
            key,
            targets=targets,
            skip_unconfigured=skip_unconfigured_targets,
        )
        last_uris = uris
        records.append({
            "group_name": group,
            "file_name": fname,
            "object_key": key,
            "size_bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
            "oci_uri": uris.get("oci"),
        })
    return records, last_uris
