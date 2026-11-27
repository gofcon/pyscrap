"""Multi-target S3-compatible object storage client (OCI primary + Cloudflare R2 addition).

v3 supports multiple named S3 targets simultaneously (e.g. ``oci`` as the
20GB primary canonical bucket and ``r2`` as an additional 10GB serving bucket
for Web UI / frequent DuckDB reads), all writing under the ``v3/`` key prefix
by default so v3 artifacts stay completely isolated from v2 objects.

Backwards compatible with v2 callers: ``get_client()`` and ``bucket_name()``
default to the ``"oci"`` target and fall back to legacy ``OCI_*`` env vars.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urlsplit

V3_DEFAULT_PREFIX = "v3"
VALID_V3_LAYERS = frozenset({"raw_clean", "curated", "ref", "mart", "binary"})


@dataclass(frozen=True)
class S3TargetConfig:
    name: str
    endpoint_url: str
    bucket: str
    access_key: str
    secret_key: str
    region: str = "auto"

    @property
    def endpoint_host(self) -> str:
        """Host[:port] without scheme, as DuckDB ``CREATE SECRET`` expects."""
        parsed = urlsplit(self.endpoint_url)
        return parsed.netloc or parsed.path.rstrip("/")

    @property
    def use_ssl(self) -> bool:
        return not self.endpoint_url.lower().startswith("http://")


def v3_prefix() -> str:
    """Bucket key prefix for all v3 artifacts (default ``'v3'``)."""
    raw = os.environ.get("V3_BUCKET_PREFIX", V3_DEFAULT_PREFIX).strip().strip("/")
    return raw or V3_DEFAULT_PREFIX


def build_v3_key(layer: str, *parts: str, prefix: str | None = None) -> str:
    """Build a normalized ``v3/<layer>/<part1>/...`` object key.

    Rejects empty segments and ``..`` path traversal so callers cannot escape
    the ``v3/`` namespace into v2 objects.
    """
    if layer not in VALID_V3_LAYERS:
        raise ValueError(f"unknown v3 layer {layer!r}, expected one of {sorted(VALID_V3_LAYERS)}")
    root = (prefix if prefix is not None else v3_prefix()).strip().strip("/")
    cleaned: list[str] = [root, layer] if root else [layer]
    for raw in parts:
        for segment in str(raw).replace("\\", "/").split("/"):
            seg = segment.strip()
            if not seg or seg == ".":
                continue
            if seg == "..":
                raise ValueError(f"path traversal segment '..' is not allowed in object key: {parts!r}")
            cleaned.append(seg)
    return "/".join(cleaned)


def _load_target_from_env(name: str) -> S3TargetConfig | None:
    key = name.strip().upper()
    prefix = f"S3_{key}_"

    endpoint = os.environ.get(f"{prefix}ENDPOINT_URL")
    bucket = os.environ.get(f"{prefix}BUCKET")
    access_key = os.environ.get(f"{prefix}ACCESS_KEY")
    secret_key = os.environ.get(f"{prefix}SECRET_KEY")
    region = os.environ.get(f"{prefix}REGION", "auto")

    if endpoint and bucket and access_key and secret_key:
        return S3TargetConfig(
            name=name.lower(),
            endpoint_url=endpoint.rstrip("/"),
            bucket=bucket,
            access_key=access_key,
            secret_key=secret_key,
            region=region,
        )

    # Legacy OCI_* fallback for the default "oci" target so v2 .env keeps working.
    if key == "OCI":
        ns = os.environ.get("OCI_NAMESPACE")
        oci_region = os.environ.get("OCI_REGION")
        oci_bucket = os.environ.get("OCI_BUCKET_NAME")
        oci_ak = os.environ.get("OCI_ACCESS_KEY")
        oci_sk = os.environ.get("OCI_SECRET_KEY")
        if ns and oci_region and oci_bucket and oci_ak and oci_sk:
            return S3TargetConfig(
                name="oci",
                endpoint_url=f"https://{ns}.compat.objectstorage.{oci_region}.oraclecloud.com",
                bucket=oci_bucket,
                access_key=oci_ak,
                secret_key=oci_sk,
                region=oci_region,
            )
    return None


def get_target_config(target: str = "oci") -> S3TargetConfig:
    """Return S3 configuration for a named target (e.g. ``'oci'``, ``'r2'``)."""
    cfg = _load_target_from_env(target)
    if cfg is None:
        upper = target.strip().upper()
        raise KeyError(
            f"S3 target {target!r} is not configured "
            f"(set S3_{upper}_ENDPOINT_URL, S3_{upper}_BUCKET, "
            f"S3_{upper}_ACCESS_KEY, S3_{upper}_SECRET_KEY)"
        )
    return cfg


def configured_targets(candidates: Iterable[str] = ("oci", "r2")) -> list[str]:
    """List which S3 targets are currently configured in the environment."""
    return [name for name in candidates if _load_target_from_env(name) is not None]


def get_client(target: str = "oci"):
    """Create a boto3 S3 client for the named target.

    Disables flexible checksum streaming (``aws-chunked``) because OCI's
    S3-compatible endpoint rejects it; the same Config also works cleanly
    against Cloudflare R2 and standard S3.
    """
    import boto3
    from botocore.config import Config

    cfg = get_target_config(target)
    return boto3.client(
        "s3",
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        endpoint_url=cfg.endpoint_url,
        region_name=cfg.region,
        config=Config(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            s3={"payload_signing_enabled": False},
        ),
    )


def bucket_name(target: str = "oci") -> str:
    return get_target_config(target).bucket


def upload_bytes_to_targets(
    data: bytes,
    key: str,
    targets: Iterable[str] = ("oci",),
    *,
    skip_unconfigured: bool = False,
) -> dict[str, str]:
    """Upload ``data`` to one or more S3 targets (e.g. ``('oci', 'r2')``).

    Returns ``{target_name: 's3://<bucket>/<key>'}`` for every target written.
    """
    uploaded: dict[str, str] = {}
    for target in targets:
        cfg = _load_target_from_env(target)
        if cfg is None:
            if skip_unconfigured:
                continue
            raise KeyError(f"S3 target {target!r} is not configured")
        client = get_client(target)
        client.put_object(Bucket=cfg.bucket, Key=key, Body=data)
        uploaded[cfg.name] = f"s3://{cfg.bucket}/{key}"
    return uploaded


def upload_file_to_targets(
    local_path: Path,
    key: str,
    targets: Iterable[str] = ("oci",),
    *,
    delete_local: bool = False,
    skip_unconfigured: bool = False,
) -> dict[str, str]:
    """Upload a local file to every requested S3 target and optionally delete it."""
    body = local_path.read_bytes()
    uploaded = upload_bytes_to_targets(
        body,
        key,
        targets=targets,
        skip_unconfigured=skip_unconfigured,
    )
    if delete_local and uploaded:
        local_path.unlink(missing_ok=True)
    return uploaded


def _validate_v3_cleanup_prefix(key_prefix: str, *, prefix: str | None = None) -> str:
    """Ensure cleanup prefixes are strictly scoped beneath ``v3/<layer>/<subpath>``.

    Prevents accidental wipes of the bucket root, v1/v2 prefixes, or an entire
    v3 layer (e.g. ``v3/curated/``).
    """
    norm = key_prefix.replace("\\", "/").lstrip("/")
    if not norm or ".." in norm.split("/"):
        raise ValueError(f"invalid cleanup prefix: {key_prefix!r}")
    root = (prefix if prefix is not None else v3_prefix()).strip().strip("/")
    parts = [p for p in norm.split("/") if p]
    if root:
        if len(parts) < 3 or parts[0] != root or parts[1] not in VALID_V3_LAYERS:
            raise ValueError(
                f"cleanup prefix {key_prefix!r} must be scoped under '{root}/<layer>/<target>/...'"
            )
    else:
        if len(parts) < 2 or parts[0] not in VALID_V3_LAYERS:
            raise ValueError(f"cleanup prefix {key_prefix!r} must be scoped under '<layer>/<target>/...'")
    return norm


def list_keys_with_prefix(key_prefix: str, target: str = "oci") -> list[str]:
    """List every object key under ``key_prefix`` in the given S3 target."""
    cfg = get_target_config(target)
    client = get_client(target)
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=cfg.bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def delete_keys_from_targets(
    keys: Iterable[str],
    targets: Iterable[str] = ("oci",),
    *,
    skip_unconfigured: bool = True,
) -> dict[str, int]:
    """Delete explicit object keys from one or more S3 targets."""
    key_list = [k for k in keys if k]
    if not key_list:
        return {}
    counts: dict[str, int] = {}
    for target in targets:
        cfg = _load_target_from_env(target)
        if cfg is None:
            if skip_unconfigured:
                continue
            raise KeyError(f"S3 target {target!r} is not configured")
        client = get_client(target)
        deleted = 0
        for i in range(0, len(key_list), 1000):
            chunk = key_list[i : i + 1000]
            try:
                client.delete_objects(
                    Bucket=cfg.bucket,
                    Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
                )
                deleted += len(chunk)
            except Exception:
                # Some S3-compatible gateways reject batch delete_objects; fall back safely.
                for k in chunk:
                    client.delete_object(Bucket=cfg.bucket, Key=k)
                    deleted += 1
        counts[cfg.name] = deleted
    return counts


def delete_prefix_from_targets(
    key_prefix: str,
    targets: Iterable[str] = ("oci",),
    *,
    exclude_keys: Iterable[str] = (),
    dry_run: bool = False,
    skip_unconfigured: bool = True,
    prefix: str | None = None,
) -> dict[str, list[str]]:
    """Delete every object under a validated ``v3/<layer>/<subpath>`` prefix.

    Returns ``{target_name: [deleted_or_matched_keys]}``.
    """
    norm = _validate_v3_cleanup_prefix(key_prefix, prefix=prefix)
    excluded = set(exclude_keys)
    result: dict[str, list[str]] = {}
    for target in targets:
        cfg = _load_target_from_env(target)
        if cfg is None:
            if skip_unconfigured:
                continue
            raise KeyError(f"S3 target {target!r} is not configured")
        matched = [k for k in list_keys_with_prefix(norm, target=target) if k not in excluded]
        if matched and not dry_run:
            delete_keys_from_targets(matched, targets=(target,), skip_unconfigured=skip_unconfigured)
        result[cfg.name] = matched
    return result
