"""Local runtime token/session store (replaces mutating ``.env`` for ``persist_env``).

Stores dynamic API tokens (e.g. ``KIS_ACCESS_TOKEN``, ``KIS_ACCESS_TOKEN_2``)
in ``data/cache/session_store.json`` with atomic writes and owner-only file
permissions (0600 on POSIX), while strictly blocking infrastructure secret
namespaces (``OCI_*``, ``S3_*``, ``NEON_*``, ``ORACLE_*``, etc.).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_DEFAULT_STORE_PATH = Path(os.environ.get("V3_SESSION_STORE_PATH", "data/cache/session_store.json"))
_DEFAULT_STATE_DIR = Path(os.environ.get("BROWSER_STATE_DIR", "data/state"))
_BLOCKED_PREFIXES = (
    "ORACLE_", "OCI_", "S3_", "NEON_", "PG", "POSTGRES_", "AWS_", "ADMIN_",
)
_BLOCKED_NAMES = frozenset({"DATABASE_URL", "SECRET_KEY", "PATH", "HOME", "USER"})


def _check_allowed_key(name: str) -> None:
    upper = name.strip().upper()
    if not upper or upper in _BLOCKED_NAMES or upper.startswith(_BLOCKED_PREFIXES):
        raise PermissionError(f"restricted key {name!r} cannot be stored in session_store")


def load_session_store(path: Path | None = None) -> dict[str, str]:
    target = path or _DEFAULT_STORE_PATH
    if not target.exists():
        return {}
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return {}
    tokens = raw.get("tokens", raw) if isinstance(raw, dict) else {}
    return {str(k): str(v) for k, v in tokens.items() if isinstance(v, (str, int, float))}


def get_session_token(name: str, *, path: Path | None = None) -> str | None:
    return load_session_store(path).get(name)


def save_session_tokens(updates: dict[str, Any], *, path: Path | None = None) -> dict[str, str]:
    """Persist one or more dynamic tokens atomically into ``session_store.json``."""
    if not updates:
        return {}
    target = path or _DEFAULT_STORE_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    current = load_session_store(target)
    written: dict[str, str] = {}
    for k, v in updates.items():
        if v is None or str(v).strip() == "":
            continue
        _check_allowed_key(k)
        val = str(v).strip()
        current[k] = val
        written[k] = val
    if not written:
        return {}

    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "tokens": current,
    }
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass
    os.replace(tmp, target)
    return written


def session_state_path(state_key: str, *, state_dir: Path | None = None) -> Path:
    """Path for Playwright/HTTP shared cookie storage state (``data/state/<key>.json``)."""
    safe = "".join(ch if (ch.isalnum() or ch in "._-") else "_" for ch in state_key).strip("._") or "default"
    return (state_dir or _DEFAULT_STATE_DIR) / f"{safe}.json"


def save_http_cookies(
    state_key: str,
    cookies: list[dict[str, Any]],
    *,
    state_dir: Path | None = None,
) -> Path:
    """Persist HTTP cookies in Playwright-compatible storage_state JSON format."""
    path = session_state_path(state_key, state_dir=state_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "cookies": cookies,
        "origins": [],
    }
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass
    os.replace(tmp, path)
    return path


def load_http_cookies(state_key: str, *, state_dir: Path | None = None) -> list[dict[str, Any]]:
    """Load saved cookies from ``data/state/<state_key>.json`` (empty list if missing)."""
    path = session_state_path(state_key, state_dir=state_dir)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    cookies = data.get("cookies", []) if isinstance(data, dict) else []
    return [c for c in cookies if isinstance(c, dict) and c.get("name") and c.get("value") is not None]


def clear_session_state(state_key: str, *, state_dir: Path | None = None) -> None:
    session_state_path(state_key, state_dir=state_dir).unlink(missing_ok=True)
