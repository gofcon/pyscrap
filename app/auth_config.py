"""Per-site scraper secret resolution.

Secrets referenced from DB config (e.g. ``api_mst.header_json`` holding an
API key as ``{"AUTH_KEY": "${KRX_AUTH_KEY}"}``) are resolved from
``os.environ`` at request-build time -- deliberately *not* declared as
``app.db_config.Settings`` fields one by one, since that would mean a code
change (new field + wiring) for every new scraped site's secret. This module
owns the ``.env`` -> ``os.environ`` load that makes that resolution work
(``app.db_config.Settings`` doesn't need it -- pydantic-settings reads ``.env``
into itself directly).
"""

import os
import re

from dotenv import find_dotenv, load_dotenv, set_key
from loguru import logger

_DOTENV_PATH = find_dotenv() or ".env"
load_dotenv(_DOTENV_PATH)

_ENV_VAR_RE = re.compile(r"\$\{(\w+)\}")


def resolve_env_placeholders(value: str) -> str:
    """Replace '${ENV_VAR_NAME}' with os.environ values. Left unchanged
    (with a warning) if the env var isn't set, rather than sending "${...}"
    literally to the site."""

    def _replace(m: re.Match) -> str:
        name = m.group(1)
        if name not in os.environ:
            logger.warning("env var '{}' referenced in header_json is not set", name)
            return m.group(0)
        return os.environ[name]

    return _ENV_VAR_RE.sub(_replace, value)


def persist_env_var(name: str, value: str) -> None:
    """Update NAME in both os.environ (this process) and the .env file on
    disk (so future runs pick it up without a manual copy-paste)."""
    os.environ[name] = value
    set_key(_DOTENV_PATH, name, value)

