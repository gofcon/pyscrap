"""Logging setup, called once at process startup by every entrypoint
(app.cli's batch commands, app.api.main's FastAPI app).

Every systemd-timer invocation is its own short-lived process (Type=oneshot)
that calls setup_logging() once at startup -- stderr is still captured by
journald as before, but that alone depends on the instance's journald
retention config (volatile vs persistent, SystemMaxUse=, ...) for how long
anything actually survives. A file sink here is the app's own, predictable
retention: it doesn't depend on that config, and unlike ApiJobLog.error_message
(truncated to 4000 chars) it keeps the full traceback. app.api.main is
long-running rather than one-shot, but the same sink config still applies --
just accumulating across the process's whole lifetime instead of one tick.
"""

import os
import sys
from pathlib import Path

from loguru import logger

from app.db_config import settings

_LOG_DIR = Path(os.environ.get("LOG_DIR", "data/logs"))


def setup_logging() -> None:
    logger.remove()
    logger.add(sys.stderr, level=settings.log_level)

    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger.add(
        _LOG_DIR / "pyscrap.log",
        level=settings.log_level,
        rotation="00:00",  # roll over to a new file at midnight
        retention="30 days",  # delete rotated files older than this
        compression="zip",  # rotated files get compressed
        enqueue=True,  # process-safe appends (relevant if two timers overlap)
        backtrace=True,
        diagnose=True,  # full local-variable dump in tracebacks, not just the message
    )
