"""Full-day local config snapshot & run ledger buffer for Neon DB Scale-to-Zero.

Applies uniformly to EVERY daily and intraday batch (``3m``, ``5m``, ``1h``,
``daily_*``):
1. Pre-market (once/day): pull ``ops.api_mst``, ``ops.host_policy``,
   ``ops.duckdb_job``, and ``ref.user_*`` from Neon into a local JSON snapshot.
2. All day: every systemd timer reads only the local snapshot and appends its
   execution outcomes to a local NDJSON ledger (zero Neon connections during
   collection; immune to Neon latency or brief maintenance).
3. End of day (once/day): bulk-push the full day's ``LocalRunLedger`` entries
   to Neon ``ops.run_ledger`` in a single connection.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_DEFAULT_CACHE_DIR = Path(os.environ.get("V3_CACHE_DIR", "data/cache"))
_DEFAULT_LEDGER_DIR = Path(os.environ.get("V3_LEDGER_DIR", "data/logs/ledger"))


@dataclass
class DailyConfigSnapshot:
    generated_at: str
    apis: dict[str, dict[str, Any]] = field(default_factory=dict)
    host_policies: dict[str, dict[str, Any]] = field(default_factory=dict)
    duckdb_jobs: dict[str, dict[str, Any]] = field(default_factory=dict)
    user_tables: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def save(self, path: Path | None = None) -> Path:
        target = path or (_DEFAULT_CACHE_DIR / "config_snapshot.json")
        target.parent.mkdir(parents=True, exist_ok=True)
        # Atomic write via tmp + replace so overlapping timers never read a
        # half-written snapshot file.
        tmp = target.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(asdict(self), ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        os.replace(tmp, target)
        return target

    @classmethod
    def load(cls, path: Path | None = None) -> "DailyConfigSnapshot":
        target = path or (_DEFAULT_CACHE_DIR / "config_snapshot.json")
        payload = json.loads(target.read_text(encoding="utf-8"))
        return cls(
            generated_at=payload["generated_at"],
            apis=payload.get("apis", {}),
            host_policies=payload.get("host_policies", {}),
            duckdb_jobs=payload.get("duckdb_jobs", {}),
            user_tables=payload.get("user_tables", {}),
        )


@dataclass(frozen=True)
class RunLedgerEntry:
    api_id: str
    partition_key: str
    target_date: str | None
    status: str  # SUCCESS | FAILED | EMPTY | BLOCKED | SKIPPED
    error_code: str | None = None
    error_message: str | None = None
    record_count: int | None = None
    duration_ms: int | None = None
    attempt_no: int = 1
    output_uris: dict[str, str] = field(default_factory=dict)
    executed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class LocalRunLedger:
    """Append-only daily NDJSON execution ledger flushed once at end of day."""

    def __init__(self, day_str: str, base_dir: Path | None = None):
        self.day_str = day_str
        self.base_dir = base_dir or _DEFAULT_LEDGER_DIR
        self.path = self.base_dir / f"run_ledger_{day_str}.ndjson"
        self.flushed_marker = self.base_dir / f"run_ledger_{day_str}.flushed"

    def append(self, entry: RunLedgerEntry) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        line = json.dumps(asdict(entry), ensure_ascii=False, default=str) + "\n"
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(line)

    def read_all(self) -> list[RunLedgerEntry]:
        if not self.path.exists():
            return []
        entries: list[RunLedgerEntry] = []
        for raw in self.path.read_text(encoding="utf-8").splitlines():
            if not raw.strip():
                continue
            entries.append(RunLedgerEntry(**json.loads(raw)))
        return entries

    def succeeded_partitions(self, api_id: str) -> set[tuple[str, str | None]]:
        """Set of ``(partition_key, target_date)`` already succeeded today."""
        return {
            (e.partition_key, e.target_date)
            for e in self.read_all()
            if e.api_id == api_id and e.status == "SUCCESS"
        }

    def flush_to_neon(self, dsn: str | None = None) -> int:
        """Bulk-insert the full day's ledger entries into Neon ``ops.run_ledger``.

        Uses a single Neon connection with ``psycopg`` ``executemany`` so Neon
        wakes once at end of day and immediately returns to Scale-to-Zero.
        """
        entries = self.read_all()
        if not entries:
            return 0
        import psycopg

        conn_dsn = dsn or os.environ["NEON_DATABASE_URL"]
        sql = """
            INSERT INTO ops.run_ledger (
                api_id, partition_key, target_date, status,
                error_code, error_message, record_count, duration_ms,
                attempt_no, output_uris, executed_at
            ) VALUES (
                %(api_id)s, %(partition_key)s, %(target_date)s, %(status)s,
                %(error_code)s, %(error_message)s, %(record_count)s, %(duration_ms)s,
                %(attempt_no)s, %(output_uris)s::jsonb, %(executed_at)s::timestamptz
            )
        """
        rows = [
            {**asdict(e), "output_uris": json.dumps(e.output_uris, ensure_ascii=False)}
            for e in entries
        ]
        with psycopg.connect(conn_dsn, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
        self.flushed_marker.write_text(
            json.dumps({"flushed_at": datetime.now(timezone.utc).isoformat(), "count": len(rows)}),
            encoding="utf-8",
        )
        return len(rows)
