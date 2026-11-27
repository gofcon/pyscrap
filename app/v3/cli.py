"""v3 CLI entrypoint (zero Oracle / typer / loguru dependencies).

Usage::

    python3 -m app.v3.cli sync-config [--mirror-user-parquet]
    python3 -m app.v3.cli dry-run <api_id> [--param K=V ...]
    python3 -m app.v3.cli run-job <job_id> [--type api|etl] [--date YYYYMMDD] [--limit N]
    python3 -m app.v3.cli run-cycle <schedule_tag> [--date YYYYMMDD]
    python3 -m app.v3.cli flush-log [--date YYYYMMDD]
"""

from __future__ import annotations

import argparse
import json
import sys

from app.v3.batch_cache import DailyConfigSnapshot, LocalRunLedger
from app.v3.executor import dry_run_api, execute_job, execute_schedule_cycle, now_kst
from app.v3.neon_sync import load_dotenv_into_env, mirror_user_tables_to_parquet, pull_daily_snapshot


def _parse_kv(items: list[str] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in items or []:
        if "=" not in item:
            raise SystemExit(f"expected KEY=VALUE for --param, got {item!r}")
        k, v = item.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def main(argv: list[str] | None = None) -> int:
    load_dotenv_into_env()

    parser = argparse.ArgumentParser(prog="python3 -m app.v3.cli", description="pyscrap v3 batch CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_sync = sub.add_parser("sync-config", help="Pull daily snapshot from Neon DB (1 connection/day)")
    p_sync.add_argument("--mirror-user-parquet", action="store_true",
                        help="Also upload manual user_* tables to v3/ref/*.parquet")

    p_dry = sub.add_parser("dry-run", help="Test 1 API fetch + cleanse + contract without uploading")
    p_dry.add_argument("api_id")
    p_dry.add_argument("--param", action="append", help="Repeatable KEY=VALUE")
    p_dry.add_argument("--sample", type=int, default=3)

    p_job = sub.add_parser("run-job", help="Run one api_job or etl_job from local snapshot")
    p_job.add_argument("job_id")
    p_job.add_argument("--type", dest="job_type", choices=["api", "etl", "API", "ETL"], default=None,
                       help="Optional explicit job type (auto-detected if omitted)")
    p_job.add_argument("--date", dest="target_date", default=None, help="YYYYMMDD (default: today KST)")
    p_job.add_argument("--limit", type=int, default=None, help="Cap expanded partitions (for api_job pilots)")

    p_cyc = sub.add_parser("run-cycle", help="Run all active api_job + etl_job rows for a schedule_tag")
    p_cyc.add_argument("schedule_tag")
    p_cyc.add_argument("--date", dest="target_date", default=None, help="YYYYMMDD (default: today KST)")

    p_flush = sub.add_parser("flush-log", help="Bulk-push full day's local ledger to Neon job_log")
    p_flush.add_argument("--date", dest="target_date", default=None, help="YYYYMMDD (default: today KST)")

    args = parser.parse_args(argv)

    if args.cmd == "sync-config":
        snap, path = pull_daily_snapshot()
        print(
            f"synced snapshot -> {path} "
            f"(api_mst={len(snap.apis)}, api_job={len(snap.api_jobs)}, "
            f"etl_job={len(snap.etl_jobs)}, host_policy={len(snap.host_policies)})"
        )
        if args.mirror_user_parquet:
            uris = mirror_user_tables_to_parquet(snap)
            for tbl, u in uris.items():
                print(f"  mirrored {tbl} -> {u}")
        return 0

    if args.cmd == "dry-run":
        snap = DailyConfigSnapshot.load()
        res = dry_run_api(snap, args.api_id, _parse_kv(args.param), sample_limit=args.sample)
        print(
            f"{res.api_id}: raw={res.raw_count} cleaned={res.cleaned_count} "
            f"contract={res.contract.action} (ok={res.contract.ok})"
        )
        print(f"columns ({len(res.columns)}): {res.columns}")
        if res.contract.violations:
            print(f"violations: {res.contract.violations}")
        for idx, row in enumerate(res.sample_rows, 1):
            print(f"  [{idx}] {json.dumps(row, ensure_ascii=False, default=str)}")
        return 0 if res.contract.ok else 1

    if args.cmd == "run-job":
        snap = DailyConfigSnapshot.load()
        res = execute_job(
            snap,
            args.job_id,
            job_type=args.job_type,
            target_date=args.target_date,
            limit=args.limit,
        )
        print(json.dumps(res, ensure_ascii=False))
        return 0

    if args.cmd == "run-cycle":
        snap = DailyConfigSnapshot.load()
        results = execute_schedule_cycle(snap, args.schedule_tag, target_date=args.target_date)
        for r in results:
            print(json.dumps(r, ensure_ascii=False))
        return 0

    if args.cmd == "flush-log":
        day = args.target_date or now_kst().strftime("%Y%m%d")
        count = LocalRunLedger(day).flush_to_neon()
        print(f"flushed {count} log row(s) for {day} -> Neon job_log")
        return 0

    return 2


if __name__ == "__main__":
    sys.exit(main())
