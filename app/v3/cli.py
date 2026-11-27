"""v3 CLI entrypoint (zero Oracle / typer / loguru dependencies).

Usage::

    python3 -m app.v3.cli sync-config [--mirror-user-parquet]
    python3 -m app.v3.cli dry-run <api_id> [--param K=V ...]
    python3 -m app.v3.cli run-job <job_id> [--type api|etl] [--date YYYYMMDD | --from YYYYMMDD --to YYYYMMDD] [--limit N]
    python3 -m app.v3.cli run-cycle <schedule_tag> [--date YYYYMMDD | --from YYYYMMDD --to YYYYMMDD]
    python3 -m app.v3.cli flush-log [--date YYYYMMDD]
    python3 -m app.v3.cli purge-raw [--keep-days 30] [--target NAME] [--apply]
    python3 -m app.v3.cli purge-daily <target_name> [--keep-months 3] [--apply]
    python3 -m app.v3.cli purge-log [--keep-days 90] [--apply]
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta

from app.v3.batch_cache import DailyConfigSnapshot, LocalRunLedger
from app.v3.cleanup import purge_old_job_logs, purge_raw_clean, purge_verified_daily
from app.v3.executor import dry_run_api, execute_job, execute_schedule_cycle, now_kst
from app.v3.neon_sync import load_dotenv_into_env, mirror_user_tables_to_parquet, pull_daily_snapshot


def iter_target_dates(
    target_date: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    *,
    include_weekends: bool = False,
) -> list[str]:
    """Resolve ``--date`` or ``--from``/``--to`` into an ordered list of ``YYYYMMDD`` dates."""
    if target_date and (date_from or date_to):
        raise ValueError("cannot combine --date with --from/--to")
    if date_from or date_to:
        if not (date_from and date_to):
            raise ValueError("both --from and --to are required for range backfill")
        d_start = datetime.strptime(date_from, "%Y%m%d").date()
        d_end = datetime.strptime(date_to, "%Y%m%d").date()
        if d_start > d_end:
            raise ValueError(f"--from ({date_from}) must be <= --to ({date_to})")
        days: list[str] = []
        cur = d_start
        while cur <= d_end:
            if include_weekends or cur.weekday() < 5:
                days.append(cur.strftime("%Y%m%d"))
            cur += timedelta(days=1)
        return days

    if target_date:
        datetime.strptime(target_date, "%Y%m%d")
        return [target_date]
    return [now_kst().strftime("%Y%m%d")]


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
    p_job.add_argument("--date", dest="target_date", default=None, help="Single target date YYYYMMDD (default: today KST)")
    p_job.add_argument("--from", dest="date_from", default=None, help="Backfill start date YYYYMMDD (inclusive)")
    p_job.add_argument("--to", dest="date_to", default=None, help="Backfill end date YYYYMMDD (inclusive)")
    p_job.add_argument("--include-weekends", action="store_true", help="Include Sat/Sun when using --from/--to")
    p_job.add_argument("--limit", type=int, default=None, help="Cap expanded partitions (for api_job pilots)")

    p_cyc = sub.add_parser("run-cycle", help="Run all active api_job + etl_job rows for a schedule_tag")
    p_cyc.add_argument("schedule_tag")
    p_cyc.add_argument("--date", dest="target_date", default=None, help="Single target date YYYYMMDD (default: today KST)")
    p_cyc.add_argument("--from", dest="date_from", default=None, help="Backfill start date YYYYMMDD (inclusive)")
    p_cyc.add_argument("--to", dest="date_to", default=None, help="Backfill end date YYYYMMDD (inclusive)")
    p_cyc.add_argument("--include-weekends", action="store_true", help="Include Sat/Sun when using --from/--to")

    p_flush = sub.add_parser("flush-log", help="Bulk-push full day's local ledger to Neon job_log")
    p_flush.add_argument("--date", dest="target_date", default=None, help="YYYYMMDD (default: today KST)")

    p_praw = sub.add_parser("purge-raw", help="Delete v3/raw_clean/ partitions older than N days (dry-run by default)")
    p_praw.add_argument("--keep-days", type=int, default=30, help="Retention window in days (default: 30)")
    p_praw.add_argument("--target", dest="target_name", default=None, help="Restrict to one raw_clean target folder")
    p_praw.add_argument("--apply", action="store_true", help="Actually delete matched objects (default: dry-run)")

    p_pday = sub.add_parser(
        "purge-daily",
        help="Delete v3/daily/<target>/ym=* older than N months ONLY after COUNT+HASH match with v3/monthly/",
    )
    p_pday.add_argument("target_name")
    p_pday.add_argument("--keep-months", type=int, default=3, help="Keep recent N months in daily/ (default: 3)")
    p_pday.add_argument("--apply", action="store_true", help="Actually delete verified daily files (default: dry-run)")

    p_plog = sub.add_parser("purge-log", help="Delete Neon job_log rows older than N days (dry-run by default)")
    p_plog.add_argument("--keep-days", type=int, default=90, help="Retention window in days (default: 90)")
    p_plog.add_argument("--apply", action="store_true", help="Actually delete rows in Neon (default: dry-run)")

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
        dates = iter_target_dates(
            args.target_date,
            args.date_from,
            args.date_to,
            include_weekends=args.include_weekends,
        )
        for day in dates:
            res = execute_job(
                snap,
                args.job_id,
                job_type=args.job_type,
                target_date=day,
                limit=args.limit,
            )
            print(json.dumps({"target_date": day, **res}, ensure_ascii=False))
        return 0

    if args.cmd == "run-cycle":
        snap = DailyConfigSnapshot.load()
        dates = iter_target_dates(
            args.target_date,
            args.date_from,
            args.date_to,
            include_weekends=args.include_weekends,
        )
        for day in dates:
            results = execute_schedule_cycle(snap, args.schedule_tag, target_date=day)
            for r in results:
                print(json.dumps({"target_date": day, **r}, ensure_ascii=False))
        return 0

    if args.cmd == "flush-log":
        day = args.target_date or now_kst().strftime("%Y%m%d")
        count = LocalRunLedger(day).flush_to_neon()
        print(f"flushed {count} log row(s) for {day} -> Neon job_log")
        return 0

    if args.cmd == "purge-raw":
        summary = purge_raw_clean(
            keep_days=args.keep_days,
            target_name=args.target_name,
            dry_run=not args.apply,
        )
        print(json.dumps(summary, ensure_ascii=False))
        return 0

    if args.cmd == "purge-daily":
        summary = purge_verified_daily(
            args.target_name,
            keep_months=args.keep_months,
            dry_run=not args.apply,
        )
        print(json.dumps(summary, ensure_ascii=False))
        return 0

    if args.cmd == "purge-log":
        summary = purge_old_job_logs(
            keep_days=args.keep_days,
            dry_run=not args.apply,
        )
        print(json.dumps(summary, ensure_ascii=False))
        return 0

    return 2


if __name__ == "__main__":
    sys.exit(main())
