import oracledb
import typer
from loguru import logger
from sqlalchemy import text
from sqlmodel import Session

from app.db.engine import engine
from app.logging_config import setup_logging
from app.services.discovery import discover_period
from app.services.execution import generate_jobs, generate_jobs_for_builder, run_cycle
from app.services.export import finalize_pending_exports, purge_csv_buffers
from app.services.sql_sources import compare as compare_sql, pull as pull_sql

app = typer.Typer()

# How many DBMS_OUTPUT lines to fetch per round trip. Not a limit on what a
# procedure may write -- the buffer is read until it is empty.
_OUTPUT_BATCH_LINES = 200


@app.callback()
def main():
    """pyscrap batch entrypoints (run from systemd timers/services)."""


@app.command("generate-jobs")
def generate_jobs_cmd(
    execution_cycle: str = typer.Argument(..., help="ApiJobBuilder.execution_cycle to generate, e.g. daily, 5m, 1h"),
):
    """Generate every active ApiJobBuilder's job(s) for this execution_cycle
    (see app.services.execution.generate_jobs) -- separate from actually
    executing them (see run-cycle). Call this before run-cycle for the same
    execution_cycle."""
    setup_logging()

    with Session(engine) as session:
        created = generate_jobs(session, execution_cycle)
        # job_ids extracted while the session is still open: `created` holds
        # ORM instances, and Session.commit() (inside generate_jobs, via
        # build_jobs_from_builder) expires their attributes by default -- an
        # attribute access after the `with` block closes then has to reload
        # from the DB and fails with DetachedInstanceError instead (verified
        # live -- intermittent, since a job whose job_id happened to get
        # re-read for some other reason before session close stays cached
        # and doesn't hit this).
        job_ids = [job.job_id for job in created]

    typer.echo(f"{execution_cycle}: generated {len(job_ids)} new job(s)")
    for job_id in job_ids:
        typer.echo(f"  {job_id}")


@app.command("generate-builder")
def generate_builder_cmd(
    build_id: str = typer.Argument(..., help="ApiJobBuilder.build_id to generate jobs for"),
):
    """Generate one specific builder's jobs by id, regardless of its
    execution_cycle or is_active (see
    app.services.execution.generate_jobs_for_builder).

    generate-jobs is scoped to a cycle and only looks at *active* builders,
    which is right for the scheduled path but wrong for a builder that exists
    to be run by hand -- a backfill left inactive so no timer can start it
    would silently generate nothing. This addresses the builder directly, so
    "inactive" keeps meaning "not scheduled" rather than "not runnable"."""
    setup_logging()

    with Session(engine) as session:
        created = generate_jobs_for_builder(session, build_id)
        job_ids = [job.job_id for job in created]

    typer.echo(f"{build_id}: generated {len(job_ids)} new job(s)")


@app.command("run-cycle")
def run_cycle_cmd(
    execution_cycle: str = typer.Argument(..., help="ApiJobBuilder.execution_cycle to run, e.g. daily, 5m, 1h"),
):
    """Execute every currently-pending ApiJob for this execution_cycle (see
    app.services.execution.run_cycle) -- does NOT generate jobs itself, run
    generate-jobs first. Register one systemd timer per distinct
    execution_cycle value in use, each calling generate-jobs then this with
    its own value."""
    setup_logging()

    # expire_on_commit=False: by default every commit expires all loaded
    # objects, so the next attribute read (this job's own row, the ApiMst
    # config the following job needs) goes back to the DB for a value we
    # already have. That's a ~10ms Oracle round-trip each, per job -- fine at
    # one job per cycle, but a sharded snapshot cycle runs hundreds, where it
    # became a measurable slice of the tick's budget. Scoped to this command
    # rather than to the engine: a batch process reads its own writes and
    # exits, while the API app (app.api.deps.SessionDep) keeps the default,
    # where a request handler re-reading post-commit state is the safer
    # assumption.
    with Session(engine, expire_on_commit=False) as session:
        results = run_cycle(session, execution_cycle)

    typer.echo(f"{execution_cycle}: executed {len(results)} job(s)")
    for job_id, description in results.items():
        typer.echo(f"  {job_id} -> {description}")


@app.command("discover-contracts")
def discover_contracts_cmd(
    period: str = typer.Argument(..., help="Maturity period to backfill: YYYY or YYYY-MM, e.g. 2019, 2019-10"),
):
    """Reconstruct contracts that settled before the instrument master files
    begin, by probing the strike ladder (see app.services.discovery).

    A backfill tool, not part of the daily batch: newer contracts arrive
    through the master file, so this only covers the stretch that predates it.
    Scoped to one period per run because a full pass is tens of thousands of
    rate-paced requests -- and resumable, since a contract already collected is
    skipped rather than re-fetched."""
    setup_logging()

    with Session(engine, expire_on_commit=False) as session:
        results = discover_period(session, period)

    probed = sum(r["probed"] for r in results.values())
    contracts = sum(r["contracts"] for r in results.values())
    typer.echo(f"{period}: {len(results)} maturities, {probed} probes, {contracts} contracts")
    for key, st in results.items():
        span = "not listed" if st["call_edge"] is None else f"{st['put_edge']}..{st['call_edge']}"
        typer.echo(f"  {key} -> {span}, {st['contracts']} contracts ({st['probed']} probes)")


def _log_procedure_output(cursor, name: str) -> None:
    """Log whatever the procedure wrote to DBMS_OUTPUT.

    Read in batches until a short one comes back, since the buffer holds more
    than one batch: sp_run_export over a month of days writes a line per
    target per day, and stopping at the first batch would drop the tail
    without any sign that it had."""
    while True:
        lines = cursor.arrayvar(str, _OUTPUT_BATCH_LINES)
        written = cursor.var(int)
        written.setvalue(0, _OUTPUT_BATCH_LINES)
        cursor.callproc("dbms_output.get_lines", [lines, written])

        count = written.getvalue()
        for line in lines.getvalue()[:count]:
            logger.info("{}: {}", name, line)
        if count < _OUTPUT_BATCH_LINES:
            return


def _call_procedure(name: str, *args: object, out_count: bool = True) -> int | None:
    """Run a DB-side procedure, logging its output and returning its count.

    callproc rather than exec_driver_sql: these procedures report through an
    OUT parameter, which needs a real bind variable. The raw handle is fetched
    per call rather than held across the commit, which invalidates it. They
    deliberately don't commit themselves, so that a caller can group one with
    other work; here there is none, so commit right after.

    ``args`` are the IN parameters and the OUT count is appended after them,
    which is why the procedures that report one take it last (see
    scripts/sql/sp_export_parquet.sql). ``out_count=False`` for a procedure
    that reports through DBMS_OUTPUT instead -- sp_run_export leaves the
    per-target counts to the procedure it calls rather than summing them,
    which is also what keeps this file out of the way when a target is added
    to it."""
    with Session(engine) as session:
        with session.connection().connection.cursor() as cursor:
            cursor.callproc("dbms_output.enable", [None])

            count = cursor.var(oracledb.NUMBER) if out_count else None
            cursor.callproc(name, [*args, count] if out_count else list(args))
            result = int(count.getvalue()) if out_count else None

            _log_procedure_output(cursor, name)
        session.commit()
    return result


@app.command("sync-index-his")
def sync_index_his_cmd():
    """Fold scraped index bars (kis_index_daily) into the stock_index_his
    series, via sp_stock_index_his_sync.

    Separate step because stock_index_his is reference data, not a scrape
    target: it has no job_id, so the engine cannot write it directly (see
    app.services.export._discover_table_registry). The procedure also fills
    the day-over-day columns the endpoint doesn't return."""
    setup_logging()
    typer.echo(f"stock_index_his: {_call_procedure('sp_stock_index_his_sync')} row(s) merged")


@app.command("sync-mst-fuopt")
def sync_mst_fuopt_cmd():
    """Fold any newly-listed contracts from fo_idx_code_mst into mst_fuopt, by
    calling the DB-side procedure sp_mst_fuopt_sync (a MERGE that inserts
    unseen short_codes and leaves existing rows alone).

    Belongs to the batch rather than to a trigger on fo_idx_code_mst: the raw
    master is only ever refreshed by the daily_start cycle, so "whenever rows
    arrive" and "once a day, after the refresh" are the same moment -- and a
    trigger paid for that equivalence, re-running the whole MERGE once per
    INSERT statement of the ~9k-row reload (measured at 0.8ms per row, ~7s a
    day) while making failures land somewhere with no log. Schedule this
    after the daily_start cycle and before any job generation that selects
    from mst_fuopt."""
    setup_logging()
    typer.echo(f"mst_fuopt: {_call_procedure('sp_mst_fuopt_sync')} new contract(s)")


@app.command("finalize-exports")
def finalize_exports_cmd():
    """Upload every staged document to object storage (see
    app.services.export.finalize_pending_exports).

    Buffered CSVs are no longer part of this: Parquet is exported from the
    result tables themselves by sp_export_parquet, which does not depend on
    what happens to be staged and so covers rows this never could. What is
    left here is the case the database cannot hold -- a scrape whose result
    is a file. Register as its own systemd timer, after the cycles that might
    have downloaded one."""
    setup_logging()

    results = finalize_pending_exports()

    # Local buffers are kept back to the previous expiry and no further. That
    # boundary rather than a fixed number of days because a contract's life is
    # measured in maturities: everything before the last expiry belongs to
    # contracts that have settled, and the DB (and the Parquet exported from
    # it) already holds all of it.
    with Session(engine) as session:
        keep_from = session.exec(text("""
            SELECT prev_mat_date FROM meta_maturity
             WHERE prod_type = 'K2I'
               AND mat_date = (SELECT MIN(mat_date) FROM meta_maturity
                                WHERE prod_type = 'K2I' AND mat_date >= TRUNC(SYSDATE))""")).one()[0]

    typer.echo(f"finalized {len(results)} document upload(s)")
    for name, key in results.items():
        typer.echo(f"  {name} -> {key}")

    if keep_from is None:
        typer.echo("no maturity calendar entry ahead of today; buffers left alone")
        return
    removed, freed = purge_csv_buffers(keep_from.date() if hasattr(keep_from, "date") else keep_from)
    typer.echo(f"purged {removed} buffered CSV(s) before {keep_from:%Y-%m-%d}, "
               f"freeing {freed / 1024 / 1024:.1f} MB")


@app.command("run-export")
def run_export_cmd(
    day: str = typer.Argument(None, help="Last trading day to export, YYYYMMDD (default: today in KST)"),
    since: str = typer.Option(None, "--from", help="Reach back to this day too, inclusive"),
):
    """Export a day's collected rows to object storage as Parquet, by calling
    sp_run_export (see scripts/sql/).

    What gets exported is not decided here. sp_run_export is a list of
    sp_export_parquet calls, one per target; this command passes a date and
    logs what the database reports back. Adding a table or a view to the
    export is a change to that procedure alone -- nothing here names a target
    or counts one, so this file does not move when the export grows, and a
    new target shows up in the batch log without being taught to Python.

    The work happens in the database too: DBMS_CLOUD.EXPORT_DATA writes the
    objects directly, so no rows travel through this process and nothing
    depends on which CSV buffers happen to be staged. That also makes it safe
    to repeat -- each day's prefix is cleared before it is rewritten -- so a
    failed run can simply be run again, and a range can be re-exported after a
    backfill.

    Dates are anchored on the most recent day and reach backwards: the
    argument is the last day to export and defaults to today, while --from
    says how far back to go. The daily batch is the common case and takes
    neither, so register it with no argument at all; --from is for putting a
    backfill into the bucket after the fact."""
    setup_logging()

    span = day or "today (KST)"
    typer.echo(f"exporting {f'{since} .. ' if since else ''}{span}")
    _call_procedure("sp_run_export", day, since, out_count=False)
    typer.echo("export complete")


@app.command("purge-jobs")
def purge_jobs_cmd(
    days: int = typer.Option(90, "--days", min=1, help="Keep jobs whose last run is newer than this"),
):
    """Delete finished jobs and their logs once they are older than --days, by
    calling sp_purge_jobs (see scripts/sql/).

    These tables grow with the shape of the APIs behind them: a request takes
    one instrument for one day, so a job exists per instrument per day and the
    minute-bar and daily-bar builders alone add about three thousand a day.
    What they leave behind is bookkeeping -- the results live in the result
    tables -- and it stops being worth keeping after a few months.

    Active jobs are never touched, whatever their age: still-pending work, a
    failed job waiting to be retried on the next tick, and repeated jobs all
    show as active, and deleting any of them would stop collection.

    Register as a late step of the daily batch, alongside the other retention
    work (see app.services.export.purge_csv_buffers)."""
    setup_logging()
    deleted = _call_procedure("sp_purge_jobs", days)
    typer.echo(f"purged {deleted:,} job(s) older than {days} day(s)")


@app.command("check-sql")
def check_sql_cmd(
    pull: list[str] = typer.Option(None, "--pull", help="Object to overwrite its file with the database's version; repeatable"),
):
    """Report where scripts/sql and the compiled objects disagree.

    Nothing loads those files at runtime -- Python calls the procedures by
    name -- so they are a record, and a record no one checks is worse than
    none: it is believed. This is the check. The database stays the source of
    truth, which is the point of splitting the work; --pull brings a change
    made there back into the file so it can be committed with its reason.

    Run it after any session that touched the database, and in the daily batch
    if you want the drift to find you rather than the other way round."""
    setup_logging()

    for name in pull or []:
        typer.echo(f"pulled {name} -> {pull_sql(name)}")

    results = compare_sql()
    for r in results:
        typer.echo(f"  {r.name:<26} {r.status:<8} {r.detail}")

    drifted = [r for r in results if r.status in ("differ", "missing", "orphan")]
    if drifted:
        typer.echo("")
        typer.echo(f"{len(drifted)} object(s) out of step; --pull <name> to take the database's version")
        raise typer.Exit(1)
    typer.echo("")
    typer.echo(f"{sum(1 for r in results if r.status == 'match')} object(s) match")


if __name__ == "__main__":
    app()
