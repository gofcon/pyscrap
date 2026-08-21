import oracledb
import typer
from sqlmodel import Session

from app.db.engine import engine
from app.logging_config import setup_logging
from app.services.discovery import discover_period
from app.services.execution import generate_jobs, run_cycle
from app.services.export import finalize_pending_exports

app = typer.Typer()


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


def _call_procedure(name: str) -> int:
    """Run a DB-side sync procedure and return the row count it reports.

    callproc rather than exec_driver_sql: these procedures report progress
    through an OUT parameter, which needs a real bind variable. The raw handle
    is fetched per call rather than held across the commit, which invalidates
    it. They deliberately don't commit themselves, so that a caller can group
    one with other work; here there is none, so commit right after."""
    with Session(engine) as session:
        with session.connection().connection.cursor() as cursor:
            count = cursor.var(oracledb.NUMBER)
            cursor.callproc(name, [count])
            result = int(count.getvalue())
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
    """Turn every currently-pending buffered CSV into a Parquet upload (see
    app.services.export.finalize_pending_exports). Register this as its own
    systemd timer/service, scheduled as the very last step of the day's
    batch -- after every cycle's generate-jobs + run-cycle has already run."""
    setup_logging()

    results = finalize_pending_exports()

    typer.echo(f"finalized {len(results)} job export(s)")
    for job_id, key in results.items():
        typer.echo(f"  {job_id} -> {key}")


if __name__ == "__main__":
    app()
