import typer
from sqlmodel import Session

from app.db.engine import engine
from app.logging_config import setup_logging
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

    with Session(engine) as session:
        results = run_cycle(session, execution_cycle)

    typer.echo(f"{execution_cycle}: executed {len(results)} job(s)")
    for job_id, description in results.items():
        typer.echo(f"  {job_id} -> {description}")


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
