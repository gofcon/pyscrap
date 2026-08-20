"""Action endpoints over ApiJob/ApiJobBuilder that aren't plain CRUD --
mounted at the same "/jobs" prefix as the ApiJob CRUD router (see
make_crud_router(ApiJob, ...) in app.api.main), since these act on that same
resource. Thin wrappers only: all the actual logic lives in
app.services.execution, the same module app.cli's systemd-timer commands
call.

Three generate endpoints, one per granularity (see
app.services.execution's module docstring for why all three exist):
individual builder, one execution_cycle, everything. All nested under
"/generate/..." with a distinguishing literal segment (cycle/builder/all) --
NOT e.g. "/generate/{execution_cycle}" next to "/generate/{build_id}": those
are the exact same path *shape* to the router (a parameter's name doesn't
factor into route matching at all), so the one registered first would
silently swallow every request meant for the other -- no startup error, just
a route that never fires. A literal segment before the param is what
actually disambiguates them."""

from fastapi import APIRouter, HTTPException

from app.api.deps import SessionDep
from app.db.models import ApiJob
from app.services.execution import generate_all_jobs, generate_jobs, generate_jobs_for_builder, run_job

router = APIRouter(prefix="/jobs", tags=["api_job"])


@router.post("/generate/cycle/{execution_cycle}")
def generate_by_cycle(execution_cycle: str, session: SessionDep) -> list[str]:
    """Create any still-missing ApiJob rows for this execution_cycle -- same
    as `python -m app.cli generate-jobs <execution_cycle>` (see app.cli).
    Returns the newly-created job_ids (empty if everything already existed)."""
    created = generate_jobs(session, execution_cycle)
    return [job.job_id for job in created]


@router.post("/generate/all")
def generate_all(session: SessionDep) -> list[str]:
    """Create any still-missing ApiJob rows for every active builder,
    regardless of execution_cycle -- a manual "generate everything now",
    e.g. for a one-off full refresh outside the normal per-cycle schedule."""
    created = generate_all_jobs(session)
    return [job.job_id for job in created]


@router.post("/generate/builder/{build_id}")
def generate_for_builder(build_id: str, session: SessionDep) -> list[str]:
    """Create job(s) for one specific ApiJobBuilder by id, regardless of its
    execution_cycle/is_active -- e.g. to try out a builder you just created
    (or edited) without waiting for its scheduled cycle or flipping it
    active first."""
    try:
        created = generate_jobs_for_builder(session, build_id)
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc
    return [job.job_id for job in created]


@router.post("/run/{job_id}")
def run(job_id: str, session: SessionDep) -> dict[str, str | None]:
    """Execute one already-existing ApiJob by id, regardless of its
    execution_cycle/is_active -- the same app.services.execution.run_job a
    scheduled run-cycle tick would call, just triggered by id instead of
    scanned for. Logs SUCCESS/FAILED to ApiJobLog same as any other run."""
    job = session.get(ApiJob, job_id)
    if job is None:
        raise HTTPException(404, f"ApiJob '{job_id}' not found")
    run_job(session, job)
    return {"job_id": job.job_id, "description": job.description}
