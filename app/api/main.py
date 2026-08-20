"""FastAPI application entrypoint.

Separate from app.cli (the systemd-driven batch entrypoint) -- both sit on
top of the same app.services.* business logic and app.db.engine session
factory, so a future router just calls the same functions run_cycle/
generate_jobs/finalize_pending_exports already call, instead of duplicating
any of it. This process is meant to run continuously (e.g. under its own
systemd service, or however it's deployed) rather than per-timer-tick like
the batch side.

Run locally with:
    uvicorn app.api.main:app --reload
"""

from fastapi import FastAPI

from app.api.routers import health, job_logs, jobs, results
from app.api.routers.crud import make_crud_router
from app.db.models import ApiJob, ApiJobBuilder, ApiMst
from app.logging_config import setup_logging

setup_logging()

app = FastAPI(title="orascrap")

app.include_router(health.router)

# Config-table CRUD (see app.api.routers.crud) -- one call per table, kept
# explicit rather than looped/auto-discovered since this set is small and
# fixed (same three as app.services.export._BOOKKEEPING_TABLES).
app.include_router(make_crud_router(ApiMst, id_field="api_id", prefix="/api-mst", tag="api_mst"))
app.include_router(make_crud_router(ApiJobBuilder, id_field="build_id", prefix="/job-builders", tag="api_job_builder"))
app.include_router(make_crud_router(ApiJob, id_field="job_id", prefix="/jobs", tag="api_job"))

# ApiJob action endpoints (generate/run) -- mounted at the same "/jobs"
# prefix as its CRUD router above; see app.api.routers.jobs for why the
# routes don't collide.
app.include_router(jobs.router)

# Read-only result-table browsing (see app.api.routers.results).
app.include_router(results.router)

# Read-only ApiJobLog browsing (see app.api.routers.job_logs).
app.include_router(job_logs.router)
