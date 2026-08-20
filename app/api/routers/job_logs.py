"""Read-only endpoint over ApiJobLog -- the append-only execution history
(one row per run attempt, SUCCESS/FAILED -- see app.services.execution's
_log()/run_job). Doesn't fit app.api.routers.crud (not one of the three
config tables users edit) or app.api.routers.results (TABLE_REGISTRY
explicitly excludes it as a bookkeeping table, same as api_mst/
api_job_builder/api_job -- see app.services.export._BOOKKEEPING_TABLES), so
it gets its own small router.

No create/update/delete: rows are only ever written by _log() inside
run_job, and this table is meant to stay a plain, unedited history.

Existing endpoints like POST /jobs/run/{job_id} always return 200 even when
the run itself failed internally (the exception is caught and logged, not
re-raised -- see run_job's docstring/comments), so this is also the only
reliable way to confirm success/failure over the API."""

from typing import cast

from fastapi import APIRouter, Query
from sqlalchemy import Column
from sqlmodel import select

from app.api.deps import SessionDep
from app.db.models import ApiJobLog

router = APIRouter(prefix="/job-logs", tags=["job_logs"])

# cast: ApiJobLog.executed_at is declared Optional[datetime] (the instance
# attribute's Python type), so a type checker resolves the *class*-level
# access ApiJobLog.executed_at the same way and infers plain `datetime |
# None` -- which has no .desc(). At runtime, class-level access actually
# returns SQLAlchemy's InstrumentedAttribute (a Column-like ORM construct
# that does have .desc()); same stub-is-stricter-than-runtime gap as
# job_id_column in app.services.execution._clear_previous_results.
_EXECUTED_AT = cast(Column, ApiJobLog.executed_at)


@router.get("/")
def list_recent(
    session: SessionDep,
    status: str | None = Query(default=None, description="Filter by SUCCESS or FAILED"),
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
) -> list[ApiJobLog]:
    """Most recent log entries across every job, newest first -- e.g. for
    spotting recent failures without already knowing which job_id to look
    at."""
    query = select(ApiJobLog)
    if status is not None:
        query = query.where(ApiJobLog.status == status)
    query = query.order_by(_EXECUTED_AT.desc()).offset(offset).limit(limit)
    return list(session.exec(query).all())


@router.get("/{job_id}")
def list_for_job(
    job_id: str,
    session: SessionDep,
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
) -> list[ApiJobLog]:
    """Execution history for one job_id, newest first -- e.g. to confirm a
    manual POST /jobs/run/{job_id} actually succeeded."""
    query = (
        select(ApiJobLog)
        .where(ApiJobLog.job_id == job_id)
        .order_by(_EXECUTED_AT.desc())
        .offset(offset)
        .limit(limit)
    )
    return list(session.exec(query).all())
