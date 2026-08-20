"""Liveness/readiness check -- confirms the process is up *and* can reach
Oracle, independent of any future business router. Same DB the batch side
(app.cli) uses, just queried over HTTP instead of a systemd timer."""

from typing import Any, cast


from fastapi import APIRouter
from requests import get
from sqlalchemy import text

from app.api.deps import SessionDep

router = APIRouter(tags=["health"])


@router.get("/health")
def health_check(session: SessionDep) -> dict[str, str]:
    # cast: see app.services.job_builder._run_query_rows -- Session.exec()'s
    # stub doesn't accept a bare TextClause even though it works at runtime.
    session.exec(cast(Any, text("SELECT 1 FROM DUAL")))
    return {"status": "ok"}


@router.get("/ip")
def get_ip():
    return get('https://api.ipify.org').text