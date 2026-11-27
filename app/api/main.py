"""FastAPI v3 application entrypoint (Neon DB control plane + DuckDB Parquet API).

Run locally with:
    uvicorn app.api.main:app --reload
"""

from __future__ import annotations

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
import psycopg

from app.api.deps import verify_admin_api_key
from app.api.routers import config, health, job_logs, jobs, results

app = FastAPI(title="pyscrap v3", version="3.0.0")
_AUTH_DEPS = [Depends(verify_admin_api_key)]


@app.exception_handler(psycopg.errors.UniqueViolation)
def _handle_unique_violation(_request: Request, exc: psycopg.errors.UniqueViolation) -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": str(exc).splitlines()[0]})


@app.exception_handler(psycopg.errors.IntegrityError)
def _handle_integrity_error(_request: Request, exc: psycopg.errors.IntegrityError) -> JSONResponse:
    return JSONResponse(status_code=422, content={"detail": str(exc).splitlines()[0]})


app.include_router(health.router)
app.include_router(config.router, dependencies=_AUTH_DEPS)
app.include_router(jobs.router, dependencies=_AUTH_DEPS)
app.include_router(job_logs.router, dependencies=_AUTH_DEPS)
app.include_router(results.router, dependencies=_AUTH_DEPS)
