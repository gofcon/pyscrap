"""FastAPI v3 application entrypoint (Neon DB control plane + DuckDB Parquet API).

Run locally with:
    uvicorn app.api.main:app --reload
"""

from __future__ import annotations

from fastapi import Depends, FastAPI

from app.api.deps import verify_admin_api_key
from app.api.routers import config, health, job_logs, jobs, results

app = FastAPI(title="pyscrap v3", version="3.0.0")
_AUTH_DEPS = [Depends(verify_admin_api_key)]

app.include_router(health.router)
app.include_router(config.router, dependencies=_AUTH_DEPS)
app.include_router(jobs.router, dependencies=_AUTH_DEPS)
app.include_router(job_logs.router, dependencies=_AUTH_DEPS)
app.include_router(results.router, dependencies=_AUTH_DEPS)
