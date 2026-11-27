"""Shared FastAPI dependencies.

Re-exports app.db.engine.get_session as a typed Annotated alias so every
router imports SessionDep from one place instead of wiring up
Depends(get_session) itself repeatedly -- keeps session-scoping a single
thing to change later (e.g. if request-scoped session behavior ever needs
to differ from the CLI's plain `with Session(engine) as session:` usage).
"""

import hmac
import os
from typing import Annotated

from fastapi import Depends, Header, HTTPException
from sqlmodel import Session

from app.db.engine import get_session

SessionDep = Annotated[Session, Depends(get_session)]


def verify_admin_api_key(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    authorization: str | None = Header(default=None),
) -> None:
    """Enforce API-key auth when ``ADMIN_API_KEY`` is configured in the environment.

    Unset leaves local development frictionless; setting ``ADMIN_API_KEY`` in
    production immediately locks down all config/job/result routers.
    """
    expected = os.environ.get("ADMIN_API_KEY")
    if not expected:
        return
    provided = x_api_key
    if not provided and authorization and authorization.lower().startswith("bearer "):
        provided = authorization[7:].strip()
    if not provided or not hmac.compare_digest(provided, expected):
        raise HTTPException(401, "Invalid or missing API key")
