"""Shared FastAPI dependencies.

Re-exports app.db.engine.get_session as a typed Annotated alias so every
router imports SessionDep from one place instead of wiring up
Depends(get_session) itself repeatedly -- keeps session-scoping a single
thing to change later (e.g. if request-scoped session behavior ever needs
to differ from the CLI's plain `with Session(engine) as session:` usage).
"""

from typing import Annotated

from fastapi import Depends
from sqlmodel import Session

from app.db.engine import get_session

SessionDep = Annotated[Session, Depends(get_session)]
