"""Read-only endpoints over every scraped result table -- the same
auto-discovered, growing set app.services.export.TABLE_REGISTRY writes to
(see its own docstring for why it's auto-discovered rather than a hardcoded
list, unlike the three config tables in app.api.routers.crud). No create/
update/delete here: these tables are the scraping engine's own output,
written only by app.services.execution.run_job -- writing to them any other
way would just get overwritten/duplicated the next time that job_id runs
(save_mode='overwrite') or diverge from what the source API actually
returned (save_mode='append')."""

from typing import Any, cast

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import Column
from sqlmodel import SQLModel, select

from app.api.deps import SessionDep
from app.services.export import TABLE_REGISTRY

router = APIRouter(prefix="/results", tags=["results"])


def _get_model(table_name: str) -> type[SQLModel]:
    model = TABLE_REGISTRY.get(table_name)
    if model is None:
        raise HTTPException(404, f"unknown result table '{table_name}' (see GET /results for the list)")
    return model


@router.get("/")
def list_tables() -> list[str]:
    return sorted(TABLE_REGISTRY)


@router.get("/{table_name}")
def list_rows(
    table_name: str,
    session: SessionDep,
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
) -> list[dict[str, Any]]:
    model = _get_model(table_name)
    # cast: every TABLE_REGISTRY model has a real `id` column (its identity
    # PK -- see models.py), but SQLModel's own base class doesn't declare
    # one, so a type checker can't confirm that through a dynamic
    # getattr(). Same pattern as job_id_column in
    # app.services.execution._clear_previous_results.
    id_column = cast(Column, getattr(model, "id"))
    rows = session.exec(select(model).order_by(id_column.desc()).offset(offset).limit(limit)).all()
    return [row.model_dump() for row in rows]


@router.get("/{table_name}/{row_id}")
def get_row(table_name: str, row_id: int, session: SessionDep) -> dict[str, Any]:
    model = _get_model(table_name)
    row = session.get(model, row_id)
    if row is None:
        raise HTTPException(404, f"{table_name} row {row_id} not found")
    return row.model_dump()
