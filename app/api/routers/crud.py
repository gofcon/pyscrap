"""Generic CRUD router factory for the three small, fixed config tables
(ApiMst, ApiJobBuilder, ApiJob -- see app.services.export._BOOKKEEPING_TABLES)
that drive the scraping engine. Deliberately NOT auto-discovered the way
app.services.export.TABLE_REGISTRY discovers result tables (see
app.api.routers.results) -- this set is small and stable, so each one is
wired up by an explicit make_crud_router(...) call in app.api.main, same
spirit as _BOOKKEEPING_TABLES itself being a hardcoded set rather than
inferred.

One factory instead of three hand-written near-duplicate routers, since all
three share the same shape: a single string primary key, no FKs/
Relationships to worry about (see models.py's header comment on why those
were dropped project-wide), heavy JSON columns. Adding a fourth config table
later is one make_crud_router(...) call, not five new endpoint functions to
keep in sync by hand.

Create/update request bodies are accepted as plain dict[str, Any], passed
through model.model_validate() / setattr, rather than typed per-model
Create/Update schema classes. The obvious-looking alternative -- typing the
create body directly as `row: model` -- can't actually work here: `model` is
a parameter of this factory function, not a name that exists at each route's
*definition* site the way an imported class name would, so it isn't a valid
*static* type annotation for a dynamically-generated function (a type
checker can't -- and shouldn't be expected to -- resolve "whatever type this
runtime variable happens to hold" as a type). Losing the per-field OpenAPI
request-body schema is an acceptable trade for an internal admin API with no
public consumers; model_validate() still runs full pydantic validation
(required fields, type coercion) at request time, it's just not reflected in
the generated docs.

IMPORTANT: this module must NOT gain `from __future__ import annotations`
(unlike most of this codebase) -- response_model=model / select(model) below
rely on `model` being evaluated eagerly as the real class object, not
deferred as a string that would need resolving against this module's
globals (where the closure-local `model` doesn't exist)."""

from typing import Any

from fastapi import APIRouter, HTTPException
from sqlmodel import SQLModel, select

from app.api.deps import SessionDep


def make_crud_router(model: type[SQLModel], id_field: str, prefix: str, tag: str) -> APIRouter:
    router = APIRouter(prefix=prefix, tags=[tag])

    @router.get("/", response_model=list[model])
    def list_all(session: SessionDep) -> list[SQLModel]:
        return list(session.exec(select(model)).all())

    @router.get("/{row_id}", response_model=model)
    def get_one(row_id: str, session: SessionDep) -> SQLModel:
        row = session.get(model, row_id)
        if row is None:
            raise HTTPException(404, f"{model.__name__} '{row_id}' not found")
        return row

    @router.post("/", response_model=model, status_code=201)
    def create(body: dict[str, Any], session: SessionDep) -> SQLModel:
        row_id = body.get(id_field)
        if row_id is not None and session.get(model, row_id) is not None:
            raise HTTPException(409, f"{model.__name__} '{row_id}' already exists")
        body.pop("updated_at", None)  # DB-managed -- server_default/onupdate, see models.py
        row = model.model_validate(body)
        session.add(row)
        session.commit()
        session.refresh(row)
        return row

    @router.patch("/{row_id}", response_model=model)
    def update(row_id: str, body: dict[str, Any], session: SessionDep) -> SQLModel:
        row = session.get(model, row_id)
        if row is None:
            raise HTTPException(404, f"{model.__name__} '{row_id}' not found")
        body.pop(id_field, None)
        body.pop("updated_at", None)
        unknown = set(body) - set(model.model_fields)
        if unknown:
            raise HTTPException(400, f"unknown field(s): {sorted(unknown)}")
        for key, value in body.items():
            setattr(row, key, value)
        session.add(row)
        session.commit()
        session.refresh(row)
        return row

    @router.delete("/{row_id}", status_code=204)
    def delete(row_id: str, session: SessionDep) -> None:
        row = session.get(model, row_id)
        if row is None:
            raise HTTPException(404, f"{model.__name__} '{row_id}' not found")
        session.delete(row)
        session.commit()

    return router
