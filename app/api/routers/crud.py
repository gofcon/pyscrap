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

from typing import Any, cast

from fastapi import APIRouter, HTTPException, Query, Request, Response
from sqlalchemy import Column, func
from sqlmodel import SQLModel, select

from app.api.deps import SessionDep
from app.api.paging import apply_filters, page


def make_crud_router(
    model: type[SQLModel],
    id_field: str,
    prefix: str,
    tag: str,
    children: dict[str, tuple[type[SQLModel], str]] | None = None,
) -> APIRouter:
    """``children`` maps a path segment to (child model, the child column
    holding this row's id) -- {"jobs": (ApiJob, "api_id")} mounts
    GET {prefix}/{row_id}/jobs.

    Declared per mount rather than derived, because the models carry no
    relationships to derive it from (see models.py on why those were dropped
    project-wide) and a column named api_id on two tables is a naming
    convention, not a statement that they are related.

    Children are their own paged endpoints and are not embedded in the parent
    -- one api_mst row owns 167,146 jobs, so a detail response carrying its
    children would be the same unbounded read this router just stopped doing.
    {prefix}/{row_id}/children gives the counts alone, which is what a page
    showing "42 builders, 167,146 jobs" actually needs."""
    router = APIRouter(prefix=prefix, tags=[tag])

    # cast: class-level column access resolves to the field's declared Python
    # type for a type checker, which has no .desc(); at runtime it is
    # SQLAlchemy's InstrumentedAttribute, which does. Same gap as
    # _EXECUTED_AT in app.api.routers.job_logs.
    updated_at_column = cast(Column, model.updated_at)
    id_column = cast(Column, getattr(model, id_field))

    @router.get("/", response_model=list[model])
    def list_page(
        session: SessionDep,
        response: Response,
        request: Request,
        limit: int = Query(default=100, le=1000, description="Rows per page"),
        offset: int = Query(default=0, ge=0, description="Rows to skip"),
        q: str | None = Query(default=None, description=f"Substring of {id_field}"),
    ) -> list[SQLModel]:
        """One page of rows, most recently updated first.

        Paged because two of these three tables are not the small fixed
        config sets this router was written for any more: api_job holds a row
        per generated job, which a year of minute-bar backfill took past
        160,000. Returning all of them serialized the whole table into one
        response and the UI could not open the page at all.

        Ordered by updated_at with the primary key as tiebreaker, not by
        updated_at alone. Job generation stamps a whole batch within the same
        instant, so the database is free to return those rows in a different
        arrangement per query -- and then paging re-reads some rows and skips
        others. The key breaks every tie the timestamp leaves.

        The count goes in an X-Total-Count header rather than wrapping the
        rows in an envelope, which would change this response's shape for
        every existing caller to serve the page controls of one. It counts
        what the filters match, not the table, so page controls stay right
        while a filter is on.

        Any field of the model can be used as an exact-match filter --
        ?execution_cycle=3m_call&is_active=true -- and ``q`` matches a
        substring of the primary key, which is the search box a UI wants.
        Filters rather than paging alone because most of api_job is finished
        backfill: paging through 160,000 rows to reach the one job you care
        about is not a workable way to find it.

        The filterable set is not declared per mount. Every field is offered
        and an unknown one is rejected, the same way update() checks its
        body: a list to maintain would go stale against the models, and this
        router already trades the generated schema for staying generic (see
        the module docstring)."""
        statement = apply_filters(model, select(model), request, {"limit", "offset", "q"})
        if q is not None:
            statement = statement.where(id_column.like(f"%{q}%"))

        return page(session, response, statement,
                     (updated_at_column.desc(), id_column.asc()), limit, offset)

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

    def require_parent(row_id: str, session: SessionDep) -> None:
        # A missing parent is a 404, not an empty list: a mistyped id would
        # otherwise read as "this one has no children".
        if session.get(model, row_id) is None:
            raise HTTPException(404, f"{model.__name__} '{row_id}' not found")

    @router.get("/{row_id}/children")
    def child_counts(row_id: str, session: SessionDep) -> dict[str, int]:
        """How many rows each child set holds, without reading any of them."""
        require_parent(row_id, session)
        counts: dict[str, int] = {}
        for segment, (child_model, fk_field) in (children or {}).items():
            counts[segment] = session.exec(
                select(func.count())
                .select_from(child_model)
                .where(cast(Column, getattr(child_model, fk_field)) == row_id)
            ).one()
        return counts

    for segment, (child_model, fk_field) in (children or {}).items():

        def make_child_route(child_model: type[SQLModel] = child_model,
                             fk_field: str = fk_field) -> Any:
            # Defaults bind this iteration's values: a closure over the loop
            # variables would leave every route pointing at the last child.
            child_updated_at = cast(Column, child_model.updated_at)
            # The child's own key as tiebreaker, for the reason the parent
            # list has one: a batch of generated jobs shares an instant, and
            # rows tied on the sort column can come back arranged differently
            # per query -- which makes paging repeat some and skip others.
            child_key = cast(Column, list(child_model.__table__.primary_key.columns)[0])

            def list_children(
                row_id: str,
                session: SessionDep,
                response: Response,
                limit: int = Query(default=100, le=1000, description="Rows per page"),
                offset: int = Query(default=0, ge=0, description="Rows to skip"),
            ) -> list[SQLModel]:
                require_parent(row_id, session)
                statement = select(child_model).where(
                    cast(Column, getattr(child_model, fk_field)) == row_id)
                return page(session, response, statement,
                             (child_updated_at.desc(), child_key.asc()), limit, offset)

            return list_children

        router.add_api_route(
            f"/{{row_id}}/{segment}",
            make_child_route(),
            methods=["GET"],
            response_model=list[child_model],
            summary=f"{child_model.__name__} rows whose {fk_field} is this row",
        )

    return router
