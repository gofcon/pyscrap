"""Paging and filtering shared by the routers that list rows.

Both the config router (app.api.routers.crud) and the result-table router
(app.api.routers.results) answer the same question -- one page of a table,
narrowed by whatever the caller asked for -- over tables that outgrew being
read whole: api_job passed 160,000 rows and kis_futopt_chart is past nine
million. They had the same code twice, which is how the two drifted: one
returned 400 for a value a column could not take and the other let it become
a 500.

Filters are not declared per table. Every field of the model is offered and
an unknown one is rejected, because a hand-kept list of filterable fields
would go stale against the models, and both routers already trade the
generated request schema for staying generic.
"""

from typing import Any, cast

from fastapi import HTTPException, Request, Response
from pydantic import TypeAdapter, ValidationError
from sqlalchemy import Column, func
from sqlmodel import SQLModel, select

# '*' is the only wildcard a caller gets. LIKE's own '%' and '_' are escaped
# to their literal selves, because these ids are full of underscores --
# KIS_FUTOPT_PRICE_A2, KIS_INDEX_DAILY_2001_20260825_20260825 -- and letting
# '_' mean "any character" quietly widens a search past what was asked for.
# '*' rather than '%' because a caller types these into a URL, where '%' has
# to be written %25.
#
# One translate pass rather than chained replaces: escaping the backslash
# first and the others after would re-escape the backslashes just added.
_LIKE_LITERALS = str.maketrans({"\\": r"\\", "%": r"\%", "_": r"\_"})


def like_pattern(raw: str) -> str:
    """A caller's '*' pattern as a LIKE pattern, everything else literal."""
    return raw.translate(_LIKE_LITERALS).replace("*", "%")


def apply_filters(model: type[SQLModel], statement: Any, request: Request,
                  reserved: set[str]) -> Any:
    """Narrow `statement` by the request's query parameters.

    ``reserved`` names the parameters that steer the endpoint itself (limit,
    offset, ...) rather than a column; everything else has to be a field of
    the model or the request is rejected, so a typo returns a 400 instead of
    a full unfiltered page that looks like an answer.
    """
    filters = {k: v for k, v in request.query_params.items() if k not in reserved}
    unknown = set(filters) - set(model.model_fields)
    if unknown:
        raise HTTPException(400, f"unknown filter field(s): {sorted(unknown)}")

    for field, raw in filters.items():
        # Coerced through the field's own annotation so "true" reaches a
        # boolean column as True and "1" reaches an integer one as 1 -- a
        # query string carries neither type. Same validation the model would
        # apply to a write, and a value it refuses is the caller's mistake:
        # a 400, not a 500 from an uncaught ValidationError.
        try:
            value = TypeAdapter(model.model_fields[field].annotation).validate_python(raw)
        except ValidationError as exc:
            raise HTTPException(400, f"bad value for {field}: {raw!r}") from exc

        column = cast(Column, getattr(model, field))
        if isinstance(value, str) and "*" in raw:
            # A pattern anchored at the front ('C0160*') still uses the
            # column's index; one that starts with '*' cannot, and reads the
            # whole table. Left to the caller -- both are worth having, and
            # only they know which they meant.
            statement = statement.where(column.like(like_pattern(raw), escape="\\"))
        else:
            statement = statement.where(column == value)
    return statement


def page(session: Any, response: Response, statement: Any,
         order_by: tuple[Column, ...], limit: int, offset: int) -> list[SQLModel]:
    """One page of `statement`, with the full match count in a header.

    Counted from the statement's own subquery rather than the table, so the
    number describes what the caller asked for and page controls stay right
    when a filter or a parent narrows it. X-Total-Count rather than an
    envelope around the rows, which would change the response's shape for
    every caller to serve the page controls of one.

    ``order_by`` must end in something unique. Ordering by a column with ties
    lets rows tied on it come back arranged differently per query, and paging
    then repeats some and skips others.
    """
    total = session.exec(select(func.count()).select_from(statement.subquery())).one()
    response.headers["X-Total-Count"] = str(total)
    rows = session.exec(statement.order_by(*order_by).offset(offset).limit(limit)).all()
    return list(rows)
