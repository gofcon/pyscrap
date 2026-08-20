"""Expands an ``ApiJobBuilder`` row into one or more concrete ``ApiJob`` rows.

``macro_params_json`` is a dict whose *keys* may be a single param name or a
comma-separated list of param names (for multi-column query mapping), and
whose *values* are one of:

- a literal (``str``/``int``/...): the same value in every generated job.
- ``{"db": "<SQL query>"}``: the query is run and each result row becomes one
  combination this parameter (or, for a comma-separated key, this *group* of
  parameters -- one column per key, in order) fans out over.
- ``{"date": "today"|"yesterday"|"last_month"|"last_eom"|"last_yymm"|"last_bday", "format": "..."}``:
  resolved to a date string as of generation time. ``format`` is optional --
  each keyword has its own default (``%Y%m%d``, except ``last_yymm`` which
  defaults to ``%Y%m``) that ``format`` overrides if given. Only usable with
  a single (non comma-separated) key.

Do NOT put a capture-timestamp value here to key snapshot/repeated jobs (e.g.
a quote endpoint polled every few minutes to build a time series) -- a value
here becomes part of *job generation*, so a fresh timestamp on every call
would mint a brand new ApiJob row every time (job_id is meant to stay stable
and reusable -- see ``key_params_list`` below). Use the reserved
``"NOW"`` entry in ``key_params_list`` instead: it's resolved once per
*execution*, not per job-generation, so the job_id/row count stays flat while
each run still stamps its own capture time onto the saved rows.

Each top-level entry is one independent "axis"; the final job set is the
cartesian product across all axes (literal/date axes are single-valued, so
they don't multiply the job count -- only ``db`` axes with >1 row do).

Example::

    {
        "KACD": {"db": "SELECT api_id FROM api_mst"},        # fans out, 1 col
        "A,B": {"db": "SELECT col_a, col_b FROM some_table"}, # fans out, 2 cols/row, kept together
        "RUN_DATE": {"date": "yesterday"},
        "GROUP": "110110",                                    # literal
    }

Only *generates* ApiJob rows; no log entry is written until the job actually
runs (see :func:`app.services.execution.run_job`, which logs SUCCESS/FAILED).

``ApiMst.key_params_list`` (a list of param names, e.g. ``["BASDD"]``, or
``{param_name: column_name}`` dicts -- see :func:`normalize_key_param`) makes
job_id deterministic from those params' values instead of a random uuid, so
regenerating with the same values reuses the same job_id -- and a job whose
id already exists is skipped as a duplicate rather than re-created. Without
it, job_id is always random and every call creates fresh jobs. It lives on
``ApiMst`` rather than ``ApiJobBuilder`` because it's a property of the API's
own response/output-table shape (which fields it naturally keys records by),
not of any particular builder's parameter choices -- every builder targeting
the same api_id needs the exact same value anyway.

The reserved entry ``"NOW"`` (or ``{"NOW": "<column_name>"}``) is special: it
must never appear in ``macro_params_json``, and it's excluded from job_id
construction entirely (a job's identity has to stay stable across repeated
executions, or every run would mint a fresh job forever -- exactly the
explosion this reservation avoids for something like a 1-minute snapshot
poll). Instead it's resolved fresh at *execution* time, in
:func:`app.services.execution.run_job`, via :func:`resolve_now` --
floored to the job's ``ApiJobBuilder.execution_cycle`` bucket (see
:func:`_floor_datetime`) so repeated runs of the same scheduled job get
timestamps that line up with each other -- and stamped onto the saved result
row the same way any other key param is (``key_params_json`` for generic
tables, a real column for typed tables). One static job executes over and
over; each execution just produces a new timestamped row.
"""

from __future__ import annotations

import calendar
import hashlib
import itertools
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, cast

from loguru import logger
from sqlalchemy import text
from sqlmodel import Session

from app.db.models import ApiJob, ApiJobBuilder, ApiMst


def _run_query_rows(session: Session, query: str) -> list[tuple]:
    """Execute a SQL query and return all rows as tuples."""
    # cast: TextClause genuinely works fine with session.exec() at runtime
    # (verified live), but Session.exec()'s overloaded signature (Select |
    # SelectOfScalar | Executable[_TSelectParam] | UpdateBase) doesn't accept
    # a bare TextClause, and a cast to plain Executable still isn't
    # assignable to the UpdateBase overload per pyright's resolution. cast()
    # to Any sidesteps the overload check entirely rather than trying to find
    # a member of that union that satisfies it -- same category of
    # stub-is-stricter-than-runtime gap as the Column '==' cast in
    # app.services.execution._clear_previous_results.
    result = session.exec(cast(Any, text(query)))
    return [tuple(row) for row in result]


# Every source scraped here is Korean-market data, so both kinds of
# generated timestamp (RESERVED_NOW_KEY capture stamps and the _DATE_KEYWORDS
# below) have to be anchored to KST rather than to whatever the host happens
# to be set to -- an Oracle Cloud instance defaults to UTC, which would put a
# "5m" snapshot's trade_at 9 hours off the tick that produced it, and would
# make a "today" resolved during the 08:35 pre-open job land on the *previous*
# date (08:35 KST = 23:35 UTC the day before). A fixed offset rather than
# zoneinfo("Asia/Seoul"): KST has been a constant UTC+9 with no DST since
# 1988, and zoneinfo needs a tzdata database that isn't present everywhere
# this runs (verified missing on the Windows dev machine).
KST = timezone(timedelta(hours=9))


def _now_kst() -> datetime:
    return datetime.now(KST)


def _add_months(d: date, months: int) -> date:
    month_index = d.month - 1 + months
    year = d.year + month_index // 12
    month = month_index % 12 + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _last_eom(d: date) -> date:
    """Last day of the previous month."""
    return date(d.year, d.month, 1) - timedelta(days=1)


def _last_bday(d: date) -> date:
    """Most recent business day before d, skipping Sat/Sun only -- no public
    holiday calendar, so it won't skip e.g. Lunar New Year/Chuseok."""
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:  # 5=Sat, 6=Sun
        prev -= timedelta(days=1)
    return prev


# ApiJobBuilder.execution_cycle, e.g. "1m", "5m", "1h", "1d", "1w" -- how
# often the scheduler is meant to invoke this builder. Older rows use
# free-form values like "daily"/"once"; those simply don't match this pattern
# and _floor_datetime leaves the timestamp unaligned (no-op), so they're
# unaffected.
#
# An optional "_<shard>" suffix ("3m_a", "3m_b", ...) is accepted and ignored
# for flooring purposes. Splitting one logical cycle across several cycle
# values is how a workload gets divided over per-account rate limits: each
# shard is scanned (run_cycle) and scheduled (its own systemd timer) on its
# own, but they all fire at the same wall-clock tick, so their snapshots have
# to floor to the *same* bucket -- which is exactly what dropping the suffix
# here gets. Without it "3m_a" wouldn't match at all and every shard would
# stamp its own raw wall-clock time instead.
_CYCLE_RE = re.compile(r"^(\d+)([mhdw])(?:_\w+)?$")


def _floor_datetime(dt: datetime, execution_cycle: str | None) -> datetime:
    """Floor dt down to the start of its execution_cycle bucket, e.g. with a
    "5m" cycle, 09:07:42 -> 09:05:00.

    The point: a snapshot job's timestamp should reflect its *scheduled*
    slot, not raw wall-clock jitter -- a scheduler invoked a few seconds late
    (or a manual re-run) shouldn't produce a one-off timestamp that doesn't
    line up with every other snapshot in the same series. Returns dt
    unchanged if execution_cycle is missing or not in "<N><m|h|d|w>" form.
    """
    if not execution_cycle:
        return dt
    m = _CYCLE_RE.match(execution_cycle.strip())
    if not m:
        return dt
    n, unit = int(m.group(1)), m.group(2)

    if unit == "m":
        floored_minute = dt.minute - (dt.minute % n)
        return dt.replace(minute=floored_minute, second=0, microsecond=0)
    if unit == "h":
        floored_hour = dt.hour - (dt.hour % n)
        return dt.replace(hour=floored_hour, minute=0, second=0, microsecond=0)
    if unit == "d":
        day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        if n == 1:
            return day_start
        epoch = date(1970, 1, 1)
        offset = (day_start.date() - epoch).days
        floored_offset = offset - (offset % n)
        return datetime.combine(epoch + timedelta(days=floored_offset), dt.min.time(), dt.tzinfo)
    if unit == "w":
        day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        monday = day_start - timedelta(days=day_start.weekday())
        if n == 1:
            return monday
        epoch_monday = date(1970, 1, 5)  # first Monday on/after the epoch
        offset_weeks = (monday.date() - epoch_monday).days // 7
        floored_weeks = offset_weeks - (offset_weeks % n)
        return datetime.combine(epoch_monday + timedelta(weeks=floored_weeks), dt.min.time(), dt.tzinfo)
    return dt


# keyword -> (resolver, default strftime format), all day-granularity --
# for execution-time (not job-generation-time) timestamps see resolve_now().
_DATE_KEYWORDS: dict[str, tuple[Callable[[date], date], str]] = {
    "today": (lambda d: d, "%Y%m%d"),
    "yesterday": (lambda d: d - timedelta(days=1), "%Y%m%d"),
    "last_month": (lambda d: _add_months(d, -1), "%Y%m%d"),
    "last_eom": (_last_eom, "%Y%m%d"),
    "last_yymm": (lambda d: _add_months(d, -1), "%Y%m"),
    "last_bday": (_last_bday, "%Y%m%d"),
}


def _resolve_date(keyword: str, fmt: str | None = None) -> str:
    try:
        resolver, default_fmt = _DATE_KEYWORDS[keyword]
    except KeyError:
        raise ValueError(
            f"Unknown date keyword '{keyword}', expected one of {sorted(_DATE_KEYWORDS)}"
        ) from None
    return resolver(_now_kst().date()).strftime(fmt or default_fmt)


# Reserved key_params_list entry (see module docstring): resolved once
# per *execution* by app.services.execution.run_job via resolve_now(),
# never during job generation -- must never appear in macro_params_json.
RESERVED_NOW_KEY = "NOW"


def resolve_now(execution_cycle: str | None = None, fmt: str = "%Y%m%d%H%M%S") -> str:
    """Capture timestamp for a RESERVED_NOW_KEY entry, floored to
    execution_cycle's bucket (see _floor_datetime) so repeated executions of
    the same scheduled job produce timestamps that line up with each other
    instead of drifting on scheduler jitter."""
    return _floor_datetime(_now_kst(), execution_cycle).strftime(fmt)


def is_repeated_api(api: ApiMst) -> bool:
    """True if api.key_params_list contains RESERVED_NOW_KEY, i.e. it's
    a repeated/snapshot poll (a quote endpoint re-executed every few minutes
    to build a time series) rather than a one-shot job. This same check
    governs several different decisions across the app (whether run_job
    stamps a fresh timestamp, whether run_cycle re-executes an
    already-existing job), so it's centralized here rather than re-derived
    at each call site -- see app.services.execution."""
    return bool(
        api.key_params_list
        and any(normalize_key_param(e)[0] == RESERVED_NOW_KEY for e in api.key_params_list)
    )


def _build_axes(session: Session, params_spec: dict[str, Any]) -> list[list[dict[str, Any]]]:
    """Turn each macro_params_json entry into an "axis": a list of partial
    param dicts, one item per value this entry can take (>1 only for db
    entries with multiple rows)."""
    axes: list[list[dict[str, Any]]] = []

    for raw_key, spec in params_spec.items():
        keys = [k.strip() for k in raw_key.split(",")]

        if isinstance(spec, dict) and "db" in spec:
            rows = _run_query_rows(session, spec["db"])
            if not rows:
                raise ValueError(f"query for '{raw_key}' returned no rows")
            axis = []
            for row in rows:
                if len(row) < len(keys):
                    raise ValueError(
                        f"query for '{raw_key}' returned {len(row)} column(s), expected {len(keys)}"
                    )
                axis.append(dict(zip(keys, row)))
            axes.append(axis)

        elif isinstance(spec, dict) and "date" in spec:
            if len(keys) != 1:
                raise ValueError(f"'date' spec must map to a single key, got '{raw_key}'")
            value = _resolve_date(spec["date"], spec.get("format"))
            axes.append([{keys[0]: value}])

        else:
            if len(keys) != 1:
                raise ValueError(f"literal value can't be assigned to multiple keys '{raw_key}'")
            axes.append([{keys[0]: spec}])

    return axes


_JOB_ID_MAX_LENGTH = 150  # matches ApiJob.job_id column (see app/db/models.py)


def normalize_key_param(entry: str | dict[str, str]) -> tuple[str, str]:
    """A key_params_list entry is either a plain param name (e.g.
    ``"SHORT_CODE"`` -> param key and result-table column are the same,
    column auto-lowercased to ``"short_code"``), or an explicit single-key
    ``{param_name: column_name}`` dict for cases where they must differ --
    e.g. Oracle reserves ``date`` as an identifier, so a job param literally
    named ``DATE`` needs ``{"DATE": "trade_date"}`` to land in a real column.
    Returns ``(param_key, column_name)``."""
    if isinstance(entry, dict):
        # Not `((param_key, column_name),) = entry.items()`: that nested
        # single-tuple-unpacking form is valid Python (and does the same
        # thing -- also raising ValueError if entry has more than one key),
        # but PyCharm's inference doesn't follow it correctly and infers both
        # param_key and column_name as tuple[str, str] (the *item* type)
        # instead of str each. Spelled out in two plain steps instead, which
        # every checker infers correctly.
        if len(entry) != 1:
            raise ValueError(f"expected a single-key {{param_name: column_name}} dict, got {entry!r}")
        param_key, column_name = next(iter(entry.items()))
        return param_key, column_name
    return entry, entry.lower()


def _build_job_id(builder: ApiJobBuilder, api: ApiMst, params: dict[str, Any]) -> tuple[str, bool]:
    """Returns (job_id, is_deterministic).

    Without ``api.key_params_list``, job_id is random (``uuid``) and
    every call produces a new job -- no duplicate check possible.

    With it (a list of param names), job_id is built from those params'
    *values* instead of a uuid, so re-generating with the same values always
    yields the same job_id -- which is what lets the caller detect "this
    exact job already exists" and skip it (e.g. a weekend run resolving
    ``last_bday`` to the same Friday as the prior day's run).

    RESERVED_NOW_KEY entries are excluded from this -- job identity has to
    stay stable across repeated executions of the same job, not mint a new
    job per tick (see module docstring / resolve_now). If that's the *only*
    key_params_list entry, there's nothing left to vary job_id by, so one
    static job_id is used for the whole builder (still deterministic --
    regenerating just finds it already exists and skips it).

    Many/long key values could push this past the DB column's max_length, so
    it falls back to a short deterministic hash of the key values instead of
    the raw ones once that would happen -- still deterministic (same values
    -> same hash), just not human-readable in that case."""
    key_params = api.key_params_list
    if not key_params:
        return f"{builder.api_id}_{uuid.uuid4().hex[:12]}", False

    identity_entries = [e for e in key_params if normalize_key_param(e)[0] != RESERVED_NOW_KEY]
    if not identity_entries:
        return f"{builder.api_id}_{builder.build_id}", True

    param_keys = [normalize_key_param(e)[0] for e in identity_entries]
    missing = [k for k in param_keys if k not in params]
    if missing:
        raise ValueError(
            f"{builder.build_id}: key_params_list references {missing}, "
            f"not present in generated params {list(params)}"
        )

    key_part = "_".join(str(params[k]) for k in param_keys)
    job_id = f"{builder.api_id}_{key_part}"

    if len(job_id) > _JOB_ID_MAX_LENGTH:
        key_hash = hashlib.sha256(key_part.encode("utf-8")).hexdigest()[:16]
        job_id = f"{builder.api_id}_{key_hash}"[:_JOB_ID_MAX_LENGTH]

    return job_id, True


def build_jobs_from_builder(session: Session, build_id: str) -> list[ApiJob]:
    builder:ApiJobBuilder | None = session.get(ApiJobBuilder, build_id)
    if builder is None:
        raise ValueError(f"No ApiJobBuilder row for build_id={build_id!r}")

    api :ApiMst | None = session.get(ApiMst, builder.api_id)
    if api is None:
        raise ValueError(f"No ApiMst row for api_id={builder.api_id!r}")

    if RESERVED_NOW_KEY in (builder.macro_params_json or {}):
        raise ValueError(
            f"{build_id}: '{RESERVED_NOW_KEY}' is reserved for ApiMst.key_params_list "
            f"(resolved at execution time) -- it must not appear in macro_params_json, "
            f"or every generation call would mint a brand new job."
        )

    axes = _build_axes(session, builder.macro_params_json or {})

    if axes:
        param_sets = []
        for combo in itertools.product(*axes):
            merged: dict[str, Any] = {}
            for part in combo:
                merged.update(part)
            param_sets.append(merged)
    else:
        param_sets = [{}]

    jobs = []
    seen_in_batch: dict[str, dict[str, Any]] = {}
    for params in param_sets:
        job_id, deterministic = _build_job_id(builder, api, params)

        if deterministic:
            # Two different param combos landing on the same job_id means
            # key_params_list doesn't actually distinguish them (e.g. only
            # BASDD is keyed but MKT_TYPE also fans out) -- fail loudly rather
            # than silently dropping one of them as a "duplicate".
            prior = seen_in_batch.get(job_id)
            if prior is not None:
                if prior != params:
                    raise ValueError(
                        f"{build_id}: ApiMst.key_params_list {api.key_params_list} is not "
                        f"specific enough -- both {prior} and {params} produce job_id={job_id!r}. "
                        f"Add the differing param(s) to key_params_list."
                    )
                continue  # identical params generated twice this batch; harmless no-op
            seen_in_batch[job_id] = params

            existing = session.get(ApiJob, job_id)
            if existing is not None:
                if existing.params_json != params:
                    raise ValueError(
                        f"{build_id}: job_id={job_id!r} already exists with different params "
                        f"{existing.params_json} vs new {params} -- ApiMst.key_params_list "
                        f"{api.key_params_list} may be insufficient."
                    )
                logger.info("{}: job {} already exists, skipping duplicate", build_id, job_id)
                continue

        job = ApiJob(
            job_id=job_id,
            api_id=builder.api_id,
            build_id=builder.build_id,
            params_json=params,
            # Was previously never copied from the builder, so every
            # generated ApiJob silently fell back to the field's own default
            # ("overwrite") regardless of what the builder said -- harmless
            # while save_mode was unenforced, but load-bearing now that
            # run_job actually deletes-before-insert on "overwrite" (see
            # app.services.execution._clear_previous_results).
            save_mode=builder.save_mode or "overwrite",
            # Also previously never copied -- load-bearing now that run_cycle
            # scans ApiJob directly by execution_cycle (see
            # app.services.execution.run_cycle) instead of only ever going
            # through the builder.
            execution_cycle=builder.execution_cycle,
        )
        session.add(job)
        jobs.append(job)

    session.commit()

    return jobs
