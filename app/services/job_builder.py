"""Expands an ``ApiJobBuilder`` row into one or more concrete ``ApiJob`` rows.

``cartesian_param_json`` is a dict whose *keys* may be a single param name or a
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
and reusable -- see ``job_key_params`` below). Use the reserved
``"NOW"`` entry in ``job_key_params`` instead: it's resolved once per
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

``ApiMst.job_key_params`` (a list of param names, e.g. ``["BASDD"]``, or
``row_stamp_json`` 이 {파라미터: 컬럼} 을 담는다) makes
job_id deterministic from those params' values instead of a random uuid, so
regenerating with the same values reuses the same job_id -- and a job whose
id already exists is skipped as a duplicate rather than re-created. Without
it, job_id is always random and every call creates fresh jobs. It lives on
``ApiMst`` rather than ``ApiJobBuilder`` because it's a property of the API's
own response/output-table shape (which fields it naturally keys records by),
not of any particular builder's parameter choices -- every builder targeting
the same api_id needs the exact same value anyway.

The reserved entry ``"NOW"`` (or ``{"NOW": "<column_name>"}``) is special: it
must never appear in ``cartesian_param_json``, and it's excluded from job_id
construction entirely (a job's identity has to stay stable across repeated
executions, or every run would mint a fresh job forever -- exactly the
explosion this reservation avoids for something like a 1-minute snapshot
poll). Instead it's resolved fresh at *execution* time, in
:func:`app.services.execution.run_job`, via :func:`resolve_now` --
floored to the job's ``ApiJobBuilder.schedule_tag`` bucket (see
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


# ApiJobBuilder.schedule_tag, e.g. "1m", "5m", "1h", "1d", "1w" -- how
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


def _floor_datetime(dt: datetime, snapshot_unit: str | None) -> datetime:
    """Floor dt down to the start of its snapshot_unit bucket, e.g. with a
    "5m" cycle, 09:07:42 -> 09:05:00.

    The point: a snapshot job's timestamp should reflect its *scheduled*
    slot, not raw wall-clock jitter -- a scheduler invoked a few seconds late
    (or a manual re-run) shouldn't produce a one-off timestamp that doesn't
    line up with every other snapshot in the same series. Returns dt
    unchanged if snapshot_unit is missing or not in "<N><m|h|d|w>" form.
    """
    if not snapshot_unit:
        return dt
    m = _CYCLE_RE.match(snapshot_unit.strip())
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


def resolve_now(snapshot_unit: str | None = None, fmt: str = "%Y%m%d%H%M%S") -> str:
    """``ApiMst.snapshot_column`` 에 찍을 관측 시각.

    ``snapshot_unit`` 버킷으로 내려 자른다(_floor_datetime) -- 같은 스케줄
    잡의 반복 실행이 스케줄러 흔들림만큼 어긋나지 않고 서로 줄을 맞춘다.

    v1 은 이 단위를 ``execution_cycle`` 에서 받았는데, 그 컬럼은 "어느 타이머가
    집어 가는가" 도 겸하고 있어서 ``daily_batch2`` 같은 값이 패턴에 안 맞아
    **절삭이 조용히 no-op** 이 됐다. v2 는 둘을 갈랐다(schedule_tag /
    snapshot_unit)."""
    return _floor_datetime(_now_kst(), snapshot_unit).strftime(fmt)


def _build_axes(session: Session, params_spec: dict[str, Any]) -> list[list[dict[str, Any]]]:
    """Turn each cartesian_param_json entry into an "axis": a list of partial
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


def _build_job_id(builder: ApiJobBuilder, api: ApiMst, params: dict[str, Any]) -> tuple[str, bool]:
    """Returns (job_id, is_deterministic).

    ``api.job_key_params`` 가 없으면 job_id 는 무작위(uuid)이고 매 호출이 새
    잡을 만든다 -- 중복 검사가 불가능하다.

    있으면(파라미터 이름 목록) job_id 를 그 파라미터들의 **값**으로 만든다.
    같은 값으로 다시 생성하면 같은 job_id 가 나오므로, 부르는 쪽이 "이 잡은
    이미 있다" 를 알아보고 건너뛸 수 있다(주말 실행이 ``last_bday`` 를 전날과
    같은 금요일로 풀는 경우).

    관측 시각은 여기 들어오지 않는다 -- 잡의 정체성은 반복 실행 사이에
    고정돼야 하고, 틱마다 새 잡을 만들어서는 안 된다(resolve_now 참고).
    v1 은 그래서 job_key_params 에서 'NOW' 항목을 걸러내야 했는데, v2 는
    job_key_params 가 애초에 정체성만 담는다 -- 시각은 snapshot_column 이고
    서로 다른 컬럼이다.

    job_key_params 가 비어 있으면(그러나 is_repeated 면) 변주할 것이 없으므로
    빌더 하나에 정적 job_id 하나를 쓴다. 여전히 결정적이다 -- 다시 생성하면
    이미 있는 것을 찾아 건너뛴다.

    키 값이 많거나 길면 컬럼 max_length 를 넘을 수 있어, 그때는 키 값들의 짧은
    결정적 해시로 떨어진다 -- 같은 값이면 같은 해시이므로 결정성은 유지되고,
    사람이 읽을 수 없게 되는 것만 잃는다."""
    param_keys = list(api.job_key_params or [])
    if not param_keys:
        # 반복 잡은 정적 job_id 하나로 돈다. 그 외에는 무작위 -- 중복 검사가
        # 불가능하므로 매 호출이 새 잡을 만든다.
        if api.is_repeated:
            return f"{builder.api_id}_{builder.build_id}", True
        return f"{builder.api_id}_{uuid.uuid4().hex[:12]}", False

    missing = [k for k in param_keys if k not in params]
    if missing:
        raise ValueError(
            f"{builder.build_id}: job_key_params references {missing}, "
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

    axes = _build_axes(session, builder.cartesian_param_json or {})

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
            # job_key_params doesn't actually distinguish them (e.g. only
            # BASDD is keyed but MKT_TYPE also fans out) -- fail loudly rather
            # than silently dropping one of them as a "duplicate".
            prior = seen_in_batch.get(job_id)
            if prior is not None:
                if prior != params:
                    raise ValueError(
                        f"{build_id}: ApiMst.job_key_params {api.job_key_params} is not "
                        f"specific enough -- both {prior} and {params} produce job_id={job_id!r}. "
                        f"Add the differing param(s) to job_key_params."
                    )
                continue  # identical params generated twice this batch; harmless no-op
            seen_in_batch[job_id] = params

            existing = session.get(ApiJob, job_id)
            if existing is not None:
                if existing.params_json != params:
                    raise ValueError(
                        f"{build_id}: job_id={job_id!r} already exists with different params "
                        f"{existing.params_json} vs new {params} -- ApiMst.job_key_params "
                        f"{api.job_key_params} may be insufficient."
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
            # scans ApiJob directly by schedule_tag (see
            # app.services.execution.run_cycle) instead of only ever going
            # through the builder.
            schedule_tag=builder.schedule_tag,
            snapshot_unit=builder.snapshot_unit,
        )
        session.add(job)
        jobs.append(job)

    session.commit()

    return jobs
