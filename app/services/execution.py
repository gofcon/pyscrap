"""Runs ``ApiJob`` rows against the scraping engine and orchestrates
schedule-driven batches of them.

:func:`run_job` runs one already-existing job (logging SUCCESS/FAILED to
``ApiJobLog``). :func:`run_cycle` is the entrypoint a systemd timer should
call: it executes every currently-pending ``ApiJob`` whose ``execution_cycle``
matches (after job generation has created them).

Job *generation* comes in three granularities, all dedup-safe/idempotent
(see build_jobs_from_builder): :func:`generate_jobs` (one execution_cycle --
what a systemd timer calls), :func:`generate_all_jobs` (every active builder,
any cycle), :func:`generate_jobs_for_builder` (one builder by id, regardless
of cycle/is_active) -- the latter two exist for manual/API-triggered
generation (see app.api.routers.jobs), where the systemd-timer-shaped
"one cycle at a time" scoping isn't always what's wanted.
"""

from __future__ import annotations

import os
import time
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any, cast

import httpx
from loguru import logger
from sqlalchemy import Column, delete as sa_delete, func
from sqlmodel import Session, select
from tenacity import RetryError

from app.db.models import ApiJob, ApiJobBuilder, ApiJobLog, ApiMst
from app.scrapers import make_scraper
from app.scrapers.base import SiteBlocked
from app.auth_config import resolve_env_placeholders
from app.services.export import TABLE_REGISTRY
from app.services.job_builder import (
    NO_COLUMN,
    RESERVED_NOW_KEY,
    build_jobs_from_builder,
    is_repeated_api,
    normalize_key_param,
    resolve_now,
)


def _log(session: Session, job_id: str, status: str, error_message: str | None = None,
         commit: bool = True) -> ApiJobLog:
    """Append one execution-history row for a job. Never updates existing rows.

    ``commit=False`` just stages the row, leaving the commit to the caller --
    used by the success path in run_job, which has an ApiJob update and a
    batch of result rows to persist in the same transaction anyway (one
    ~32ms round-trip instead of three)."""
    log = ApiJobLog(
        job_id=job_id,
        status=status,
        error_message=error_message,
        # .utcnow() is deprecated (returns a naive datetime that's *implicitly*
        # UTC -- easy to mix up with local time by accident). .now(timezone.utc)
        # is the replacement; the column itself is a plain DateTime (no tz), so
        # the tzinfo marker is dropped on write/read either way -- same stored
        # value, verified live -- this only removes the deprecation, no
        # behavior change.
        executed_at=datetime.now(timezone.utc),
    )
    session.add(log)
    if commit:
        session.commit()
    return log


def _clear_previous_results(session: Session, api: ApiMst, job_id: str) -> None:
    """Delete this job_id's previously-saved rows from every table it
    writes to, before it runs again -- for save_mode='overwrite' (the
    field's default), so re-running the same job_id replaces stale rows
    instead of accumulating duplicates every time. A repeated/snapshot job
    (RESERVED_NOW_KEY -- see run_job) must use save_mode='append'
    instead, or every execution would wipe out the history it's building."""
    for table_name in set((api.output_tables_json or {}).values()):
        model_cls = TABLE_REGISTRY.get(table_name)
        if model_cls is None:
            continue
        # No job_id check needed: TABLE_REGISTRY only admits models that have
        # one -- that's what defines a result table (see
        # app.services.export._discover_table_registry).
        # model_cls is only known to be *some* SQLModel subclass here, not
        # specifically one with a job_id column (that's what the
        # model_fields check above already verified at runtime -- a type
        # checker can't follow that check as narrowing). cast() forces the
        # static type to a real Column regardless of how any given checker's
        # attribute-resolution handles a dynamic getattr() -- without it,
        # '==' below gets inferred as plain Python object equality (-> bool)
        # instead of SQLAlchemy's overloaded '==' (-> ColumnElement[bool],
        # a SQL condition), which is what .where() actually needs.
        job_id_column = cast(Column, getattr(model_cls, "job_id"))
        session.exec(sa_delete(model_cls).where(job_id_column == job_id))
    session.commit()


def run_job(session: Session, job: ApiJob) -> ApiJob:
    """Run an already-existing ``ApiJob`` (e.g. created ahead of time via
    :func:`app.services.job_builder.build_jobs_from_builder`), logging the
    outcome to ``ApiJobLog`` (SUCCESS or FAILED only -- no RUNNING marker)."""

    # Explicit annotation: PyCharm's own type stubs mis-resolve
    # Session.get()'s generic (_EntityBindKey[_O]) signature and infer
    # type[ApiMst] (the class) instead of ApiMst | None (an instance) --
    # verified pyright doesn't have this issue, so this is a PyCharm-only
    # inference gap, not an actual runtime/type problem. An explicit
    # annotation overrides whatever a checker infers on its own.
    api: ApiMst | None = session.get(ApiMst, job.api_id)
    if api is None:
        raise ValueError(f"No ApiMst row for api_id={job.api_id!r}")

    if job.save_mode == "overwrite":
        _clear_previous_results(session, api, job.job_id)

    # is_repeated / key_params_list are ApiMst properties (see its field
    # comment in app.db.models) -- always available, no build_id/builder
    # needed.
    is_repeated = is_repeated_api(api)

    # Only the key subset (not all of params_json) gets stamped onto result
    # rows. Dict keys here are already the target *column* names (see
    # normalize_key_param), not necessarily the raw params_json key names.
    key_params: dict[str, Any] = {}
    for entry in api.key_params_list or []:
        param_key, column_name = normalize_key_param(entry)
        if column_name == NO_COLUMN:
            # The entry earns its place by shaping job_id (and, for
            # RESERVED_NOW_KEY, by marking the API repeated) -- there is no
            # column to land in. Skipped rather than stamped-and-dropped so
            # the intent is on the record instead of relying on pydantic
            # discarding a key that matches no field.
            continue
        if param_key == RESERVED_NOW_KEY:
            # Capture time for repeated/snapshot jobs (e.g. a quote endpoint
            # polled every few minutes) -- resolved fresh on every execution,
            # never baked into job_id/params_json, so the same job runs over
            # and over instead of one job being generated per tick. Floored
            # to job.execution_cycle -- copied from the builder at
            # generation time (see build_jobs_from_builder), so no builder
            # lookup is needed here; a manually-constructed job with no
            # build_id just has the field's own default instead. See
            # job_builder.resolve_now.
            key_params[column_name] = resolve_now(job.execution_cycle)
        elif param_key in job.params_json:
            key_params[column_name] = job.params_json[param_key]

    try:
        # HTTP or browser, by the row's own request_type -- see
        # app.scrapers.make_scraper. Everything from here down is the same
        # either way.
        scraper = make_scraper(api, params=job.params_json)
        counts = scraper.run_and_save(session, job_id=job.job_id, key_params=key_params)
        job.description = ", ".join(f"{table}: {n}" for table, n in counts.items())
        # A row can say that an empty reply means "not yet" rather than
        # "nothing" (response_parse_json['empty_is_pending']). KRX's open API
        # answers a day it has not published with zero rows and a 200, and
        # the job used to close on that -- 2026-09-01's index bars were
        # missed that way and put back by hand twelve days later. With the
        # flag the job stays pending and the next cycle asks again; a day
        # that never comes (a holiday) is retired by sp_retire_expired_jobs
        # once it is a week old. Scoped by configuration like logged_out and
        # blocked: an empty reply is a real answer for most rows.
        pending_empty = (not is_repeated
                         and bool((api.response_parse_json or {}).get("empty_is_pending"))
                         and sum(counts.values()) == 0)
        if pending_empty:
            job.description = f"no data yet ({job.description})"
        elif not is_repeated:
            # One-shot job: done for good once it succeeds -- flip inactive
            # so run_cycle's ApiJob-based scan (execution_cycle + is_active)
            # won't pick it up again on a future tick (a *failed* one-shot
            # job stays active, so it's naturally retried next tick instead
            # of needing separate retry logic). A repeated job is never
            # flipped -- staying active forever is the whole point (see
            # RESERVED_NOW_KEY).
            job.is_active = False
        # Export finalization (CSV -> Parquet -> upload) is *not* done here
        # -- it's a separate step, the same way generate_jobs was pulled out
        # of run_cycle: see app.services.export.finalize_pending_exports,
        # meant to run once as the last step of the day's batch, after every
        # cycle's jobs have already executed.
        # One commit for everything this job produced: the result rows staged
        # by run_and_save, the ApiJob update above, and the SUCCESS log entry.
        session.add(job)
        _log(session, job.job_id, "SUCCESS", commit=False)
        session.commit()
    except Exception as exc:  # noqa: BLE001 - persisted for later inspection
        logger.exception("{}: job {} failed", job.api_id, job.job_id)
        # Discard whatever the failed job already added to the session before
        # it raised (run_and_save stages rows selector by selector and leaves
        # committing to us). Without this, _log's own commit below would push
        # that half-saved state to the DB alongside the FAILED marker, and any
        # later job reusing this session inherits the dirty state -- harmless
        # when a cycle held one job, but a cycle now runs hundreds of them in
        # a single session, so one failure would otherwise spread down the
        # rest of the run.
        session.rollback()
        # fetch() retries through tenacity, and a request that failed three
        # times comes out wrapped: str(exc) is "RetryError[<Future ... raised
        # HTTPStatusError>]", the class name and nothing else. Ten days of
        # TIGER_ETF_PDF failures read exactly that and could not say whether
        # the site answered 403, 429 or 500 -- the one thing the log needed
        # to record. Log the last attempt's own exception instead, which
        # for httpx carries the status and the URL.
        if isinstance(exc, RetryError) and exc.last_attempt.failed:
            exc = exc.last_attempt.exception()
        _log(session, job.job_id, "FAILED", error_message=str(exc)[:4000])
        # A refusal is logged like any failure -- once, on the job that met
        # it -- and then handed up: run_cycle has to stop sending to that
        # host, which no single job can decide. Every other failure stays
        # here, since it says nothing about the next job.
        if isinstance(exc, SiteBlocked):
            raise exc

    return job


def _generate_jobs_for_builders(session: Session, builders: Sequence[ApiJobBuilder]) -> list[ApiJob]:
    created: list[ApiJob] = []
    for builder in builders:
        created.extend(build_jobs_from_builder(session, builder.build_id))
    return created


def generate_jobs(session: Session, execution_cycle: str) -> list[ApiJob]:
    """Generate every active ApiJobBuilder's job(s) for this execution_cycle
    (via build_jobs_from_builder) -- e.g. today's daily job, or a
    repeated/snapshot job's one static row (see RESERVED_NOW_KEY in
    app.services.job_builder). Deliberately separate from actually executing
    anything (see run_cycle): generation and execution are independently
    callable/schedulable steps, not fused into one. build_jobs_from_builder
    is idempotent/dedup-safe, so calling this repeatedly (or calling it
    without ever calling run_cycle) is harmless.

    The systemd-timer level (see app.cli/scripts) -- one call per
    execution_cycle in use. For the other two granularities, see
    generate_jobs_for_builder (one builder, regardless of cycle/is_active)
    and generate_all_jobs (every active builder, regardless of cycle).

    Returns the newly-created ApiJob rows (excludes anything that already
    existed -- see build_jobs_from_builder)."""
    builders = session.exec(
        select(ApiJobBuilder).where(
            ApiJobBuilder.is_active == True,  # noqa: E712 - Oracle native BOOLEAN column rejects IS-based binds (ORA-00908), needs plain equality
            ApiJobBuilder.execution_cycle == execution_cycle,
        )
    ).all()
    return _generate_jobs_for_builders(session, builders)


def generate_all_jobs(session: Session) -> list[ApiJob]:
    """Generate every active ApiJobBuilder's job(s), regardless of
    execution_cycle -- e.g. a manual "generate everything now" trigger
    (see app.api.routers.jobs). Same idempotent/dedup-safe guarantee as
    generate_jobs; just not scoped to one cycle."""
    builders = session.exec(
        select(ApiJobBuilder).where(ApiJobBuilder.is_active == True)  # noqa: E712 - see generate_jobs
    ).all()
    return _generate_jobs_for_builders(session, builders)


def generate_jobs_for_builder(session: Session, build_id: str) -> list[ApiJob]:
    """Generate one specific ApiJobBuilder's job(s) by id, regardless of its
    execution_cycle/is_active -- e.g. a manual trigger for a single builder
    from the API (see app.api.routers.jobs), including testing a builder
    that's still inactive. Thin pass-through: build_jobs_from_builder does
    all the actual work and is already idempotent/dedup-safe; this exists so
    callers only need app.services.execution's three generate_* functions
    instead of reaching into app.services.job_builder directly."""
    return build_jobs_from_builder(session, build_id)


# How long a refused host is left alone before its deferred jobs are tried
# again. KRX's window was measured at about ten minutes (refused from 00:00,
# serving again by 00:10, on 2026-09-17); a longer wait costs nothing but
# the clock, a shorter one risks a second refusal that may lengthen the
# window.
_BLOCK_COOLDOWN_SEC = float(os.environ.get("BLOCK_COOLDOWN_SEC", "600"))


class _HostIndex:
    """Which host each job talks to, looked up once per api_id."""

    def __init__(self, session: Session):
        self._session = session
        self._by_api: dict[str, str] = {}

    def of(self, job: ApiJob) -> str:
        host = self._by_api.get(job.api_id)
        if host is None:
            api = self._session.get(ApiMst, job.api_id)
            url = resolve_env_placeholders(api.api_url) if api and api.api_url else ""
            host = httpx.URL(url).host if url else ""
            self._by_api[job.api_id] = host
        return host


def _run_deferred(session: Session, host: str, pending: list[ApiJob]) -> dict[str, str | None]:
    """One more pass over a refused host's jobs, after the refusal window.

    Waits the window out, logs in again (the refusal may have voided the
    session, and the login row is the cheapest request to test the water
    with), then runs the jobs in order. A second refusal -- from the login
    or from any job -- ends the pass: the host is given up for this run and
    whatever is left stays active for the next one. That is the whole
    difference from before: a refused night used to fail every remaining
    job one by one, ten thousand lines of the same error, while asking the
    site ten thousand more times."""
    results: dict[str, str | None] = {}
    logger.warning("{}: {} job(s) deferred -- waiting {:.0f}s before trying again",
                   host, len(pending), _BLOCK_COOLDOWN_SEC)
    time.sleep(_BLOCK_COOLDOWN_SEC)
    api = session.get(ApiMst, pending[0].api_id)
    try:
        if api is not None and (api.response_parse_json or {}).get("login"):
            make_scraper(api, params=pending[0].params_json)._login(session)
    except Exception as exc:  # noqa: BLE001 - any refusal of the login means the host is still closed
        logger.error("{}: login refused after the wait ({}) -- giving the host up for this run, "
                     "{} job(s) left pending", host, str(exc)[:120], len(pending))
        return results
    for i, job in enumerate(pending):
        try:
            run_job(session, job)
        except SiteBlocked:
            logger.error("{}: refused again at job {} -- giving the host up for this run, "
                         "{} job(s) left pending", host, job.job_id, len(pending) - i)
            break
        results[job.job_id] = job.description
    return results


def _login_rows_first(session: Session, jobs: Sequence[ApiJob]) -> list[ApiJob]:
    """Order a cycle's jobs so that its login rows run before anything else.

    A login row (``ApiMst.response_type == 'session'``) leaves a cookie jar
    behind -- in the process-wide httpx.Client for an HTTP row, in a storage
    state file for a browser one -- and everything else on that site depends
    on it having run. Nothing else in this engine cares what order jobs run
    in, so this is deliberately not a general dependency graph: the row's own
    response_type already says "this exists to precede others", and a second
    kind of ordering can grow its own column when there is a second kind.

    Note what this does *not* solve: a session that expires mid-batch, which
    ordering cannot help with because the expiry lands between two jobs that
    are already correctly ordered. That is handled where it has to be -- the
    job that hits it logs in again and retries once (see
    app.scrapers.base.BaseScraper.collect). This just saves the first job of a
    run from taking that path every time."""
    login_apis = set(session.exec(
        select(ApiMst.api_id).where(func.lower(ApiMst.response_type) == "session")
    ).all())
    if not login_apis:
        return list(jobs)
    # sorted() is stable, so everything else keeps the order the DB gave it.
    return sorted(jobs, key=lambda job: 0 if job.api_id in login_apis else 1)


def run_cycle(session: Session, execution_cycle: str) -> dict[str, str | None]:
    """Execute every currently-pending ApiJob for this execution_cycle --
    the entrypoint a systemd timer should call (e.g.
    ``python -m app.cli run-cycle 5m``), one timer per distinct
    execution_cycle value in use. Does NOT generate jobs itself -- call
    generate_jobs first (a separate step; see its docstring for why).

    What actually runs is decided by scanning ApiJob directly
    (execution_cycle + is_active=True). is_active=True means "still
    pending": a repeated job is never flipped inactive (see run_job), so
    it's always in this set and keeps re-running forever; a one-shot job is
    flipped inactive the moment it succeeds, so a past period's
    already-succeeded job drops out of this set on its own, while a *failed*
    one naturally stays and gets retried next tick -- no separate retry
    bookkeeping needed.

    Returns {job_id: description}, one entry per job actually executed."""
    results: dict[str, str | None] = {}
    jobs = session.exec(
        select(ApiJob).where(
            ApiJob.execution_cycle == execution_cycle,
            ApiJob.is_active == True,  # noqa: E712 - Oracle native BOOLEAN column rejects IS-based binds (ORA-00908), needs plain equality
        )
    ).all()
    # A host that has refused us is set aside rather than hammered: its
    # remaining jobs are deferred, the rest of the cycle goes on, and the
    # deferred ones get one more chance after the refusal window has passed
    # (see _run_deferred). Jobs are grouped by host because that is the
    # unit a site refuses at -- one login, one rate limit -- and a cycle
    # interleaves several hosts, none of which should pay for another's.
    hosts = _HostIndex(session)
    deferred: dict[str, list[ApiJob]] = {}
    for job in _login_rows_first(session, jobs):
        host = hosts.of(job)
        if host in deferred:
            deferred[host].append(job)
            continue
        try:
            run_job(session, job)
        except SiteBlocked as exc:
            logger.warning("{} refused us at job {} -- deferring its remaining jobs", exc.host, job.job_id)
            deferred.setdefault(exc.host, []).append(job)
            continue
        results[job.job_id] = job.description
    for host, pending in deferred.items():
        results.update(_run_deferred(session, host, pending))
        logger.info("run_cycle({}): executed {}", execution_cycle, job.job_id)

    return results
