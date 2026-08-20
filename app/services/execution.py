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

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any, cast

from loguru import logger
from sqlalchemy import Column, delete as sa_delete
from sqlmodel import Session, select

from app.db.models import ApiJob, ApiJobBuilder, ApiJobLog, ApiMst
from app.scrapers.dynamic import DynamicApiScraper
from app.services.export import TABLE_REGISTRY
from app.services.job_builder import (
    RESERVED_NOW_KEY,
    build_jobs_from_builder,
    is_repeated_api,
    normalize_key_param,
    resolve_now,
)


def _log(session: Session, job_id: str, status: str, error_message: str | None = None) -> ApiJobLog:
    """Append one execution-history row for a job. Never updates existing rows."""
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
        if model_cls is None or "job_id" not in model_cls.model_fields:
            continue
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
        scraper = DynamicApiScraper(api, params=job.params_json)
        counts = scraper.run_and_save(session, job_id=job.job_id, key_params=key_params)
        job.description = ", ".join(f"{table}: {n}" for table, n in counts.items())
        if not is_repeated:
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
        session.add(job)
        session.commit()
        _log(session, job.job_id, "SUCCESS")
    except Exception as exc:  # noqa: BLE001 - persisted for later inspection
        logger.exception("{}: job {} failed", job.api_id, job.job_id)
        _log(session, job.job_id, "FAILED", error_message=str(exc)[:4000])

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
    for job in jobs:
        run_job(session, job)
        results[job.job_id] = job.description
        logger.info("run_cycle({}): executed {}", execution_cycle, job.job_id)

    return results
