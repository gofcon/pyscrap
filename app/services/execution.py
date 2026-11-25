"""Runs ``ApiJob`` rows against the scraping engine and orchestrates
schedule-driven batches of them.

:func:`run_job` runs one already-existing job (logging SUCCESS/FAILED to
``ApiJobLog``). :func:`run_cycle` is the entrypoint a systemd timer should
call: it executes every currently-pending ``ApiJob`` whose ``schedule_tag``
matches (after job generation has created them).

Job *generation* comes in three granularities, all dedup-safe/idempotent
(see build_jobs_from_builder): :func:`generate_jobs` (one schedule_tag --
what a systemd timer calls), :func:`generate_all_jobs` (every active builder,
any cycle), :func:`generate_jobs_for_builder` (one builder by id, regardless
of cycle/is_active) -- the latter two exist for manual/API-triggered
generation (see app.api.routers.jobs), where the systemd-timer-shaped
"one cycle at a time" scoping isn't always what's wanted.
"""

from __future__ import annotations

import os
import re
import time
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any, cast

import httpx
from loguru import logger
from sqlalchemy import Column, delete as sa_delete, func
from sqlmodel import Session, select
from tenacity import RetryError

from app.auth_config import resolve_env_placeholders
from app.db.models import ApiJob, ApiJobBuilder, ApiJobLog, ApiMst
from app.errors import ScrapeError, SourceRefused, classify, detail_of
from app.scrapers import make_scraper
from app.services.export import TABLE_REGISTRY
from app.services.job_builder import (
    build_jobs_from_builder,
    resolve_now,
)


def _log(session: Session, job_id: str, status: str, error_message: str | None = None,
         commit: bool = True, *, error_code: str | None = None,
         error_detail: str | None = None, record_count: int | None = None,
         duration_ms: int | None = None, attempt_no: int = 1,
         target_date: str | None = None, executed_at: datetime | None = None) -> ApiJobLog:
    """Append one execution-history row for a job. Never updates existing rows.

    ``commit=False`` just stages the row, leaving the commit to the caller --
    used by the success path in run_job, which has an ApiJob update and a
    batch of result rows to persist in the same transaction anyway (one
    ~32ms round-trip instead of three).

    v1 은 status 와 error_message 둘만 채웠다. 나머지 다섯(error_code ·
    error_detail · record_count · duration_ms · target_date)은 v2 에서 컬럼이
    생겼고 전부 NULL 이었다 -- 컬럼만 있고 채우는 쪽이 없으면 "어제 상대 쪽
    문제는 몇 건인가" 를 여전히 문자열로 추측해야 한다 (docs/SCHEMA-V2.md
    6.3). 호출부는 execution 안의 셋뿐이다: 성공/실패 경로와 차단기가 건너뛴
    잡."""
    log = ApiJobLog(
        job_id=job_id,
        status=status,
        error_code=error_code,
        error_message=error_message,
        error_detail=error_detail,
        record_count=record_count,
        duration_ms=duration_ms,
        attempt_no=attempt_no,
        target_date=target_date,
        # .utcnow() is deprecated (returns a naive datetime that's *implicitly*
        # UTC -- easy to mix up with local time by accident). .now(timezone.utc)
        # is the replacement; the column itself is a plain DateTime (no tz), so
        # the tzinfo marker is dropped on write/read either way -- same stored
        # value, verified live -- this only removes the deprecation, no
        # behavior change.
        #
        # executed_at 을 받는 이유: 차단기가 건너뛴 잡의 로그는 사이클 끝에서
        # 한 번에 적는데(_FailStreak.report), 그때 찍으면 건너뛴 시각이 아니라
        # 사이클이 끝난 시각이 남는다.
        executed_at=executed_at or datetime.now(timezone.utc),
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
    instead, or every execution would wipe out the history it's building.

    Deliberately does NOT commit. The delete and the rows that replace it are
    one change: committing here made a failed fetch destructive, because
    run_job's rollback then had nothing left to undo. On 2026-10-02 the KRX
    portal answered the 07:00 login with a notice page, all 27 listing jobs
    failed on the reply, and krx_deriv_info -- 23,000 rows, the source the
    expiry calendar and the futures/options master are built from -- was
    emptied. Nothing had replaced it; the previous day's copy was simply
    gone. Leaving the delete uncommitted means a failure leaves yesterday's
    rows in place, which is what "overwrite" should mean: replaced, or not
    touched.
    """
    for table_name in set((api.record_map_json or {}).values()):
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


_DAY_RE = re.compile(r"\D")


def _target_date(key_params: dict[str, Any]) -> str | None:
    """이 실행이 **어느 날짜분**을 받은 것인가 (YYYYMMDD), 모르면 None.

    결과 행에 찍히는 값들(key_params)에서 찾는다. 그 자리에 이미 둘 다 모여
    있기 때문이다 -- 조회일을 받는 API 는 row_stamp_json 이 지목한 날짜
    파라미터가, 스냅샷 API 는 snapshot_column 에 들어갈 관측 시각이 거기 있다.
    빌더를 다시 찾거나 job_id 를 파싱하지 않는다.

    왜 이 컬럼이 필요한가: 지금 "그날 것을 이미 받았나" 는 job_id 에 날짜가
    구워져 있는 경우에만 답할 수 있고, 스냅샷 API 는 job_id 가 고정이라 아예
    답이 없다. 로그에 날짜가 있으면 둘 다 같은 질의가 된다.

    날짜인지 판정을 값으로 한다. 숫자만 남겨 8자리이고 연·월·일이 말이 되면
    날짜로 본다 -- 운용사마다 표기가 달라서('2026-11-24', '2026.11.24',
    '20261124') 형식을 하나로 고정할 수 없고, 파라미터 **이름**으로 거르면
    원천마다 다른 이름(BAS_DD, trdDd, std_dt, inqDt…)을 다 알아야 한다.
    6자리(YYYYMM)나 종목코드는 자연히 떨어진다."""
    for value in key_params.values():
        if not isinstance(value, str):
            continue
        digits = _DAY_RE.sub("", value)
        if len(digits) != 8:
            continue
        year, month, day = int(digits[:4]), int(digits[4:6]), int(digits[6:])
        if 1990 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
            return digits
    return None


def run_job(session: Session, job: ApiJob) -> ApiJob:
    """Run an already-existing ``ApiJob`` (e.g. created ahead of time via
    :func:`app.services.job_builder.build_jobs_from_builder`), logging the
    outcome to ``ApiJobLog`` (one row per run, never a RUNNING marker)."""
    _execute_job(session, job)
    return job


def _execute_job(session: Session, job: ApiJob, attempt_no: int = 1) -> ScrapeError | None:
    """run_job's body, with the outcome returned instead of discarded: None if
    the job succeeded, otherwise the **graded** failure.

    v1 은 bool 을 돌려줬고 run_cycle 은 그걸로 연속 실패를 셌다. 등급이
    생기면서 bool 로는 모자라다 -- 우리 설정이 틀린 것과 상대가 죽은 것이
    같은 False 였고, 둘 다 세면 설정 오류 하나가 그 api 의 멀쩡한 잡들까지
    사이클에서 빼버린다. 이제 호출부가 ``exc.counts_toward_retire`` 를 본다.

    ``attempt_no`` 는 **이 사이클 안에서** 이 잡이 몇 번째로 시도되는가다.
    거절된 호스트의 잡이 창을 기다린 뒤 다시 도는 경우(_run_deferred)에만
    1 보다 크다. 사이클을 넘는 재시도는 여기서 세지 않는다 -- 로그에 행이
    하나씩 더 쌓이므로 ``COUNT(*) ... WHERE job_id = ?`` 가 이미 답이고,
    잡마다 그 COUNT 를 하면 사이클당 수백 번의 왕복이 된다."""

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

    # v1 은 이 셋을 key_params_list 한 컬럼에서 끌어내야 했고, 그래서 예약어
    # 'NOW'(반복이라는 뜻)와 'none_mapped'(컬럼 자리에 컬럼 아닌 것)가
    # 필요했다. v2 는 컬럼이 각각 한 가지만 뜻한다.
    is_repeated = api.is_repeated

    # params_json 전체가 아니라 지목된 것만 결과 행에 찍는다. row_stamp_json 의
    # 키는 파라미터 이름이고 값이 **컬럼** 이름이다 -- 둘이 다를 수 있다
    # (Oracle 이 date 를 예약해서, DATE 라는 파라미터는 trade_date 로 가야 한다).
    #
    # 찍지 않을 파라미터는 그냥 적지 않는다 -- v1 의 none_mapped 가 하던 일이
    # 없어진다.
    key_params: dict[str, Any] = {}
    for param_key, column_name in (api.row_stamp_json or {}).items():
        if param_key in job.params_json:
            key_params[column_name] = job.params_json[param_key]

    # 관측 시각. 조회일을 받지도 돌려주지도 않는 스냅샷 API 용이고, 실행마다
    # 새로 풀어서 job_id/params_json 에 굽지 않는다 -- 그래서 같은 잡이 계속
    # 돌고, 틱마다 잡이 하나씩 생기지 않는다. job.snapshot_unit 으로 내려
    # 자르는데, 그 값은 생성 때 빌더에서 복사돼 있으므로 빌더를 다시 찾지
    # 않는다 (job_builder.resolve_now).
    if api.snapshot_column:
        key_params[api.snapshot_column] = resolve_now(job.snapshot_unit)

    # 로그에 적을 둘. 날짜는 key_params 가 다 모인 **뒤에** 뽑는다 -- 스냅샷
    # API 는 바로 위에서 들어간 관측 시각이 유일한 날짜다.
    target_date = _target_date(key_params)
    started = time.monotonic()

    try:
        # HTTP or browser, by the row's own protocol -- see
        # app.scrapers.make_scraper. Everything from here down is the same
        # either way.
        scraper = make_scraper(api, params=job.params_json)
        counts = scraper.run_and_save(session, job_id=job.job_id, key_params=key_params)
        job.engine_note = ", ".join(f"{table}: {n}" for table, n in counts.items())
        # A row can say that an empty reply means "not yet" rather than
        # "nothing" (options_json['empty_is_pending']). KRX's open API
        # answers a day it has not published with zero rows and a 200, and
        # the job used to close on that -- 2026-09-01's index bars were
        # missed that way and put back by hand twelve days later. With the
        # flag the job stays pending and the next cycle asks again; a day
        # that never comes (a holiday) is retired by sp_retire_expired_jobs
        # once it is a week old. Scoped by configuration like logged_out and
        # blocked: an empty reply is a real answer for most rows.
        #
        # api_rst 를 세는지는 그 행에서 api_rst 가 **무엇인지**에 달렸다.
        # v1 에서 api_rst 는 곁다리였다 -- 타입 테이블에 넣으면서 원본 응답도
        # 같이 보관하는 자리였고, 그 셀렉터는 응답 전체를 한 레코드로 주므로
        # 데이터가 없어도 1을 센다. 그래서 2026-09-19 에 세는 것에서 뺐다.
        #
        # v2 에서는 같은 표가 여덟 소스의 **본 저장소**다 (두 층 -- 타입
        # 테이블 대신 api_rst + v_* 뷰). 거기서는 api_rst 의 10이 곧 받은
        # 10건이라, 그대로 빼면 데이터가 들어왔는데도 "아직 아니다" 가 된다.
        #
        # 둘을 record_map_json 으로 가른다: api_rst 가 다른 표와 **함께**
        # 나오면 곁다리고, 혼자면 본 저장소다.
        targets = set((api.record_map_json or {}).values())
        sidecars = {"api_rst"} if len(targets) > 1 and "api_rst" in targets else set()
        pending_empty = (not is_repeated
                         and bool((api.options_json or {}).get("empty_is_pending"))
                         and sum(n for table, n in counts.items() if table not in sidecars) == 0)
        if pending_empty:
            job.engine_note = f"no data yet ({job.engine_note})"
        elif not is_repeated:
            # One-shot job: done for good once it succeeds -- flip inactive
            # so run_cycle's ApiJob-based scan (schedule_tag + is_active)
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
        # 0행을 "아직 아니다" 로 보는 행은 status 가 EMPTY 다 (SourceEmpty 의
        # 등급 -- app.errors). 실패가 아니라서 error_code 는 비어 있고, 그
        # 둘의 조합이 "성공했지만 받은 것이 없다" 를 뜻한다. v1 은 이것이
        # SUCCESS 와 구별되지 않아서 engine_note 문자열로만 남았다.
        #
        # record_count 는 곁다리까지 포함한 **적재한 전부**다. 위 판정은
        # "데이터가 왔나" 를 묻기 때문에 곁다리를 빼지만, "몇 건 넣었나" 의
        # 답은 넣은 전부다.
        _log(session, job.job_id, "EMPTY" if pending_empty else "SUCCESS", commit=False,
             record_count=sum(counts.values()),
             duration_ms=int((time.monotonic() - started) * 1000),
             attempt_no=attempt_no, target_date=target_date)
        session.commit()
        return None
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

        # 등급을 붙인다. 직접 올린 것(MappingError 등)은 그대로 오고, httpx 와
        # 드라이버가 올린 것은 여기서 꽂힌다 -- 그러지 않으면 error_code 가
        # 'Exception' 으로 수렴해서 컬럼을 더한 보람이 없다 (app.errors.classify).
        err = classify(exc)
        if err.alert:
            # 알림 채널은 없다. 사람이 봐야 하는 것은 error_code 로 질의되고,
            # 로그에서는 등급 이름을 앞에 세워 눈에 걸리게 한다.
            logger.error("{}: {} -- {}", job.api_id, type(err).__name__, err)
        _log(session, job.job_id, err.log_status,
             error_code=type(err).__name__,
             error_message=str(err)[:4000],
             # 응답 본문 앞부분 + 트레이스백. v1 은 4000자 한 칸뿐이라 응답을
             # 담을 수 없어서 'HTTPStatusError' 만 남는 경우가 있었다.
             error_detail=detail_of(exc),
             duration_ms=int((time.monotonic() - started) * 1000),
             attempt_no=attempt_no, target_date=target_date)
        # A refusal is logged like any failure -- once, on the job that met
        # it -- and then handed up: run_cycle has to stop sending to that
        # host, which no single job can decide. Every other failure stays
        # here, since it says nothing about the next job.
        if err.trips_breaker:
            # classify 가 이미 __cause__ 를 달았으므로 'from exc' 를 붙이지
            # 않는다 -- 직접 올린 예외는 err is exc 이고, 그러면 자기 자신이
            # 원인이 된다.
            raise err
        return err


def _generate_jobs_for_builders(session: Session, builders: Sequence[ApiJobBuilder]) -> list[ApiJob]:
    created: list[ApiJob] = []
    for builder in builders:
        created.extend(build_jobs_from_builder(session, builder.build_id))
    return created


def generate_jobs(session: Session, schedule_tag: str) -> list[ApiJob]:
    """Generate every active ApiJobBuilder's job(s) for this schedule_tag
    (via build_jobs_from_builder) -- e.g. today's daily job, or a
    repeated/snapshot job's one static row (see RESERVED_NOW_KEY in
    app.services.job_builder). Deliberately separate from actually executing
    anything (see run_cycle): generation and execution are independently
    callable/schedulable steps, not fused into one. build_jobs_from_builder
    is idempotent/dedup-safe, so calling this repeatedly (or calling it
    without ever calling run_cycle) is harmless.

    The systemd-timer level (see app.cli/scripts) -- one call per
    schedule_tag in use. For the other two granularities, see
    generate_jobs_for_builder (one builder, regardless of cycle/is_active)
    and generate_all_jobs (every active builder, regardless of cycle).

    Returns the newly-created ApiJob rows (excludes anything that already
    existed -- see build_jobs_from_builder)."""
    builders = session.exec(
        select(ApiJobBuilder).where(
            ApiJobBuilder.is_active == True,  # noqa: E712 - Oracle native BOOLEAN column rejects IS-based binds (ORA-00908), needs plain equality
            ApiJobBuilder.schedule_tag == schedule_tag,
        )
    ).all()
    return _generate_jobs_for_builders(session, builders)


def generate_all_jobs(session: Session) -> list[ApiJob]:
    """Generate every active ApiJobBuilder's job(s), regardless of
    schedule_tag -- e.g. a manual "generate everything now" trigger
    (see app.api.routers.jobs). Same idempotent/dedup-safe guarantee as
    generate_jobs; just not scoped to one cycle."""
    builders = session.exec(
        select(ApiJobBuilder).where(ApiJobBuilder.is_active == True)  # noqa: E712 - see generate_jobs
    ).all()
    return _generate_jobs_for_builders(session, builders)


def generate_jobs_for_builder(session: Session, build_id: str) -> list[ApiJob]:
    """Generate one specific ApiJobBuilder's job(s) by id, regardless of its
    schedule_tag/is_active -- e.g. a manual trigger for a single builder
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

# How many consecutive failures of one api_id end that api's run (see
# _FailStreak). Five is about a minute of timeouts -- enough that a site
# having a bad moment still gets several chances, little enough that a dead
# one costs a minute instead of a night. 0 turns the breaker off.
_FAIL_STREAK_LIMIT = int(os.environ.get("FAIL_STREAK_LIMIT", "5"))


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


class _FailStreak:
    """Counts an api_id's consecutive failures and says when to stop asking.

    A source can die quietly. KB moved its ETF holdings site to another
    domain over the 2026-10-03 holiday; the old host answers every request
    with a 302 to a host that never completes a TCP connection, so every job
    times out after three minutes. Nothing retires a job that has not
    succeeded -- that is what keeps a failed one-shot job coming back
    tomorrow -- and the builder added 142 more every night, so by 10-12 the
    night cycle had 852 of them. It spent 21:02 to past 01:30 on jobs that
    could not work, two successes to show for it, and the KSD bond migration
    and the other managers' holdings queued behind them never ran at all.
    The first few failures are worth paying for; the eight hundredth says
    nothing the fifth did not.

    Counted per api_id, not per host: the endpoint is what breaks, and one
    host serves several rows (data.krx.co.kr serves a dozen). A host
    refusing us wholesale is a different thing, already handled better --
    SourceRefused defers it and tries again after the window (see
    _run_deferred) instead of giving up for the night.

    Any success resets the api's count, so a long run where a few codes
    legitimately fail never accumulates toward a shutdown; it takes
    _FAIL_STREAK_LIMIT in a row, unbroken.

    **모든 실패를 세지는 않는다.** 이 장치가 재는 것은 "소스가 죽었나" 이고,
    우리 설정이 틀린 것은 그 질문의 답이 아니다 -- 세면 설정 오류 하나가 그
    api 의 멀쩡한 잡들까지 사이클에서 빼버린다. 어느 실패가 세는지는 예외의
    등급이 정한다 (``ScrapeError.counts_toward_retire`` -- app.errors).
    세지 않는 실패는 카운트를 올리지도, 되돌리지도 않는다: 아무 말도 하지
    않은 것으로 둔다.

    Deliberately in-memory and per-cycle: the next cycle starts fresh and
    asks again, which is what should happen when the site comes back. The
    cost of being wrong is one more minute of timeouts, once per cycle."""

    def __init__(self, limit: int = _FAIL_STREAK_LIMIT):
        self._limit = limit
        self._streak: dict[str, int] = {}
        self.skipped: dict[str, int] = {}
        # 건너뛴 잡의 (job_id, 시각). 사이클 끝에 한 번에 로그로 적는다
        # (report) -- 건너뛴 잡마다 커밋하면 나쁜 밤에 수백 번의 왕복이고,
        # 다른 잡의 트랜잭션에 얹어 두면 그 잡이 실패할 때 rollback 에 쓸려
        # 사라진다.
        self._skipped_jobs: list[tuple[str, str, datetime]] = []

    def tripped(self, api_id: str, job_id: str | None = None) -> bool:
        """Has this api already failed its way out of this cycle? Counts the
        job as skipped when it has, for the summary at the end of the run."""
        if self._limit <= 0 or self._streak.get(api_id, 0) < self._limit:
            return False
        self.skipped[api_id] = self.skipped.get(api_id, 0) + 1
        if job_id is not None:
            self._skipped_jobs.append((api_id, job_id, datetime.now(timezone.utc)))
        return True

    def record(self, api_id: str, err: ScrapeError | None) -> None:
        """성공(``None``)이면 카운트를 지우고, 세는 실패면 올린다.

        v1 은 bool 을 받아서 모든 실패를 셌다. 설정 오류와 적재 오류는 이제
        세지 않고 **카운트를 건드리지 않는다** -- 0으로 되돌리면 그 사이에
        쌓인 진짜 실패를 지우게 되고, 올리면 설정 오류가 소스를 죽은 것으로
        만든다."""
        if err is None:
            self._streak[api_id] = 0
            return
        if not err.counts_toward_retire:
            return
        self._streak[api_id] = self._streak.get(api_id, 0) + 1
        if self._streak[api_id] == self._limit:
            logger.error("{}: {} consecutive failures -- skipping the rest of its jobs this cycle",
                         api_id, self._limit)

    def report(self, session: Session | None = None) -> None:
        """사이클 끝 요약. 세션을 주면 건너뛴 잡을 SKIPPED 로 적는다.

        "차단기가 몇 건을 먹었나" 가 로그 파일에만 있으면 SQL 로 물어볼 수
        없다. status=SKIPPED 행이 그 답이고, 커밋은 한 번이다."""
        for api_id, n in self.skipped.items():
            logger.warning("{}: {} job(s) skipped by the failure breaker -- still pending for the "
                           "next cycle", api_id, n)
        if session is None or not self._skipped_jobs:
            return
        for api_id, job_id, when in self._skipped_jobs:
            _log(session, job_id, "SKIPPED", commit=False,
                 # 등급 이름을 그대로 쓴다 -- 보낸 적이 없으니 예외가 없고,
                 # 왜 건너뛰었는지는 차단기라고 적는 것이 유일한 설명이다.
                 error_code="BreakerSkipped",
                 error_message=f"{api_id}: {self._limit} consecutive failures this cycle",
                 executed_at=when)
        session.commit()


def _run_deferred(session: Session, host: str, pending: list[ApiJob],
                  streak: _FailStreak, attempts: dict[str, int]) -> dict[str, str | None]:
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
        if api is not None and (api.session_json or {}).get("login"):
            make_scraper(api, params=pending[0].params_json).login(session)
    except Exception as exc:  # noqa: BLE001 - any refusal of the login means the host is still closed
        logger.error("{}: login refused after the wait ({}) -- giving the host up for this run, "
                     "{} job(s) left pending", host, str(exc)[:120], len(pending))
        return results
    for i, job in enumerate(pending):
        if streak.tripped(job.api_id, job.job_id):
            continue
        # 이 잡의 이번 사이클 시도 횟수. 거절을 만난 잡(이 목록의 첫 번째)은
        # 이미 한 번 돌았고, 그 뒤의 잡들은 시도되기 전에 미뤄진 것이라 0회다
        # -- 그래서 순서로 가정하지 않고 세어 둔 것을 쓴다.
        attempts[job.job_id] = attempts.get(job.job_id, 0) + 1
        try:
            err = _execute_job(session, job, attempt_no=attempts[job.job_id])
        except SourceRefused:
            logger.error("{}: refused again at job {} -- giving the host up for this run, "
                         "{} job(s) left pending", host, job.job_id, len(pending) - i)
            break
        streak.record(job.api_id, err)
        results[job.job_id] = job.engine_note
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


def run_cycle(session: Session, schedule_tag: str) -> dict[str, str | None]:
    """Execute every currently-pending ApiJob for this schedule_tag --
    the entrypoint a systemd timer should call (e.g.
    ``python -m app.cli run-cycle 5m``), one timer per distinct
    schedule_tag value in use. Does NOT generate jobs itself -- call
    generate_jobs first (a separate step; see its docstring for why).

    What actually runs is decided by scanning ApiJob directly
    (schedule_tag + is_active=True). is_active=True means "still
    pending": a repeated job is never flipped inactive (see run_job), so
    it's always in this set and keeps re-running forever; a one-shot job is
    flipped inactive the moment it succeeds, so a past period's
    already-succeeded job drops out of this set on its own, while a *failed*
    one naturally stays and gets retried next tick -- no separate retry
    bookkeeping needed.

    Not every pending job is necessarily tried: a host that refuses us is
    set aside and retried after a window (SourceRefused/_run_deferred), and an
    api that fails several times in a row is dropped for the rest of the
    cycle (_FailStreak). Either way the jobs stay active and the next cycle
    picks them up.

    Returns {job_id: description}, one entry per job actually executed."""
    results: dict[str, str | None] = {}
    jobs = session.exec(
        select(ApiJob).where(
            ApiJob.schedule_tag == schedule_tag,
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
    # An api that has just failed several times running is not asked again
    # this cycle -- a dead source otherwise eats the whole night (see
    # _FailStreak).
    streak = _FailStreak()
    deferred: dict[str, list[ApiJob]] = {}
    # 이 사이클 안에서 잡마다 몇 번 돌았나. 거절된 호스트의 잡만 2가 된다
    # (_run_deferred). api_job_log.attempt_no 로 들어간다 -- 사이클을 넘는
    # 재시도는 로그 행 수가 이미 답이라 여기서 세지 않는다.
    attempts: dict[str, int] = {}
    for job in _login_rows_first(session, jobs):
        if streak.tripped(job.api_id, job.job_id):
            continue
        host = hosts.of(job)
        if host in deferred:
            deferred[host].append(job)
            continue
        attempts[job.job_id] = attempts.get(job.job_id, 0) + 1
        try:
            err = _execute_job(session, job, attempt_no=attempts[job.job_id])
        except SourceRefused as exc:
            logger.warning("{} refused us at job {} -- deferring its remaining jobs", exc.host, job.job_id)
            deferred.setdefault(exc.host, []).append(job)
            continue
        streak.record(job.api_id, err)
        results[job.job_id] = job.engine_note
        logger.info("run_cycle({}): executed {}", schedule_tag, job.job_id)
    for host, pending in deferred.items():
        results.update(_run_deferred(session, host, pending, streak, attempts))
    # 세션을 넘겨 건너뛴 잡을 SKIPPED 로 남긴다 (커밋 한 번).
    streak.report(session)

    return results
