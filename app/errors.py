"""실패를 **누구 탓인가**로 가른 예외 계층. docs/SCHEMA-V2.md 6.2.

왜 이게 필요한가. v1 은 예외 클래스가 둘(`SessionExpired`, `SiteBlocked`)
뿐이고 나머지 전부를 `run_job` 한 곳에서 문자열로 적었다. 그래서 **SQL 로
물어볼 수 없었다** -- "어제 실패 2,400건 중 상대 쪽 문제는 몇 건인가" 를
`error_message LIKE` 로 추측해야 했다.

가르는 기준은 다음 행동이다. 상대 쪽 문제면 기다렸다 다시 묻고, 우리 설정이
틀렸으면 다시 물어도 같고, DB 쪽이면 사람을 불러야 한다. 그 판단을
`isinstance` 분기로 코드에 흩지 않고 **클래스 속성**에 적는다. 그러면
``api_job_log.error_code`` 가 클래스 이름 하나로 그 등급을 가리키고, 엔진은
``exc.retryable`` 을 읽는다.

알림 채널은 이 저장소에 없다. ``alert`` 는 그래서 "logger.error 로 올리고
error_code 로 질의 가능하게 둔다" 까지만 뜻한다 -- 채널이 생기면 이 플래그가
그 입력이 된다.
"""

from __future__ import annotations

import re
from typing import Any


class ScrapeError(Exception):
    """수집 중 생긴 실패의 뿌리.

    속성 다섯이 docs/SCHEMA-V2.md 6.2 의 등급 표다. 기본값은 **아껴 쓰는
    쪽**이다: 재시도 안 하고, 호스트를 중단시키지 않고, 알림 없고, FAILED 로
    적는다. 새 예외를 아무 생각 없이 상속하면 요청을 더 보내지 않는 쪽으로
    떨어지고, 더 보내려면 명시해야 한다.

    ``counts_toward_retire`` 만 기본이 True 다. 모르는 실패가 계속 나면 그
    api 는 이 사이클에서 빠지는 것이 맞다 -- 차단기는 밤을 지키는 장치이고,
    "왜 실패하는지 모르겠다" 는 밤을 내줄 이유가 못 된다. 세지 않는 것들은
    각자 왜 안 세는지 docstring 에 적혀 있다."""

    #: tenacity 가 같은 요청을 다시 보낼 것인가 (app.scrapers.dynamic.fetch).
    retryable: bool = False
    #: 이 호스트로 더 보내지 말 것인가. run_cycle 이 그 호스트의 남은 잡을
    #: 미뤘다가 창이 지난 뒤 한 번 더 돈다 (_run_deferred).
    trips_breaker: bool = False
    #: 이 api_id 의 연속 실패로 셀 것인가 (_FailStreak -- 죽은 소스가 밤을
    #: 다 먹지 않게 한다).
    counts_toward_retire: bool = True
    #: 사람이 봐야 하는가. 채널은 없고 logger.error 로 올린다 (모듈 docstring).
    alert: bool = False
    #: api_job_log.status 에 적을 값. ck_api_job_log_status 가 허용하는 다섯 중
    #: 하나여야 한다.
    log_status: str = "FAILED"

    def caused_by(self, exc: BaseException) -> "ScrapeError":
        """원래 예외를 ``__cause__`` 로 달고 자신을 돌려준다.

        ``raise ... from exc`` 와 같은 연결인데, :func:`classify` 는 raise 하지
        않고 객체를 돌려주므로 손으로 붙인다. 트레이스백에 'The above exception
        was the direct cause of' 로 남아 error_detail 에 원래 예외가 보인다.
        자신을 돌려주는 것은 분류를 한 줄로 쓰기 위해서다."""
        self.__cause__ = exc
        return self


# ---------------------------------------------------------------------------
# 상대 쪽. 우리 설정은 맞다.
# ---------------------------------------------------------------------------


class SourceError(ScrapeError):
    """원천 쪽 문제. 같은 설정으로 나중에 다시 물으면 될 수 있다."""


class SourceUnavailable(SourceError):
    """5xx · 타임아웃 · 연결 끊김. 지금 안 되는 것뿐이다.

    재시도 대상이다. 그리고 **연속 실패로 센다** -- 죽은 소스가 바로 이
    모양으로 죽는다. KB 가 2026-10-03 에 ETF 사이트를 옮기고 옛 호스트가
    연결 안 되는 곳으로 302 를 돌려줬을 때, 잡마다 3분씩 타임아웃이었고
    10-12 밤 사이클은 852개를 들고 다섯 시간을 쓰고 성공 둘로 끝났다."""

    retryable = True


class SourceRefused(SourceError):
    """차단 페이지가 왔다. 데이터 대신 "그만 보내라" 가 온 것이다.

    로그아웃과 다른 점은 **다음에 할 일**이다. 로그아웃은 지금 한 번 다시
    로그인하면 풀린다. 차단은 안 풀린다 -- KRX 데이터포털은 요청이 몰리면
    에러 페이지를 주고 그게 몇 분 지속되며, 그 창 안의 모든 요청이 또
    거절되고 창이 길어질 수 있다. 그래서 여기서 재시도하지 않는다. 사이클
    러너가 그 호스트로 보내는 것을 멈추고, 창을 기다리고, 한 번 로그인하고
    이어간다 (app.services.execution._run_deferred).

    ``host`` 를 들고 다니는 이유: 한 사이클이 여러 호스트를 섞어 돌므로 어느
    사이트가 거절했는지 예외가 알고 있어야 한다.

    연속 실패로 세지 않는다. 거절은 그 호스트에 대한 사실이고 이미 차단기가
    다루는데, 여기서 또 세면 은퇴 판단까지 건드려 한 번의 거절이 api 를
    사이클에서 빼버린다."""

    trips_breaker = True
    counts_toward_retire = False
    log_status = "BLOCKED"

    def __init__(self, message: str, host: str):
        super().__init__(message)
        self.host = host


class SessionExpired(SourceError):
    """로그인이 안 된 것처럼 응답했다.

    알아챈 전송 계층이 올린다 -- 브라우저 행은 페이지에서 ``logged_out``
    마커를 보고, HTTP 행은 응답 본문에서 같은 마커를 본다. 그래서
    :meth:`app.scrapers.base.BaseScraper.collect` 가 잡을 실패시키는 대신 다시
    로그인하고 한 번 재시도한다.

    재시도는 일부러 한 번이다. 비밀번호가 틀렸을 때 로그인 루프가 되어 계정을
    잠그면 안 된다.

    ``retryable`` 이 False 인 이유는 tenacity 얘기다. 같은 요청을 그대로 세 번
    더 보내는 것은 로그아웃을 풀지 못하고 백오프 10초만 쓴다 -- 재로그인은
    그 위층(collect)이 한다."""

    counts_toward_retire = False


class SourceEmpty(SourceError):
    """0건. 실패가 아닐 수 있다 -- 휴장일이면 그게 맞는 답이다.

    현재 엔진은 이것을 예외로 올리지 않는다. 0행 성공을 "아직 아니다" 로 볼지는
    행 설정(``options_json['empty_is_pending']``)이 정하고, 그 경로는 성공
    경로 안에 있다 (execution._execute_job). 이 클래스는 **등급 이름**으로
    존재한다 -- 로그 status 가 EMPTY 인 행이 그 뜻이고, error_code 는 비어
    있다."""

    counts_toward_retire = False
    log_status = "EMPTY"


class SourceChanged(SourceError):
    """필드나 구조가 달라졌다. 사람이 봐야 한다.

    재시도가 무의미하다 -- 사이트가 바뀐 것이고 다시 물어도 바뀐 것이 온다.

    연속 실패로는 **센다**(기본값 유지). 구조가 바뀌면 그 api 의 모든 잡이
    똑같이 실패하므로, 밤을 그걸로 보내지 않는 쪽이 맞다. docs/SCHEMA-V2.md
    6.2 의 표에는 "-" 로 적혀 있었는데, 그 칸은 고쳤다."""

    alert = True


# ---------------------------------------------------------------------------
# 우리 설정. 재시도해도 같다.
# ---------------------------------------------------------------------------


class ConfigError(ScrapeError):
    """api_mst · 빌더 설정이 틀렸다. 다시 물어도 같은 결과다.

    연속 실패로 세지 않는다. 차단기는 "소스가 죽었나" 를 재는 장치이고, 우리
    설정이 틀린 것은 그 질문의 답이 아니다 -- 세면 설정 오류 하나가 그 api 의
    멀쩡한 잡들까지 사이클에서 빼버린다."""

    counts_toward_retire = False
    alert = True


class MappingError(ConfigError):
    """대상 테이블이 없거나, 셀렉터가 응답과 맞지 않거나, 파싱 명세가 모자라다.

    ``record_map_json`` 이 가리키는 표가 레지스트리에 없는 경우가 대표다. v1 은
    이것이 실행 시점에 ValueError 로 났고 다른 모든 실패와 한 칸에 섞였다."""


class ParamError(ConfigError):
    """필수 파라미터가 없거나 치환이 안 됐다."""


class SchemaError(ConfigError):
    """``row_stamp_json`` · ``snapshot_column`` 이 없는 컬럼을 가리킨다.

    이 검사는 실행이 아니라 **생성 시점**에 있어야 한다. 생성은 하루 한 번이고
    실행은 수천 번이다 (docs/SCHEMA-V2.md 6.4)."""


# ---------------------------------------------------------------------------
# DB 쪽. 적재에서 깨졌다.
# ---------------------------------------------------------------------------


class StoreError(ScrapeError):
    """받아는 왔는데 넣다가 깨졌다. 받은 것과 넣는 것 중 어디가 문제인지
    구분되어야 원천을 의심할지 우리 스키마를 의심할지 정해진다."""

    counts_toward_retire = False
    alert = True


class StoreConstraint(StoreError):
    """길이 초과 · 타입 불일치 · 제약 위반. 원천이 보낸 값이 우리 컬럼에
    안 들어간다 -- 대개 원천이 넓어진 것이고, 다시 넣어도 같다."""


class StoreUnavailable(StoreError):
    """커넥션 · 테이블스페이스 · 인스턴스. 데이터는 멀쩡하고 DB 가 지금 안 된다.

    등급 표의 "조건부 재시도" 가 여기다. 지금은 재시도하지 않는다 -- 사이클이
    세션 하나를 공유하고 있어서, 적재 재시도는 그 세션의 상태를 되감는 일과
    엮인다. 다음 사이클이 같은 잡을 다시 집는다(실패한 one-shot 잡은 active 로
    남는다)는 것이 현재의 답이다."""


# ---------------------------------------------------------------------------
# 분류 -- 아직 올라오지 않는 예외를 등급에 꽂는다
# ---------------------------------------------------------------------------

# Oracle 쪽 번호로 "데이터가 안 맞다" 와 "DB 가 안 된다" 를 가른다. 숫자로
# 가르는 이유: SQLAlchemy 가 씌우는 클래스(DataError/IntegrityError/
# OperationalError)는 드라이버마다 같은 오류를 다르게 싼다. Oracle 번호는
# 안 바뀐다.
_ORA_CONSTRAINT = frozenset({
    1,      # unique constraint violated
    1400,   # cannot insert NULL
    1401, 12899,  # value too large for column (버전에 따라 둘 중 하나)
    1438,   # value larger than specified precision
    1722,   # invalid number
    1858, 1861,   # not a valid month / does not match format
    2290, 2291, 2292,  # check / parent key / child record
})
_ORA_UNAVAILABLE = frozenset({
    1033, 1034, 1089,  # instance starting up / not available / shutdown
    3113, 3114, 3135,  # connection lost / not connected / lost contact
    12154, 12170, 12541,  # TNS: resolve / timeout / no listener
    1652, 1653, 1654,  # unable to extend temp / table / index
    28, 2396,          # session killed / idle timeout
})

_ORA_CODE_RE = re.compile(r"ORA-(\d{1,5})")


def _ora_code(exc: BaseException) -> int | None:
    """예외에서 Oracle 오류번호, 없으면 None.

    세 겹을 본다. SQLAlchemy 는 드라이버 예외를 ``DBAPIError.orig`` 로 감싸고,
    python-oracledb 는 번호를 ``args[0].code`` 에 담는다. 둘 다 못 찾으면
    메시지의 'ORA-나나나나나' 를 읽는다 -- 프로시저를 거쳐 온 오류처럼 객체가
    안 남는 경우가 있다."""
    for obj in (exc, getattr(exc, "orig", None)):
        args = getattr(obj, "args", None) or ()
        if args and isinstance(getattr(args[0], "code", None), int):
            return args[0].code
    match = _ORA_CODE_RE.search(str(exc))
    return int(match.group(1)) if match else None


# httpx 를 import 하지 않는다. 이 모듈은 전송 계층을 모르는 쪽(예: 적재,
# 빌더)에서도 import 되므로, 클래스 이름으로만 본다.
_RETRYABLE_TRANSPORT = frozenset({
    "ConnectError", "ConnectTimeout", "ReadTimeout", "WriteTimeout", "PoolTimeout",
    "TimeoutException", "ReadError", "WriteError", "RemoteProtocolError",
    "NetworkError", "TransportError", "ProxyError",
})


def classify(exc: BaseException) -> ScrapeError:
    """임의의 예외를 등급이 붙은 :class:`ScrapeError` 로 바꿔 돌려준다.

    모든 raise 지점을 고치지 않고도 ``error_code`` 가 의미를 갖게 하는 것이
    목적이다. 명확한 곳은 직접 올리고(:class:`MappingError` 등), 나머지 --
    httpx 가 올리는 것, 드라이버가 올리는 것, 남의 라이브러리가 올리는 것 --
    는 여기서 꽂힌다. 그러지 않으면 error_code 가 'Exception' 으로 수렴해서
    컬럼을 더한 보람이 없다.

    이미 :class:`ScrapeError` 면 그대로 돌려준다. 원래 예외는 ``__cause__`` 로
    달려 있어 error_detail 의 트레이스백에 남는다."""
    if isinstance(exc, ScrapeError):
        return exc

    name = type(exc).__name__

    # DB 먼저 본다. 적재 예외가 네트워크 예외 이름과 겹치는 경우가 있고
    # (OperationalError), 번호가 있으면 그게 더 확실하다.
    code = _ora_code(exc)
    if code is not None:
        # 메시지에 이미 번호가 있으면 앞에 또 붙이지 않는다 -- 드라이버가
        # 넣어주는 경우가 대부분이고, 둘이 겹치면 error_message 가
        # 'ORA-12899: ... ORA-12899: ...' 로 읽힌다.
        label = str(exc) if f"ORA-{code:05d}" in str(exc) else f"ORA-{code:05d}: {exc}"
        if code in _ORA_CONSTRAINT:
            return StoreConstraint(label).caused_by(exc)
        if code in _ORA_UNAVAILABLE:
            return StoreUnavailable(label).caused_by(exc)
        # 번호는 있는데 어느 쪽인지 모르겠다. StoreError 로 두면 알림이 붙고
        # 사람이 보고 위 집합에 넣는다 -- 조용히 FAILED 로 흘리지 않는다.
        return StoreError(label).caused_by(exc)

    if name in _RETRYABLE_TRANSPORT:
        return SourceUnavailable(f"{name}: {exc}").caused_by(exc)

    # HTTP 상태. 상태코드로 더 가르지 않는 이유: 지금 돌아가는 동작이
    # "상태 오류는 재시도" 이고 그건 실측으로 검증된 쪽이다. KRX 는 로그아웃을
    # HTTP 400 + 본문 'LOGOUT' 으로 주고(마커가 이미 잡는다), 운용사 사이트의
    # 403 은 대개 과속이라 기다리면 풀린다. 4xx 를 ConfigError 로 가르면
    # 재시도가 빠지는데, 그게 맞는지 측정한 적이 없다.
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if status is not None:
        return SourceUnavailable(f"HTTP {status}: {exc}").caused_by(exc)

    # 설정이 틀렸을 때 우리 코드가 올리는 것. ValueError 는 "이 행의 설정으로는
    # 요청을 만들 수 없다" 로 쓰여 왔다 (payload_type, 파싱 명세).
    if isinstance(exc, (ValueError, KeyError, TypeError)):
        return ConfigError(f"{name}: {exc}").caused_by(exc)

    return ScrapeError(f"{name}: {exc}").caused_by(exc)


def detail_of(exc: BaseException, limit: int = 8000) -> str:
    """``api_job_log.error_detail`` (CLOB) 에 넣을 본문.

    트레이스백 앞에 **응답 본문 앞부분**을 붙인다. v1 은 4000자 한 칸뿐이라
    응답을 담을 수 없어서, 무엇이 왔는지 모른 채 'HTTPStatusError' 만 남는
    경우가 있었다 -- 그게 이 컬럼이 생긴 이유다."""
    import traceback

    parts: list[str] = []
    response = getattr(exc, "response", None)
    if response is not None:
        head: Any = getattr(response, "text", None)
        url = getattr(response, "url", "")
        status = getattr(response, "status_code", "")
        if isinstance(head, str):
            parts.append(f"--- reply {status} {url}\n{head[:2000]}")
    parts.append("".join(traceback.format_exception(exc)))
    return "\n\n".join(parts)[:limit]
