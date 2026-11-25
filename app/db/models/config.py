"""엔진 부기 -- api_mst · api_job_builder · api_job · api_job_log · api_rst.

v2 에서 컬럼 이름과 구조가 바뀐 다섯 표다. 정본은 scripts/sql/v2/
01_scrap_config.sql 과 03_scrap_api_rst.sql 이고, 바꾼 이유는 거기 주석과
docs/SCHEMA-V2.md 4절에 있다. 여기서는 모델이 그것과 맞는지만 지킨다.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import CheckConstraint, Column, DateTime, Identity, Integer, Text, func
from sqlmodel import Field, SQLModel

from app.db.models.base import ResultTable
from app.db.types import JSONText, OracleBoolean, XMLText


class ApiMst(SQLModel, table=True):
    __tablename__ = "api_mst"
    __table_args__ = (
        CheckConstraint("protocol IN ('HTTP', 'BROWSER')", name="ck_api_mst_protocol"),
        CheckConstraint(
            # 요청 방식이 몸통의 모양을 정한다. HTTP 는 payload_*, BROWSER 는
            # behavior_json 으로 서로 배타적이고, protocol 이 어느 스크래퍼로
            # 갈지를 혼자 결정한다 (app.scrapers.make_scraper).
            "(protocol = 'BROWSER' AND behavior_json IS NOT NULL "
            "                      AND payload_type IS NULL AND method IS NULL) OR "
            "(protocol = 'HTTP'    AND behavior_json IS NULL "
            "                      AND method IN ('GET', 'POST'))",
            name="ck_api_mst_body",
        ),
        CheckConstraint(
            # payload_type 은 몸통을 어느 httpx 인자로 보낼지 말한다
            # (app.scrapers.dynamic.DynamicApiScraper.build_request):
            #   NULL          -> 몸통 없음 (GET 이 보통 그렇다)
            #   'json'/'data' -> payload_json 의 dict
            #   'content'     -> payload_xml 의 원문 텍스트
            "(payload_type IS NULL AND payload_json IS NULL AND payload_xml IS NULL) OR "
            "(payload_type IN ('json', 'data') AND payload_json IS NOT NULL AND payload_xml IS NULL) OR "
            "(payload_type = 'content' AND payload_xml IS NOT NULL AND payload_json IS NULL)",
            name="ck_api_mst_payload",
        ),
        CheckConstraint(
            # 시각을 남길 컬럼이 있으면 반복 작업이다. 반대는 성립하지 않는다 --
            # 반복이면서 시각을 안 남기는 경우가 있다(v1 의 none_mapped 자리).
            "snapshot_column IS NULL OR is_repeated = TRUE",
            name="ck_api_mst_snapshot",
        ),
    )

    api_id: str = Field(primary_key=True, max_length=150)
    api_name: str = Field(max_length=100)

    # v1 api_group 을 두 축으로 가른 것. 13개 값이 소스/용도/전송방식을 섞어
    # 쓰고 있었다 (docs/SCHEMA-V2.md 7.4).
    #
    # 이름이 source 가 아니라 source_name 인 이유는 사람이 읽기 좋게 하려는
    # 것이다. Oracle 에서 source 는 예약어가 아니다(측정: CREATE·SELECT·
    # WHERE·ORDER BY·VIEW 전 자리에서 통한다) -- 다만 너무 일반적인 말이고,
    # 이 저장소에는 "원천" 이라는 뜻으로 쓰이는 자리가 여럿이다
    # (pagination_json 의 "source" 키는 응답의 어느 자리를 보라는 뜻이고,
    # 결과 모델 주석의 source 는 그 API 를 가리킨다).
    #
    # host 가 아니다. 값이 기관이고, AMC 하나가 운용사 사이트 아홉 곳이며
    # KRX 는 세 호스트다. 호스트는 api_url 에서 뽑아 쓰는 별개 개념이다
    # (execution._HostIndex -- 호출 간격과 차단기의 단위).
    source_name: str = Field(index=True, max_length=20)         # KIS KRX KSD DART AMC
    domain: Optional[str] = Field(default=None, index=True, max_length=20)  # DERIV ETF BOND …

    # v1 request_type 이 요청 방식과 HTTP 메서드를 겸했다. 가르면 체크 제약이
    # 단순해지고 BROWSER 행에 메서드가 없다는 사실이 드러난다.
    protocol: str = Field(default="HTTP", max_length=10)        # HTTP BROWSER
    method: Optional[str] = Field(default=None, max_length=10)  # GET POST

    api_url: str = Field(max_length=500)
    # v1 은 NOT NULL 이었는데 빈 dict 인 행이 많았다. 없으면 없는 것으로 둔다.
    header_json: Optional[dict] = Field(default=None, sa_type=JSONText)

    payload_type: Optional[str] = Field(default=None, max_length=20)
    payload_json: Optional[dict] = Field(default=None, sa_type=JSONText)
    payload_xml: Optional[str] = Field(default=None, sa_type=XMLText)

    # protocol = 'BROWSER': 실제 브라우저에서 **무엇을 하는가**. 추출 전에
    # 순서대로 실행되는 {"action": ...} 단계들이다 --
    # goto/fill/select/click/press/wait_for/sleep/frame/eval/download.
    # payload_json/payload_xml(요청이 **무엇인가**)의 브라우저 짝이라 그 옆에
    # 있고 서로 배타적이다(위 ck_api_mst_body). 단계의 값도 url·payload 와
    # 같은 ':KEY' / '${VAR}' 치환을 거치므로, 잡 파라미터가 검색창에 입력되는
    # 값이 될 수 있다. 단계 어휘는
    # app.scrapers.browser.BrowserScraper.run_behavior 에 있다.
    behavior_json: Optional[list] = Field(default=None, sa_type=JSONText)

    # NULL -> 응답이 XML 인지 JSON 인지 알아서 가린다(content-type/본문을 보고,
    # DynamicApiScraper.parse 참고).
    # 'zip_delimited' -> 구분자 텍스트 한 개가 들어 있는 ZIP (KRX/KIS .mst).
    # 'delimited' -> 본문 자체가 구분자 텍스트 (.csv/.txt 다운로드).
    # 'binary' -> 응답이 결과 그 자체인 문서(PDF·이미지·압축). 통째로 디스크에
    #   놓고 결과 표에는 설명만 남긴다. staging_json 이 그 위치를 말한다.
    # 'html_table' / 'xlsx' / 'xls' -> 표 형태.
    #
    # BROWSER 행은 같은 컬럼을 자기 값으로 읽는다 -- behavior 가 남긴 것이
    # 무엇이냐에 따라: 'dom'(기본, parse_json 의 CSS 명세로 페이지에서 추출),
    # 'xhr'(페이지가 스스로 받은 JSON 을 그대로 두고 HTTP 행과 같은 셀렉터로
    # 추출), 'binary'(클릭이 다운로드를 냈다 -- HTTP 와 같게 놓는다).
    #
    # 'session' 은 어느 엔진 것도 아니다: "로그인만 하고 세션만 남긴다" 는
    # 뜻이고, output_tables 가 비어도 되는 유일한 response_type 이다.
    response_type: Optional[str] = Field(default=None, max_length=20)

    # v1 response_parse_json 을 넷으로 가른 것. 로그인 신호와 파일 적재 위치는
    # 파싱 명세가 아니다 (docs/SCHEMA-V2.md 4.4).
    #
    # 가르는 기준은 "누가 한 벌로 읽는가" 다. 앞의 셋은 **묶음 명세**이고 --
    # 키 하나만 보면 뜻이 없다 -- 읽는 주체가 각각 다르므로 그 컴포넌트에
    # dict 를 통째로 넘길 수 있다.
    parse_json: Optional[dict] = Field(default=None, sa_type=JSONText)
    """delimiter fields has_header skip_rows encoding inner_file table_index
    sheet / CSS 명세. 읽는 곳: dynamic 의 파서."""

    session_json: Optional[dict] = Field(default=None, sa_type=JSONText)
    """{"login": "<api_id>", "logged_out": "...", "blocked": "...",
    "expect": "..."} -- 이 응답은 데이터가 아니다, 알아보는 법과 그다음에 할 일.
    읽는 곳: base/dynamic/browser 의 응답 검사.

    blocked("에러페이지 - 한국거래소", 3개 API)가 세션과 직접 관계는 없지만
    여기 둔다. logged_out · expect 와 같은 경로가 읽고 같은 방식으로 판정한다
    -- 본문에서 문자열을 찾는다. 이름이 조금 넓게 쓰이는 것을 받아들이는 쪽이,
    넷을 둘로 쪼개 읽는 곳을 둘로 만드는 것보다 낫다. 옮기지 말 것."""

    staging_json: Optional[dict] = Field(default=None, sa_type=JSONText)
    """{"group": "...", "name": ":KEY.pdf"} -- response_type='binary' 가 놓을
    위치. 읽는 곳: base 의 파일 적재."""

    # 엔진 동작을 켜고 끄는 스위치들. 필요할 때 키를 더하는 자리다.
    #
    # 지금 쓰는 것:
    #   empty_is_pending  0행 성공을 "끝났다" 로 보지 않는다. KRX 오픈API 는
    #                     아직 안 낸 날을 0행으로 답하므로, 그 행은 0행이면
    #                     잡을 닫지 않고 다음 사이클에 다시 묻는다
    #
    # 무엇이 여기 들어가는가 -- 세 질문에 다 '예' 여야 한다:
    #   1. 다른 키 없이 혼자 뜻이 있는가? (delimiter 는 fields 없이는 없다)
    #   2. 읽는 곳이 특정 메커니즘이 아닌가? (파서·응답검사·적재가 읽는 것은
    #      그 컴포넌트의 컬럼으로)
    #   3. 키 목록이 미리 정해지지 않는가? (나머지 셋은 고정돼 문서에 적힌다)
    #
    # 제약을 걸지 않는다. JSON 스키마로 오타를 막으면 새 스위치마다
    # ALTER CONSTRAINT 가 필요해서, 자유롭게 더할 수 있다는 성질을 포기하면서
    # 오타 검사만 사는 거래가 된다 -- 애초에 원한 것이 그 자유였다.
    # 오타는 엔진이 경고로 알린다(app.services.execution 의 스위치 이름 대조).
    #
    # 쓰는 규칙: 불리언은 반드시 true 로 쓴다(JSON_VALUE 가 LAX 라 "true" 도
    # TRUE 로 읽히지만 1 은 NULL 이 된다). 의미는 **키가 있으면 켠 것**이고,
    # false 를 명시적 off 로 쓰지 않는다.
    #
    # SQL 쪽도 JSON 경로를 직접 읽는다 -- 가상 컬럼으로 노출하지 않는다.
    # 스위치 하나를 컬럼으로도 두면 어느 쪽이 정본인지 흐려지고(생성 컬럼이라
    # 쓸 수도 없다), 스위치가 늘 때마다 ALTER 가 따라붙어 이 컬럼을 둔 이유가
    # 줄어든다. sp_retire_expired_jobs 가 그렇게 읽는 예다.
    options_json: Optional[dict] = Field(default=None, sa_type=JSONText)

    # {source_dot_path: ENV_VAR_NAME} -- JSON 응답을 파싱한 뒤 각 값을 뽑아
    # app.auth_config.persist_env_var 로 남긴다(os.environ 과 디스크의 .env
    # 양쪽). OAuth 토큰 갱신처럼 부수효과만 있는 잡용이다: 예를 들어
    # {"access_token": "KIS_ACCESS_TOKEN"} 이면 이후 모든 잡의
    # "${KIS_ACCESS_TOKEN}" 이 새 값을 집어 간다 -- systemd 타이머 잡은 각각
    # 제 프로세스라, app.auth_config 가 import 시점에 .env 를 다시 읽는다.
    persist_env_json: Optional[dict] = Field(default=None, sa_type=JSONText)

    # v1 output_tables_json. {셀렉터: 테이블} 인데 키가 테이블이 아니라
    # 셀렉터라 이름을 고쳤다.
    record_map_json: Optional[dict] = Field(default=None, sa_type=JSONText)

    # 응답 어디서든 값을 끌어와 어느 셀렉터의 레코드에 넣는다. 예:
    # {"output1": {"output3.bstp_nmix_prpr": "kospi200_idx"}} -- 다른 최상위
    # 가지의 값이 같은 행에 들어가므로, 조인 없이 같은 응답·같은 시점이
    # 보장된다 (app.scrapers.dynamic._apply_merge_fields).
    merge_fields_json: Optional[dict] = Field(default=None, sa_type=JSONText)

    # v1 key_params_list 가 한 컬럼으로 세 가지를 결정했고, 그래서 예약어
    # 'NOW' 와 'none_mapped' 가 필요했다 (docs/SCHEMA-V2.md 4.1). 넷으로
    # 가르면 각각 한 가지만 뜻한다.
    job_key_params: Optional[list] = Field(default=None, sa_type=JSONText)
    """job_id 재료가 되는 파라미터 이름들. ["SHORT_CODE"]"""

    row_stamp_json: Optional[dict] = Field(default=None, sa_type=JSONText)
    """결과 행에 찍을 것. {파라미터: 컬럼}. 값은 실존 컬럼이어야 한다."""

    is_repeated: bool = Field(default=False, sa_type=OracleBoolean)
    """True 면 성공 후에도 활성으로 남아 매 주기 재실행된다."""

    snapshot_column: Optional[str] = Field(default=None, max_length=30)
    """실행 시각을 받을 결과 컬럼. 조회일이 없는 스냅샷 API 용."""

    # 한 요청이 전부를 주지 않을 때 나머지를 어떻게 더 묻는가. 소스마다 "더
    # 있다" 를 말하는 방식이 달라 세 모양이다 (dynamic.fetch_all_pages):
    #
    #   {"mode": "page", "param": "PAGE", "start": 1}
    #       클라이언트가 올리는 페이지 번호. NULL 이면서 url/payload 에
    #       ':PAGE' 가 있으면 이것과 같은 뜻이다.
    #
    #   {"mode": "cursor", "param": "HHMM", "from": "output2[].stck_cntg_hour",
    #    "pick": "min", "min_records": 99}
    #       페이지 번호가 없고, 방금 받은 답에서 읽은 값에 다음 요청을 맨다.
    #       "min_records" 가 끊김의 신호다 -- 짧은 답은 series 가 끝난 것이다.
    #
    #   {"mode": "token", "params": {"CTX_FK": "header:ctx_area_fk100"},
    #    "continue_when": {"source": "header:tr_cont", "in": ["F", "M"]}}
    #       되돌려줘야 하는 불투명한 연속키 + 더 남았는지 알리는 깃발 --
    #       KIS 의 tr_cont/ctx_area_* 짝이다.
    #
    # "max_pages" 는 모든 모드의 안전망이다.
    pagination_json: Optional[dict] = Field(default=None, sa_type=JSONText)

    note: Optional[str] = Field(default=None, max_length=400)
    """사람이 적는다. v1 description(100자)을 넓힌 것."""

    updated_at: Optional[datetime] = Field(default=None,
        sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class ApiJobBuilder(SQLModel, table=True):
    __tablename__ = "api_job_builder"
    __table_args__ = (
        CheckConstraint("save_mode IN ('overwrite', 'append')",
                        name="ck_api_job_builder_save"),
    )

    build_id: str = Field(primary_key=True, max_length=50)
    # ApiMst.api_id 를 가리키지만 FK 로 걸지 않는다 -- 표를 드롭/재생성하며
    # 옮기는 일이 많고, Oracle 은 컬럼 순서를 바꿀 수 없어 그 방식을 쓸
    # 수밖에 없다. ApiRst.job_id / ApiJobLog.job_id 와 같은 느슨한 결합이다.
    api_id: str = Field(index=True, max_length=150)

    # v1 macro_params_json. 각 항목이 하나의 축이고 최종 작업 집합은 축들의
    # **곱집합**이다 -- 이름이 그 사실을 말한다. 항목을 하나 더하면 작업 수가
    # 더해지는 게 아니라 곱해진다는 것이 이 컬럼의 유일한 함정이다.
    cartesian_param_json: dict = Field(sa_type=JSONText)

    is_active: bool = Field(default=False, sa_type=OracleBoolean)
    save_mode: Optional[str] = Field(default="overwrite", max_length=20)

    # v1 execution_cycle 이 "어느 타이머가 집어 가는가" 와 "스냅샷 시각을 어떤
    # 단위로 절삭하는가" 를 겸해서 46가지로 늘었고, 절삭이 조용히 안 걸렸다
    # (daily_batch2 는 1m/5m/1h/1d 패턴에 안 맞아 no-op).
    schedule_tag: str = Field(default="daily", index=True, max_length=30)
    snapshot_unit: Optional[str] = Field(default=None, max_length=10)
    """스냅샷 시각을 어느 단위까지 내려 자르나(_floor_datetime).
    1m 5m 1h 1d 1w. NULL 이면 자르지 않는다."""

    note: Optional[str] = Field(default=None, max_length=400)
    updated_at: Optional[datetime] = Field(default=None,
        sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class ApiJob(SQLModel, table=True):
    __tablename__ = "api_job"
    __table_args__ = (
        CheckConstraint("save_mode IN ('overwrite', 'append')", name="ck_api_job_save"),
    )

    job_id: str = Field(primary_key=True, max_length=150)
    # ApiJobBuilder.build_id / ApiMst.api_id 를 가리키지만 FK 가 아니다
    # (ApiJobBuilder.api_id 와 같은 이유).
    build_id: Optional[str] = Field(default=None, index=True, max_length=50)
    api_id: str = Field(max_length=150)
    params_json: dict = Field(sa_type=JSONText)

    is_active: bool = Field(default=True, sa_type=OracleBoolean)
    save_mode: Optional[str] = Field(default="overwrite", max_length=20)
    schedule_tag: str = Field(default="daily", max_length=30)
    snapshot_unit: Optional[str] = Field(default=None, max_length=10)

    # v1 은 사람이 적은 설명과 마지막 실행 결과가 같은 칸(description,
    # VARCHAR2(100))을 썼다. 실행이 설명을 덮어서 빌더에 적어 둔 메모가 첫
    # 실행에 사라졌다 (docs/SCHEMA-V2.md 7.2).
    note: Optional[str] = Field(default=None, max_length=400)
    """사람이 적는다. 엔진이 안 건드린다."""

    engine_note: Optional[str] = Field(default=None, max_length=400)
    """엔진이 덮어쓴다. note 와 같은 명사에 주인만 다르게 해서, 컬럼 이름만
    보고 어느 쪽이 사람 것인지 알 수 있게 한다. 'log' 를 피한 이유는
    api_job_log 가 따로 있어서다 -- 이것은 이력이 아니라 마지막 한 줄이다."""

    updated_at: Optional[datetime] = Field(default=None,
        sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class ApiJobLog(SQLModel, table=True):
    """ApiJob 의 실행 이력. 한 번 돌 때마다 한 행이고 지우지 않는다.

    job_id 는 FK 가 아닌 평범한 인덱스 컬럼이다 -- 로그의 insert/delete 를
    api_job 과 독립으로 두기 위해서다.

    v1 은 다섯 컬럼이라 SQL 로 물어볼 수 있는 것이 거의 없었다. "어제 실패 중
    상대 쪽 문제는 몇 건인가" 를 error_message LIKE 로 추측해야 했고, 몇 건
    받았는지는 로그에 아예 없었다 -- api_job.description 문자열에 있고 다음
    실행이 덮었다 (docs/SCHEMA-V2.md 6.3)."""

    __tablename__ = "api_job_log"
    __table_args__ = (
        CheckConstraint("status IN ('SUCCESS', 'FAILED', 'SKIPPED', 'BLOCKED', 'EMPTY')",
                        name="ck_api_job_log_status"),
    )

    id: Optional[int] = Field(default=None,
        sa_column=Column(Integer, Identity(start=1), primary_key=True))
    job_id: str = Field(index=True, max_length=150)

    status: str = Field(max_length=20)
    """SUCCESS | FAILED | SKIPPED | BLOCKED | EMPTY"""

    error_code: Optional[str] = Field(default=None, index=True, max_length=40)
    """예외 클래스 이름('SourceRefused', 'MappingError' …). 성공이면 NULL.
    등급이 생기면 재시도/차단기/은퇴 판단이 isinstance 분기가 아니라 데이터로
    결정된다 (docs/SCHEMA-V2.md 6.2)."""

    error_message: Optional[str] = Field(default=None, max_length=4000)
    error_detail: Optional[str] = Field(default=None, sa_type=Text)
    """스택이나 응답 앞부분. v1 은 4000자 한 칸뿐이라 응답 본문을 담을 수 없어,
    무엇이 왔는지 모른 채 'HTTPStatusError' 만 남는 경우가 있었다."""

    record_count: Optional[int] = Field(default=None)
    """받아서 적재한 행 수."""
    duration_ms: Optional[int] = Field(default=None)
    attempt_no: Optional[int] = Field(default=1)
    target_date: Optional[str] = Field(default=None, max_length=8)
    """YYYYMMDD. 어느 날짜분을 받았나 -- job_id 에서 날짜를 빼는 경우
    멱등성과 "그날 이미 받았나" 판단이 이 컬럼으로 옮겨온다."""

    executed_at: Optional[datetime] = Field(default=None, sa_column=Column(DateTime))


class ApiRst(ResultTable, table=True):
    """타입 테이블이 없는 소스의 결과. 조회는 v_* 뷰로 한다.

    api_id 로 리스트 파티션돼 있어(03_scrap_api_rst.sql) 한 소스를 읽을 때
    다른 소스의 행은 읽지 않는다."""

    __tablename__ = "api_rst"

    id: Optional[int] = Field(default=None,
        sa_column=Column(Integer, Identity(start=1), primary_key=True))
    # 타입 결과 표와 달리 여기는 api_id 를 인덱스한다: 표를 여러 소스가
    # 공유하므로 api_id 가 실제로 부분집합을 고른다.
    api_id: str = Field(index=True, max_length=150)
    job_id: str = Field(index=True, max_length=150)

    # 레코드 한 건 + row_stamp_json 이 지목한 값까지 **합쳐서** 넣는다 --
    # 타입 테이블에 저장될 행과 같은 모양이다.
    #
    # v1 은 이것을 둘로 나눠 담았다(key_params_json + result_json). 그래서 한
    # 행이 두 컬럼에 걸쳐 있었고, 뷰를 씌우면 컬럼마다 어느 쪽에서 꺼낼지를
    # 알아야 했다 -- 실제로 ace_etf_pdf 의 fundcd/std_dt 는 result_json 에
    # 없었고, 처음 생성한 뷰가 그 여덟 컬럼을 NULL 로 돌려줬다.
    #
    # 키는 소문자로 접어서 넣는다 -- 뷰의 JSON 경로가 '$.<컬럼명>' 으로
    # 예측 가능해야 하고, 원본 키는 소스마다 표기가 다르다(ACE 는 std_DT,
    # KRX 는 전부 대문자).
    result_json: dict = Field(default_factory=dict, sa_type=JSONText)

    target_date: Optional[str] = Field(default=None, index=True, max_length=8)
    """어느 날짜분인가. 보존 정리와 재적재(그날 것만 지우고 다시 넣기)가 쓴다."""

    updated_at: Optional[datetime] = Field(default=None,
        sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))
