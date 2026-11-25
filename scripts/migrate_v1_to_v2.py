"""v1 의 api_mst 58행 · api_job_builder 95행을 v2 모양으로 옮긴다.

한 번 쓰고 버리는 스크립트가 아니라 **매핑의 기록**이다. v1 의 한 컬럼이
v2 의 여러 컬럼으로 갈린 자리마다 "어느 값이 어디로 갔는지" 가 여기 적혀
있고, 그게 docs/SCHEMA-V2.md 4절의 결정을 실제 데이터에 적용한 모습이다.

읽기는 v1(``ORACLE_*_V1``), 쓰기는 v2 SCRAP(``ORACLE_*``).

두 접두사가 한 번 뒤바뀌었다. 처음에는 ``ORACLE_*`` 가 v1 이고 v2 가
``V2_*`` 였는데, v2 가 별도 인스턴스이고 계정 이름·자격이 v1 과 같아서
``ORACLE_*`` 를 v2 로 돌리는 것이 깔끔했다(그러면 app.db_config 과
app.db.engine 을 고칠 것이 없다). 그 뒤로 v1 은 ``_V1`` 로만 닿는다 --
_v1_connect 참고. 쓰기를
SQLModel 로 하는 이유: JSON·XMLTYPE·BOOLEAN 바인딩을 엔진이 런타임에 쓰는 그
경로로 통과시킨다 -- 옮긴 행이 엔진으로 다시 읽히는지가 이행의 합격 조건이고,
raw INSERT 로는 그것을 검증하지 못한다 (JSON 컬럼에 문자열을 바인딩하면 JSON
**스칼라**로 들어가는 함정을 이미 한 번 밟았다).

기본은 dry-run 이다. ``--apply`` 를 줘야 쓴다.

    python scripts/migrate_v1_to_v2.py            # 변환 결과만 출력
    python scripts/migrate_v1_to_v2.py --apply    # v2 에 적재 (일회성)

이행이 끝난 뒤 v2 에서 설정을 고치면(ISIN_BOND_INFO 가 그렇다) 다시
적재하면 그 작업이 사라진다. --apply 는 v1 에 없는 api_id 가 v2 에
있으면 멈춘다 -- apply_to_v2 참고.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import oracledb
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------------------------------------------------------------------------
# api_group 13개 -> source_name + domain
# ---------------------------------------------------------------------------
# v1 의 api_group 은 세 가지 분류 체계가 섞여 있었다 (docs/SCHEMA-V2.md 7.4):
# KIS_* 는 소스 기준, ETF_* 는 용도 기준, Seibro_api/Seibro_web 은 소스+전송
# 방식. 그래서 "어디에 넣을지" 가 새 행마다 판단이었다.
#
# 두 축으로 가른다. source_name 은 누가 주는가 -- 그 사이트가 막히거나 바뀌면 함께
# 멈추는 단위다. domain 은 무엇에 관한가 -- 분석 쪽에서 묶는 단위다.
#
# api_id 접두사로 정한다. 그룹 값으로는 안 된다: ETF_PDF 한 그룹에 운용사 5곳과
# KRX 와 KIS 가 섞여 있어서, 그룹 -> source_name 이 함수가 아니다.
_SOURCE_DOMAIN: dict[str, tuple[str, str | None]] = {
    # 운용사가 자기 ETF 를 공시하는 것. 사이트가 저마다 달라 함께 죽지 않지만,
    # 하나가 죽으면 그 운용사 것만 죽는다는 점에서 한 소스로 묶인다.
    "ACE_":    ("AMC", "ETF"),
    "KODEX_":  ("AMC", "ETF"),
    "PLUS_":   ("AMC", "ETF"),
    "SOL_":    ("AMC", "ETF"),
    "KB_":     ("AMC", "ETF"),
    "KIWOOM_": ("AMC", "ETF"),
    "NH_":     ("AMC", "ETF"),
    "TIGER_":  ("AMC", "ETF"),
    "TIME_":   ("AMC", "ETF"),
    # 한국투자증권 오픈API. AUTH 가 따로 있는 이유: 토큰 갱신은 데이터를 주는
    # 것이 아니라 **다른 행이 돌기 위한 선행 조건**이라 성질이 다르다.
    "KIS_TOKEN_REFRESH": ("KIS", "AUTH"),
    "KIS_FUTOPT_":       ("KIS", "DERIV"),
    "FO_IDX_CODE_MST":   ("KIS", "DERIV"),
    "KIS_INDEX_":        ("KIS", "INDEX"),
    "KIS_ETF_":          ("KIS", "ETF"),
    # 거래소. 오픈API(KRX_API)와 데이터포털(KRX_DATA)을 source_name 으로 가르지 않는
    # 이유: 같은 기관이고, 포털이 막힐 때 오픈API 는 멀쩡하므로 그 구분은
    # 호스트 단위 차단기가 이미 하고 있다 (execution._HostIndex).
    "KRX_DATA_LOGIN":    ("KRX", "AUTH"),
    "KRX_DERIV_":        ("KRX", "DERIV"),
    "KRX_FUT_":          ("KRX", "DERIV"),
    "KRX_STFUT_":        ("KRX", "DERIV"),
    "KRX_OPTION_":       ("KRX", "DERIV"),
    "KRX_STOPT_":        ("KRX", "DERIV"),
    "KRX_ETF":           ("KRX", "ETF"),
    "KRX_ETN_":          ("KRX", "ETN"),
    "KRX_INDEX_":        ("KRX", "INDEX"),
    "KRX_STOCK_":        ("KRX", "STOCK"),
    "KRX_ISIN_":         ("KRX", "STOCK"),
    # ISIN 하나의 표준코드 상세 팝업. KRX_ISIN_SRCH(브라우저)와 겹쳐 보이지만
    # 그쪽은 코드 앞자리로 **목록**을 긁고 이쪽은 ISIN 하나의 상세를 받는다.
    # 아직 돈 적은 없고 쓰기로 결정된 행이다 (2026-11-25).
    "ISIN_INFO":         ("KRX", "STOCK"),
    # 예탁결제원(세이브로). api/web 은 전송 방식이라 source_name 이 아니다.
    "KSD_":   ("KSD", "BOND"),
    "DART_":  ("DART", "DISCLOSURE"),
}

# 옮기지 않는 행. v1 에는 남겨 둔다(그쪽은 돌고 있는 시스템이고 이 스크립트는
# 읽기만 한다) -- v2 에만 넣지 않는다.
#
# TEST 는 api_url 이 'https://' 뿐인 스텁이고 api_job 이 하나도 없었다.
# 옮기면 source_name 축에 'TEST' 가 하나 남는데, 7.4 가 없애자고 한 바로 그
# 분류체계 혼입이다. 그 행을 겨누는 빌더(TEST_BUILDER)도 함께 빠진다.
_DROP = {
    "TEST": "api_url 이 'https://' 뿐인 스텁. api_job 이 하나도 없다",
}

# ---------------------------------------------------------------------------
# response_parse_json 한 컬럼 -> 네 컬럼
# ---------------------------------------------------------------------------
# v1 은 파싱과 무관한 것이 이 컬럼에 섞여 있었다 (docs/SCHEMA-V2.md 4.4).
# 어느 키가 어디로 가는지는 **v2 엔진이 실제로 읽는 자리**로 정했다 -- 아래
# 주석의 파일:함수가 그 근거다.
_PARSE_KEYS = {        # 응답을 레코드로 바꾸는 규칙
    "fields", "has_header", "skip_rows", "delimiter", "encoding",
    "inner_file", "rows", "url", "table_index", "sheet",
}
_SESSION_KEYS = {      # 로그인·차단 (dynamic._check_reply_markers, base._login_api_id)
    "login", "logged_out", "blocked", "expect", "state",
}
_STAGING_KEYS = {      # 내려받은 문서를 어디에 무슨 이름으로 (base._stage_binary)
    "group", "name", "suffix", "unzip",
}
_OPTION_KEYS = {       # 엔진 로직을 선택적으로 켜는 스위치 (base.KNOWN_OPTIONS)
    "empty_is_pending", "min_interval_sec", "page_delay_sec", "timeout_sec",
}

# v1 의 예약어. {"NOW": col} 은 "이 행은 반복이고 관측 시각을 col 에 찍는다" 는
# 뜻이었고, "none_mapped" 는 "컬럼 자리지만 컬럼이 아니다" 였다. v2 는 둘 다
# 컬럼으로 갈랐으므로 예약어가 사라진다.
_RESERVED_NOW = "NOW"
_NO_COLUMN = "none_mapped"

# 두 층으로 내려보낸 결과 표 (scripts/sql/v2/gen_ddl.py 의 TO_GENERIC). 이
# 표를 쓰던 행의 record_map_json 은 api_rst 를 가리켜야 한다 -- 조회는 v_* 뷰로
# 한다.
_TO_GENERIC = {
    "ace_etf", "ace_etf_pdf", "kodex_etf", "kodex_etf_pdf",
    "plus_etf", "plus_etf_pdf", "sol_etf", "sol_etf_pdf",
}


def _source_domain(api_id: str) -> tuple[str, str | None]:
    """가장 긴 접두사가 이긴다 -- KIS_ETF_ 가 KIS_ 보다 먼저 잡히게."""
    for prefix in sorted(_SOURCE_DOMAIN, key=len, reverse=True):
        if api_id.startswith(prefix):
            return _SOURCE_DOMAIN[prefix]
    raise SystemExit(f"{api_id}: source_name/domain 매핑이 없다 -- _SOURCE_DOMAIN 에 더해라")


def _split_parse(blob: dict[str, Any] | None) -> tuple[dict, dict, dict, dict]:
    """response_parse_json 을 (parse, session, staging, options) 로 가른다.

    모르는 키는 parse_json 으로 떨어뜨리지 않고 **멈춘다**. 조용히 한쪽에
    넣으면 그 행의 기능 하나가 말없이 사라지고, 증상은 "아무 일도 일어나지
    않음" 이라 찾기 어렵다."""
    parse: dict[str, Any] = {}
    session: dict[str, Any] = {}
    staging: dict[str, Any] = {}
    options: dict[str, Any] = {}
    for key, value in (blob or {}).items():
        if key in _PARSE_KEYS:
            parse[key] = value
        elif key in _SESSION_KEYS:
            session[key] = value
        elif key in _STAGING_KEYS:
            staging[key] = value
        elif key in _OPTION_KEYS:
            options[key] = value
        else:
            raise SystemExit(
                f"response_parse_json 의 키 {key!r} 가 어디로 가는지 모르겠다. "
                f"_PARSE_KEYS / _SESSION_KEYS / _STAGING_KEYS / _OPTION_KEYS 중 하나에 더해라"
            )
    return parse, session, staging, options


def _split_key_params(entries: list[Any] | None) -> tuple[list[str], dict[str, str], bool, str | None, set[str]]:
    """key_params_list 한 컬럼 -> (job_key_params, row_stamp_json, is_repeated, snapshot_column).

    v1 의 한 항목은 세 가지를 동시에 뜻했다 (docs/SCHEMA-V2.md 4.1):

    * ``"ISIN"``              -> 파라미터 ISIN, 컬럼은 소문자(isin), 둘 다
    * ``{"BASDD": "bas_dd"}`` -> 이름이 달라야 할 때 (Oracle 이 date 를 예약)
    * ``{"X": "none_mapped"}``-> job_id 만 만들고 행에는 안 찍는다
    * ``{"NOW": "trade_at"}`` -> 반복 행. 관측 시각을 trade_at 에 찍는다

    v1 의 _build_job_id 는 NOW 를 뺀 **모든** 항목의 파라미터 이름으로 job_id
    를 만들었다. 그래서 job_key_params 에는 문자열 항목만이 아니라 dict 항목의
    키도 들어가야 한다 -- 안 그러면 옮긴 뒤 job_id 가 달라지고, 어제까지 받은
    잡과 오늘 만든 잡이 다른 것이 된다.

    다섯 번째로 ``derived`` 를 돌려준다: 컬럼 이름을 **문자열에서 끌어낸**
    파라미터들이다. 그 추론은 틀릴 수 있고(컬럼이 없을 수 있다) 틀려도 v1 은
    조용히 값을 버렸으므로, 부르는 쪽이 실존 컬럼인지 보고 거를 수 있어야
    한다. 명시적 dict 는 사람이 적은 것이라 여기 들어가지 않는다 -- 그것이
    틀렸으면 오류로 드러나야 한다."""
    job_key_params: list[str] = []
    row_stamp: dict[str, str] = {}
    derived: set[str] = set()
    is_repeated = False
    snapshot_column: str | None = None

    for entry in entries or []:
        if isinstance(entry, dict):
            if len(entry) != 1:
                raise SystemExit(f"key_params_list 항목이 단일 키가 아니다: {entry!r}")
            param, column = next(iter(entry.items()))
        else:
            param, column = entry, entry.lower()
            derived.add(param)

        if param == _RESERVED_NOW:
            is_repeated = True
            # 'none_mapped' 는 "반복이지만 시각을 찍을 컬럼은 없다" 는 뜻이었다.
            snapshot_column = None if column == _NO_COLUMN else column
            continue

        job_key_params.append(param)
        if column != _NO_COLUMN:
            row_stamp[param] = column

    return job_key_params, row_stamp, is_repeated, snapshot_column, derived


def _schedule(execution_cycle: str | None) -> tuple[str, str | None]:
    """execution_cycle 한 컬럼 -> (schedule_tag, snapshot_unit).

    v1 은 이 컬럼이 둘을 겸했다: 어느 타이머가 집어 가는가, 그리고 관측 시각을
    어느 버킷으로 내려 자르는가. 그래서 ``daily_batch2`` 같은 값은 절삭 패턴에
    안 맞아 **절삭이 조용히 no-op** 이었다 (docs/SCHEMA-V2.md 4.3).

    옮길 때 그 동작을 그대로 보존한다: v1 에서 실제로 절삭됐던 값만
    snapshot_unit 을 받고, 나머지는 NULL 이다. 여기서 임의로 단위를 채우면
    이행이 동작 변경을 몰래 끼워 넣는 일이 된다 -- 반복 행인데 단위가 없는
    경우는 출력 끝에 따로 보고하고, 사람이 정한다."""
    from app.services.job_builder import _CYCLE_RE

    tag = execution_cycle or "once"
    match = _CYCLE_RE.match(tag.strip())
    if not match:
        return tag, None
    # '3m_call' -> '3m'. 샤드 접미사는 떼고 같은 버킷으로 맞춘다 (한 논리
    # 사이클을 계정별 한도로 나눈 것이라 같은 틱에 돌아야 한다).
    return tag, f"{match.group(1)}{match.group(2)}"


def _record_map(blob: dict[str, str] | None) -> dict[str, str]:
    """output_tables_json -> record_map_json. 두 층으로 내린 표는 api_rst 로.

    v1 에서 api_rst 는 곁다리였다(타입 표에 넣으면서 원본 응답도 보관). 그
    행들은 그대로 두고, TO_GENERIC 에 든 8개 표만 api_rst 로 바꾼다 -- 그
    표들은 v2 에 존재하지 않고 v_* 뷰가 그 자리를 대신한다."""
    return {selector: ("api_rst" if table in _TO_GENERIC else table)
            for selector, table in (blob or {}).items()}


# ---------------------------------------------------------------------------
# 읽기 (v1)
# ---------------------------------------------------------------------------

_API_MST_SQL = """
SELECT t.api_id, t.api_name, t.api_group, t.request_type, t.api_url,
       JSON_SERIALIZE(t.header_json), t.payload_type, JSON_SERIALIZE(t.payload_json),
       t.payload_xml.getclobval(), t.response_type,
       JSON_SERIALIZE(t.response_parse_json), JSON_SERIALIZE(t.persist_env_json),
       JSON_SERIALIZE(t.output_tables_json), JSON_SERIALIZE(t.merge_fields_json),
       JSON_SERIALIZE(t.key_params_list), JSON_SERIALIZE(t.pagination_json),
       t.description, JSON_SERIALIZE(t.behavior_json)
  FROM api_mst t ORDER BY t.api_id
"""

_BUILDER_SQL = """
SELECT build_id, api_id, JSON_SERIALIZE(macro_params_json), is_active,
       save_mode, execution_cycle, description
  FROM api_job_builder ORDER BY build_id
"""


def _v1_connect():
    """v1 로 접속한다. **v1 전용 변수**를 읽는다.

    처음에는 ``ORACLE_*`` 를 읽었다. 그때는 그것이 v1 이고 v2 는 ``V2_*`` 로
    따로 있었다. 지금은 뒤바뀌었다 -- v2 가 별도 인스턴스이고 계정 이름과
    자격이 v1 과 같아서, ``ORACLE_*`` 가 그대로 **v2** 를 가리키고 v1 과의
    차이는 DSN 뿐이다.

    그래서 그대로 두면 이 스크립트가 v2 를 읽어 v2 에 쓴다. 실제로 그렇게
    됐고, v1 컬럼 이름으로 질의하다 ORA-00904 로 죽었다 -- 망가지지는
    않았지만 "엉뚱한 DB 를 읽고 있다" 는 것이 그 메시지에 안 나온다.

    이름을 ``ORACLE_*_V1`` 로 못 박는다. 없으면 멈추고 왜인지 말한다 --
    이행은 끝났고 이 스크립트는 일회성이므로, 그 변수가 없는 것이 정상이다."""
    missing = [k for k in ("ORACLE_USER_V1", "ORACLE_PASSWORD_V1", "ORACLE_DSN_V1")
               if not os.environ.get(k)]
    if missing:
        raise SystemExit(
            "v1 접속 정보가 없다 -- .env 에 " + ", ".join(missing) + " 가 필요하다.\n\n"
            "이행은 2026-11-25 에 끝났고 이 스크립트는 일회성 부트스트랩이다.\n"
            "ORACLE_* 는 이제 v2(SCRAP)를 가리키므로 그쪽을 읽게 두면 v2 를\n"
            "v2 로 '이행' 하게 된다. v1 을 다시 읽어야 할 때만 위 변수를 넣는다."
        )
    kw: dict[str, Any] = dict(user=os.environ["ORACLE_USER_V1"],
                              password=os.environ["ORACLE_PASSWORD_V1"],
                              dsn=os.environ["ORACLE_DSN_V1"])
    # 월렛은 인스턴스가 달라도 같은 것을 쓰고 있어 공용 변수를 그대로 받는다.
    # v1 전용 월렛이 필요하면 ORACLE_WALLET_DIR_V1 로 덮는다.
    wallet = os.environ.get("ORACLE_WALLET_DIR_V1") or os.environ.get("ORACLE_WALLET_DIR")
    if wallet:
        kw.update(config_dir=wallet, wallet_location=wallet,
                  wallet_password=(os.environ.get("ORACLE_WALLET_PASSWORD_V1")
                                   or os.environ.get("ORACLE_WALLET_PASSWORD")))
    return oracledb.connect(**kw)


def _j(raw: Any) -> Any:
    """JSON_SERIALIZE 가 돌려준 것을 파이썬 객체로. CLOB 도 받는다."""
    if raw is None:
        return None
    if hasattr(raw, "read"):
        raw = raw.read()
    return json.loads(raw)


def read_v1() -> tuple[list[dict], list[dict]]:
    con = _v1_connect()
    cur = con.cursor()

    cur.execute(_API_MST_SQL)
    apis = []
    for row in cur:
        (api_id, api_name, api_group, request_type, api_url, header, payload_type,
         payload_json, payload_xml, response_type, response_parse, persist_env,
         output_tables, merge_fields, key_params, pagination, description,
         behavior) = row
        if hasattr(payload_xml, "read"):
            payload_xml = payload_xml.read()
        apis.append(dict(
            api_id=api_id, api_name=api_name, api_group=api_group,
            request_type=request_type, api_url=api_url, header_json=_j(header),
            payload_type=payload_type, payload_json=_j(payload_json),
            payload_xml=payload_xml, response_type=response_type,
            response_parse_json=_j(response_parse), persist_env_json=_j(persist_env),
            output_tables_json=_j(output_tables), merge_fields_json=_j(merge_fields),
            key_params_list=_j(key_params), pagination_json=_j(pagination),
            description=description, behavior_json=_j(behavior),
        ))

    cur.execute(_BUILDER_SQL)
    builders = [dict(build_id=b, api_id=a, macro_params_json=_j(m), is_active=bool(act),
                     save_mode=sm, execution_cycle=ec, description=d)
                for b, a, m, act, sm, ec, d in cur]
    con.close()
    return apis, builders


# ---------------------------------------------------------------------------
# 변환
# ---------------------------------------------------------------------------


def _typed_columns(record_map: dict[str, str]) -> set[str] | None:
    """record_map_json 이 가리키는 **타입** 표들의 컬럼 합집합.

    범용 저장 경로(api_rst)뿐이면 None -- 거기서는 모든 이름이 result_json 의
    키라 "없는 컬럼" 이라는 것이 없다 (validate_api_config 와 같은 판정)."""
    from app.services.export import TABLE_REGISTRY

    columns: set[str] = set()
    found_typed = False
    for table in set(record_map.values()):
        model = TABLE_REGISTRY.get(table)
        if model is None or "result_json" in model.model_fields:
            continue
        found_typed = True
        columns |= set(model.model_fields)
    return columns if found_typed else None


def transform_api(row: dict, demoted: list[str] | None = None) -> dict:
    source_name, domain = _source_domain(row["api_id"])
    parse, session, staging, options = _split_parse(row["response_parse_json"])
    job_keys, row_stamp, is_repeated, snapshot_column, derived = \
        _split_key_params(row["key_params_list"])
    record_map = _record_map(row["output_tables_json"])

    # 컬럼 이름을 문자열에서 끌어낸 스탬프 중, 그 표에 실제로 없는 것은
    # row_stamp_json 에서 뺀다. job_key_params 에는 남는다 -- job_id 를
    # 만드는 역할은 그대로다.
    #
    # 세 행이 그렇다 -- KIS_FUTOPT_CHART 의 BAR_SEC, KIS_FUTOPT_DAILY 와
    # KIS_INDEX_DAILY 의 DATE1/DATE2. **저장하지 않는 것이 의도였다**(작성자
    # 확인): 요청에는 필요하고(fid_input_date_1 · fid_input_date_2 ·
    # fid_hour_cls_code) 응답이 되돌려주지 않으며, 행마다 찍을 날짜는 API 가
    # 주는 stck_bsop_date 다 -- 한 요청이 여러 날을 돌려주므로 DATE1 도 DATE2
    # 도 아니다.
    #
    # v1 설정이 그 의도를 적을 수 없었던 것이 문제였다. 문자열 항목은 컬럼
    # 이름을 소문자로 끌어내므로 "이 컬럼에 찍어라" 로 읽히고, 의도대로
    # 적으려면 {"BAR_SEC": "none_mapped"} 라야 했다. 그렇게 안 적혀 있어서
    # 엔진은 찍으려 했고, 컬럼이 없으니 pydantic 이 모르는 필드로 무시했다 --
    # 결과는 의도와 같았지만 경로가 "설정에 적힌 대로" 가 아니라 "조용히
    # 떨어짐" 이었다.
    #
    # v2 는 두 컬럼이 갈려 있어 예약어 없이 그대로 표현된다. 그래서 여기서
    # 빼는 것은 기능 변경이 아니라 의도를 설정에 적는 것이다. 사람이 적은
    # dict 는 건드리지 않는다: 그게 틀렸으면 SchemaError 로 드러나야 한다.
    known = _typed_columns(record_map)
    if known is not None:
        for param in sorted(row_stamp):
            if param in derived and row_stamp[param] not in known:
                if demoted is not None:
                    demoted.append(f"{row['api_id']}.{param} (-> {row_stamp[param]!r} 없음)")
                del row_stamp[param]

    # request_type 한 컬럼이 전송 방식과 HTTP 메서드를 겸했다. 'BROWSER' 는
    # 메서드가 아니고, 'GET'/'POST' 는 전송 방식이 아니다 (4.5).
    raw = (row["request_type"] or "GET").upper()
    if raw == "BROWSER":
        protocol, method = "BROWSER", None
    else:
        protocol, method = "HTTP", raw

    return dict(
        api_id=row["api_id"], api_name=row["api_name"],
        source_name=source_name, domain=domain,
        protocol=protocol, method=method, api_url=row["api_url"],
        header_json=row["header_json"] or None,
        payload_type=row["payload_type"], payload_json=row["payload_json"],
        payload_xml=row["payload_xml"], behavior_json=row["behavior_json"],
        response_type=row["response_type"],
        parse_json=parse or None, session_json=session or None,
        staging_json=staging or None, options_json=options or None,
        persist_env_json=row["persist_env_json"],
        record_map_json=record_map or None,
        merge_fields_json=row["merge_fields_json"],
        job_key_params=job_keys or None, row_stamp_json=row_stamp or None,
        is_repeated=is_repeated, snapshot_column=snapshot_column,
        pagination_json=row["pagination_json"],
        note=row["description"] or None,
    )


def transform_builder(row: dict) -> dict:
    schedule_tag, snapshot_unit = _schedule(row["execution_cycle"])
    return dict(
        build_id=row["build_id"], api_id=row["api_id"],
        cartesian_param_json=row["macro_params_json"] or {},
        is_active=row["is_active"], save_mode=row["save_mode"],
        schedule_tag=schedule_tag, snapshot_unit=snapshot_unit,
        note=row["description"] or None,
    )


# ---------------------------------------------------------------------------
# 쓰기 (v2 SCRAP, SQLModel 경유)
# ---------------------------------------------------------------------------


def apply_to_v2(apis: list[dict], builders: list[dict], force: bool = False) -> None:
    """v2 의 두 표를 비우고 새로 채운다. **일회성 부트스트랩이다.**

    api_job / api_job_log 는 건드리지 않는다. 로그는 job_id 가 FK 가 아닌
    평범한 컬럼이라 일부러 독립이고(ApiJobLog docstring), 검증한 기록을
    남겨 둔다. 다만 빌더가 사라지면 그걸 참조하는 잡은 고아가 되므로 잡은
    지운다.

    **갈라진 v2 에는 쓰지 않는다.** 이행이 끝난 뒤의 설정 변경은 v2 에서
    일어나고 v1 에는 없다 -- 예를 들어 ISIN_BOND_INFO 는 v1 의 ISIN_INFO 를
    이름부터 고쳐 html_block 설정을 손으로 채운 행이다. 그 상태에서 다시
    적재하면 v1 에 있는 것만 남아 그 작업이 전부 사라지고, 잡 수만 행도 같이
    지워진다. 그래서 **v1 에 없는 api_id 가 v2 에 있으면 멈춘다.**

    이것을 override 표로 풀지 않은 이유: 그러면 이 스크립트가 설정 관리
    도구가 되고, 같은 설정이 두 곳(여기와 DB)에 살면서 어느 쪽이 정본인지
    흐려진다. 이행 스크립트는 이행만 한다."""
    # 환경변수를 바꿔 끼우지 않는다 -- ORACLE_* 가 곧 v2(SCRAP)이고,
    # v1 은 _v1_connect 가 _V1 변수로 따로 연다.
    from sqlalchemy import delete as sa_delete
    from sqlmodel import Session, select

    from app.db.engine import engine
    from app.db.models import ApiJob, ApiJobBuilder, ApiMst

    with Session(engine) as s:
        incoming = {row["api_id"] for row in apis}
        existing = set(s.exec(select(ApiMst.api_id)).all())
        drifted = sorted(existing - incoming)
        if drifted and not force:
            raise SystemExit(
                "v2 가 v1 에서 갈라져 있다 -- v1 에 없는 api_id 가 v2 에 "
                f"{len(drifted)}개 있다:\n  " + "\n  ".join(drifted)
                + "\n\n다시 적재하면 그 행과 그 설정이 사라진다. 이행은 이미 끝났고,"
                "\n이 스크립트는 일회성 부트스트랩이다. 정말 v1 상태로 되돌리려면"
                "\n--force 를 준다 (그 행들과 모든 api_job 이 지워진다)."
            )
        if drifted:
            print(f"--force: v1 에 없는 {len(drifted)}개를 지운다 -- {', '.join(drifted)}")
        # 빌더 -> api 순으로 지운다. FK 는 없지만 읽는 사람에게 순서가 뜻을 준다.
        s.exec(sa_delete(ApiJob))
        s.exec(sa_delete(ApiJobBuilder))
        s.exec(sa_delete(ApiMst))
        for row in apis:
            s.add(ApiMst(**row))
        s.commit()
        for row in builders:
            s.add(ApiJobBuilder(**row))
        s.commit()
        print(f"적재: api_mst {len(apis)}행 · api_job_builder {len(builders)}행")


def validate(apis: list[dict]) -> list[str]:
    """옮긴 행이 엔진의 설정 검증을 통과하는가 (job_builder.validate_api_config).

    쓰기 전에 돈다. 이행이 통째로 실패하는 쪽이, 절반 적재된 상태로 멈춰
    "어디까지 들어갔지" 를 세는 쪽보다 낫다."""
    from app.db.models import ApiMst
    from app.errors import ScrapeError
    from app.services.job_builder import validate_api_config

    problems = []
    for row in apis:
        try:
            validate_api_config(ApiMst(**row))
        except ScrapeError as exc:
            problems.append(f"{row['api_id']}: {type(exc).__name__} -- {exc}")
    return problems


# ---------------------------------------------------------------------------


def main() -> int:
    load_dotenv(".env")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="v2 에 실제로 적재한다")
    parser.add_argument("--force", action="store_true",
                        help="v2 가 v1 에서 갈라져 있어도 적재한다 (갈라진 행이 지워진다)")
    args = parser.parse_args()

    v1_apis, v1_builders = read_v1()
    print(f"v1: api_mst {len(v1_apis)}행 · api_job_builder {len(v1_builders)}행")

    # 옮기지 않는 행과 그 행을 겨누는 빌더를 먼저 걸러낸다. _source_domain 이
    # 그 api_id 를 모르는 상태라 변환보다 앞이어야 한다.
    v1_apis = [r for r in v1_apis if r["api_id"] not in _DROP]
    v1_builders = [r for r in v1_builders if r["api_id"] not in _DROP]
    print(f"옮김: api_mst {len(v1_apis)}행 · api_job_builder {len(v1_builders)}행\n")

    demoted: list[str] = []
    apis = [transform_api(r, demoted) for r in v1_apis]
    builders = [transform_builder(r) for r in v1_builders]

    # source_name x domain 격자
    grid: dict[tuple[str, str | None], int] = {}
    for row in apis:
        key = (row["source_name"], row["domain"])
        grid[key] = grid.get(key, 0) + 1
    print("source_name x domain:")
    for (source_name, domain), n in sorted(grid.items(), key=lambda kv: (kv[0][0], str(kv[0][1]))):
        print(f"  {source_name:6} {str(domain):12} {n:3}")

    # 갈라진 컬럼이 실제로 몇 행에 값을 남겼나
    print("\nresponse_parse_json -> 네 컬럼:")
    for column in ("parse_json", "session_json", "staging_json", "options_json"):
        print(f"  {column:14} {sum(1 for r in apis if r[column])}행")

    repeated = [r for r in apis if r["is_repeated"]]
    print(f"\nkey_params_list -> 반복 {len(repeated)}행 "
          f"(그중 snapshot_column 있는 것 {sum(1 for r in repeated if r['snapshot_column'])}행)")
    print(f"  job_key_params 있는 행 {sum(1 for r in apis if r['job_key_params'])}")
    print(f"  row_stamp_json 있는 행 {sum(1 for r in apis if r['row_stamp_json'])}")

    units = {}
    for row in builders:
        units[row["snapshot_unit"]] = units.get(row["snapshot_unit"], 0) + 1
    print(f"\nexecution_cycle -> snapshot_unit: "
          + ", ".join(f"{k or 'NULL'}={v}" for k, v in sorted(units.items(), key=lambda kv: str(kv[0]))))

    # 반복 행인데 그 빌더에 버킷이 없는 경우 -- v1 에서 절삭이 no-op 이던 것
    repeated_ids = {r["api_id"] for r in repeated if r["snapshot_column"]}
    unfloored = sorted({(b["api_id"], b["schedule_tag"]) for b in builders
                        if b["api_id"] in repeated_ids and not b["snapshot_unit"]})
    if unfloored:
        print("\n[확인 필요] 관측 시각을 찍는데 버킷이 없는 빌더 "
              "(v1 에서도 절삭이 no-op 이었다 -- 동작을 바꾸지 않으려고 NULL 로 뒀다):")
        for api_id, tag in unfloored:
            print(f"  {api_id:24} schedule_tag={tag}")

    moved = [r["api_id"] for r in apis
             if r["record_map_json"] and "api_rst" in r["record_map_json"].values()]
    print(f"\nrecord_map_json 이 api_rst 를 가리키는 행 {len(moved)}: {', '.join(moved)}")

    if _DROP:
        print("\n옮기지 않은 행 (v1 에는 남는다 -- 이 스크립트는 그쪽을 읽기만 한다):")
        for api_id, why in _DROP.items():
            print(f"  {api_id:12} {why}")

    if demoted:
        print(f"\n[이행 중 명시] row_stamp_json 에서 뺀 파라미터 {len(demoted)}개 -- 요청에만 "
              "필요하고 저장하지 않는 값이다 (job_key_params 에는 남는다):")
        for line in demoted:
            print(f"  {line}")

    problems = validate(apis)
    if problems:
        print(f"\n설정 검증 실패 {len(problems)}건 -- 적재하지 않는다:")
        for line in problems:
            print(f"  {line}")
        return 1
    print(f"\n설정 검증: {len(apis)}행 전부 통과 (validate_api_config)")

    if not args.apply:
        print("\ndry-run 이다. 적재하려면 --apply")
        return 0

    apply_to_v2(apis, builders, force=args.force)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
