"""결과/사전/상태 표가 함께 쓰는 베이스와 믹스인."""

import re
from datetime import date, datetime
from typing import Any, Optional

from pydantic import ConfigDict, model_validator
from sqlalchemy import CheckConstraint, Column, Date, DateTime, Identity, Integer, String, func
from sqlmodel import Field, SQLModel

from app.db.types import JSONText, OracleBoolean, XMLText

# Convention: on every table below, `updated_at` (when present) is declared
# as the LAST field, so it's also the last physical DB column. Keep new
# fields on existing tables (and new tables) consistent with this -- insert
# them before `updated_at`, never after.


# tags: List[str] = Field(default_factory=list, sa_type=JSON)



class ResultTable(SQLModel):
    """엔진이 쓰는 표. SCRAP 계정.

    수집물이 들어오는 곳이다. ``output_tables_json`` 이 가리킬 수 있는 표는
    이것뿐이고, app.services.export.TABLE_REGISTRY 가 이 마커로 모은다.

    v1 은 "job_id 컬럼이 있으면 결과 표" 라는 규칙으로 찾았다. 지금은 맞지만
    우연히 맞는 것이었다 -- api_job 은 job_id 로 키가 잡히고 api_job_log 는
    그것을 참조해서 둘 다 걸렸으므로, 이름 네 개를 따로 하드코딩해 빼야 했다.
    마커로 말하면 그 집합이 필요 없다.
    """


class StateTable(SQLModel):
    """수집 상태를 적는 표. SCRAP 계정이지만 결과는 아니다.

    "물어봤는데 없었다" 처럼 **우리 수집 이력**인 것들이다. 종목의 성질이
    아니므로 사전 데이터가 아니고, 엔진이 수집한 레코드도 아니므로 결과가
    아니다. job_id 가 없어서 v1 의 규칙으로는 분류할 자리가 없었다.

    경계를 "누가 쓰는가" 로 긋는다 -- 쓰는 쪽이 수집 계정이면 여기다
    (docs/SCHEMA-V2.md 9.5).
    """


class ReferenceTable(SQLModel):
    """사전 데이터. REF 계정. 엔진은 읽기만 한다.

    종목·만기·지수가 무엇인가를 적은 표이고, 채우는 것은 sp_*_sync
    프로시저다. 계정이 갈려서 권한이 그 사실을 강제한다 -- 파이썬이 실수로
    mst_etf 에 INSERT 할 수 없다(ORA-41900 으로 확인).

    user_etf 가 이 분류의 이유를 가장 잘 보여준다: 손으로 적은 값이라 다시
    만들 수 없는데, v1 에서는 날마다 비워지는 결과 표와 같은 계정에 있었다.
    """


class VendorRecordBase(SQLModel):
    """Shared record normalization for result tables fed by a source that
    spells its records the way KRX and KSD/SEIBRO do -- the ``krx_*`` and
    ``ksd_*`` tables below, and ``kis_etf*`` for the blank handling alone
    (KIS already sends lower-case keys, so the folding is a no-op there).

    Both differ from the KIS endpoints in the same two ways, and a typed
    column can absorb neither on its own:

    - keys come back upper-cased (``BAS_DD``, ``CODEVALUE``), and
      ``model_validate`` matches on the field name, so nothing would land;
    - a value the source has nothing to put in is blank rather than null --
      an option strike that did not trade, a bond with no rating -- and
      pydantic refuses ``""`` for ``Optional[float]``/``Optional[int]``.
      Most rows on an option endpoint are untraded strikes, so this is the
      common case, not an edge one: left as-is, a whole day's job fails on
      the first idle strike. "Blank" is not always ``""``: SEIBRO also sends
      a single space, and an ideographic one (``　``), for the same
      thing -- so the test is whether the value is *only* whitespace, not
      whether it is empty. Interior spacing is left alone, because on the
      KRX instrument names it is fixed-width padding that belongs to the
      value.

    One before-hook per table rather than a dozen-plus per-field aliases and
    validators each: both quirks are properties of the *source*, so they are
    dealt with once, where the record enters.

    ``coerce_numbers_to_str`` is on for a third habit: a vendor may send a
    date or a code as a *number* where the column wants its digits as text
    (SOL's product list answers ``WORK_DT: 20260830`` unquoted, and ``0`` for
    its internal rows). Pydantic v2 refuses int -> str by default, so without
    this the whole job dies on a field whose value was never in doubt. It
    only ever widens what a str column accepts; the numeric columns are
    unaffected."""

    model_config = ConfigDict(coerce_numbers_to_str=True)  # type: ignore[assignment]

    @model_validator(mode="before")
    @classmethod
    def _normalize_vendor_record(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        return {k.lower(): (None if isinstance(v, str) and not v.strip() else v)
                for k, v in data.items()}


class MdcRecordBase(VendorRecordBase):
    """VendorRecordBase 에 data.krx.co.kr 화면의 습관 둘을 더한 것.

    오픈 API(data-dbg.krx.co.kr)는 대체로 숫자를 그대로 보내지만, 로그인해서
    보는 화면은 표에 그리던 문자열을 그대로 준다. 소스의 성질이므로 표마다
    검증기를 다는 대신 여기서 한 번에 접는다 -- VendorRecordBase 가 대문자
    키와 빈칸을 접는 것과 같은 자리다.

    이름은 화면(MDC)에서 왔지만 오픈 API 에도 예외가 하나 있다:
    ``idx/kospi_dd_trd`` 는 지수값을 '1,313.00' 으로 보낸다. 그래서
    KrxIndexDaily 도 이 베이스를 쓴다 -- 규칙은 '어느 호스트냐'가 아니라
    '숫자를 사람이 읽는 모양으로 보내느냐'다."""

    @model_validator(mode="before")
    @classmethod
    def _normalize_mdc_record(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = {}
        for k, v in data.items():
            if isinstance(v, str):
                s = v.strip()
                if s == "-":
                    v = None
                elif "," in s and _MDC_NUMERIC_RE.match(s):
                    v = s.replace(",", "")
            out[k] = v
        return out


class EtfPdfDateMixin:
    """``std_dt`` 를 YYYYMMDD 로 맞춘다.

    운용사가 저마다 다른 표기로 날짜를 준다 -- ACE 는 ``2026-08-31``, 키움은
    ``2026.08.31``, 나머지는 잡 파라미터가 찍는 ``20260831`` 이다. 소스가 준
    값이 잡 파라미터를 이기도록 되어 있어(dynamic.run_and_save 에서 레코드를
    나중에 병합한다) 파라미터 형식을 맞추는 것만으로는 통일되지 않는다.

    그 규칙과 싸우는 대신 받은 값에서 숫자만 남긴다. 어느 쪽이 이기든 결과가
    같아지고, 한 컬럼에 세 형식이 섞여 export 쪽에서 파싱이 갈리는 일이 없다."""

    @model_validator(mode="before")
    @classmethod
    def _normalize_std_dt(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # 해당하는 키를 **전부** 고친다. 잡 파라미터가 찍는 ``std_dt`` 와
            # 소스가 주는 ``std_DT``(ACE) 가 한 딕셔너리에 같이 들어오는데,
            # 키를 소문자로 접는 VendorRecordBase 의 검증기를 지나면 둘이 한
            # 칸으로 합쳐지면서 뒤엣것이 이긴다. 하나만 고치면 어느 쪽이 남는지가
            # 검증기 실행 순서에 달리게 되므로, 둘 다 같은 값으로 만들어 둔다.
            fixed = {}
            for key, value in data.items():
                if key.lower() != "std_dt" or not isinstance(value, str) or not value.strip():
                    continue
                digits = re.sub(r"\D", "", value)
                if len(digits) == 8:
                    fixed[key] = digits
            if fixed:
                data = {**data, **fixed}
        return data
