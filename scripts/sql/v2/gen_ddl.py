"""SCRAP 계정의 결과 테이블 DDL 과 뷰를 app.db.models 에서 뽑는다.

손으로 적지 않는 이유는 분량(34개 표, 700컬럼)이 아니라 **어긋남**이다. 모델이
정본이고 DDL 은 그것의 표현이라, 둘을 따로 관리하면 언젠가 한쪽만 고쳐진다.
부기 4개 표(01_scrap_config.sql)는 v2 에서 구조가 바뀌므로 거기서만 손으로 적는다.

두 층 (docs/SCHEMA-V2.md, 사용자 결정 2026-11-24):

- 타입 테이블로 남는 것 — 운용사별 표를 뺀 나머지. 행 수의 대부분이 여기 있고,
  수집 DB 에 직접 집계를 돌리는 대상이다.
- api_rst + 뷰로 내리는 것 — 운용사별 결과 표 8개. 운용사가 늘 때마다 모델을
  더해야 하는 것이 그 표들이었다. api_id 로 리스트 파티션되므로 한 소스를 읽을
  때 다른 소스 행은 읽지 않는다.

뷰의 컬럼 목록은 **내려보낸 모델에서 그대로 뽑는다.** 그래서 ace_etf_pdf 를
조회하던 질의가 v_ace_etf_pdf 에 그대로 걸린다 -- 컬럼 이름도 타입도 같다.

전제 하나: 범용 저장 경로가 레코드 키를 소문자로 접어야 한다. 지금은 원본
키를 그대로 넣어서(ACE 는 ``std_DT``, KRX 는 대문자) JSON 경로가 소스마다
달라진다. 타입 테이블은 VendorRecordBase 가 이미 접고 있으니, 같은 접기를
범용 경로에도 적용하면 경로가 ``$.<컬럼명>`` 으로 예측 가능해진다.
"""

from __future__ import annotations

import pathlib
import sys

from sqlalchemy.dialects import oracle
from sqlalchemy.schema import CreateIndex, CreateTable

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3]))

import app.db.models as _models  # noqa: E402
from app.services.export import TABLE_REGISTRY  # noqa: E402

OUT = pathlib.Path(__file__).parent

# 운용사별 결과 표 -> 그 표를 채우던 api_id 들. api_rst 로 내리고 뷰로 조회한다.
# 지금은 표마다 api_id 하나지만(수집 DB 실측) 목록으로 둔다 -- 한 표를 여러
# api 가 채우는 경우가 이미 있고(amc_etf_pdf 는 5개 운용사), 운용사가 늘면
# 여기 한 줄을 더하는 것으로 뷰가 따라온다.
TO_GENERIC = {
    "ace_etf":       ["ACE_ETF_LIST"],
    "ace_etf_pdf":   ["ACE_ETF_PDF"],
    "kodex_etf":     ["KODEX_ETF_LIST"],
    "kodex_etf_pdf": ["KODEX_ETF_PDF"],
    "plus_etf":      ["PLUS_ETF_LIST"],
    "plus_etf_pdf":  ["PLUS_ETF_PDF"],
    "sol_etf":       ["SOL_ETF_LIST"],
    "sol_etf_pdf":   ["SOL_ETF_PDF"],
}

_DIALECT = oracle.dialect()
_SKIP_IN_VIEW = {"id", "api_id", "job_id", "updated_at"}

_BOOKKEEPING = {"api_mst", "api_job_builder", "api_job", "api_job_log"}
# SQLModel 은 table=True 가 아닌 베이스 클래스에도 __tablename__ 을 만들어 준다.
# 실제 표가 아니므로 빼낸다 (docs/SCHEMA-V2.md 1절의 "허수 2개").
_NOT_TABLES = {"vendorrecordbase", "mdcrecordbase"}


def _all_models() -> dict:
    import inspect

    from sqlmodel import SQLModel

    out = {}
    for obj in vars(_models).values():
        if not (inspect.isclass(obj) and issubclass(obj, SQLModel) and obj is not SQLModel):
            continue
        name = getattr(obj, "__tablename__", None)
        if name and name not in _NOT_TABLES:
            out[name] = obj
    return out


def ref_tables() -> list[str]:
    """REF 계정이 갖는 표 -- 결과도 부기도 아닌 것 전부.

    엔진이 쓰지 않고 프로시저가 관리하는 사전 데이터다. 전부 자연키 PK 라
    identity 시퀀스가 없다 -- 결과 테이블과 달리 행이 어디서 왔는지가 아니라
    무엇인지로 식별된다."""
    return sorted(set(_all_models()) - set(TABLE_REGISTRY) - _BOOKKEEPING)


def _header(title: str) -> str:
    return (f"-- {title}\n"
            f"--\n"
            f"-- gen_ddl.py 가 app/db/models.py 에서 뽑은 것이다. 손으로 고치지 말고\n"
            f"-- 모델을 고친 뒤 다시 뽑는다.\n"
            f"--   python scripts/sql/v2/gen_ddl.py\n\n")


def typed_ddl() -> str:
    names = sorted(set(TABLE_REGISTRY) - set(TO_GENERIC) - {"api_rst"})
    out = [_header(f"SCRAP 계정: 타입 결과 테이블 {len(names)}개")]
    for name in names:
        table = TABLE_REGISTRY[name].__table__
        out.append(str(CreateTable(table).compile(dialect=_DIALECT)).strip() + ";\n")
        for index in sorted(table.indexes, key=lambda i: i.name or ""):
            out.append(str(CreateIndex(index).compile(dialect=_DIALECT)).strip() + ";")
        out.append("")
    return "\n".join(out)


def ref_ddl() -> str:
    names = ref_tables()
    models = _all_models()
    out = [_header(f"REF 계정: 사전 데이터 테이블 {len(names)}개")]
    out.append(
        "-- 엔진은 읽기만 한다. 채우는 것은 sp_*_sync 프로시저이고, 그 둘을\n"
        "-- 계정으로 가르면 권한이 그 사실을 강제한다 -- 파이썬이 실수로\n"
        "-- mst_etf 에 INSERT 할 수 없다.\n"
        "--\n"
        "-- user_etf 가 이 계정을 나누는 가장 큰 이유다. 손으로 적은 값이라\n"
        "-- 다시 만들 수 없는데 v1 에서는 날마다 비워지는 결과 테이블과 같은\n"
        "-- 계정에 있었다.\n")
    for name in names:
        table = models[name].__table__
        doc = (models[name].__doc__ or "").strip().splitlines()
        if doc:
            out.append(f"-- {name}: {doc[0].strip()}")
        out.append(str(CreateTable(table).compile(dialect=_DIALECT)).strip() + ";\n")
        for index in sorted(table.indexes, key=lambda i: i.name or ""):
            out.append(str(CreateIndex(index).compile(dialect=_DIALECT)).strip() + ";")
        out.append("")
    return "\n".join(out)


def _json_path(column) -> str:
    """뷰에서 쓸 JSON_VALUE 식. 모델의 컬럼 타입을 그대로 돌려준다.

    타입은 **DDL 을 만든 그 컴파일러로** 얻는다. 그래서 뷰가 돌려주는 타입이
    타입 테이블의 컬럼 타입과 반드시 같아진다 -- 승격해도 질의가 안 바뀌는
    것이 이 두 층 설계의 전제다.

    모델의 파이썬 타입을 보려는 시도는 두 번 실패했다. SQLModel 의
    ``AutoString`` 은 ``sqlalchemy.types.String`` 의 서브클래스가 아니라
    ``TypeDecorator`` 라 ``isinstance(t, String)`` 이 False 이고,
    ``python_type`` 은 ``NotImplementedError`` 를 던진다. 둘 다 조용히
    VARCHAR2(4000) 으로 떨어져서 길이가 전부 뭉개졌다."""
    sql_type = column.type.compile(dialect=_DIALECT).upper()
    if sql_type.startswith("VARCHAR2"):
        # 'VARCHAR2(20 CHAR)' -> 'VARCHAR2(20 CHAR)' 그대로 쓸 수 있다.
        returning = sql_type
    elif sql_type.startswith(("NUMBER", "INTEGER", "FLOAT", "BINARY_DOUBLE", "DECIMAL")):
        returning = "NUMBER"
    else:
        # DATE/CLOB/JSON 등은 문자열로 꺼내고 쓰는 쪽에서 변환한다. JSON 에는
        # 원래 문자열로 들어가 있고, 여기서 TO_DATE 를 걸면 형식이 하나로
        # 고정되어 소스가 다른 표기를 보내는 순간 전체 질의가 죽는다.
        returning = "VARCHAR2(4000)"
    return (f"JSON_VALUE(result_json, '$.{column.name}' RETURNING {returning})"
            f" AS {column.name}")


def views_ddl() -> str:
    out = [_header(f"SCRAP 계정: api_rst 위의 뷰 {len(TO_GENERIC)}개")]
    out.append("-- 컬럼 목록은 내려보낸 모델에서 뽑았다. 이 뷰를 조회하는 질의는\n"
               "-- 타입 테이블을 조회하던 것과 같다 -- 이름도 타입도 바뀌지 않는다.\n"
               "-- 나중에 타입 테이블로 승격해도 뷰 이름을 유지하면 질의는 그대로다.\n")
    for name, api_ids in TO_GENERIC.items():
        model = TABLE_REGISTRY[name]
        table = model.__table__
        cols = [c for c in table.columns if c.name not in _SKIP_IN_VIEW]
        body = ",\n".join("       " + _json_path(c) for c in cols)
        doc = (model.__doc__ or "").strip().splitlines()
        first = doc[0] if doc else ""
        ids = ", ".join(f"'{a}'" for a in api_ids)
        out.append(f"-- {name}  ({len(cols)}컬럼)  {first}")
        # api_id 를 뷰에 남긴다. 한 뷰가 여러 api_id 를 덮는 경우(운용사가
        # 늘면 그렇게 된다) 행이 어디서 왔는지 알 수 있어야 하고, 타입
        # 테이블도 api_id 를 컬럼으로 갖고 있었다.
        out.append(f"CREATE OR REPLACE VIEW v_{name} AS\n"
                   f"SELECT api_id, job_id,\n{body}\n"
                   f"  FROM api_rst\n"
                   f" WHERE api_id IN ({ids});\n")
    out.append("-- 조회에 쓰는 JSON 경로마다 함수기반 인덱스를 둔다. 없으면 그\n"
               "-- 질의가 전체 스캔으로 떨어진다 (측정: 인덱스 있으면 타입\n"
               "-- 테이블의 1.1x, 없으면 전수 스캔 2.5x). LOCAL 이라 파티션\n"
               "-- 프루닝과 함께 동작한다.\n"
               "-- 예:\n"
               "-- CREATE INDEX ix_api_rst_std_dt ON api_rst\n"
               "--   (JSON_VALUE(result_json, '$.std_dt' RETURNING VARCHAR2(8))) LOCAL;")
    return "\n".join(out)


if __name__ == "__main__":
    (OUT / "02_scrap_results.sql").write_text(typed_ddl(), encoding="utf-8")
    (OUT / "04_scrap_views.sql").write_text(views_ddl(), encoding="utf-8")
    (OUT / "05_ref_tables.sql").write_text(ref_ddl(), encoding="utf-8")
    names = sorted(set(TABLE_REGISTRY) - set(TO_GENERIC) - {"api_rst"})
    cols = sum(len(TABLE_REGISTRY[n].__table__.columns) for n in names)
    models = _all_models()
    rcols = sum(len(models[n].__table__.columns) for n in ref_tables())
    print(f"02_scrap_results.sql  타입 {len(names)}개 표, {cols}컬럼")
    print(f"04_scrap_views.sql    뷰 {len(TO_GENERIC)}개")
    print(f"05_ref_tables.sql     사전 {len(ref_tables())}개 표, {rcols}컬럼")
