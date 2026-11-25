"""v1 의 결과 데이터 일부를 v2 로 떠 온다. **DB 쪽 절반을 검증하기 위한 것이다.**

무엇을 검증하나:

* 동기화 프로시저 8개가 실제 데이터로 접히나 (지금은 전부 0건이라 아무것도
  확인되지 않는다 -- 단계가 지나간다는 것만 안다)
* ``v_*`` 뷰 8개가 타입과 정규화를 맞게 돌려주나
* ``sp_src_code_sync`` 가 아래층으로 내려 빠졌던 코드 컬럼 11개를 되찾나
* 내보내기 경로(``sp_run_export``)가 도나

무엇을 **검증하지 못하나**: ``parse_json`` · ``record_map_json`` ·
``row_stamp_json`` · ``session_json`` · ``staging_json``. v1 데이터를 복사하면
스크래퍼를 건너뛰므로, 설정 변환이 맞는지는 요청을 실제로 보내 봐야 드러난다
(docs/SCHEMA-V2.md 8.1). 이 스크립트는 그 절반을 대신하지 않는다.

**한 방향이다.** v1 은 읽기만 한다.

조각을 랜덤으로 뜨지 않는다. 동기화 프로시저는 표 사이의 조인으로 접으므로
(mst_bond 는 ksd_bond_isin ⋈ ksd_bond_info), 서로 안 맞는 조각을 뜨면 또 0건이
된다. 그래서 규칙을 표마다 적는다 -- 아래 PLAN 참고.

    python scripts/load_sample_from_v1.py            # 무엇을 뜰지만 출력
    python scripts/load_sample_from_v1.py --apply    # v2 에 적재
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

# 두 층으로 내린 표. v1 에는 표, v2 에는 api_rst + v_* 뷰다.
TO_GENERIC = {"ace_etf", "ace_etf_pdf", "kodex_etf", "kodex_etf_pdf",
              "plus_etf", "plus_etf_pdf", "sol_etf", "sol_etf_pdf"}

# 표마다 어떻게 뜰지. 값은 (방식, 인자).
#
#   "all"   전부. 작아서 자를 이유가 없거나, 동기화가 전체를 봐야 하는 것.
#   "day"   그 표의 날짜 컬럼 최신값 하루. 날짜별로 쌓이는 큰 표.
#   "top"   앞 N 행. 날짜 컬럼이 없고 큰 표 -- 모양만 보면 되는 것.
#   "join"  다른 표에 있는 키만. 조각이 서로 맞아야 동기화가 접는다.
PLAN: dict[str, tuple[str, Any]] = {
    # --- 동기화 입력. 여기가 맞아야 mst_* 가 채워진다 -----------------------
    # sp_mst_stock_sync 는 krx_stock_base 를 접어 mst_stock 으로 넣는다.
    "krx_stock_base":     ("all", None),
    # sp_meta_maturity_sync / sp_mst_fuopt_sync 가 보는 전종목 기본정보.
    "krx_deriv_info":     ("all", None),
    "krx_deriv_expyy":    ("all", None),
    # sp_mst_bond_sync 는 ksd_bond_isin ⋈ ksd_bond_info 다. info 를 전부 뜨고
    # isin 은 **그 ISIN 만** 뜬다 -- 1,899,498 행을 다 가져올 이유가 없고,
    # 랜덤으로 뜨면 조인이 비어 또 0건이 된다.
    "ksd_bond_info":      ("all", None),
    "ksd_bond_isin":      ("day_join", ("ksd_bond_info", "isin")),
    "ksd_bond_no_detail": ("all", None),
    # sp_mst_etf_sync 는 krx_etf_daily 와 운용사 목록을 본다. 목록은 작고,
    # 일별 표는 하루치로 충분하다.
    "krx_etf_daily":      ("day", None),
    "krx_etf_list":       ("day", None),
    "ace_etf":            ("all", None),
    "kodex_etf":          ("all", None),
    "plus_etf":           ("all", None),
    "sol_etf":            ("all", None),

    # --- 코드 수확 대상. 아래층 뷰가 읽히는지와 함께 본다 -------------------
    "ace_etf_pdf":        ("day", None),
    "kodex_etf_pdf":      ("day", None),
    "plus_etf_pdf":       ("day", None),
    "sol_etf_pdf":        ("all", None),
    "amc_etf_pdf":        ("day", None),
    "ksd_kacd_code":      ("all", None),

    # --- 나머지 결과 표. 뷰·내보내기·dedup 이 볼 것이 있으면 된다 ----------
    "kis_index_daily":    ("all", None),
    "kis_futopt_daily":   ("day", None),
    "kis_futopt_price":   ("day", None),
    "kis_futopt_chart":   ("day", None),
    "kis_etf":            ("day", None),
    "kis_etf_pdf":        ("day", None),
    "fo_idx_code_mst":    ("top", 5_000),
    "krx_fut_daily":      ("day", None),
    "krx_opt_daily":      ("day", None),
    "krx_etn_daily":      ("day", None),
    "krx_index_daily":    ("day", None),
    "krx_stock_daily":    ("day", None),
    "krx_etf_pdf":        ("day", None),
    "krx_deriv_hist":     ("top", 5_000),
    "ksd_bond_credit":    ("top", 5_000),
    "ksd_bond_int":       ("top", 5_000),
    "ksd_bond_prin":      ("top", 5_000),
    "ksd_bond_redem":     ("top", 5_000),
    "ksd_bond_redem_his": ("all", None),
    "ksd_bond_stopt":     ("all", None),
    "ksd_bond_stopt_his": ("all", None),
    "ksd_bond_stopt_prc": ("all", None),
    "ksd_bond_yield":     ("all", None),
    "ksd_bond_info_api":  ("all", None),
    "ksd_bond_int_api":   ("all", None),
}

# 날짜 컬럼을 손으로 지정하는 표. _day_column 은 **컬럼 순서상 처음** 오는
# 날짜꼴 이름을 고르는데, 후보가 둘일 때 그게 맞는다는 보장이 없다.
#
# 실측으로 후보가 둘인 표가 넷이고 셋은 맞게 골랐다(kis_futopt_price 는
# trade_at 을 futs_last_tr_date 보다, krx_etf_list 는 bas_dd 를 list_dd 보다).
# 틀린 하나가 ksd_bond_isin 이다 -- issu_dt 는 **채권의 발행일**이고 수집일이
# 아니다. 최신 발행일 하루를 뜨면 10행이 나온다(그날 발행된 채권 수).
# 수집 스탬프는 trade_at 이고, 그걸로 뜨면 29,473행 = 그날 받은 전체 목록이다.
DAY_COL: dict[str, str] = {
    "ksd_bond_isin": "TRADE_AT",
}

# 복사에서 빼는 컬럼. id 는 양쪽 IDENTITY 가 따로 돌고, updated_at 은 적재
# 시각이라 v2 의 기본값이 맞다.
SKIP = {"ID", "UPDATED_AT"}

# 날짜처럼 생긴 컬럼 이름. 수확 프로시저와 같은 관례를 쓴다 (docs 6.5).
_DAY_SUFFIX_3 = ("_DD", "_DT", "_AT")
# "YMD" 를 밑줄 없이도 받는다 -- kodex 의 gijunymd 가 그 모양이다.
_DAY_SUFFIX_4 = ("DATE", "_YMD", "YYMM")
_DAY_SUFFIX_3 = _DAY_SUFFIX_3 + ("YMD",)


def _connect(which: str):
    suffix = {"v2": "", "v1": "_V1"}[which]
    user = os.environ.get("ORACLE_USER" + suffix)
    if not user:
        raise SystemExit(f"ORACLE_USER{suffix} 가 .env 에 없다")
    wallet = os.environ.get("ORACLE_WALLET_DIR" + suffix) or os.environ["ORACLE_WALLET_DIR"]
    return oracledb.connect(
        user=user,
        password=os.environ["ORACLE_PASSWORD" + suffix],
        dsn=os.environ.get("ORACLE_DSN" + suffix) or os.environ["ORACLE_DSN"],
        config_dir=wallet, wallet_location=wallet,
        wallet_password=(os.environ.get("ORACLE_WALLET_PASSWORD" + suffix)
                         or os.environ["ORACLE_WALLET_PASSWORD"]))


def _columns(cur, table: str) -> list[str]:
    cur.execute("""SELECT column_name FROM user_tab_columns
                    WHERE table_name = :1 ORDER BY column_id""", [table.upper()])
    return [r[0] for r in cur.fetchall()]


def _col_width(cur, table: str, column: str) -> int:
    cur.execute("""SELECT NVL(data_length, 0) FROM user_tab_columns
                    WHERE table_name = :1 AND column_name = :2""",
                [table.upper(), column.upper()])
    row = cur.fetchone()
    return row[0] if row else 0


def _day_expr(cur, table: str, column: str) -> str:
    """하루를 가리키는 식. 초까지 적는 컬럼은 앞 8자리로 자른다.

    kis 쪽 스냅샷 API 는 조회일을 받지도 돌려주지도 않아 관측 시각을
    YYYYMMDDHHMMSS 로 적는다(snap_at, trade_at). 그걸 그대로 MAX 하면 하루가
    아니라 **한 틱**이 나온다 -- 처음 돌렸을 때 kis_etf 가 3행이었다."""
    return f"SUBSTR({column}, 1, 8)" if _col_width(cur, table, column) > 8 else column


def _day_column(columns: list[str], table: str | None = None) -> str | None:
    """이 표에서 '거래일' 로 쓸 컬럼. 이름 관례로 찾는다.

    fn_export_day_col 을 쓰지 않는 이유: 그 함수는 내보내기 대상 10개만 알고,
    여기서는 43개 전부가 필요하다. 식(SUBSTR, REPLACE)을 돌려주는 경우도 있어
    컬럼 이름으로 쓸 수 없다. 후보가 둘인 표는 DAY_COL 로 지정한다."""
    if table and table.lower() in DAY_COL:
        return DAY_COL[table.lower()]
    for name in columns:
        if name in SKIP or name in ("API_ID", "JOB_ID"):
            continue
        if name.endswith(_DAY_SUFFIX_3) or name.endswith(_DAY_SUFFIX_4):
            return name
    return None


def _select_sql(cur, table: str, columns: list[str], how: str, arg: Any,
                day_col: str | None) -> tuple[str, str]:
    """(SQL, 설명). 설명은 dry-run 출력에 쓴다."""
    cols = ", ".join(c for c in columns if c not in SKIP)
    if how == "all":
        return f"SELECT {cols} FROM {table}", "전부"
    if how == "top":
        return f"SELECT {cols} FROM {table} WHERE ROWNUM <= {int(arg)}", f"앞 {arg:,}행"
    if how == "join":
        other, key = arg
        return (f"SELECT {cols} FROM {table} t"
                f" WHERE EXISTS (SELECT 1 FROM {other} o WHERE o.{key} = t.{key})",
                f"{other}.{key} 에 있는 것만")
    if how == "day":
        if day_col is None:
            return f"SELECT {cols} FROM {table} WHERE ROWNUM <= 5000", "날짜컬럼 없음 -> 앞 5,000행"
        expr = _day_expr(cur, table, day_col)
        return (f"SELECT {cols} FROM {table}"
                f" WHERE {expr} = (SELECT MAX({expr}) FROM {table})",
                f"{day_col.lower()} 최신 하루")
    if how == "day_join":
        # 하루치 + 다른 표에 있는 키만. ksd_bond_isin 이 그 경우다 -- 매일 전체
        # 목록을 덧붙이는 표라, ISIN 으로만 좁히면 모든 날짜가 걸린다(실측:
        # 1,882,422행).
        other, key = arg
        expr = _day_expr(cur, table, day_col) if day_col else None
        where = [f"EXISTS (SELECT 1 FROM {other} o WHERE o.{key} = t.{key})"]
        if expr:
            where.append(f"{expr} = (SELECT MAX({expr}) FROM {table})")
        return (f"SELECT {cols} FROM {table} t WHERE " + " AND ".join(where),
                f"{(day_col or '?').lower()} 최신 하루 + {other}.{key} 에 있는 것만")
    raise SystemExit(f"{table}: 모르는 방식 {how!r}")


def _copy_typed(v1, v2, table: str, how: str, arg: Any, apply: bool) -> tuple[int, str]:
    r1, r2 = v1.cursor(), v2.cursor()
    cols = [c for c in _columns(r1, table) if c not in SKIP]
    cols_v2 = set(_columns(r2, table))
    missing = [c for c in cols if c not in cols_v2]
    if missing:
        return 0, f"건너뜀 -- v2 에 없는 컬럼 {missing}"
    sql, note = _select_sql(r1, table, cols + list(SKIP), how, arg, _day_column(cols, table))
    if not apply:
        r1.execute(f"SELECT COUNT(*) FROM ({sql})")
        return r1.fetchone()[0], note

    r1.arraysize = 5000
    r1.execute(sql)
    placeholders = ", ".join(f":{i+1}" for i in range(len(cols)))
    insert = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
    total = 0
    while True:
        rows = r1.fetchmany(5000)
        if not rows:
            break
        r2.executemany(insert, rows)
        total += len(rows)
    v2.commit()
    return total, note


def _copy_generic(v1, v2, table: str, how: str, arg: Any, apply: bool) -> tuple[int, str]:
    """아래층으로 내린 표. 행을 api_rst 의 result_json 으로 접는다.

    엔진과 같은 모양이어야 v_* 뷰가 읽는다 -- 키를 소문자로 접고, 스탬프된
    값과 레코드를 한 객체에 합친다 (app.scrapers.base._fold_keys). v1 의 행은
    이미 그 둘이 컬럼으로 합쳐져 있으므로, id/updated_at 만 떼면 된다."""
    r1, r2 = v1.cursor(), v2.cursor()
    cols = [c for c in _columns(r1, table) if c not in SKIP]
    sql, note = _select_sql(r1, table, cols + list(SKIP), how, arg, _day_column(cols, table))
    if not apply:
        r1.execute(f"SELECT COUNT(*) FROM ({sql})")
        return r1.fetchone()[0], note + " -> api_rst"

    r1.arraysize = 5000
    r1.execute(sql)
    idx = {c: i for i, c in enumerate(cols)}
    payload_cols = [c for c in cols if c not in ("API_ID", "JOB_ID")]
    insert = ("INSERT INTO api_rst (api_id, job_id, result_json) "
              "VALUES (:1, :2, :3)")
    total = 0
    while True:
        rows = r1.fetchmany(5000)
        if not rows:
            break
        batch = []
        for row in rows:
            record = {c.lower(): row[idx[c]] for c in payload_cols
                      if row[idx[c]] is not None}
            batch.append((row[idx["API_ID"]], row[idx["JOB_ID"]],
                          json.dumps(record, ensure_ascii=False, default=str)))
        r2.setinputsizes(None, None, oracledb.DB_TYPE_JSON)
        # 문자열을 DB_TYPE_JSON 으로 바인딩하면 JSON **스칼라**로 들어간다
        # (1절 측정에서 밟은 함정). 객체로 넣어야 하므로 파싱해서 준다.
        r2.executemany(insert, [(a, b, json.loads(c)) for a, b, c in batch])
        total += len(batch)
    v2.commit()
    return total, note + " -> api_rst"


def main() -> int:
    load_dotenv(".env")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="v2 에 실제로 적재한다")
    parser.add_argument("--only", help="이 표만 (쉼표로 여러 개)")
    args = parser.parse_args()

    wanted = set(args.only.split(",")) if args.only else set(PLAN)
    v1, v2 = _connect("v1"), _connect("v2")

    print(f"{'표':24} {'행':>10}  방식")
    grand = 0
    for table in sorted(PLAN):
        if table not in wanted:
            continue
        how, arg = PLAN[table]
        copier = _copy_generic if table in TO_GENERIC else _copy_typed
        n, note = copier(v1, v2, table, how, arg, args.apply)
        grand += n
        print(f"  {table:22} {n:>10,}  {note}")
    print(f"\n합계 {grand:,}행" + ("" if args.apply else "  (dry-run -- 적재하려면 --apply)"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
