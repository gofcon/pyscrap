"""사전 데이터 -- 종목·만기·지수가 무엇인가. 엔진은 읽기만 한다."""

import re
from datetime import date, datetime
from typing import Any, Optional

from pydantic import ConfigDict, model_validator
from sqlalchemy import CheckConstraint, Column, Date, DateTime, Identity, Integer, String, func
from sqlmodel import Field, SQLModel

from app.db.types import JSONText, OracleBoolean, XMLText
from app.db.models.base import (
    EtfPdfDateMixin,
    MdcRecordBase,
    ReferenceTable,
    VendorRecordBase,
)




class StockIndexHis(ReferenceTable, SQLModel, table=True):
    """Daily OHLC/volume history for a stock index, one row per index per
    trading day -- ``mv_id`` names which index (KOSPI200, KOSDAQ150, ...), so
    a single table covers all of them rather than one table per index.

    Unlike KisFutoptChart/KisFutoptPrice, whose date/time fields are kept as
    strings (fixed format, no DB-side date math needed), ``trade_date`` here
    is a real DATE: a history table's whole purpose is range queries
    (BETWEEN, last N sessions, joins against other daily series), and doing
    those against a YYYYMMDD string means either a TO_DATE on every row or
    lexicographic comparisons that quietly break the moment a format changes.
    Converting the source's own YYYYMMDD text is the loading procedure's job
    (TO_DATE(..., 'YYYYMMDD')) -- note mst_fuopt.mat_date is that raw text
    form, so joining the two needs the conversion spelled out.

    Keyed on its own data rather than on an id/api_id/job_id trio, because
    this is not a scrape target: it is maintained DB-side by procedures. The
    absence of job_id is what states that -- it keeps the table out of
    TABLE_REGISTRY (see app.services.export._discover_table_registry), so an
    output_tables_json pointed here fails loudly instead of half-working.
    The natural key is also what keeps a settled daily series from
    double-counting: re-inserting a stored (date, index) collides rather
    than duplicating."""

    __tablename__ = "stock_index_his"

    # Composite natural key (trade_date, mv_id): one row per index per
    # session. primary_key goes on the Column rather than on Field() --
    # SQLModel rejects primary_key=True and sa_column= together, and a real
    # DATE column needs the sa_column form. Neither may be Optional: a
    # primary key column is NOT NULL by definition.
    trade_date: date = Field(sa_column=Column(Date, primary_key=True))
    # Index identifier, e.g. the KRX index code the row belongs to. Indexed
    # separately from the primary key because every read of this table is
    # "this index, over this date range" -- mv_id is the key's *trailing*
    # column, which the PK index alone can't serve a lookup on.
    mv_id: str = Field(sa_column=Column(String(20), primary_key=True, index=True))

    close_price: Optional[float] = Field(default=None)      # 종가
    price_change: Optional[float] = Field(default=None)           # 전일대비
    change_rate: Optional[float] = Field(default=None)      # 전일대비율(%)
    open_price: Optional[float] = Field(default=None)       # 시가
    high_price: Optional[float] = Field(default=None)       # 고가
    low_price: Optional[float] = Field(default=None)        # 저가
    volume: Optional[int] = Field(default=None)             # 거래량
    trading_value: Optional[int] = Field(default=None)      # 거래대금
    listed_market_cap: Optional[float] = Field(default=None)  # 상장시가총액

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class MetaFuoptInfo(ReferenceTable, SQLModel, table=True):
    """What each product family is written on: one row per (``prod_type``,
    ``ul_code``), prod_type being the vocabulary MetaMaturity and MstFuopt
    key on, so a contract reaches its underlying and its multiplier the way
    it reaches its expiry. The key is the pair, not prod_type alone, because
    an index family has one underlying while a single-stock family has
    hundreds -- the day stock futures and options are collected, the same
    prod_type will carry one row per stock, each with its own multiplier.
    For a family with a single underlying, sp_mst_fuopt_sync stamps the
    master by prod_type; for the others the underlying must come with the
    contract from its listing, and this table supplies the name and the
    multiplier for the pair. Until this table the underlying lived on
    mst_fuopt as ul_code/ul_nm, filled from the KIS master per contract,
    which meant one fact (the KOSPI200 family is written on KOSPI200)
    repeated 60,000 times and missing on the 43,000 rows the KIS master
    never listed.

    META_ names what is managed outside the system: like MetaMaturity it
    carries no api_id/job_id/id, which keeps it out of TABLE_REGISTRY and
    the scraping engine's reach. Four rows, entered by hand, read by
    sp_mst_fuopt_sync to stamp the master.

    ``code_key`` is what ties a contract to its row: the second and third
    characters of its short code, which name the product-and-underlying in
    both the exchange's and KIS's code -- 01 for KOSPI200 monthlies, 05
    mini, 09 and AF the two weeklies, 11 Samsung Electronics options -- and
    which no two of the 64 listed stock families share. The sync procedures
    join on it, so a new family is one row here and no change in SQL. The
    exchange's prodId is not enough for that: KRDRVOPEQU names every stock
    option of every underlying. ``krx_prod_ids`` and ``kis_info_types``
    record how the two listings spell the family, for the reader.

    ``exp_dow`` and ``exp_week`` are the family's expiry rule -- the weekday
    it expires on and, for a monthly, which one of the month (the second
    Thursday); a weekly's week number is in its own code, so exp_week is
    NULL there. sp_meta_maturity_sync derives a date from these when
    neither listing knows one. Whether a code is weekly is not a column:
    the code says so itself (2609W3, and a W in KIS's sixth position).

    ``krx_idx_nm`` and ``mv_id`` are the underlying's names in the two spot
    series (krx_index_daily, stock_index_his), so that a join from a contract
    to its spot needs no string literal. ``cont_mult`` is per product type
    because it is one: the mini family is 50,000, the rest 250,000."""
    __tablename__ = "meta_fuopt_info"

    prod_type: str = Field(primary_key=True, max_length=20)
    ul_code: str = Field(primary_key=True, max_length=20)                # 기초자산 코드 (KIS unas_short_code)
    ul_nm: str = Field(max_length=100)                                   # 기초자산명
    krx_idx_nm: Optional[str] = Field(default=None, max_length=100)      # krx_index_daily.idx_nm (지수 계열만)
    mv_id: Optional[str] = Field(default=None, max_length=20)            # stock_index_his.mv_id
    cont_mult: float = Field()                                           # 거래승수
    code_key: str = Field(unique=True, max_length=2)                     # 단축코드 2~3번째 글자 -- 계열×기초자산의 코드
    exp_dow: str = Field(max_length=9)                                   # 만기 요일 (THURSDAY / MONDAY)
    exp_week: Optional[int] = Field(default=None)                        # 월물: 그 달의 몇째 exp_dow (2); 위클리: NULL (코드의 n)
    krx_prod_ids: Optional[str] = Field(default=None, max_length=100)    # 이 계열의 거래소 prodId 들 (쉼표 구분)
    kis_info_types: Optional[str] = Field(default=None, max_length=100)  # 이 계열의 KIS 마스터 info_type 들 (쉼표 구분)
    description: Optional[str] = Field(default=None, max_length=100)
    updated_at: Optional[datetime] = Field(default=None,
                                           sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class MetaMaturity(ReferenceTable, SQLModel, table=True):
    """Contract expiry calendar: one row per maturity of one product type.

    Reference data rather than scraped output, so unlike the result tables it
    carries no api_id/job_id/id -- (prod_type, mat_code) is the natural key
    the source already provides, and a row's identity is that pair, not the
    run that happened to write it. Populated DB-side by a procedure; having
    no job_id is what keeps it out of TABLE_REGISTRY and therefore out of the
    scraping engine's reach (see
    app.services.export._discover_table_registry).

    ``mat_code`` alone is NOT unique, which is why ``prod_type`` leads the
    key: the weekly series reuse each other's codes, so e.g. "2308W1" names a
    Thursday-expiring contract and a Monday-expiring one that mature four
    days apart (verified against the source master -- 99 codes collide that
    way). Sharing ``prod_type``'s name and width with MstFuopt is deliberate:
    the two join on it.

    ``prev_mat_date`` sitting alongside ``mat_date`` is what makes a maturity
    row self-contained: "the period this contract covers" is answerable from
    one row, with no self-join or window function against the rest of the
    calendar."""

    __tablename__ = "meta_maturity"

    # Leads the composite primary key -- declaration order is the key's
    # column order, and "every maturity of this product type" is the lookup
    # this table exists to serve.
    prod_type: str = Field(primary_key=True, max_length=20)
    mat_code: str = Field(primary_key=True, max_length=20)
    mat_date: Optional[date] = Field(default=None, sa_column=Column(Date, index=True))
    prev_mat_date: Optional[date] = Field(default=None, sa_column=Column(Date))
    mat_scd: Optional[str] = Field(default=None, index=True, max_length=20)  # 만기단축코드 ( 'E3' , 'E4')
    description: Optional[str] = Field(default=None, max_length=100)
    updated_at: Optional[datetime] = Field(default=None,
                                           sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class MstFuopt(ReferenceTable, SQLModel, table=True):
    """Curated futures/options instrument master -- the list of contracts
    actually worth polling, as opposed to FoIdxCodeMst, which is the raw
    exchange master file reloaded verbatim every morning.

    Reference data like MetaMaturity -- derived DB-side by a procedure, not
    written by the scraping engine, and kept out of TABLE_REGISTRY by having
    no job_id (see app.services.export._discover_table_registry). So no
    id/api_id/job_id: ``short_cd`` is the natural key, and it is the same
    code the *exchange* issues -- KIS spells the same contract differently, so
    joining to FoIdxCodeMst/KisFutoptPrice goes through ``kis_short_cd``.
    ``is_active`` is what makes it a *filter* rather than a copy -- a contract
    drops out of polling by being flipped to 'N' here, which is also how a
    matured contract stops being polled without deleting the history that
    references it.

    Note the deliberate width difference from FoIdxCodeMst.short_code
    (VARCHAR2(20)): 10 is enough for every KIS code seen so far (9 chars,
    e.g. 'B01609335'), and this column being the narrower of the two means an
    over-long code fails here on insert rather than silently becoming a row
    that never matches anything."""

    __tablename__ = "mst_fuopt"

    short_code: str = Field(primary_key=True, max_length=10)
    # 거래소가 발급한 코드가 아니라 KIS 가 자기 마스터에 쓰는 표기. 두 코드는
    # 만기 칸의 자릿수만 다르다 -- KIS 는 월을 두 자리로(202609 -> '609'),
    # 위클리는 뒤에 'W' 를 붙이고, 선물은 행사가 자리가 아예 없다:
    #   월물   KRX B0169335 <-> KIS B01609335
    #   위클리 KRX B09FB892 <-> KIS B09FBW892
    #   선물   KRX A0166000 <-> KIS A01606
    # KIS 로 나가는 요청은 전부 이 값을 쓰고(빌더들의 SELECT f.kis_short_cd),
    # kis_futopt_chart/price/daily 의 short_code 도 이 코드다. 거래소 기준으로
    # 옮기면서 둘을 갈랐다 -- 과거 종목까지 KRX 가 목록으로 주므로 코드를
    # 추론할 필요가 없어졌기 때문이다 (app.db.models.KrxDerivHist 참고).
    kis_short_cd: Optional[str] = Field(default=None, index=True, max_length=10)
    prod_nm: str = Field(max_length=100)                                  # 종목명
    prod_type: str = Field(max_length=20)                                 # 상품구분 (지수월물, 위클리목, 위클리월, 지수미니)
    call_put_cd :str = Field(max_length=20)                               # 콜/풋/선물구분 (  CALL/PUT/FUT )

    ul_code: Optional[str] = Field(default=None, index=True, max_length=20)  # 기초자산 코드
    ul_nm: Optional[str] = Field(default=None, max_length=100)            # 기초자산명
    cont_mult: float = Field()                                            # 거래승수
    mat_code: Optional[str] = Field(default=None, max_length=20)          # 만기코드

    # Kept as text (YYYYMMDD), matching the string date columns on
    # KisFutoptChart/KisFutoptPrice rather than the real DATE on MetaMaturity
    # /StockIndexHis. Worth knowing when joining: meta_maturity.mat_date is a
    # DATE, so `mst_fuopt.mat_date = meta_maturity.mat_date` will not compare
    # -- one side needs TO_DATE(m.mat_date, 'YYYYMMDD') (or TO_CHAR on the
    # other) for that join to work.
    mat_date: Optional[date] = Field(default=None, sa_column=Column(Date, index=True))          # 만기일자
    front_date: Optional[date] = Field(default=None, sa_column=Column(Date, index=True))        # 근월물 시작일자
    strike_prc: Optional[float] = Field(default=None)                     # 행사가

    description: Optional[str] = Field(default=None, max_length=100)
    # NOT NULL with no server-side default, so every writer must set it
    # explicitly -- unlike every other updated_at in this module, which the
    # DB fills and maintains on its own (server_default/onupdate). Text
    # (YYYYMMDDHH24MISS) rather than DateTime, per this table's own DDL.
    updated_at: Optional[datetime] = Field(default=None,
                                           sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class MstIndex(ReferenceTable, SQLModel, table=True):
    """Curated index master -- the indices this project actually follows, one
    row per internal id.

    Reference data like MstEtf/MstFuopt: no job_id, so it stays out of
    TABLE_REGISTRY, and it is maintained by hand rather than by a job.

    ``mv_id`` is the key because this table is the *common* one: it is what
    stock_index_his and v_k2i_atm are keyed by, and what anything downstream
    means when it says "the index". A source's own identifiers stay in that
    source's master -- KRX numbers an index by a (family, number) pair, which
    lives in krx_index_mst along with the mv_id it maps to.

    So the two tables divide as sources and vocabulary do: krx_index_mst
    holds all 168 indices the exchange publishes, this holds the handful that
    have an internal name."""

    __tablename__ = "mst_index"

    mv_id: str = Field(primary_key=True, max_length=10)                # 내부 지수 id (K2I, KOSPI ...)

    idx_nm: str = Field(index=True, max_length=100)                    # 지수명 -- 시세는 이름으로만 온다
    idx_eng_nm: Optional[str] = Field(default=None, max_length=150)    # 영문 지수명
    bas_tm: Optional[str] = Field(default=None, max_length=10)         # 기준시점
    annc_tm: Optional[str] = Field(default=None, max_length=10)        # 발표일
    bas_idx: Optional[float] = Field(default=None)                     # 기준지수

    # KIS 가 같은 지수를 부르는 이름 (FID_INPUT_ISCD). 코스피200 이 '2001' 인
    # 것을 29일치 OHLC 전건 대조로 확인했다 -- 코스피/코스피100 과는 한 칸도
    # 맞지 않는다. 시세는 이제 거래소에서 받지만(sp_stock_index_his_sync),
    # kis_index_daily 로 들어온 과거분과 잇는 열쇠는 이 값뿐이다.
    kis_short_cd: Optional[str] = Field(default=None, index=True, max_length=10)

    description: Optional[str] = Field(default=None, max_length=200)
    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KrxIndexMst(ReferenceTable, SQLModel, table=True):
    """KRX's own index master -- every index the exchange publishes, as it
    numbers them (전체지수 기본정보, MDCSTAT00401).

    Reference data, maintained by hand: 168 rows that change only when the
    exchange adds or retires an index. An index is identified by the pair --
    ``ind_tp_cd`` names the family, ``idx_ind_cd`` numbers it within that
    family -- and neither is unique alone.

    This is also where the exchange's vocabulary meets ours: ``mv_id`` is
    filled on the indices that have an internal name, and MstIndex is keyed
    by that. Left NULL on the rest, which is most of them.

    Price feeds carry neither code -- the open API (``idx/kospi_dd_trd``) and
    the screen (MDCSTAT00101) both name an index and stop -- so ``idx_nm`` is
    what a price row joins on."""

    __tablename__ = "krx_index_mst"

    ind_tp_cd: str = Field(primary_key=True, max_length=4)             # 계열코드
    idx_ind_cd: str = Field(primary_key=True, max_length=4)            # 지수코드

    idx_nm: str = Field(index=True, max_length=100)                    # 지수명
    idx_eng_nm: Optional[str] = Field(default=None, max_length=150)    # 영문 지수명
    mid_clss_cd: Optional[str] = Field(default=None, max_length=2)     # 화면의 계열 구분 (01~04)

    bas_tm: Optional[str] = Field(default=None, max_length=10)         # 기준시점
    annc_tm: Optional[str] = Field(default=None, max_length=10)        # 발표일
    bas_idx: Optional[float] = Field(default=None)                     # 기준지수
    calc_cycle: Optional[str] = Field(default=None, max_length=20)     # 산출주기
    calc_tm: Optional[str] = Field(default=None, max_length=40)        # 산출시간
    compst_isu_cnt: Optional[int] = Field(default=None)                # 구성종목수

    mv_id: Optional[str] = Field(default=None, index=True, max_length=10)  # 내부 지수 id (mst_index)

    description: Optional[str] = Field(default=None, max_length=200)
    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class MstBond(ReferenceTable, SQLModel, table=True):
    """Curated bond instrument master -- the list of issues worth collecting
    detail for, as opposed to KsdBondIsin, which is the raw SEIBRO listing
    appended in full every day.

    Reference data like MstFuopt: derived DB-side by sp_mst_bond_sync, not
    written by the scraping engine, and kept out of TABLE_REGISTRY by having
    no job_id (see app.services.export._discover_table_registry). So no
    id/api_id/job_id -- ``isin`` is the natural key, and it is the same code
    ksd_bond_isin.isin and every ksd_bond_* detail table use, so this joins
    straight to all of them.

    Insert-only, the same way mst_fuopt is. The procedure adds ISINs it has
    not seen before and leaves existing rows alone, so a value edited here by
    hand survives the next run. That is also what makes ``first_seen``
    meaningful: it is the snapshot day a bond first appeared in the listing,
    which is the only evidence of a new issue there is -- SEIBRO's response
    says nothing about whether a bond is new, so it can only be read as the
    difference between two days' listings.

    A delisted bond is not removed. It simply stops appearing in new
    ksd_bond_isin snapshots while its row stays here, which keeps the detail
    already collected against it referencable."""

    __tablename__ = "mst_bond"

    isin: str = Field(primary_key=True, max_length=12)                       # 표준코드
    kor_secn_nm: Optional[str] = Field(default=None, max_length=200)         # 종목명
    secn_kacd: Optional[str] = Field(default=None, index=True, max_length=10)   # 종류코드
    codevalue_nm: Optional[str] = Field(default=None, max_length=100)        # 대분류명 (국채/특수채)
    issuco_custno: Optional[str] = Field(default=None, max_length=20)        # 발행회사 고객번호
    issu_dt: Optional[str] = Field(default=None, index=True, max_length=8)   # 발행일

    # 이 종목이 처음 목록에 나타난 스냅샷 일자 (YYYYMMDD). 신규 상장 여부는
    # 이 값으로만 알 수 있고, 상세 수집 순서를 정하는 축이기도 하다.
    first_seen: Optional[str] = Field(default=None, index=True, max_length=8)

    description: Optional[str] = Field(default=None, max_length=100)
    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class MstStock(ReferenceTable, SQLModel, table=True):
    """Curated share instrument master -- every share the exchange has listed
    since 2010, one row each, including the ones that no longer trade.

    Reference data like MstEtf and MstFuopt: derived DB-side by
    sp_mst_stock_sync from krx_stock_base, not written by the scraping
    engine, and kept out of TABLE_REGISTRY by having no job_id.

    **Column names are the internal system's, not the exchange's.** The
    downstream systems that read the mst_* tables have their own vocabulary
    -- short_code, prod_nm, list_dt -- and these masters answer to it, which
    is what makes them worth having as a layer at all. The raw feed's own
    names stay in krx_stock_base; sp_mst_stock_sync is where the two
    vocabularies meet, and it is the only place the mapping is written down.

    The key crossing over is part of that. KRX names the ISIN ``isu_cd`` and
    the short code ``isu_srt_cd``; here the short code is ``short_code`` and
    the ISIN is ``isin``, each under the name that says what it is.

    Accumulates and is never emptied, which is the point: the exchange's
    per-day listing only shows what was listed *that* day, so a share that
    delisted in 2014 exists in exactly one place -- an old day's response.
    The backfill walks backwards through those days and each new code lands
    here with the last state it was ever published in.

    ``last_seen_dt`` is that basis date, and doubles as the listed/delisted
    signal: a row whose ``last_seen_dt`` is the most recent trading day is
    still listed, and one that stopped years ago is not. Recorded rather than
    inferred -- KRX publishes no delisting date on this feed.

    It also decides what an update may overwrite. The backfill runs newest to
    oldest, so most rows it offers are *older* than what is already here; the
    sync only takes a row whose ``bas_dd`` is newer, and an old day can add a
    share but never rewrite a current one. ``description`` is left alone
    either way -- it is the hand-edited column, the same arrangement as
    MstEtf's manager codes."""

    __tablename__ = "mst_stock"

    short_code: str = Field(primary_key=True, max_length=20)             # 단축코드
    isin: Optional[str] = Field(default=None, index=True, max_length=12)  # ISIN
    prod_nm: Optional[str] = Field(default=None, max_length=100)         # 종목명
    prod_snm: Optional[str] = Field(default=None, max_length=60)         # 종목단축명
    prod_enm: Optional[str] = Field(default=None, max_length=120)        # 종목영문명

    list_dt: Optional[str] = Field(default=None, max_length=8)           # 상장일
    mkt_div: Optional[str] = Field(default=None, index=True, max_length=20)   # 시장구분
    secu_grp: Optional[str] = Field(default=None, max_length=40)         # 증권그룹
    secu_type: Optional[str] = Field(default=None, max_length=40)        # 증권유형
    stock_kind_type: Optional[str] = Field(default=None, max_length=20)  # 종류주식
    face_amt: Optional[str] = Field(default=None, max_length=20)         # 액면가 ('무액면' 이 온다)
    list_cnt: Optional[int] = Field(default=None)                        # 상장주수

    # 마지막으로 거래소 목록에 있던 기준일. 상장/폐지 여부가 여기서 읽힌다.
    last_seen_dt: Optional[str] = Field(default=None, index=True, max_length=8)

    description: Optional[str] = Field(default=None, max_length=200)     # 설명
    updated_at: Optional[datetime] = Field(default=None,                 # 수정일시
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class MstEtf(ReferenceTable, SQLModel, table=True):
    """Curated ETF instrument master -- the universe with each manager's own
    lookup code beside it.

    Reference data like MstFuopt and MstBond: derived DB-side by
    sp_mst_etf_sync, not written by the scraping engine, and kept out of
    TABLE_REGISTRY by having no job_id.

    Filled from krx_etf_list -- the exchange's own ETF listing, which
    carries the fund's facts as well as its codes: what index it tracks and
    how (``trace_mtd`` says 일반 / 2X 레버리지 / 1X 인버스), whether it holds
    the assets or a swap (``replica_mtd``), its CU size, its total fee, how
    it is taxed. The earlier source, krx_etf_daily, has prices and a name and
    nothing else, so it stays only as the fallback that keeps a delisted
    fund's row from vanishing when the listing stops showing it.

    ``amc`` is the exchange's manager name (삼성자산운용), not a brand key.
    ``amc_etf_cd`` is the one column no exchange feed can supply, and it is
    why this table exists: the holdings endpoints are keyed on codes nobody
    else uses. KODEX asks for ``2ETF01``, SOL for ``211096``, PLUS for ``006184``,
    and none of those appear anywhere in KRX's data. Each manager's product
    list carries the KRX short code beside its own, so folding the lists in
    here turns "which instruments does this manager have" into one column and
    lets every holdings builder select from the same place.

    Column names are the internal system's rather than the exchange's, as
    in MstStock -- the mst_* layer exists to speak the downstream systems'
    vocabulary, and sp_mst_etf_sync is where that mapping is written down.

    ``short_code`` is the key rather than ``isin`` because no source collected
    today publishes an ETF's ISIN -- krx_etf_daily has no such column, PLUS's
    ``krfundcd`` is null on all 84 rows, and the rest give the short code
    alone. It could be *derived* (KR7 + code + check digit) but that would be
    inventing an identifier rather than recording one. ``isin`` stays here,
    nullable, for whenever a source turns up; the sync fills it then without
    anything else moving.

    Insert-only for the universe and NULL-fill for the manager columns, the
    same shape as sp_mst_fuopt_sync: a value edited here by hand survives the
    next run, and an ETF whose manager list arrives later still gets its code
    filled in.

    Only four managers are filled by the sync -- KODEX, SOL, PLUS and ACE,
    the ones that publish a product list as JSON. The other five (TIGER, KB,
    NH, Kiwoom, TIME) publish theirs as a page whose codes live in link
    hrefs, in data- attributes, or inside a sentence in a cell, and reading
    those would mean teaching the engine to walk a DOM rather than read a
    table. Their rows are filled **by hand** instead, which is affordable
    because an ETF's code at its manager is set once at listing and does not
    change afterwards; only newly listed instruments ever need adding.
    ``amc_etf_cd IS NULL`` is the list of what is still missing -- ``amc``
    itself now comes from the exchange and is never blank."""

    __tablename__ = "mst_etf"

    short_code: str = Field(primary_key=True, max_length=20)             # 단축코드
    isin: Optional[str] = Field(default=None, index=True, max_length=12)  # ISIN
    prod_nm: Optional[str] = Field(default=None, max_length=200)         # 종목명
    prod_snm: Optional[str] = Field(default=None, max_length=100)        # 종목단축명
    prod_enm: Optional[str] = Field(default=None, max_length=200)        # 종목영문명
    list_dt: Optional[str] = Field(default=None, max_length=8)           # 상장일

    # 이 펀드가 무엇을 어떻게 따라가는가. 전부 거래소 목록에서 온다.
    target_idx_nm: Optional[str] = Field(default=None, max_length=200)   # 타겟 지수명
    trace_mtd: Optional[str] = Field(default=None, max_length=20)        # 추종 방법: 일반 / 2X 레버리지 / 1X 인버스
    replica_mtd: Optional[str] = Field(default=None, max_length=40)      # ETF 복제방법: 실물(패시브) / 합성(액티브)
    mkt_div: Optional[str] = Field(default=None, max_length=20)          # 시장구분: 국내 / 해외 / 국내&해외
    idx_asst_class: Optional[str] = Field(default=None, max_length=20)   # 추종 자산클래스: 주식 / 채권 / 원자재

    # 운용사와, 그 운용사가 이 ETF 에 부여한 코드. 구성종목 빌더는 이 둘만 본다.
    # 출처가 다르다: amc 는 거래소가 적은 운용사명이고, amc_etf_cd 는 그 운용사
    # 자신의 목록에서만 얻을 수 있다 -- 거래소 데이터 어디에도 없는 값이다.
    amc: Optional[str] = Field(default=None, index=True, max_length=40)   # 운용사: 삼성자산운용 / 미래에셋자산운용
    amc_etf_cd: Optional[str] = Field(default=None, max_length=30)        # 운용사 ETF 코드: 2ETF01 / 211096

    cu_qty: Optional[int] = Field(default=None)                          # CU당주수
    list_cnt: Optional[int] = Field(default=None)                        # 상장주수
    etf_tot_fee: Optional[float] = Field(default=None)                   # 총수수료
    tax_div: Optional[str] = Field(default=None, max_length=40)          # 세금구분

    first_seen: Optional[str] = Field(default=None, index=True, max_length=8)  # 최초관찰일

    description: Optional[str] = Field(default=None, max_length=100)     # 설명
    updated_at: Optional[datetime] = Field(default=None,                 # 수정일시
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class UserEtf(ReferenceTable, SQLModel, table=True):
    """운용사 ETF 코드 중 사람이 직접 넣는 것 -- mst_etf 로 접혀 들어간다.

    TIGER, KB, NH, 키움, TIME 은 상품목록이 화면뿐이고 코드가 링크 href 나
    data- 속성, 셀 문장 속에 박혀 있어 수집 대상이 못 된다(MstEtf 참고). 그
    다섯의 코드를 여기에 손으로 적고, sp_mst_etf_sync 가 JSON 목록들과 함께
    mst_etf 로 접는다.

    mst_etf 를 직접 고치지 않고 표를 따로 두는 이유는 출처가 섞이지 않게 하려는
    것이다. mst_etf 는 수집물에서 다시 만들어질 수 있는 표이고, 여기 적은 값은
    다시 만들 수 없다 -- 섞어 두면 어느 행이 손으로 넣은 것인지 구별할 방법이
    없고, 표를 비우는 순간 사라진다.

    같은 isu_cd 가 운용사 목록에도 있으면 **이쪽이 이긴다** -- 수집물은 비어
    있을 때만 채우지만 이 표는 무조건 덮어쓴다. 수집된 코드가 틀렸을 때 고칠
    자리가 여기 말고는 없어서다. 행을 지우면 다음 실행에서 수집물 쪽 값으로
    돌아간다.

    수집 엔진이 건드리지 않는 참조 데이터라 job_id 가 없다(TABLE_REGISTRY 에서
    빠진다). 채울 대상은 이 질의로 나온다::

        SELECT short_code, prod_nm, amc FROM mst_etf
         WHERE amc_etf_cd IS NULL ORDER BY amc, prod_nm;
    """

    __tablename__ = "user_etf"

    isu_cd: str = Field(primary_key=True, max_length=20)                 # KRX 단축코드
    amc: Optional[str] = Field(default=None, index=True, max_length=20)  # KODEX / TIGER / KB ...
    amc_etf_cd: Optional[str] = Field(default=None, max_length=30)       # 운용사 조회코드

    description: Optional[str] = Field(default=None, max_length=100)
    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))
