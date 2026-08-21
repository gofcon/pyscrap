from datetime import date, datetime
from typing import Optional

from sqlalchemy import CheckConstraint, Column, Date, DateTime, Identity, Integer, String, func
from sqlmodel import Field, SQLModel

from app.db.types import JSONText, OracleBoolean, XMLText

# Convention: on every table below, `updated_at` (when present) is declared
# as the LAST field, so it's also the last physical DB column. Keep new
# fields on existing tables (and new tables) consistent with this -- insert
# them before `updated_at`, never after.


# tags: List[str] = Field(default_factory=list, sa_type=JSON)


class ApiMst(SQLModel, table=True):
    __tablename__ = "api_mst"
    __table_args__ = (
        CheckConstraint(
            # payload_type names the httpx request kwarg to send the payload as
            # (see app.scrapers.dynamic.DynamicApiScraper.build_request):
            # NULL          -> no body at all (typical for GET),
            # 'json'/'data' -> dict-shaped body from payload_json,
            # 'content'     -> raw text body (XML, pre-built text, ...) from payload_xml.
            "(payload_type IS NULL AND payload_json IS NULL AND payload_xml IS NULL) OR "
            "(payload_type IN ('json', 'data') AND payload_json IS NOT NULL AND payload_xml IS NULL) OR "
            "(payload_type = 'content' AND payload_xml IS NOT NULL AND payload_json IS NULL)",
            name="ck_api_mst_payload_exclusive",
        ),
    )

    api_id: str = Field(primary_key=True, max_length=150)
    api_name: str = Field(max_length=100)
    api_group: str = Field(max_length=100)
    request_type: str = Field(max_length=20)
    api_url: str = Field(max_length=500)
    header_json: dict = Field(sa_type=JSONText)
    payload_type: Optional[str] = Field(default=None, max_length=20)
    payload_json: Optional[dict] = Field(default=None, sa_type=JSONText)
    payload_xml: Optional[str] = Field(default=None, sa_type=XMLText)

    # NULL -> response is auto-detected as XML or JSON (sniffed from
    # content-type/body, see DynamicApiScraper.parse).
    # 'zip_delimited' -> response is a binary ZIP containing one delimited
    # text file (e.g. KRX/KIS .mst master files).
    # 'delimited' -> response body itself is delimited text, no zip (a plain
    # .csv/.txt download).
    # 'binary' -> the response is a document (PDF, image, archive) that is
    # itself the result: it's staged to disk whole and only described in the
    # result table, with response_parse_json = {"group": "...", "name":
    # ":KEY.pdf"} naming where it lands. See DynamicApiScraper._stage_binary
    # and app.services.export.stage_file/upload_file.
    # Both share response_parse_json = {"encoding": "...", "delimiter": "...",
    # "fields": ["col1", ...] (or omit + "has_header": true to read column
    # names from the file's own first line), "inner_file": "..." (zip_delimited
    # only, optional, defaults to the zip's first entry)}.
    # See DynamicApiScraper._parse_zip_delimited / _parse_delimited.
    response_type: Optional[str] = Field(default=None, max_length=20)
    response_parse_json: Optional[dict] = Field(default=None, sa_type=JSONText)

    # {source_dot_path: ENV_VAR_NAME} -- after a JSON response is parsed,
    # extract each value and persist it via app.auth_config.persist_env_var
    # (writes to both os.environ and .env on disk). For a side-effect job
    # like refreshing an OAuth token: e.g. {"access_token": "KIS_ACCESS_TOKEN"}
    # means every *later* job's "${KIS_ACCESS_TOKEN}" placeholder picks up the
    # fresh value automatically -- each systemd-timer job runs as its own
    # process, and app.auth_config reloads .env at import time, so nothing
    # else needs to be wired by hand. A job that only does this may leave
    # output_tables_json empty (see DynamicApiScraper.parse).
    persist_env_json: Optional[dict] = Field(default=None, sa_type=JSONText)

    output_tables_json: Optional[dict] = Field(default=None, sa_type=JSONText)
    # Optional: inject fields sourced from anywhere in the full response into
    # a selector's records, e.g. {"output1": {"output3.bstp_nmix_prpr": "kospi200_idx"}}
    # -- so a related value from a different top-level key lands in the same
    # saved row instead of a separate one, guaranteeing same-response/same-
    # timestamp without a join (see app.scrapers.dynamic._apply_merge_fields).
    merge_fields_json: Optional[dict] = Field(default=None, sa_type=JSONText)
    # Which of a job's params identify/key its output rows -- a property of
    # this API's own response shape (which fields the source naturally keys
    # records by), not of any particular ApiJobBuilder's parameter choices,
    # so it lives here rather than per-builder (every builder targeting the
    # same api_id needs the exact same value anyway -- confirmed empirically
    # before this field was moved here from ApiJobBuilder). See
    # app.services.job_builder's module docstring for the full format
    # (including the reserved "NOW" entry) and normalize_key_param.
    key_params_list: Optional[list] = Field(default=None, sa_type=JSONText)
    description: Optional[str] = Field(default=None, max_length=100)
    updated_at: Optional[datetime] = Field(default=None,
        sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class ApiJobBuilder(SQLModel, table=True):
    __tablename__ = "api_job_builder"

    build_id: str = Field(primary_key=True, max_length=50)
    # References ApiMst.api_id, but as a plain indexed column, not a FK (same
    # loose-coupling style as ApiRst.job_id/ApiJobLog.job_id) -- avoids the
    # constraint getting in the way during table drop/recreate migrations
    # (Oracle can't reorder columns -- see the models.py header comment).
    api_id: str = Field(index=True, max_length=150)
    macro_params_json: dict = Field(sa_type=JSONText)
    # key_params_list moved to ApiMst -- see its field comment there.
    is_active: bool = Field(default=False, sa_type=OracleBoolean)
    save_mode: Optional[str] = Field(default="overwrite", max_length=20)
    execution_cycle: Optional[str] = Field(default="daily", max_length=20)
    description: Optional[str] = Field(default=None, max_length=100)
    updated_at: Optional[datetime] = Field(default=None,
        sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class ApiJob(SQLModel, table=True):
    __tablename__ = "api_job"

    job_id: str = Field(primary_key=True, max_length=150)
    # References ApiJobBuilder.build_id / ApiMst.api_id, but as plain indexed
    # columns, not FKs (same loose-coupling style as ApiRst.job_id/
    # ApiJobLog.job_id) -- avoids the constraint getting in the way during
    # table drop/recreate migrations (Oracle can't reorder columns -- see
    # the models.py header comment).
    build_id: Optional[str] = Field(default=None, index=True, max_length=50)
    api_id: str = Field(index=True, max_length=150)
    params_json: dict = Field(sa_type=JSONText)
    is_active: bool = Field(default=True, sa_type=OracleBoolean)
    save_mode: Optional[str] = Field(default="overwrite", max_length=20)
    execution_cycle: Optional[str] = Field(default="daily", max_length=20)
    description: Optional[str] = Field(default=None, max_length=100)
    updated_at: Optional[datetime] = Field(default=None,
        sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class ApiJobLog(SQLModel, table=True):
    """Append-only execution history for an ApiJob (one row per run attempt).

    job_id is a plain indexed column, not a FK (same loose-coupling style as
    ApiRst.job_id) -- keeps log inserts/deletes independent of api_job."""

    __tablename__ = "api_job_log"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    job_id: str = Field(index=True, max_length=150)
    status: str = Field(max_length=20)
    error_message: Optional[str] = Field(default=None, max_length=4000)
    executed_at: Optional[datetime] = Field(default=None, sa_column=Column(DateTime))


class ApiRst(SQLModel, table=True):
    """Generalized Output Table for API Data"""
    __tablename__ = "api_rst"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(index=True, max_length=150)
    job_id: str = Field(index=True, max_length=150)

    # The subset of the generating ApiJob's params_json named in that job's
    # ApiMst.key_params_list (see app.services.execution.run_job).
    # Generic table shared across many APIs with different key param names,
    # so this stays a flexible JSON column rather than fixed columns (contrast
    # with a dedicated table like KisFutoptChart, which gets real columns).
    key_params_json: Optional[dict] = Field(default=None, sa_type=JSONText)

    result_json: dict = Field(default_factory=dict, sa_type=JSONText)

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KisFutoptPrice(SQLModel, table=True):
    """Typed output table for KIS_FUTOPT_PRICE's ``output1`` (single-instrument
    futures/option quote snapshot) -- field names kept as KIS's own, same
    rationale as KisFutoptChart.

    Meant to be polled repeatedly (e.g. every 1m/5m via ApiJobBuilder,
    execution_cycle-driven) to build a price/greeks time series -- job_id
    stays static per instrument (key_params_list =
    ["SHORT_CODE", {"NOW": "trade_at"}]), only ``trade_at`` varies per
    execution. See RESERVED_NOW_KEY / resolve_now in app.services.job_builder
    and run_job in app.services.execution."""

    __tablename__ = "kis_futopt_price"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(index=True, max_length=150)
    job_id: str = Field(index=True, max_length=150)

    short_code: Optional[str] = Field(default=None, index=True, max_length=20)
    # RESERVED_NOW_KEY ("NOW") stamp, floored to the job's execution_cycle --
    # e.g. "20260818144000". Column renamed via the {"NOW": "trade_at"} form
    # of key_params_list (see normalize_key_param) -- a bare "NOW" entry
    # would otherwise auto-lowercase to a column literally named "now". Kept
    # as str (not datetime) for the same reason trade_date/hhmm are on
    # KisFutoptChart: format is fixed at generation, no DB-side date math.
    trade_at: Optional[str] = Field(default=None, index=True, max_length=14)

    futs_prpr: Optional[float] = Field(default=None)              # 현재가
    futs_oprc: Optional[float] = Field(default=None)              # 시가
    futs_hgpr: Optional[float] = Field(default=None)              # 고가
    futs_lwpr: Optional[float] = Field(default=None)              # 저가
    futs_mxpr: Optional[float] = Field(default=None)              # 상한가
    futs_llam: Optional[float] = Field(default=None)              # 하한가 (KIS 필드명 그대로, 의미 추정)
    futs_sdpr: Optional[float] = Field(default=None)              # 기준가(전일 정산가)
    futs_prdy_clpr: Optional[float] = Field(default=None)         # 전일종가
    futs_prdy_vrss: Optional[float] = Field(default=None)         # 전일대비
    prdy_vrss_sign: Optional[str] = Field(default=None, max_length=4)   # 전일대비부호
    futs_prdy_ctrt: Optional[float] = Field(default=None)         # 전일대비율(%)
    acml_vol: Optional[int] = Field(default=None)                 # 누적거래량
    acml_tr_pbmn: Optional[int] = Field(default=None)             # 누적거래대금
    hts_otst_stpl_qty: Optional[int] = Field(default=None)        # 미결제약정수량
    otst_stpl_qty_icdc: Optional[int] = Field(default=None)       # 미결제약정수량증감
    hts_thpr: Optional[float] = Field(default=None)               # 이론가
    hts_ints_vltl: Optional[float] = Field(default=None)          # 내재변동성(IV)
    hist_vltl: Optional[float] = Field(default=None)              # 역사적변동성(HV)
    delta_val: Optional[float] = Field(default=None)              # 델타
    gama: Optional[float] = Field(default=None)                   # 감마
    theta: Optional[float] = Field(default=None)                  # 세타
    vega: Optional[float] = Field(default=None)                   # 베가
    rho: Optional[float] = Field(default=None)                    # 로
    acpr: Optional[float] = Field(default=None)                   # 행사가(옵션)
    dprt: Optional[float] = Field(default=None)                   # 괴리율
    futs_last_tr_date: Optional[str] = Field(default=None, max_length=8)   # 최종거래일
    futs_lstn_medm_hgpr: Optional[float] = Field(default=None)    # 상장기간중 최고가
    futs_lstn_medm_lwpr: Optional[float] = Field(default=None)    # 상장기간중 최저가
    hts_rmnn_dynu: Optional[int] = Field(default=None)            # 잔존일수
    hts_kor_isnm: Optional[str] = Field(default=None, max_length=100)  # 한글종목명

    # merge_fields_json으로 output3(코스피200 지수)에서 병합 -- output1과 동일
    # 응답/동일 시점 보장 (join 없이).
    kospi200_idx: Optional[float] = Field(default=None)

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class FoIdxCodeMst(SQLModel, table=True):
    """Typed output table for the KRX/KIS index futures/option master file
    (``fo_idx_code_mts.mst``, downloaded as a ZIP -- see response_type=
    'zip_delimited' on ApiMst). One row per listed futures/option contract;
    the whole file is a full snapshot, not incremental, so its ApiJobBuilder
    should use save_mode='overwrite' (replace this job's rows every refresh)
    rather than 'append'.

    Column names/meanings taken directly from KIS's own ST_FO_IDX_CODE C
    struct (info_type/atm_cls_code/acpr/mmsc_cls_code kept verbatim; the two
    *_iscd fields renamed to short_code/std_code/unas_short_code for
    consistency with short_code on KisFutoptChart/KisFutoptPrice, since this
    table's short_code is exactly what feeds those as SHORT_CODE)."""

    __tablename__ = "fo_idx_code_mst"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(index=True, max_length=150)
    job_id: str = Field(index=True, max_length=150)

    info_type: Optional[str] = Field(default=None, max_length=2)
    # 1:지수선물 2:지수SP 3:스타선물 4:스타SP 5:지수콜옵션 6:지수풋옵션
    # 7:변동성선물 8:변동성SP 9:섹터선물 A:섹터SP B:미니선물 C:미니SP
    # D:미니콜옵션 E:미니풋옵션 J:코스닥150콜옵션 K:코스닥150풋옵션
    # L:위클리콜옵션 M:위클리풋옵션 (N/O/P/Q/R/S: 추가 위클리 콜/풋 시리즈,
    # 관측되나 원본 struct 주석엔 없음 -- 패턴은 L/M과 동일)

    short_code: Optional[str] = Field(default=None, index=True, max_length=20)   # 단축코드
    std_code: Optional[str] = Field(default=None, max_length=20)                 # 표준코드
    kor_name: Optional[str] = Field(default=None, max_length=100)                # 한글종목명
    atm_cls_code: Optional[str] = Field(default=None, max_length=2)              # ATM구분(1:ATM,2:ITM,3:OTM), 선물은 공백
    acpr: Optional[float] = Field(default=None)                                  # 행사가 (선물은 0)
    mmsc_cls_code: Optional[str] = Field(default=None, max_length=2)             # 월물구분(0:연결,1:최근월...4:차차차근월), 옵션은 공백
    unas_short_code: Optional[str] = Field(default=None, max_length=20)          # 기초자산 단축코드
    unas_kor_name: Optional[str] = Field(default=None, max_length=100)           # 기초자산명

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KisFutoptChart(SQLModel, table=True):
    """Typed output table for KIS_FUTOPT_CHART's ``output2`` (intraday
    tick/bar series) -- field names kept as KIS's own (``futs_prpr`` etc.)
    for direct 1:1 traceability back to the API docs, rather than renamed."""

    __tablename__ = "kis_futopt_chart"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(index=True, max_length=150)
    job_id: str = Field(index=True, max_length=150)

    # key_params_list for KIS_FUTOPT_CHART_1/_2 is ["SHORT_CODE", "DATE", "HHMM"];
    # 'date' itself is an Oracle reserved word (confirmed: ORA-03050), hence trade_date.
    short_code: Optional[str] = Field(default=None, index=True, max_length=20)
    trade_date: Optional[str] = Field(default=None, max_length=8)
    hhmm: Optional[str] = Field(default=None, max_length=6)

    stck_bsop_date: str = Field(index=True, max_length=8)   # 영업일자
    stck_cntg_hour: str = Field(max_length=6)                # 체결시각
    futs_prpr: Optional[float] = Field(default=None)         # 현재가
    futs_oprc: Optional[float] = Field(default=None)         # 시가
    futs_hgpr: Optional[float] = Field(default=None)         # 고가
    futs_lwpr: Optional[float] = Field(default=None)         # 저가
    cntg_vol: Optional[int] = Field(default=None)            # 체결거래량
    acml_tr_pbmn: Optional[int] = Field(default=None)        # 누적거래대금

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class StockIndexHis(SQLModel, table=True):
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


class MetaMaturity(SQLModel, table=True):
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


class MstFuopt(SQLModel, table=True):
    """Curated futures/options instrument master -- the list of contracts
    actually worth polling, as opposed to FoIdxCodeMst, which is the raw
    exchange master file reloaded verbatim every morning.

    Reference data like MetaMaturity -- derived DB-side by a procedure, not
    written by the scraping engine, and kept out of TABLE_REGISTRY by having
    no job_id (see app.services.export._discover_table_registry). So no
    id/api_id/job_id: ``short_cd`` is the natural key, and it is the same
    code FoIdxCodeMst.short_code and KisFutoptPrice.short_code use, so this
    table joins straight to both.
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


class KisFutoptDaily(SQLModel, table=True):
    """Typed output table for KIS_FUTOPT_DAILY's ``output2`` -- one row per
    trading day per contract (daily OHLC/volume), field names kept as KIS's
    own for the same 1:1-traceability reason as KisFutoptChart.

    Distinct from KisFutoptChart despite the overlapping columns: that one is
    intraday, keyed by (date, time), and is written by a snapshot poll during
    market hours. This is end-of-day history, keyed by date alone, and is
    fetched in bulk for a contract that may well have expired already -- the
    endpoint answers for delisted short_codes, which is the whole point (KIS
    only publishes *currently listed* instruments in the master file, so past
    contracts have to be queried by a code reconstructed from the maturity
    calendar rather than looked up).

    Note the response caps ``output2`` at 100 rows per call, and the API pages
    by date window (fid_input_date_1/2) rather than by page number -- so the
    engine's ``:PAGE`` pagination does not apply; a long history has to be
    fetched as successive date-range jobs."""

    __tablename__ = "kis_futopt_daily"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(index=True, max_length=150)
    job_id: str = Field(index=True, max_length=150)

    short_code: Optional[str] = Field(default=None, index=True, max_length=20)

    stck_bsop_date: str = Field(index=True, max_length=8)    # 영업일자
    futs_prpr: Optional[float] = Field(default=None)         # 종가
    futs_oprc: Optional[float] = Field(default=None)         # 시가
    futs_hgpr: Optional[float] = Field(default=None)         # 고가
    futs_lwpr: Optional[float] = Field(default=None)         # 저가
    acml_vol: Optional[int] = Field(default=None)            # 누적거래량
    acml_tr_pbmn: Optional[int] = Field(default=None)        # 누적거래대금
    mod_yn: Optional[str] = Field(default=None, max_length=1)  # 수정주가 반영 여부

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KisIndexDaily(SQLModel, table=True):
    """Typed output table for KIS_INDEX_DAILY's ``output2`` -- one row per
    index per trading day, field names kept as KIS's own for the same
    1:1-traceability reason as the other kis_* tables.

    Raw landing table rather than the series anyone queries: a procedure folds
    it into stock_index_his, which is keyed (trade_date, mv_id) and carries the
    derived columns the source does not supply (the endpoint returns OHLC and
    volume but no day-over-day change). Keeping the two apart lets the engine
    own this one -- it has a job_id, so save_mode and the export path work
    normally -- while stock_index_his stays reference data maintained DB-side.

    ``short_code`` carries the index the row belongs to, stamped from the
    job's own FID_INPUT_ISCD parameter (see ApiMst.key_params_list) because
    the response itself does not repeat it -- output2 is a bare series. That
    is what lets one table hold more than one index, and what the procedure
    maps to mv_id on the way into stock_index_his."""

    __tablename__ = "kis_index_daily"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(index=True, max_length=150)
    job_id: str = Field(index=True, max_length=150)

    short_code: Optional[str] = Field(default=None, index=True, max_length=20)  # 지수 코드 (FID_INPUT_ISCD)

    stck_bsop_date: str = Field(index=True, max_length=8)      # 영업일자
    bstp_nmix_prpr: Optional[float] = Field(default=None)      # 종가
    bstp_nmix_oprc: Optional[float] = Field(default=None)      # 시가
    bstp_nmix_hgpr: Optional[float] = Field(default=None)      # 고가
    bstp_nmix_lwpr: Optional[float] = Field(default=None)      # 저가
    acml_vol: Optional[int] = Field(default=None)              # 누적거래량
    acml_tr_pbmn: Optional[int] = Field(default=None)          # 누적거래대금
    mod_yn: Optional[str] = Field(default=None, max_length=1)  # 수정지수 반영 여부

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))
