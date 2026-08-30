from datetime import date, datetime
from typing import Any, Optional

from pydantic import model_validator
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

    # How to keep asking for the rest of a truncated result, when one request
    # doesn't return everything. Three shapes, because APIs disagree on how to
    # say "there is more" (see app.scrapers.dynamic.fetch_all_pages):
    #
    #   {"mode": "page", "param": "PAGE", "start": 1}
    #       A page number the client increments. NULL with a ':PAGE' marker in
    #       api_url/payload means exactly this, so older rows keep working.
    #
    #   {"mode": "cursor", "param": "HHMM", "from": "output2[].stck_cntg_hour",
    #    "pick": "min", "min_records": 99}
    #       No page number -- the next request is anchored on a value read out
    #       of the answer just received (the oldest bar's timestamp, an id,
    #       ...). "min_records" is the truncation tell: a short reply means the
    #       series ran out, so stop.
    #
    #   {"mode": "token", "params": {"CTX_FK": "header:ctx_area_fk100"},
    #    "continue_when": {"source": "header:tr_cont", "in": ["F", "M"]}}
    #       An opaque continuation key handed back to be echoed, plus a flag
    #       saying whether more remains -- KIS's tr_cont/ctx_area_* pairing.
    #       Sources read "header:<name>" or a dotted body path.
    #
    # "max_pages" caps every mode as a safety net against a stop condition
    # that never fires.
    pagination_json: Optional[dict] = Field(default=None, sa_type=JSONText)

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
    # Not indexed: a result table holds exactly one API's output, so api_id is
    # a single value throughout and an index on it filters nothing while
    # costing as much as a useful one -- 268MB against 5.5M rows on
    # kis_futopt_chart before it was dropped. It stays as a column for
    # traceability, not for lookup.
    api_id: str = Field(max_length=150)
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
    # Not indexed: a result table holds exactly one API's output, so api_id is
    # a single value throughout and an index on it filters nothing while
    # costing as much as a useful one -- 268MB against 5.5M rows on
    # kis_futopt_chart before it was dropped. It stays as a column for
    # traceability, not for lookup.
    api_id: str = Field(max_length=150)
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
    # Indexed here, unlike the typed result tables: this one is shared by
    # every API whose output has no table of its own, so api_id genuinely
    # selects a subset (6 values across ~114k rows) instead of matching
    # everything.
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
    # Not indexed: a result table holds exactly one API's output, so api_id is
    # a single value throughout and an index on it filters nothing while
    # costing as much as a useful one -- 268MB against 5.5M rows on
    # kis_futopt_chart before it was dropped. It stays as a column for
    # traceability, not for lookup.
    api_id: str = Field(max_length=150)
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
    Each download is a full snapshot of what is listed that day, and every
    snapshot is kept: the builder appends and ``trade_at`` stamps which run a
    row came from (``{"NOW": "trade_at"}`` in key_params_list). Keeping them
    is what makes the listing history answerable -- when a contract appeared,
    when it stopped being listed -- which a single overwritten copy cannot
    say. Readers that want "what is listed now" take the rows of the latest
    trade_at (see scripts/sql/sp_mst_fuopt_sync.sql).

    Column names/meanings taken directly from KIS's own ST_FO_IDX_CODE C
    struct (info_type/atm_cls_code/acpr/mmsc_cls_code kept verbatim; the two
    *_iscd fields renamed to short_code/std_code/unas_short_code for
    consistency with short_code on KisFutoptChart/KisFutoptPrice, since this
    table's short_code is exactly what feeds those as SHORT_CODE)."""

    __tablename__ = "fo_idx_code_mst"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    # Not indexed: a result table holds exactly one API's output, so api_id is
    # a single value throughout and an index on it filters nothing while
    # costing as much as a useful one -- 268MB against 5.5M rows on
    # kis_futopt_chart before it was dropped. It stays as a column for
    # traceability, not for lookup.
    api_id: str = Field(max_length=150)
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
    unas_kor_name: Optional[str] = Field(default=None, max_length=100)
    # Which download this row came from -- stamped per execution by the
    # reserved NOW key, not part of the file. Same name/shape as trade_at on
    # KisFutoptPrice so a capture time reads the same way across tables.
    trade_at: Optional[str] = Field(default=None, max_length=14)           # 기초자산명

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KisFutoptChart(SQLModel, table=True):
    """Typed output table for KIS_FUTOPT_CHART's ``output2`` (intraday
    tick/bar series) -- field names kept as KIS's own (``futs_prpr`` etc.)
    for direct 1:1 traceability back to the API docs, rather than renamed."""

    __tablename__ = "kis_futopt_chart"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    # Not indexed: a result table holds exactly one API's output, so api_id is
    # a single value throughout and an index on it filters nothing while
    # costing as much as a useful one -- 268MB against 5.5M rows on
    # kis_futopt_chart before it was dropped. It stays as a column for
    # traceability, not for lookup.
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    # Stamped from key_params_list (["SHORT_CODE", {"DATE": "trade_date"},
    # "BAR_SEC"]) -- 'date' is an Oracle reserved word (ORA-03050), hence the
    # explicit mapping to trade_date. HHMM used to be stamped here too and no
    # longer is: it is the cursor's starting point, always 154500, and reading
    # it as a bar's own time is a mistake this column invited -- the bar's time
    # is stck_cntg_hour. BAR_SEC has nowhere to land (no column), so a row
    # cannot say which interval it holds; every row here is 60-second.
    short_code: Optional[str] = Field(default=None, index=True, max_length=20)
    trade_date: Optional[str] = Field(default=None, max_length=8)

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
    # Not indexed: a result table holds exactly one API's output, so api_id is
    # a single value throughout and an index on it filters nothing while
    # costing as much as a useful one -- 268MB against 5.5M rows on
    # kis_futopt_chart before it was dropped. It stays as a column for
    # traceability, not for lookup.
    api_id: str = Field(max_length=150)
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
    # Not indexed: a result table holds exactly one API's output, so api_id is
    # a single value throughout and an index on it filters nothing while
    # costing as much as a useful one -- 268MB against 5.5M rows on
    # kis_futopt_chart before it was dropped. It stays as a column for
    # traceability, not for lookup.
    api_id: str = Field(max_length=150)
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


class VendorRecordBase(SQLModel):
    """Shared record normalization for result tables fed by a source that
    spells its records the way KRX and KSD/SEIBRO do -- currently the
    ``krx_*`` and ``ksd_*`` tables below.

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
    dealt with once, where the record enters."""

    @model_validator(mode="before")
    @classmethod
    def _normalize_vendor_record(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        return {k.lower(): (None if isinstance(v, str) and not v.strip() else v)
                for k, v in data.items()}


class KrxFutDaily(VendorRecordBase, table=True):
    """Typed output table for KRX's futures daily-trade endpoints, field
    names kept as KRX's own (lower-cased) for 1:1 traceability.

    Two APIs write here -- KRX_FUT_INFO (``drv/fut_bydd_trd``: index, rate
    and currency futures) and KRX_STFUT_INFO (``drv/eqsfu_stk_bydd_trd``:
    single-stock futures). Their payloads carry the identical 15 fields and
    both are "one row per contract per day", so they share a table;
    ``api_id`` says which endpoint a row came from and ``prod_nm`` separates
    the products within one.

    Unlike kis_futopt_daily this is exchange-published settlement data, not a
    broker's view: ``setl_prc`` (정산가) and ``spot_prc`` (기초자산 현물가)
    have no KIS equivalent, and every listed contract appears every day
    whether or not it traded."""

    __tablename__ = "krx_fut_daily"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    # Indexed, unlike the kis_* result tables' api_id: this table is shared by
    # two endpoints, so the column genuinely selects a subset -- though only
    # two values, so it earns its keep by pairing with bas_dd, not alone.
    api_id: str = Field(index=True, max_length=150)
    job_id: str = Field(index=True, max_length=150)

    bas_dd: str = Field(index=True, max_length=8)                  # 기준일자
    prod_nm: Optional[str] = Field(default=None, max_length=200)   # 상품군 (10년국채 선물 등)
    mkt_nm: Optional[str] = Field(default=None, max_length=20)     # 시장구분 (정규/야간)
    isu_cd: str = Field(index=True, max_length=20)                 # 종목코드
    isu_nm: Optional[str] = Field(default=None, max_length=200)    # 종목명

    tdd_clsprc: Optional[float] = Field(default=None)              # 종가
    cmpprevdd_prc: Optional[float] = Field(default=None)           # 대비
    tdd_opnprc: Optional[float] = Field(default=None)              # 시가
    tdd_hgprc: Optional[float] = Field(default=None)               # 고가
    tdd_lwprc: Optional[float] = Field(default=None)               # 저가
    spot_prc: Optional[float] = Field(default=None)                # 현물가
    setl_prc: Optional[float] = Field(default=None)                # 정산가
    acc_trdvol: Optional[int] = Field(default=None)                # 거래량
    acc_trdval: Optional[int] = Field(default=None)                # 거래대금
    acc_opnint_qty: Optional[int] = Field(default=None)            # 미결제약정

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KrxOptDaily(VendorRecordBase, table=True):
    """Typed output table for KRX's option daily-trade endpoints, field names
    kept as KRX's own (lower-cased) for 1:1 traceability.

    Shared by KRX_OPTION_DAILY (``drv/opt_bydd_trd``: index options --
    코스피200 / 미니 / 코스닥150 and their weeklies) and KRX_STOPT_INFO
    (``drv/eqsop_bydd_trd``: single-stock options), whose payloads carry the
    identical 15 fields; ``api_id`` says which, ``prod_nm`` separates the
    products.

    Same shape as krx_fut_daily minus ``mkt_nm``/``spot_prc``/``setl_prc``,
    plus what only an option has: ``rght_tp_nm`` (CALL/PUT), ``imp_volt``
    (내재변동성) and ``nxtdd_bas_prc`` (익일 기준가 -- the theoretical price an
    untraded strike is marked at, which is why it is populated on rows where
    every OHLC column is null).

    The strike itself is not a field: it is embedded in ``isu_nm``
    (``"미니코스피 C 202609   665.0 (야간)"``), so anything keyed on strike has
    to parse it out or join to mst_fuopt."""

    __tablename__ = "krx_opt_daily"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(index=True, max_length=150)
    job_id: str = Field(index=True, max_length=150)

    bas_dd: str = Field(index=True, max_length=8)                  # 기준일자
    prod_nm: Optional[str] = Field(default=None, max_length=200)   # 상품군
    rght_tp_nm: Optional[str] = Field(default=None, max_length=10)  # 권리유형 (CALL/PUT)
    isu_cd: str = Field(index=True, max_length=20)                 # 종목코드
    isu_nm: Optional[str] = Field(default=None, max_length=200)    # 종목명 (행사가 포함)

    tdd_clsprc: Optional[float] = Field(default=None)              # 종가
    cmpprevdd_prc: Optional[float] = Field(default=None)           # 대비
    tdd_opnprc: Optional[float] = Field(default=None)              # 시가
    tdd_hgprc: Optional[float] = Field(default=None)               # 고가
    tdd_lwprc: Optional[float] = Field(default=None)               # 저가
    imp_volt: Optional[float] = Field(default=None)                # 내재변동성
    nxtdd_bas_prc: Optional[float] = Field(default=None)           # 익일 기준가
    acc_trdvol: Optional[int] = Field(default=None)                # 거래량
    acc_trdval: Optional[int] = Field(default=None)                # 거래대금
    acc_opnint_qty: Optional[int] = Field(default=None)            # 미결제약정

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KrxEtfDaily(VendorRecordBase, table=True):
    """Typed output table for KRX_ETF (``etp/etf_bydd_trd``) -- one row per
    ETF per trading day, field names kept as KRX's own (lower-cased).

    Not folded into the futures/option tables: an ETF row describes a fund as
    much as a traded instrument, so half its 19 fields have no derivatives
    equivalent -- ``nav`` and ``invstasst_netasst_totamt`` (순자산총액) on the
    fund side, and ``obj_stkprc_idx`` / ``cmpprevdd_idx`` / ``fluc_rt_idx``
    describing the *tracked index* rather than the ETF, which is what lets
    tracking error be computed without joining anywhere else."""

    __tablename__ = "krx_etf_daily"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    # Not indexed: one endpoint writes here, so api_id is a single value
    # throughout -- kept as a column for traceability, not for lookup (see
    # KisFutoptDaily for what an index on it cost there).
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    bas_dd: str = Field(index=True, max_length=8)                  # 기준일자
    isu_cd: str = Field(index=True, max_length=20)                 # 종목코드
    isu_nm: Optional[str] = Field(default=None, max_length=200)    # 종목명

    tdd_clsprc: Optional[float] = Field(default=None)              # 종가
    cmpprevdd_prc: Optional[float] = Field(default=None)           # 대비
    fluc_rt: Optional[float] = Field(default=None)                 # 등락률
    nav: Optional[float] = Field(default=None)                     # 순자산가치
    tdd_opnprc: Optional[float] = Field(default=None)              # 시가
    tdd_hgprc: Optional[float] = Field(default=None)               # 고가
    tdd_lwprc: Optional[float] = Field(default=None)               # 저가
    acc_trdvol: Optional[int] = Field(default=None)                # 거래량
    acc_trdval: Optional[int] = Field(default=None)                # 거래대금
    mktcap: Optional[int] = Field(default=None)                    # 시가총액
    invstasst_netasst_totamt: Optional[int] = Field(default=None)  # 순자산총액
    list_shrs: Optional[int] = Field(default=None)                 # 상장좌수
    idx_ind_nm: Optional[str] = Field(default=None, max_length=100)  # 기초지수명
    obj_stkprc_idx: Optional[float] = Field(default=None)          # 기초지수 종가
    cmpprevdd_idx: Optional[float] = Field(default=None)           # 기초지수 대비
    fluc_rt_idx: Optional[float] = Field(default=None)             # 기초지수 등락률

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KsdKacdCode(VendorRecordBase, table=True):
    """Typed output table for KSD_KACD_LIST (SEIBRO's ``searchBondList``) --
    the 채권 종류 코드 tree, field names kept as SEIBRO's own (lower-cased).

    A two-level tree flattened into one table, which is how SEIBRO returns
    it: the 9 ``code_depth = 1`` rows are the groups (국채, 지방채, 특수채,
    ...) and hang off a single synthetic parent ``'11'``; the 53
    ``code_depth = 2`` rows are the kinds within a group and carry that
    group in ``code_parent``.

    Read it as the argument list for KSD_ISIN_BY_KACD rather than as
    reference data: a depth-1 row's ``codevalue`` is exactly that endpoint's
    ``KA_GROUP`` (see its payload_xml, ``SECN_KACD``), so a builder macro
    selecting ``WHERE code_depth = 1`` expands into one job per group.

    The depth-2 rows are *not* used that way, even though the endpoint
    accepts ``SECN_DTAIL_KACD``: querying by detail code truncates
    ``110810 일반회사채`` at 500 rows against the 9,488 its group returns, so
    walking the kinds silently loses ~9,000 issues. Measured 2026-08-30;
    the other 52 kinds summed exactly to their groups.

    Note also that the tree does not cover everything the bond list can
    return -- ``MBS`` and ``SLBS`` (1,886 issues) appear under no code here
    at all, which is why KSD_ISIN_ALL exists.

    Not a reference table like meta_maturity despite looking like one: it is
    scraped, so it has a job_id and the engine owns it. Codes are re-fetched
    whole rather than merged -- 62 rows -- so ``save_mode='overwrite'``
    leaves exactly one snapshot per job."""

    __tablename__ = "ksd_kacd_code"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    # Not indexed: one endpoint writes here, and the table is 62 rows -- see
    # KisFutoptDaily for why api_id is a traceability column, not a lookup one.
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    codevalue: str = Field(index=True, max_length=20)               # 코드값 (110110)
    codevalue_nm: Optional[str] = Field(default=None, max_length=100)  # 코드명 (국고채권)
    code_depth: Optional[int] = Field(default=None)                 # 1=그룹, 2=종류
    code_parent: Optional[str] = Field(default=None, index=True, max_length=20)  # 상위 코드

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KsdBondIsin(VendorRecordBase, table=True):
    """Typed output table for SEIBRO's bond list
    (``searchBondDepthContentList``), field names kept as SEIBRO's own
    (lower-cased).

    Two APIs write here, and they are two ways of asking the same question:

    - KSD_ISIN_ALL sends no parameters and returns every issue in one
      response (29,033 rows / 6.8MB / ~12s, measured 2026-08-30). This is
      the complete set.
    - KSD_ISIN_BY_KACD sends one ``SECN_KACD`` and returns that group
      (483 issues for 국채, 9,488 for 일반회사채). The nine groups together
      come to 27,147 -- a strict subset of the above, missing the 1,886
      ``MBS``/``SLBS`` issues that belong to no group in ksd_kacd_code.

    Which group a row came from is not in the payload, so KSD_ISIN_BY_KACD
    stamps it from the job's own parameter via ApiMst.key_params_list, the
    way kis_index_daily carries ``short_code``. It is mapped rather than
    carried over by name (``KA_GROUP`` -> ``secn_kacd``): the param name is
    the operator's, chosen to read well in a job row, while the column keeps
    SEIBRO's name from the request body -- the same 1:1-traceability rule
    the kis_* and krx_* tables follow. KSD_ISIN_ALL has no parameter to
    stamp, so its rows leave ``secn_kacd`` null and carry the classification
    only in ``codevalue_nm``, which every row has either way.

    ``secn_dtail_kacd`` is currently written by neither: collecting per
    detail code was dropped because ``110810 일반회사채`` truncates at 500
    rows (see KsdKacdCode). The column stays for a re-collection that does
    go that way -- it is the only place a kind finer than ``codevalue_nm``
    would land.

    ``isin`` is unique within neither api_id nor the table. Two things
    multiply it: running both APIs puts every issue outside MBS/SLBS here
    twice, once per endpoint (save_mode clears by job_id, so neither clears
    the other -- filter on ``api_id``, or run only one); and KSD_ISIN_ALL is
    appended daily rather than overwritten, so each run leaves a full
    snapshot of its own. ``trade_at`` separates those, and the current list
    is the one with the highest value.

    Kept as history rather than a live snapshot because listings and
    delistings are only visible as a difference between two runs -- nothing
    in the response says a bond is new. That difference is what decides which
    instruments the bond-detail jobs go and fetch. The cost is size: ~29k
    rows a run, so a year of trading days is around 7M rows."""

    __tablename__ = "ksd_bond_isin"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    # Indexed, unlike the single-endpoint result tables': two APIs write here
    # and their row sets overlap almost entirely, so this is the column that
    # picks one of them out.
    api_id: str = Field(index=True, max_length=150)
    job_id: str = Field(index=True, max_length=150)

    secn_kacd: Optional[str] = Field(default=None, index=True, max_length=20)  # 그룹코드 (GROUP)
    secn_dtail_kacd: Optional[str] = Field(default=None, index=True, max_length=20)  # 종류코드 (KACD)

    isin: str = Field(index=True, max_length=12)                    # 표준코드
    issuco_custno: Optional[str] = Field(default=None, max_length=20)  # 발행회사 고객번호
    kor_secn_nm: Optional[str] = Field(default=None, max_length=200)   # 종목명
    codevalue_nm: Optional[str] = Field(default=None, max_length=100)  # 대분류명 (국채/특수채)
    issu_dt: Optional[str] = Field(default=None, max_length=8)      # 발행일

    # 수집 시각 (KST, YYYYMMDDHH24MISS) -- 스냅샷을 서로 구분하는 축이다.
    # 응답에는 없고 ApiMst.key_params_list 의 NOW 가 실행할 때마다 찍는다.
    trade_at: Optional[str] = Field(default=None, index=True, max_length=14)

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


# ---------------------------------------------------------------------------
# SEIBRO 채권 상세 (KSD_BOND_*) -- 엔드포인트 하나에 테이블 하나.
#
# 13개 엔드포인트가 같은 ISIN 을 키로 하면서도 필드가 거의 겹치지 않는다
# (기본정보 49 / 이자 12 / 신용등급 6 / 원리금 11 / 수익률 5 ...). 하나로 합치면
# 어느 행이든 대부분의 컬럼이 NULL 이 되므로 주제별로 나눈다. 조인 키는 ``isin``.
#
# 238종목(국채/MBS/SLBS/CB/EB/후순위/영구채/변동금리 포함)으로 확인한 결과
# 어느 엔드포인트도 필드가 늘거나 줄지 않았다 -- SEIBRO 는 항상 같은 태그 집합을
# 돌려주고 해당 없는 값은 공백으로 채운다. 그래서 고정 컬럼으로 잡을 수 있다.
#
# 타입 규칙: 코드성 필드(``*_TPCD``, ``*_WHCD``, ``SECN_KACD`` ...)는 선행 0 이
# 의미를 갖는다(``'0000'`` 옵션없음, ``'01'`` 이자) -- 전부 문자열로 둔다. 날짜도
# 문자열 8자리인데, 영구채 만기가 ``'99991231'`` 로 오므로 DATE 로 바꿀 수 없다.
# 금액/수량/율만 수치형이다.
# ---------------------------------------------------------------------------


class KsdBondInfo(VendorRecordBase, table=True):
    """KSD_BOND_INFO_web (SEIBRO ``issuInfoViewEL1``) -- 채권 기본정보, 종목당 1행.

    이 계열의 중심 테이블이다. 전체 ISIN 을 커버하고(표본 238/238 응답), 발행
    조건·발행사·주관사·업종·자금용도까지 담는다. OpenAPI 쪽 ksd_bond_info_api 와
    공통 필드가 12개뿐이고 값은 전부 일치하므로, 겹치는 정보는 이쪽만 읽으면 된다.

    ``int_kind`` 가 변동금리 판별자다. 종목명의 '변' 글자에 의존할 필요 없이
    ``'변동-이표'`` / ``'고정+변동-이표'`` / ``'고정-이표'`` 처럼 값 자체로 갈린다
    (표본에서 두 집단의 값이 하나도 겹치지 않았다). 이름만 보고는 알 수 없는
    ``'고정+변동'`` 혼합형도 여기서 드러난다.

    ``xpir_dt`` 는 영구채에서 ``'99991231'`` 이 오므로 날짜형이 아니라 문자열이다.
    ``perptual_bond_yn_nm`` 과 같이 보면 된다."""

    __tablename__ = "ksd_bond_info"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    isin: str = Field(index=True, max_length=12)                        # 표준코드
    kor_secn_nm: Optional[str] = Field(default=None, max_length=200)    # 종목명
    isin_kor_secn_nm: Optional[str] = Field(default=None, max_length=200)  # 관련 종목명
    secn_kacd: Optional[str] = Field(default=None, index=True, max_length=10)  # 종류코드
    secn_kacd_nm: Optional[str] = Field(default=None, max_length=50)    # 종류명

    issu_dt: Optional[str] = Field(default=None, index=True, max_length=8)  # 발행일
    xpir_dt: Optional[str] = Field(default=None, index=True, max_length=8)  # 만기일 (영구채 99991231)
    apli_dt: Optional[str] = Field(default=None, max_length=8)          # 적용일
    issu_cur_cd: Optional[str] = Field(default=None, max_length=10)     # 발행통화

    int_kind: Optional[str] = Field(default=None, index=True, max_length=30)  # 이자유형 (고정/변동)
    coupon_rate: Optional[float] = Field(default=None)                  # 표면금리
    addn_irate: Optional[float] = Field(default=None)                   # 가산금리

    first_issu_amt: Optional[int] = Field(default=None)                 # 최초발행금액
    payin_amt: Optional[int] = Field(default=None)                      # 납입금액
    issu_rema: Optional[int] = Field(default=None)                      # 발행잔액

    recu_whcd_nm: Optional[str] = Field(default=None, max_length=30)    # 모집방법 (공모/사모)
    issu_form: Optional[str] = Field(default=None, max_length=30)       # 발행형태
    taxa_tpcd_nm: Optional[str] = Field(default=None, max_length=20)    # 과세구분
    listtpcdnm: Optional[str] = Field(default=None, max_length=30)      # 상장구분
    grty_tpcd: Optional[str] = Field(default=None, max_length=10)       # 보증구분코드
    grty_tpcd_nm: Optional[str] = Field(default=None, max_length=30)    # 보증구분
    rank_tpcd_nm: Optional[str] = Field(default=None, max_length=20)    # 선순위/후순위
    prcp_red_whcd_nm: Optional[str] = Field(default=None, max_length=30)  # 원금상환방법
    regi_org_tpcd_nm: Optional[str] = Field(default=None, max_length=20)  # 등록기관
    exer_mbody_tpnm: Optional[str] = Field(default=None, max_length=20)   # 권리행사주체
    int_estm_mann_tpnm: Optional[str] = Field(default=None, max_length=20)  # 이자계산방식

    option_tpcd: Optional[str] = Field(default=None, max_length=10)     # 옵션코드 ('0000'=없음)
    option_tpcd_nm: Optional[str] = Field(default=None, max_length=30)  # 옵션 (CALL/PUT)
    particul_bond_kind: Optional[str] = Field(default=None, max_length=30)  # 주식관련 종류 (CB/EB)
    particul_bond_kind_tpcd_nm: Optional[str] = Field(default=None, max_length=30)
    prcc_linked_yn_nm: Optional[str] = Field(default=None, max_length=30)   # 물가연동 여부
    strips_poss_yn_nm: Optional[str] = Field(default=None, max_length=30)   # STRIPS 가능 여부
    perptual_bond_yn_nm: Optional[str] = Field(default=None, max_length=20)  # 영구채 여부
    perptual_bond_tmn_dt: Optional[str] = Field(default=None, max_length=8)  # 영구채 종료일
    qib_yn_nm: Optional[str] = Field(default=None, max_length=20)       # QIB 여부
    qib_tmn_dt: Optional[str] = Field(default=None, max_length=8)       # QIB 종료일
    crwdf_yn_nm: Optional[str] = Field(default=None, max_length=20)     # 크라우드펀딩 여부

    rep_secn_nm: Optional[str] = Field(default=None, max_length=200)    # 발행회사명
    indtp_clsf_no: Optional[str] = Field(default=None, max_length=100)  # 업종
    rep_lmgr_custno: Optional[str] = Field(default=None, max_length=20)     # 대표주관사 번호
    rep_lmgr_custno_nm: Optional[str] = Field(default=None, max_length=100)  # 대표주관사
    comm_accep_org_nm: Optional[str] = Field(default=None, max_length=200)   # 인수기관
    idtr_custnm: Optional[str] = Field(default=None, max_length=100)    # 사채관리회사
    grorg_custno: Optional[str] = Field(default=None, max_length=20)    # 보증기관 번호
    grorg_custno_nm: Optional[str] = Field(default=None, max_length=100)  # 보증기관
    grorg_custno_yn: Optional[str] = Field(default=None, max_length=20)   # 보증기관 여부
    prin_rcv_fnceco: Optional[str] = Field(default=None, max_length=100)  # 원리금지급처
    afund_uses_tpcdnm: Optional[str] = Field(default=None, max_length=100)  # 자금용도
    ablsnm: Optional[str] = Field(default=None, max_length=30)          # 유동화 구분 (MBS/SLBS)

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KsdBondInfoApi(VendorRecordBase, table=True):
    """KSD_BOND_INFO (SEIBRO OpenAPI ``getBondStatInfo``) -- 채권 기본정보, 종목당 1행.

    ksd_bond_info 와 같은 주제지만 다른 데이터셋이라 따로 둔다. 필드 36개 중
    ksd_bond_info 와 겹치는 것은 12개뿐이고, 나머지는 web 이 한글명으로 주는 것을
    코드값으로 준다(``rank_tpcd`` ``'1'`` ↔ ``rank_tpcd_nm`` ``'선순위'``).
    겹치는 12개는 표본 전체에서 값이 일치했다.

    커버리지가 좁다. 표본 185종목 중 38종목만 응답했고 **그 38종목이 정확히
    일반회사채 전체**였다 -- 이 엔드포인트는 일반회사채(1108)만 답한다. 대신
    web 에 없는 것을 둘 준다: 평가사 신용등급 4종(``*_valat_grd_cd``)과
    ``dlist_dt``(상장폐지일, 상장폐지된 종목도 답한다).

    다만 신용등급 4종은 표본 43행 중 1~2행만 채워져 있었다. 등급이 실제로
    필요하면 ksd_bond_credit(web) 쪽이 평가사별로 훨씬 촘촘하다."""

    __tablename__ = "ksd_bond_info_api"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    isin: str = Field(index=True, max_length=12)                        # 표준코드 (잡 파라미터에서)
    kor_secn_nm: Optional[str] = Field(default=None, max_length=200)    # 종목명
    issuco_custno: Optional[str] = Field(default=None, max_length=20)   # 발행회사 고객번호
    secn_kacd: Optional[str] = Field(default=None, index=True, max_length=10)  # 종류코드

    issu_dt: Optional[str] = Field(default=None, index=True, max_length=8)  # 발행일
    xpir_dt: Optional[str] = Field(default=None, index=True, max_length=8)  # 만기일
    apli_dt: Optional[str] = Field(default=None, max_length=8)          # 적용일
    dlist_dt: Optional[str] = Field(default=None, max_length=8)         # 상장폐지일
    issu_cur_cd: Optional[str] = Field(default=None, max_length=10)     # 발행통화

    coupon_rate: Optional[float] = Field(default=None)                  # 표면금리
    xpired_rate: Optional[float] = Field(default=None)                  # 만기상환율
    xpir_guar_prate: Optional[float] = Field(default=None)              # 만기보장수익률
    xpir_guar_prate_tpcd: Optional[str] = Field(default=None, max_length=10)
    first_issu_amt: Optional[int] = Field(default=None)                 # 최초발행금액
    payin_amt: Optional[int] = Field(default=None)                      # 납입금액
    issu_rema: Optional[int] = Field(default=None)                      # 발행잔액

    recu_whcd: Optional[str] = Field(default=None, max_length=10)       # 모집방법
    issu_whcd: Optional[str] = Field(default=None, max_length=10)       # 발행방법
    grty_tpcd: Optional[str] = Field(default=None, max_length=10)       # 보증구분
    signa_tpcd: Optional[str] = Field(default=None, max_length=10)      # 기명구분
    rank_tpcd: Optional[str] = Field(default=None, max_length=10)       # 순위구분
    regi_org_tpcd: Optional[str] = Field(default=None, max_length=10)   # 등록기관
    prcp_red_whcd: Optional[str] = Field(default=None, max_length=10)   # 원금상환방법
    mr_chg_tpcd: Optional[str] = Field(default=None, max_length=10)     # 만기변경구분
    irate_chg_tpcd: Optional[str] = Field(default=None, max_length=10)  # 금리변동구분
    int_pay_way_tpcd: Optional[str] = Field(default=None, max_length=10)    # 이자지급방법
    sint_cint_tpcd: Optional[str] = Field(default=None, max_length=10)      # 단리/복리
    int_estm_mann_tpcd: Optional[str] = Field(default=None, max_length=10)  # 이자계산방식
    exer_mbody_tpcd: Optional[str] = Field(default=None, max_length=10)     # 권리행사주체
    option_tpcd: Optional[str] = Field(default=None, max_length=10)         # 옵션코드
    particul_bond_kind_tpcd: Optional[str] = Field(default=None, max_length=10)  # 주식관련 종류
    forc_erly_red_yn: Optional[str] = Field(default=None, max_length=1)     # 강제조기상환 여부
    eltsc_yn: Optional[str] = Field(default=None, max_length=1)             # 전자증권 여부

    kis_valat_grd_cd: Optional[str] = Field(default=None, max_length=10)   # KIS 평가등급
    nice_valat_grd_cd: Optional[str] = Field(default=None, max_length=10)  # NICE 평가등급
    sci_valat_grd_cd: Optional[str] = Field(default=None, max_length=10)   # SCI 평가등급
    kr_valat_grd_cd: Optional[str] = Field(default=None, max_length=10)    # KR 평가등급

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KsdBondInt(VendorRecordBase, table=True):
    """KSD_BOND_INT_web (SEIBRO ``intPayInfoView``) -- 이자지급 조건, 종목당 1행.

    ``before_date`` / ``after_date`` 는 **``today`` 기준 직전·직후 이자지급일**이다.
    조회 시점에 따라 값이 바뀌는 스냅샷이라, 과거 시점을 재현하려면 ``today`` 를
    함께 봐야 한다 -- 이 테이블에서 유일하게 시점 의존적인 부분이다.

    응답 XML 이 이 계열에서 유일하게 ``<result>`` 를 루트로 준다(나머지는
    ``<vector><data><result>``). 그래서 이 API 만 output_tables_json 셀렉터가
    ``'.'`` 이고 나머지는 ``'.//result'`` 다. 잘못 두면 오류 없이 0건으로 끝난다."""

    __tablename__ = "ksd_bond_int"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    isin: str = Field(index=True, max_length=12)                        # 표준코드 (잡 파라미터에서)
    issu_dt: Optional[str] = Field(default=None, max_length=8)          # 발행일

    int_pay_way_tpcd_nm: Optional[str] = Field(default=None, max_length=20)   # 이표채/복리채/할인채
    int_pay_cycle_terms: Optional[int] = Field(default=None)                  # 이자지급주기(개월)
    int_pay_cycle_tpcd: Optional[str] = Field(default=None, max_length=10)    # 주기구분코드
    int_pay_cycle_tpcd_nm: Optional[str] = Field(default=None, max_length=20)  # 주기
    int_pay_tims_tpcd_nm: Optional[str] = Field(default=None, max_length=20)  # 선급/후급/상환시
    acrint_pay_yn_nm: Optional[str] = Field(default=None, max_length=30)      # 경과이자 지급여부
    bank_holiday_int_paydd_tpcd_nm: Optional[str] = Field(default=None, max_length=20)  # 은행휴일 처리
    lawhday_int_paydd_tpcd_nm: Optional[str] = Field(default=None, max_length=20)       # 법정공휴일 처리

    today: Optional[str] = Field(default=None, max_length=8)            # 조회 기준일
    before_date: Optional[str] = Field(default=None, max_length=8)      # 직전 이자지급일
    after_date: Optional[str] = Field(default=None, max_length=8)       # 직후 이자지급일

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KsdBondIntApi(VendorRecordBase, table=True):
    """KSD_BOND_INT (SEIBRO OpenAPI ``getIntPayInfo``) -- 이자지급 조건, 종목당 1행.

    ksd_bond_int(web) 와 공통 필드가 4개뿐이고(``before_date`` ``after_date``
    ``int_pay_cycle_terms`` ``int_pay_cycle_tpcd``), 값은 전부 일치했다. 나머지는
    web 이 한글명으로 주는 것의 코드값이다.

    web 에 없는 것은 ``rvlt_severe_tpcd`` 하나뿐이고 ``coupon_rate`` 는 이미
    ksd_bond_info 에 있다. 커버리지도 일반회사채 한정이라, 코드값이 꼭 필요한
    경우가 아니면 web 쪽만으로 충분하다."""

    __tablename__ = "ksd_bond_int_api"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    isin: str = Field(index=True, max_length=12)                        # 표준코드 (잡 파라미터에서)
    coupon_rate: Optional[float] = Field(default=None)                  # 표면금리
    int_pay_way_tpcd: Optional[str] = Field(default=None, max_length=10)     # 이자지급방법
    int_pay_cycle_terms: Optional[int] = Field(default=None)                 # 이자지급주기(개월)
    int_pay_cycle_tpcd: Optional[str] = Field(default=None, max_length=10)   # 주기구분
    int_pay_tims_tpcd: Optional[str] = Field(default=None, max_length=10)    # 선급/후급
    acrint_pay_yn: Optional[str] = Field(default=None, max_length=1)         # 경과이자 지급여부
    bank_holiday_int_paydd_tpcd: Optional[str] = Field(default=None, max_length=10)  # 은행휴일 처리
    rvlt_sever_tpcd: Optional[str] = Field(default=None, max_length=10)      # 이자 절상/절사 구분
    before_date: Optional[str] = Field(default=None, max_length=8)      # 직전 이자지급일
    after_date: Optional[str] = Field(default=None, max_length=8)       # 직후 이자지급일

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KsdBondCredit(VendorRecordBase, table=True):
    """KSD_BOND_CREDIT_web (SEIBRO ``bondEntrByCreditCrdList``) -- 평가사별 신용등급.

    종목당 여러 행이다. ``valat_org_nm`` 이 평가사(KIS/KAP/NICE/...)이고 한 종목에
    보통 4개사가 붙는다. 표본에서 76종목 287행 -- 공모채 위주이고 사모 SPAC CB
    등은 0건이었다.

    ksd_bond_info_api 의 ``*_valat_grd_cd`` 4개 컬럼과 같은 주제지만 이쪽이
    사실상의 소스다: 저쪽은 코드값이고 대부분 비어 있는 반면 여기는 ``'AAA'``
    ``'AA+'`` 같은 등급 표기가 채워져 있다.

    ``val`` / ``rn_d`` 는 SEIBRO 화면 정렬용 내부 값이라 분석에 쓸 일은 없지만,
    1:1 추적을 위해 남긴다."""

    __tablename__ = "ksd_bond_credit"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    isin: str = Field(index=True, max_length=12)                        # 표준코드 (잡 파라미터에서)
    valat_org_nm: Optional[str] = Field(default=None, max_length=20)    # 평가사 (KIS/KAP/NICE)
    kis_apli_credit_grd_nm: Optional[str] = Field(default=None, max_length=20)  # 신용등급
    secn_price_std_dt: Optional[str] = Field(default=None, max_length=8)        # 평가기준일
    secn_kanm: Optional[str] = Field(default=None, max_length=50)       # 종류명
    val: Optional[str] = Field(default=None, max_length=10)             # 화면 정렬용
    rn_d: Optional[str] = Field(default=None, max_length=10)            # 화면 정렬용

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KsdBondPrin(VendorRecordBase, table=True):
    """KSD_BOND_PRIN_web (SEIBRO ``bondPrinXchgList``) -- 원리금 지급 내역.

    종목당 여러 행, 지급일 하나가 한 행이다. ``prin_tpcd_nm`` 이 ``'이자'`` 인지
    ``'원리금'`` 인지로 중간 이표와 만기 상환이 갈린다.

    ``pay_dt`` 는 실제 지급일, ``orgn_pay_dt`` 는 원래 예정일이다 -- 휴일이면
    둘이 어긋나고, ksd_bond_int 의 휴일 처리 규칙(직전/직후 영업일)이 그 차이를
    설명한다.

    요청 payload 의 ``PAGE_ON_CNT`` 가 그대로 반환 건수 상한이 된다(현재 10).
    긴 이력을 다 받으려면 그 값을 올리거나 ``PAGE_NUM`` 을 넘겨야 한다 -- 지금
    설정으로는 종목당 최근 10건까지만 들어온다.

    ``int_sum_amt2`` 는 소수점 이하가 30자리 넘게 오는 단가성 값이라 float 에
    담으면 유효숫자 16자리 밖은 잃는다. 금액 자체는 ``int_sum_amt`` 가 정수로
    갖고 있으므로 실무상 문제는 없다."""

    __tablename__ = "ksd_bond_prin"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    isin: str = Field(index=True, max_length=12)                        # 표준코드
    num: Optional[int] = Field(default=None)                            # 회차 (화면 순번)
    pay_dt: Optional[str] = Field(default=None, index=True, max_length=8)  # 지급일
    orgn_pay_dt: Optional[str] = Field(default=None, max_length=8)      # 당초 지급예정일
    prin_tpcd: Optional[str] = Field(default=None, max_length=10)       # 지급구분코드
    prin_tpcd_nm: Optional[str] = Field(default=None, max_length=20)    # 이자/원리금
    coupon_rate: Optional[float] = Field(default=None)                  # 적용금리
    prcp: Optional[int] = Field(default=None)                           # 원금
    int_sum_amt: Optional[int] = Field(default=None)                    # 이자합계
    int_sum_amt2: Optional[float] = Field(default=None)                 # 단가당 이자
    depo_qty: Optional[int] = Field(default=None)                       # 예탁수량

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KsdBondYield(VendorRecordBase, table=True):
    """KSD_BOND_YIELD_web (SEIBRO ``bondPratePList``) -- 일별 평가 수익률/단가.

    종목당 여러 행, 평가일 하나가 한 행이다. 이 계열에서 유일하게 **날마다 값이
    바뀌는 시계열**이라, 다른 테이블들이 마스터 성격인 것과 달리 반복 수집 대상이
    된다.

    요청 payload 의 ``PAGE_ON_CNT`` 가 반환 건수 상한이다(현재 15) -- 하루 한 번
    돌리면 15영업일치가 매번 겹쳐 들어오므로, ``save_mode='overwrite'`` 로 잡별
    스냅샷을 갈아끼우거나 (isin, secn_price_std_dt) 로 중복을 걸러야 한다."""

    __tablename__ = "ksd_bond_yield"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    isin: str = Field(index=True, max_length=12)                        # 표준코드
    secn_price_std_dt: Optional[str] = Field(default=None, index=True, max_length=8)  # 평가기준일
    num: Optional[int] = Field(default=None)                            # 화면 순번
    kis_aftax_unitp: Optional[float] = Field(default=None)              # 세후 단가
    kis_apli_prate: Optional[float] = Field(default=None)               # 적용 수익률

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KsdBondRedem(VendorRecordBase, table=True):
    """KSD_BOND_REDEM_web (SEIBRO ``optionXrcScheduleList``) -- 조기상환 *예정* 일정.

    종목당 여러 행, 상환 예정일 하나가 한 행이다. 콜/풋이 붙은 종목에만 있다 --
    표본 185종목 중 25종목, 다만 MBS 의 ``(콜/변)`` 종목들은 종목당 수십 건씩
    갖고 있어 행수는 금방 불어난다.

    이 테이블이 OpenAPI ``getBondOptionXrcInfo`` 를 대체한다. 그쪽은 조회에
    ``ERLY_RED_DT`` 를 요구하는데 그 날짜를 알 방법이 여기밖에 없어서 순환이 되고,
    실제로 여기서 얻은 날짜를 그대로 넣어도 0건이었다.

    ``xrc_begin_dt`` / ``xrc_expry_dt`` (행사 개시/만료일)는 대부분 비어 있고
    ``erly_red_dt`` 만 채워진다 -- 표본 2,010행 중 1,733행이 그랬다."""

    __tablename__ = "ksd_bond_redem"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    isin: str = Field(index=True, max_length=12)                        # 표준코드
    option_tpcd_nm: Optional[str] = Field(default=None, max_length=20)  # CALL/PUT
    erly_red_dt: Optional[str] = Field(default=None, index=True, max_length=8)  # 조기상환일
    xrc_begin_dt: Optional[str] = Field(default=None, max_length=8)     # 행사 개시일
    xrc_expry_dt: Optional[str] = Field(default=None, max_length=8)     # 행사 만료일

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KsdBondRedemHis(VendorRecordBase, table=True):
    """KSD_BOND_REDEM_HIS_web (SEIBRO ``optionXrcDetailsPList``) -- 조기상환 *실행* 이력.

    ksd_bond_redem 이 예정 일정이라면 이쪽은 실제로 상환된 내역이다. 같은
    ``(isin, erly_red_dt)`` 로 짝지어 보면 예정 대비 실제 행사분을 알 수 있다.

    ``erly_redamt_val`` 이 상환금액, ``xrc_ratio`` 가 행사비율(``.24`` = 24%)이다."""

    __tablename__ = "ksd_bond_redem_his"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    isin: str = Field(index=True, max_length=12)                        # 표준코드
    option_tpcd_nm: Optional[str] = Field(default=None, max_length=20)  # CALL/PUT
    erly_red_dt: Optional[str] = Field(default=None, index=True, max_length=8)  # 조기상환일
    num: Optional[int] = Field(default=None)                            # 회차
    erly_redamt_val: Optional[int] = Field(default=None)                # 조기상환금액
    xrc_ratio: Optional[float] = Field(default=None)                    # 행사비율

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KsdBondStopt(VendorRecordBase, table=True):
    """KSD_BOND_STOPT_web (SEIBRO ``exerInfoView``) -- 주식 관련 옵션(CB/EB/BW) 조건.

    종목당 1행이고, ``particul_bond_kind`` 가 ``'CB'``/``'EB'`` 인 종목에만 있다 --
    표본 185종목 중 26종목. 변동금리채는 전부 ``'주식관련해당사항없음'`` 이라 0건이다.

    ``xrc_stk_isin`` 이 전환/교환 대상 주식의 ISIN 이라, 이 컬럼으로 채권과 주식이
    이어진다. ``xrc_price`` 대비 ``curday_cpri``(당일 종가)가 전환가치 판단의
    출발점이다.

    응답에 ISIN 이 없어 잡 파라미터에서 찍는다."""

    __tablename__ = "ksd_bond_stopt"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    isin: str = Field(index=True, max_length=12)                        # 채권 표준코드 (잡 파라미터에서)
    xrc_stk_isin: Optional[str] = Field(default=None, index=True, max_length=12)  # 대상 주식 ISIN
    kor_secn_nm: Optional[str] = Field(default=None, max_length=200)    # 대상 주식명
    wrtb_isin: Optional[str] = Field(default=None, max_length=12)       # 신주인수권 ISIN
    xrc_price: Optional[float] = Field(default=None)                    # 행사가
    xrc_ratio: Optional[float] = Field(default=None)                    # 행사비율
    curday_cpri: Optional[float] = Field(default=None)                  # 당일 종가
    shar_deli_std_cd: Optional[str] = Field(default=None, max_length=10)     # 주식교부기준
    shar_deli_std_cd_nm: Optional[str] = Field(default=None, max_length=30)  # 주식교부기준명
    payin_means_tpcd: Optional[str] = Field(default=None, max_length=10)     # 납입수단
    payin_means_tpcd_nm: Optional[str] = Field(default=None, max_length=30)  # 납입수단명
    scap_payin_place: Optional[str] = Field(default=None, max_length=100)    # 납입장소

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KsdBondStoptPrc(VendorRecordBase, table=True):
    """KSD_BOND_STOPT_PRC_web (SEIBRO ``exerPricePList``) -- 행사가 변경 이력.

    종목당 여러 행. ``bf_price`` -> ``xrc_price`` 로 ``apli_dt`` 부터 바뀐다는
    뜻이라, 리픽싱(전환가 조정) 이력이 그대로 남는다. ksd_bond_stopt 의
    ``xrc_price`` 는 현재값이고 여기가 그 변천사다."""

    __tablename__ = "ksd_bond_stopt_prc"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    isin: str = Field(index=True, max_length=12)                        # 표준코드
    num: Optional[int] = Field(default=None)                            # 회차
    apli_dt: Optional[str] = Field(default=None, index=True, max_length=8)  # 적용일
    bf_price: Optional[float] = Field(default=None)                     # 변경 전 행사가
    xrc_price: Optional[float] = Field(default=None)                    # 변경 후 행사가
    xrc_ratio: Optional[float] = Field(default=None)                    # 행사비율

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KsdBondStoptHis(VendorRecordBase, table=True):
    """KSD_BOND_STOPT_HIS_web (SEIBRO ``exerDetailPList``) -- 권리행사 실행 이력.

    종목당 여러 행. 전환/교환이 실제로 일어난 회차마다 발행가(``issuprc``),
    발행금액/수량, 신주 상장일(``list_dt``)이 남는다. 상장 전 회차는 ``list_dt``
    와 ``lday_cpri`` 가 비어 있다 -- 표본 104행 중 45행이 그랬다."""

    __tablename__ = "ksd_bond_stopt_his"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    isin: str = Field(index=True, max_length=12)                        # 표준코드
    num: Optional[int] = Field(default=None)                            # 회차
    rgt_std_dt: Optional[str] = Field(default=None, index=True, max_length=8)  # 권리기준일
    issuprc: Optional[float] = Field(default=None)                      # 발행가
    issu_amt: Optional[int] = Field(default=None)                       # 발행금액
    issu_qty: Optional[int] = Field(default=None)                       # 발행수량
    list_dt: Optional[str] = Field(default=None, max_length=8)          # 상장일
    lday_cpri: Optional[float] = Field(default=None)                    # 상장일 종가

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class MstBond(SQLModel, table=True):
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
