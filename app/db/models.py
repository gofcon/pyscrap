from datetime import datetime
from typing import Optional

from sqlalchemy import CheckConstraint, Column, DateTime, Identity, Integer, func
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
