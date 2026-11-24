"""한국투자증권(KIS) 오픈API 결과 표."""

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
    ResultTable,
    VendorRecordBase,
)




class KisFutoptPrice(ResultTable, SQLModel, table=True):
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


class FoIdxCodeMst(ResultTable, SQLModel, table=True):
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


class KisFutoptChart(ResultTable, SQLModel, table=True):
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


class KisFutoptDaily(ResultTable, SQLModel, table=True):
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


class KisIndexDaily(ResultTable, SQLModel, table=True):
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


class KisEtf(ResultTable, VendorRecordBase, table=True):
    """KIS_ETF_PDF 의 ``output1`` -- ETF 한 종목의 시세와 NAV.

    같은 응답의 output2(구성종목)와 짝이다. 한 요청이 두 표를 채우는 다른
    KIS API 들과 같은 구조인데, 그쪽은 output1 을 api_rst 로 흘려보내는 데
    반해 여기는 받아둔다 -- NAV 시가/고가/저가와 CU 단위 증권 수는 이 화면
    말고는 들어오는 곳이 없다.

    ``short_code``(ETF 단축코드)와 ``snap_at`` 은 응답에 없어 잡 파라미터에서
    찍는다(ApiMst.key_params_list). 조회일을 받지도 돌려주지도 않는 현재 시점
    화면이라, 언제 본 값인지는 ``NOW`` 가 찍는 그 시각뿐이다 -- 그래서 이 API
    는 날짜로 작업을 가르지 않고 **반복 작업 하나**로 돌린다(is_repeated_api).
    같은 날 다시 받는 것이 정정이 아니라 다른 시각의 관측이므로 save_mode 도
    append 다.

    한 종목이 하루에 몇 벌 쌓일 수 있다. 하루치는 ``snap_at`` 의 범위로
    고른다(``BETWEEN '20260831000000' AND '20260831235959'``) -- 날짜만 떼어
    둔 컬럼은 두지 않는다. 같은 값을 두 벌 적으면서 어긋날 여지만 생기고,
    범위 조건이 이미 snap_at 의 인덱스를 그대로 탄다."""

    __tablename__ = "kis_etf"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    short_code: Optional[str] = Field(default=None, index=True, max_length=20)  # ETF 단축코드
    snap_at: Optional[str] = Field(default=None, index=True, max_length=14)     # 관측 시각 YYYYMMDDHH24MISS

    stck_prpr: Optional[float] = Field(default=None)            # 현재가
    prdy_vrss: Optional[float] = Field(default=None)            # 전일 대비
    prdy_vrss_sign: Optional[str] = Field(default=None, max_length=1)   # 전일 대비 부호
    prdy_ctrt: Optional[float] = Field(default=None)            # 전일 대비율
    etf_cnfg_issu_avls: Optional[int] = Field(default=None)     # ETF구성종목시가총액
    nav: Optional[float] = Field(default=None)                  # NAV
    nav_prdy_vrss_sign: Optional[str] = Field(default=None, max_length=1)  # NAV 전일 대비 부호
    nav_prdy_vrss: Optional[float] = Field(default=None)        # NAV 전일 대비
    nav_prdy_ctrt: Optional[float] = Field(default=None)        # NAV 전일 대비율
    etf_ntas_ttam: Optional[int] = Field(default=None)          # ETF 순자산 총액
    prdy_clpr_nav: Optional[float] = Field(default=None)        # NAV 전일 종가
    oprc_nav: Optional[float] = Field(default=None)             # NAV 시가
    hprc_nav: Optional[float] = Field(default=None)             # NAV 고가
    lprc_nav: Optional[float] = Field(default=None)             # NAV 저가
    etf_cu_unit_scrt_cnt: Optional[int] = Field(default=None)   # CU 단위 증권 수
    etf_cnfg_issu_cnt: Optional[int] = Field(default=None)      # 구성 종목 수 (공시 기준)

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KisEtfPdf(ResultTable, VendorRecordBase, table=True):
    """KIS_ETF_PDF 의 ``output2`` -- ETF 구성종목.

    **전량이 아니라 상위 30 종목이다.** 확인한 값으로, KODEX 200 은 공시
    구성종목이 202 개인데 30 행(비중합 84.2%)만 오고, 편입 종목이 해외인
    TIGER 미국S&P500 은 0 행이다. 응답 헤더의 ``tr_cont`` 가 비어 있어 이어받을
    수단도 없다. 그러니 운용사 PDF(amc_etf_pdf 등)를 대신하지 못한다 -- 대신
    구성종목마다 **현재가·시가총액·거래량**이 함께 오는데, 그건 운용사 PDF 가
    주지 않는 값이라 서로 채워주는 관계다.

    ``short_code`` 는 구성종목이 아니라 **그 종목을 담고 있는 ETF** 의 코드다.
    응답에는 없고 잡 파라미터에서 찍는다(kis_index_daily 와 같은 이유). 구성
    종목 자신의 코드는 ``stck_shrn_iscd`` 다 -- 둘을 헷갈리면 표 전체가
    뒤집히므로 이름을 나눠 두었다.

    ``etf_cu_unit_scrt_cnt`` 는 KIS 문서의 output2 목록에 없는데 실제로는
    내려온다. 문서가 아니라 응답을 따랐다."""

    __tablename__ = "kis_etf_pdf"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    short_code: Optional[str] = Field(default=None, index=True, max_length=20)  # 담고 있는 ETF 의 단축코드
    snap_at: Optional[str] = Field(default=None, index=True, max_length=14)     # 관측 시각 YYYYMMDDHH24MISS

    stck_shrn_iscd: Optional[str] = Field(default=None, index=True, max_length=20)  # 구성종목 단축코드
    hts_kor_isnm: Optional[str] = Field(default=None, max_length=100)   # 구성종목명
    stck_prpr: Optional[float] = Field(default=None)            # 현재가
    prdy_vrss: Optional[float] = Field(default=None)            # 전일 대비
    prdy_vrss_sign: Optional[str] = Field(default=None, max_length=1)   # 전일 대비 부호
    prdy_ctrt: Optional[float] = Field(default=None)            # 전일 대비율
    acml_vol: Optional[int] = Field(default=None)               # 누적 거래량
    acml_tr_pbmn: Optional[int] = Field(default=None)           # 누적 거래 대금
    etf_cu_unit_scrt_cnt: Optional[int] = Field(default=None)   # CU 단위 증권 수
    tday_rsfl_rate: Optional[float] = Field(default=None)       # 당일 등락 비율
    prdy_vrss_vol: Optional[int] = Field(default=None)          # 전일 대비 거래량
    tr_pbmn_tnrt: Optional[float] = Field(default=None)         # 거래대금 회전율
    hts_avls: Optional[int] = Field(default=None)               # 시가총액
    etf_cnfg_issu_avls: Optional[int] = Field(default=None)     # ETF구성종목시가총액
    etf_cnfg_issu_rlim: Optional[float] = Field(default=None)   # ETF구성종목비중
    etf_vltn_amt: Optional[int] = Field(default=None)           # ETF구성종목내평가금액

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))
