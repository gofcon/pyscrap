"""한국거래소(KRX) 오픈API · 데이터마켓 결과 표."""

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




class KrxFutDaily(ResultTable, VendorRecordBase, table=True):
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


class KrxOptDaily(ResultTable, VendorRecordBase, table=True):
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


class KrxEtfDaily(ResultTable, VendorRecordBase, table=True):
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


# krx_deriv_info 의 두 숫자 칸은 MDC 화면이 천 단위 콤마를 넣어 보낸다
# ("250,000", "1,000.00"). 이름 칸에도 콤마가 있으므로(행사가가 이름 안에
# 박혀 있다) 값 전체를 훑을 수는 없고, 숫자로 받을 칸만 골라 떼어낸다.
_KRX_DERIV_NUMERIC = {"setlmult", "exer_prc"}


class KrxDerivInfo(ResultTable, VendorRecordBase, table=True):
    """Typed output table for KRX_DERIV_INFO ([15004] 전종목 기본정보,
    ``dbms/MDC/STAT/standard/MDCSTAT12801``) -- one row per *currently
    listed* derivative contract, field names kept as KRX's own (lower-cased).

    Why this table exists: KIS publishes only what is listed today and spells
    a contract its own way, which is what app.services.discovery has to walk
    a strike ladder to reconstruct. This endpoint hands over the same set as
    a list -- short code, both dates, multiplier, right type and strike -- so
    the parts that were being guessed are simply read.

    Overwritten rather than accumulated (save_mode='overwrite' against a
    job_id fixed per product), the way the ETF manager lists are: the table
    is one current copy of what is listed, and the history lives in mst_fuopt,
    which sp_mst_fuopt_sync merges into.

    ``prod_id`` is the request's own ``prodId`` (stamped from the job's
    params, see ApiMst.key_params_list), not a response field -- the reply
    does not say which product it answered for.

    Dates arrive as ``2026/09/10`` and are kept that way rather than folded to
    YYYYMMDD: this is the source's record, and the sync that reads it is where
    a real DATE belongs."""

    __tablename__ = "krx_deriv_info"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    # 요청 파라미터에서 각인된다 (응답에는 없다)
    prod_id: str = Field(index=True, max_length=20)                 # 상품구분 (KRDRVOPK2I ...)

    isu_cd: str = Field(max_length=12)                              # 표준코드 (ISIN)
    # mst_fuopt 와 잇는 열쇠다. KRX 표기이고 KIS 의 단축코드와 다르다
    # (12월을 C 로 쓰는 등) -- mst_fuopt.short_code 주석 참고.
    isu_srt_cd: str = Field(index=True, max_length=20)              # 단축코드
    isu_nm: Optional[str] = Field(default=None, max_length=200)     # 한글종목명
    isu_abbrv: Optional[str] = Field(default=None, max_length=100)  # 한글종목약명
    isu_eng_nm: Optional[str] = Field(default=None, max_length=200) # 영문종목명

    list_dd: Optional[str] = Field(default=None, max_length=10)     # 상장일
    lsttrd_dd: Optional[str] = Field(default=None, max_length=10)   # 최종거래일 (만기)
    lst_setl_dd: Optional[str] = Field(default=None, max_length=10) # 최종결제일
    uly_tp_nm: Optional[str] = Field(default=None, max_length=30)   # 기초자산유형
    setlmult: Optional[float] = Field(default=None)                 # 거래승수
    rght_tp_nm: Optional[str] = Field(default=None, max_length=10)  # 권리유형 (콜옵션/풋옵션/-)
    exer_prc: Optional[float] = Field(default=None)                 # 행사가격

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))

    @model_validator(mode="before")
    @classmethod
    def _strip_thousands(cls, data: Any) -> Any:
        """Drop the thousands separators the MDC screens send inside numbers.

        VendorRecordBase deals with the two habits every KRX source has; this
        one is particular to the screens behind data.krx.co.kr, so it stays
        with the table that reads them rather than widening the shared base."""
        if not isinstance(data, dict):
            return data
        return {k: (v.replace(",", "") if k.lower() in _KRX_DERIV_NUMERIC and isinstance(v, str) else v)
                for k, v in data.items()}


class KrxDerivHist(ResultTable, VendorRecordBase, table=True):
    """Typed output table for KRX_DERIV_HIST -- the contract finder behind the
    derivatives screens (``dbms/comm/finder/finder_drvprodisu``), asked one
    (product, expiry year) at a time.

    This is the table that ends the guessing. [15004] 전종목 기본정보
    (krx_deriv_info) lists only what is listed *today*, and KIS publishes only
    the same, which is why app.services.discovery had to walk a strike ladder
    to rebuild an expired contract's code -- 38,700 of mst_fuopt's 62,919 rows
    were reconstructed that way. This endpoint answers for expiries going back
    to 1996 (KOSPI200 futures) and 1997 (options), so the code, the name and
    the strike are read rather than inferred.

    Four fields is all it gives -- no dates, no multiplier -- so it is the
    identity half of a contract; krx_deriv_info carries the rest for whatever
    is currently listed, and mst_fuopt keeps what history already established.

    Field names are the finder's own, lower-cased by VendorRecordBase: hence
    ``codename`` and ``expmm`` rather than code_name/exp_mm.

    A past year's answer never changes, so its builder runs on the 'once'
    cycle: the daily feed of newly listed contracts is krx_deriv_info's job."""

    __tablename__ = "krx_deriv_hist"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    # 요청 파라미터에서 각인된다 (응답에는 없다)
    prod_id: str = Field(index=True, max_length=20)                 # 상품구분
    exp_yy: Optional[str] = Field(default=None, max_length=4)       # 만기 연도

    full_code: Optional[str] = Field(default=None, max_length=12)   # 표준코드 (ISIN)
    short_code: str = Field(index=True, max_length=20)              # 단축코드 (KRX 표기)
    codename: Optional[str] = Field(default=None, max_length=200)   # 종목명 (행사가 포함)
    expmm: Optional[str] = Field(default=None, index=True, max_length=7)  # 만기 (YYYY/MM)

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KrxStockBase(ResultTable, VendorRecordBase, table=True):
    """Typed output table for KRX_STOCK_BASE_INFO (``sto/stk_isu_base_info``)
    -- one row per listed share per query date, field names kept as KRX's own
    (lower-cased).

    The instrument side of what krx_etf_daily is for funds: no prices, just
    what the share *is* -- when it listed, which board, what kind of stock,
    par value and how many are outstanding. Dated rather than static because
    the last of those moves (a capital increase changes list_shrs), so a row
    is only true as of its ``bas_dd``.

    KOSPI only, which is what the key is authorised for: the sibling
    endpoints for KOSDAQ (``sto/ksq_bydd_trd``) and KONEX answer 401. Nothing
    in the shape assumes it, so a widened key just starts filling
    ``mkt_tp_nm`` with more than one value.

    ``parval`` is text, not a number: a share with no par value comes back as
    '무액면' rather than 0 or blank."""

    __tablename__ = "krx_stock_base"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    bas_dd: str = Field(index=True, max_length=8)                      # 기준일자 (잡 파라미터)

    isu_cd: str = Field(max_length=12)                                 # 표준코드 (ISIN)
    isu_srt_cd: str = Field(index=True, max_length=10)                 # 단축코드
    isu_nm: Optional[str] = Field(default=None, max_length=100)        # 한글 종목명
    isu_abbrv: Optional[str] = Field(default=None, max_length=60)      # 한글 종목약명
    isu_eng_nm: Optional[str] = Field(default=None, max_length=120)    # 영문 종목명

    list_dd: Optional[str] = Field(default=None, max_length=8)         # 상장일
    mkt_tp_nm: Optional[str] = Field(default=None, max_length=20)      # 시장구분 (KOSPI)
    secugrp_nm: Optional[str] = Field(default=None, max_length=40)     # 증권구분 (주권/외국주권/투자회사 ...)
    sect_tp_nm: Optional[str] = Field(default=None, max_length=40)     # 소속부
    kind_stkcert_tp_nm: Optional[str] = Field(default=None, max_length=20)  # 주식종류 (보통주/우선주 ...)
    parval: Optional[str] = Field(default=None, max_length=20)         # 액면가 ('무액면' 이 온다)
    list_shrs: Optional[int] = Field(default=None)                     # 상장주식수

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


# data.krx.co.kr 화면이 숫자를 사람이 읽는 모양으로 보낸다: 천 단위 콤마
# ("1,163"), 없는 값은 '-'. 콤마가 붙은 것만 골라 떼는 이유는 종목명 안에도
# 콤마가 있기 때문이다 ("코스피200 C 202609 1,000.0" -- 행사가가 이름에 박혀
# 있다). 그래서 값 전체를 훑지 않고 숫자로만 이뤄진 문자열에만 손댄다.
_MDC_NUMERIC_RE = re.compile(r"^-?[\d,]+(?:\.\d+)?$")


class KrxStockDaily(ResultTable, VendorRecordBase, table=True):
    """Typed output table for KRX_STOCK_INFO (``sto/stk_bydd_trd``) -- one row
    per listed share per trading day, field names kept as KRX's own.

    The share-price counterpart to krx_etf_daily/krx_etn_daily. KOSPI only:
    the KOSDAQ and KONEX endpoints answer 401 for this key (see
    KrxStockBase), so ``mkt_nm`` carries one value today and would simply
    carry more if that changed."""

    __tablename__ = "krx_stock_daily"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    bas_dd: str = Field(index=True, max_length=8)                     # 기준일자
    isu_cd: str = Field(index=True, max_length=20)                    # 단축코드
    isu_nm: Optional[str] = Field(default=None, max_length=100)       # 종목명
    mkt_nm: Optional[str] = Field(default=None, max_length=20)        # 시장구분
    sect_tp_nm: Optional[str] = Field(default=None, max_length=40)    # 소속부

    tdd_clsprc: Optional[float] = Field(default=None)                 # 종가
    cmpprevdd_prc: Optional[float] = Field(default=None)              # 대비
    fluc_rt: Optional[float] = Field(default=None)                    # 등락률
    tdd_opnprc: Optional[float] = Field(default=None)                 # 시가
    tdd_hgprc: Optional[float] = Field(default=None)                  # 고가
    tdd_lwprc: Optional[float] = Field(default=None)                  # 저가
    acc_trdvol: Optional[int] = Field(default=None)                   # 거래량
    acc_trdval: Optional[int] = Field(default=None)                   # 거래대금
    mktcap: Optional[int] = Field(default=None)                       # 시가총액
    list_shrs: Optional[int] = Field(default=None)                    # 상장주식수

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KrxEtnDaily(ResultTable, VendorRecordBase, table=True):
    """Typed output table for KRX_ETN_INFO (``etp/etn_bydd_trd``) -- one row
    per ETN per trading day, field names kept as KRX's own.

    Same shape as krx_etf_daily with the ETN's own two: ``per1secu_indic_val``
    (증권당 지표가치) and ``indic_val_amt`` (지표가치총액), which are what an
    ETN has instead of a fund's NAV."""

    __tablename__ = "krx_etn_daily"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    bas_dd: str = Field(index=True, max_length=8)                     # 기준일자
    isu_cd: str = Field(index=True, max_length=20)                    # 단축코드
    isu_nm: Optional[str] = Field(default=None, max_length=100)       # 종목명

    tdd_clsprc: Optional[float] = Field(default=None)                 # 종가
    cmpprevdd_prc: Optional[float] = Field(default=None)              # 대비
    fluc_rt: Optional[float] = Field(default=None)                    # 등락률
    per1secu_indic_val: Optional[float] = Field(default=None)         # 증권당 지표가치
    tdd_opnprc: Optional[float] = Field(default=None)                 # 시가
    tdd_hgprc: Optional[float] = Field(default=None)                  # 고가
    tdd_lwprc: Optional[float] = Field(default=None)                  # 저가
    acc_trdvol: Optional[int] = Field(default=None)                   # 거래량
    acc_trdval: Optional[int] = Field(default=None)                   # 거래대금
    mktcap: Optional[int] = Field(default=None)                       # 시가총액
    indic_val_amt: Optional[int] = Field(default=None)                # 지표가치총액
    list_shrs: Optional[int] = Field(default=None)                    # 상장증권수
    idx_ind_nm: Optional[str] = Field(default=None, max_length=200)   # 기초지수명
    obj_stkprc_idx: Optional[float] = Field(default=None)             # 지수 종가
    cmpprevdd_idx: Optional[float] = Field(default=None)              # 지수 대비
    fluc_rt_idx: Optional[float] = Field(default=None)                # 지수 등락률

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KrxEtfList(ResultTable, MdcRecordBase, table=True):
    """Typed output table for KRX_ETF_LIST (MDCSTAT04601, ETF 종목 목록) --
    one row per listed ETF per query date.

    The open API's ETF feed (krx_etf_daily) has prices but no ISIN, and no
    ISIN is why 1,051 of mst_etf's 1,163 rows could not be asked for by the
    holdings endpoint. This screen gives both codes side by side, which is
    what sp_mst_etf_sync now folds into mst_etf.isin.

    Carries the fund's own facts as well -- manager, index, replication
    method, total fee, CU size -- which no other source collected here has."""

    __tablename__ = "krx_etf_list"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    bas_dd: str = Field(index=True, max_length=8)                     # 기준일자 (잡 파라미터)
    isu_cd: str = Field(max_length=12)                                # 표준코드 (ISIN)
    isu_srt_cd: str = Field(index=True, max_length=10)                # 단축코드
    isu_nm: Optional[str] = Field(default=None, max_length=200)       # 한글 종목명
    isu_abbrv: Optional[str] = Field(default=None, max_length=100)    # 한글 종목약명
    isu_eng_nm: Optional[str] = Field(default=None, max_length=200)   # 영문 종목명
    list_dd: Optional[str] = Field(default=None, max_length=10)       # 상장일
    etf_obj_idx_nm: Optional[str] = Field(default=None, max_length=200)      # 기초지수명
    idx_calc_inst_nm1: Optional[str] = Field(default=None, max_length=100)   # 지수 산출기관
    idx_calc_inst_nm2: Optional[str] = Field(default=None, max_length=100)   # 지수 구분
    etf_replica_methd_tp_cd: Optional[str] = Field(default=None, max_length=40)  # 복제방법
    idx_mkt_clss_nm: Optional[str] = Field(default=None, max_length=40)      # 지수 시장분류
    idx_asst_clss_nm: Optional[str] = Field(default=None, max_length=40)     # 지수 자산분류
    com_abbrv: Optional[str] = Field(default=None, max_length=100)     # 운용사
    cu_qty: Optional[int] = Field(default=None)                        # CU 수량
    list_shrs: Optional[int] = Field(default=None)                     # 상장좌수
    etf_tot_fee: Optional[float] = Field(default=None)                 # 총보수
    tax_tp_cd: Optional[str] = Field(default=None, max_length=20)      # 과세유형

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KrxEtfPdf(ResultTable, MdcRecordBase, table=True):
    """Typed output table for KRX_ETF_PDF (MDCSTAT05001) -- one row per
    constituent of one ETF on one date (Portfolio Deposit File).

    Keyed by the ETF being asked about (``isu_cd``, stamped from the job's
    own parameters) and the constituent (``compst_isu_cd``): the response
    only describes the holding, not whose holding it is.

    Values a screen leaves blank arrive as '-' rather than empty, which is
    what MdcRecordBase folds to NULL -- common here, since an ETF that has
    not published its weights yet still lists its constituents."""

    __tablename__ = "krx_etf_pdf"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    bas_dd: str = Field(index=True, max_length=8)                     # 기준일자 (잡 파라미터)
    isu_cd: str = Field(index=True, max_length=12)                    # 대상 ETF 의 표준코드 (잡 파라미터)

    compst_isu_cd: Optional[str] = Field(default=None, max_length=20)   # 구성종목 단축코드
    compst_isu_cd2: Optional[str] = Field(default=None, max_length=12)  # 구성종목 표준코드
    compst_isu_nm: Optional[str] = Field(default=None, max_length=200)  # 구성종목명
    mkt_id: Optional[str] = Field(default=None, max_length=10)          # 시장
    secugrp_id: Optional[str] = Field(default=None, max_length=10)      # 증권구분
    compst_isu_cu1_shrs: Optional[float] = Field(default=None)          # CU 1좌당 구성수량
    valu_amt: Optional[int] = Field(default=None)                       # 평가금액
    compst_amt: Optional[int] = Field(default=None)                     # 구성금액
    compst_rto: Optional[float] = Field(default=None)                   # 구성비중

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KrxDerivExpyy(ResultTable, MdcRecordBase, table=True):
    """Typed output table for KRX_DERIV_EXPYY (``dbms/comm/component/drv_exp_yy``)
    -- which expiry years a derivative product can be asked about.

    A dropdown's contents rather than data in its own right, and that is
    exactly its use: KRX_DERIV_HIST fans out over (product x year), and the
    years differ per product (KOSPI200 options go back to 1997, the Monday
    weekly to 2023). Kept as a table so that fan-out reads a query instead of
    a list someone pasted.

    ``value`` and ``name`` are the finder's own field names, both carrying
    the year; ``value`` is empty on the leading 전체 row, which
    VendorRecordBase folds to NULL."""

    __tablename__ = "krx_deriv_expyy"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    prod_id: str = Field(index=True, max_length=20)                    # 상품구분 (잡 파라미터)
    value: Optional[str] = Field(default=None, max_length=4)           # 만기연도
    name: Optional[str] = Field(default=None, max_length=20)           # 표시명

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KrxIndexDaily(ResultTable, MdcRecordBase, table=True):
    """Typed output table for KRX_INDEX_KOSPI (``idx/kospi_dd_trd``) -- one
    row per KOSPI-family index per trading day, field names kept as KRX's own.

    KOSPI family only, which is what the key is authorised for: idx/krx_dd_trd
    and idx/kosdaq_dd_trd answer 401. The screen (MDCSTAT00101) covers all
    four families if that is ever needed, at the cost of a login.

    No index code -- the feed names an index and nothing more, which is what
    mst_index exists to resolve. Join on ``idx_nm``.

    The 외국주포함 rows carry volume and market cap with no index value at
    all, so every price column is nullable; blanks fold to NULL through
    VendorRecordBase."""

    __tablename__ = "krx_index_daily"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    bas_dd: str = Field(index=True, max_length=8)                      # 기준일자
    idx_clss: Optional[str] = Field(default=None, max_length=20)       # 계열 (KOSPI)
    idx_nm: str = Field(index=True, max_length=100)                    # 지수명

    clsprc_idx: Optional[float] = Field(default=None)                  # 종가
    cmpprevdd_idx: Optional[float] = Field(default=None)               # 대비
    fluc_rt: Optional[float] = Field(default=None)                     # 등락률
    opnprc_idx: Optional[float] = Field(default=None)                  # 시가
    hgprc_idx: Optional[float] = Field(default=None)                   # 고가
    lwprc_idx: Optional[float] = Field(default=None)                   # 저가
    acc_trdvol: Optional[int] = Field(default=None)                    # 거래량
    acc_trdval: Optional[int] = Field(default=None)                    # 거래대금
    mktcap: Optional[int] = Field(default=None)                        # 시가총액

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))
