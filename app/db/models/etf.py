"""자산운용사가 공시하는 ETF 목록과 구성종목."""

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




class KodexEtf(ResultTable, VendorRecordBase, table=True):
    """KODEX_ETF_LIST (삼성자산운용 ``/api/v1/kodex/product.do``) -- KODEX ETF
    상품 목록.

    유니버스가 아니라 **코드 대응표**로 쓴다. 구성종목 엔드포인트가 ISIN 도
    단축코드도 아닌 삼성 자체 상품코드(``fid``, 예 ``2ETF01``)로만 조회되기
    때문에, 그 코드를 알 방법이 이 목록뿐이다. ``stkticker``(단축코드)가
    krx_etf_daily.isu_cd 와 같은 값이라 그쪽으로 이어진다. TIGER 처럼 ISIN 을
    그대로 받는 운용사에는 이런 표가 필요 없다.

    응답이 봉투 없는 최상위 배열이라 셀렉터가 ``"."`` 이다
    (app.scrapers.dynamic._extract_json_records 참고).

    38개 필드 중 화면용은 뺐다 -- ``bgColor`` ``iconNm`` ``iconPath``
    ``visualPath`` ``smallIcon*`` ``mapOrder`` ``likeYn`` ``viewCnt``, 그리고
    스칼라 컬럼에 담기지 않는 ``chart``(배열). 정의하지 않은 키는 pydantic 이
    조용히 버리므로 따로 걸러낼 필요가 없다.

    ``totalCnt`` 도 뺐다 -- 행마다 같은 값(241)이 반복되는 페이징 부산물이지
    상품의 속성이 아니다.

    매번 통째로 갈아끼운다(``save_mode='overwrite'``). 상장/상폐 이력은
    krx_etf_daily 가 날마다 남기므로 여기서 또 쌓을 이유가 없다."""

    __tablename__ = "kodex_etf"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    fid: str = Field(index=True, max_length=20)                        # 삼성 상품코드 (2ETF01)
    stkticker: Optional[str] = Field(default=None, index=True, max_length=20)  # 단축코드 (069500)
    fnm: Optional[str] = Field(default=None, max_length=200)           # 상품명
    listd: Optional[str] = Field(default=None, max_length=8)           # 상장일
    gijunymd: Optional[str] = Field(default=None, index=True, max_length=8)  # 기준일

    typenm: Optional[str] = Field(default=None, max_length=100)        # 유형 (시장지수 등)
    typelnm: Optional[str] = Field(default=None, max_length=100)       # 대분류 (국내주식 등)
    dcyn: Optional[str] = Field(default=None, max_length=20)           # 개인/기관 구분
    irpyn: Optional[str] = Field(default=None, max_length=20)          # 퇴직연금 편입 가능
    monthlyyn: Optional[str] = Field(default=None, max_length=20)      # 월배당 여부

    nav: Optional[float] = Field(default=None)                         # 순자산가치
    curp: Optional[float] = Field(default=None)                        # 현재가
    basp: Optional[float] = Field(default=None)                        # 기준가
    basrp: Optional[float] = Field(default=None)                       # 기준가 대비
    basrprt: Optional[float] = Field(default=None)                     # 기준가 등락률
    risep: Optional[float] = Field(default=None)                       # 전일대비
    riseprt: Optional[float] = Field(default=None)                     # 등락률

    yieldweek: Optional[float] = Field(default=None)                   # 1주 수익률
    yieldmon1: Optional[float] = Field(default=None)                   # 1개월
    yieldmon3: Optional[float] = Field(default=None)                   # 3개월
    yieldmon6: Optional[float] = Field(default=None)                   # 6개월
    yieldyear: Optional[float] = Field(default=None)                   # 연초 이후
    yieldyear1: Optional[float] = Field(default=None)                  # 1년
    yieldyear3: Optional[float] = Field(default=None)                  # 3년
    yieldyear5: Optional[float] = Field(default=None)                  # 5년
    yieldlist: Optional[float] = Field(default=None)                   # 상장 이후

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class KodexEtfPdf(ResultTable, VendorRecordBase, table=True):
    """KODEX_ETF_PDF (삼성자산운용 ``/api/v1/kodex/product-pdf/{fId}.do``) --
    KODEX ETF 구성종목. ETF 하나·하루에 구성종목 한 행씩.

    ``itmno`` 가 구성종목 코드인데 주식이면 단축코드(``005930``), 현금성이면
    별도 코드(``KRD010010001`` 원화예금)가 와서 길이가 다르다. 그 현금 행은
    ``ratio``/``curp`` 가 비어 있는데, 빈 문자열을 NULL 로 접는 것이
    VendorRecordBase 를 쓰는 이유다.

    ``fid`` 와 ``gijunymd`` 는 응답 행에 없고 잡 파라미터에서 찍는다
    (ApiMst.key_params_list). 어느 ETF 의 언제치인지는 이 두 컬럼으로만 알 수
    있다. ``gijunymd`` 가 점 찍힌 ``2026.08.28`` 인 것은 조회 파라미터 형식이
    그렇기 때문이고, 잡 파라미터를 그대로 남긴다는 원칙을 따랐다 --
    krx_etf_daily.bas_dd(``20260828``) 와 조인할 때는 한쪽을 변환해야 한다.

    ETF 의 ISIN 은 여기 없다. ``fid`` -> kodex_etf.stkticker ->
    krx_etf_daily.isu_cd 로 이어진다."""

    __tablename__ = "kodex_etf_pdf"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    fid: str = Field(index=True, max_length=20)                        # 삼성 상품코드 (잡 파라미터에서)
    gijunymd: Optional[str] = Field(default=None, index=True, max_length=10)  # 기준일 (2026.08.28)

    # 12자로 잡았다가 해외 편입분에서 터졌다 -- 국내는 단축코드 6자,
    # 현금성은 KRD010010001(12자)인데 해외는 블룸버그 티커가 그대로 온다
    # ('939 HK Equity', 'TATACONS IN Equity' 18자). 표본 648행의 최대가 18이라
    # 여유를 둔다.
    itmno: Optional[str] = Field(default=None, index=True, max_length=50)  # 구성종목 코드
    secnm: Optional[str] = Field(default=None, max_length=200)         # 구성종목명
    applyq: Optional[float] = Field(default=None)                      # 수량 (CU 1좌당)
    evala: Optional[float] = Field(default=None)                       # 평가금액
    ratio: Optional[float] = Field(default=None)                       # 비중(%)
    curp: Optional[float] = Field(default=None)                        # 현재가
    risep: Optional[float] = Field(default=None)                       # 전일대비
    basrprt: Optional[float] = Field(default=None)                     # 기준가 등락률
    pdftype: Optional[str] = Field(default=None, max_length=20)        # 구분

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class SolEtf(ResultTable, VendorRecordBase, table=True):
    """SOL_ETF_LIST (신한자산운용 ``/api/common/searchByEtfNameOrFilter``) --
    SOL ETF 상품 목록.

    kodex_etf 와 같은 역할이다: 구성종목 조회가 신한 내부 펀드코드
    (``fund_cd``, 예 ``211096``)로만 되기 때문에 그 코드를 얻으려고 받는다.
    다른 점은 ``etf_cd6`` 가 **KRX 단축코드 그대로**라는 것 -- ``0092B0`` 이
    krx_etf_daily.isu_cd 에 그대로 있다. 그래서 KRX 유니버스와의 연결에 별도
    변환이 필요 없다.

    ``viewCount`` 를 크게 주면 한 번에 전량이 온다(86행). 응답에 ``etf_cd6``
    가 빈 행이 두어 개 섞이는데(내부용으로 보인다), 빈 문자열은
    VendorRecordBase 가 NULL 로 접으므로 구성종목 빌더가 ``IS NOT NULL`` 로
    걸러낸다.

    53개 필드 중 화면/태그용 배열(``pensionNameList`` 등)과 벤치마크 대비
    지표(``BM_*``)는 뺐다 -- 이 표의 쓸모가 코드 대응이라 수익률 몇 개면
    충분하다."""

    __tablename__ = "sol_etf"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    fund_cd: str = Field(index=True, max_length=20)                     # 신한 펀드코드 (211096)
    etf_cd6: Optional[str] = Field(default=None, index=True, max_length=20)  # KRX 단축코드 (0092B0)
    name: Optional[str] = Field(default=None, max_length=200)           # 상품명
    engname: Optional[str] = Field(default=None, max_length=200)        # 영문명
    work_dt: Optional[str] = Field(default=None, index=True, max_length=8)   # 기준일
    list_dt: Optional[str] = Field(default=None, max_length=8)          # 상장일

    tot_asset: Optional[float] = Field(default=None)                    # 순자산총액
    fund_pri: Optional[float] = Field(default=None)                     # 기준가
    total_fee: Optional[float] = Field(default=None)                    # 총보수

    asset_tag: Optional[str] = Field(default=None, max_length=50)       # 자산 분류
    theme_tag: Optional[str] = Field(default=None, max_length=50)       # 테마 분류
    pension_tag: Optional[str] = Field(default=None, max_length=50)     # 연금 분류

    d_rtn: Optional[float] = Field(default=None)                        # 1일 수익률
    w_rtn: Optional[float] = Field(default=None)                        # 1주
    m1_rtn: Optional[float] = Field(default=None)                       # 1개월
    m3_rtn: Optional[float] = Field(default=None)                       # 3개월
    m6_rtn: Optional[float] = Field(default=None)                       # 6개월
    m12_rtn: Optional[float] = Field(default=None)                      # 1년
    ytd: Optional[float] = Field(default=None)                          # 연초 이후
    cum_rtn: Optional[float] = Field(default=None)                      # 누적
    list_rtn: Optional[float] = Field(default=None)                     # 상장 이후

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class SolEtfPdf(ResultTable, VendorRecordBase, table=True):
    """SOL_ETF_PDF (신한자산운용 ``/api/fund/pdfList``) -- SOL ETF 구성종목.

    ``fund_cd`` 와 ``work_dt`` 가 응답 행에 이미 들어 있어(삼성과 달리) 잡
    파라미터로 찍을 필요가 없다. ``etf_cd6`` 도 행마다 오므로 KRX 와의 조인이
    이 표 하나로 끝난다.

    봉투 없는 최상위 배열이라 셀렉터가 ``"."`` 이다. 페이징이 없고 한 번에
    전량이 온다(202행짜리 확인).

    ``wt_disp`` 가 ``'19.18%'`` 처럼 **% 기호가 붙은 문자열**이다. 소스 그대로
    두고 숫자 변환은 통합 뷰에서 한다 -- 운용사마다 비중 표기가 달라서
    (삼성 33.68, 한화 0.0725 아닌 퍼센트값, 신한 문자열) 변환 규칙이 소스별로
    다르고, 그건 수집이 아니라 해석의 몫이다."""

    __tablename__ = "sol_etf_pdf"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    fund_cd: Optional[str] = Field(default=None, index=True, max_length=20)  # 신한 펀드코드
    etf_cd6: Optional[str] = Field(default=None, index=True, max_length=20)  # KRX 단축코드
    work_dt: Optional[str] = Field(default=None, index=True, max_length=8)   # 기준일
    seq_no: Optional[int] = Field(default=None)                         # 순번

    stock_code: Optional[str] = Field(default=None, index=True, max_length=20)  # 구성종목 코드
    sec_nm: Optional[str] = Field(default=None, max_length=200)         # 구성종목명
    qty: Optional[float] = Field(default=None)                          # 수량
    price: Optional[float] = Field(default=None)                        # 평가금액
    wt_disp: Optional[str] = Field(default=None, max_length=20)         # 비중 ('19.18%')

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class PlusEtf(ResultTable, VendorRecordBase, table=True):
    """PLUS_ETF_LIST (한화자산운용 ``/api/v1/product/find/list``) -- PLUS ETF
    상품 목록.

    ``id``(``006184``)가 구성종목 조회키이고 ``namecode``(``152100``)가 KRX
    단축코드다 -- 후자가 krx_etf_daily.isu_cd 와 그대로 맞는 것을 확인했다
    (152100 = PLUS 200). ``krfundcd`` 는 펀드 표준코드로 또 다르다.

    Spring 페이징(``content``/``last``)이고 page 가 **0부터** 시작한다 --
    ApiMst.pagination_json 에 ``start: 0`` 을 적어 둔 이유다.

    ``optionList`` ``fileInfo`` ``etfPriceList`` ``prList`` 같은 중첩 배열은
    뺐다. 스칼라 컬럼에 담기지 않고, 이 표의 쓸모와도 무관하다."""

    __tablename__ = "plus_etf"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    prod_id: str = Field(index=True, max_length=20)                     # 한화 상품코드 (006184)
    namecode: Optional[str] = Field(default=None, index=True, max_length=20)  # KRX 단축코드 (152100)
    krfundcd: Optional[str] = Field(default=None, max_length=20)        # 펀드 표준코드
    displayname: Optional[str] = Field(default=None, max_length=200)    # 상품명
    basicinfo: Optional[str] = Field(default=None, max_length=200)      # 기초지수
    fundtype: Optional[str] = Field(default=None, max_length=50)        # 유형
    productoption: Optional[str] = Field(default=None, max_length=50)   # 국내/해외
    wkdate: Optional[str] = Field(default=None, index=True, max_length=8)  # 기준일

    marketprice: Optional[float] = Field(default=None)                  # 시장가
    navprice: Optional[float] = Field(default=None)                     # NAV
    etfprice: Optional[float] = Field(default=None)                     # 기준가
    disparateratio: Optional[float] = Field(default=None)               # 괴리율
    returnrate: Optional[float] = Field(default=None)                   # 수익률

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))

    @model_validator(mode="before")
    @classmethod
    def _rename_id(cls, data: Any) -> Any:
        """소스의 ``id``(상품코드)를 ``prod_id`` 로 옮기고 원래 키는 버린다.

        이 표의 대리키 이름도 ``id`` 라, 그대로 두면 ``006184`` 가 기본키
        자리로 들어가려 한다. 검증 전에 손대는 훅이라 대리키는 여느 때처럼
        비어 온 것으로 취급되고 시퀀스가 채운다."""
        if isinstance(data, dict) and "id" in data and "prod_id" not in data:
            data = {**data, "prod_id": data["id"]}
            data.pop("id", None)
        return data


class PlusEtfPdf(ResultTable, VendorRecordBase, table=True):
    """PLUS_ETF_PDF (한화자산운용 ``/api/v1/product/pdf/list``) -- PLUS ETF
    구성종목.

    운용사 중 유일하게 **구성종목의 ISIN**(``krjmcd``)을 준다. 나머지는
    단축코드뿐이라, 구성종목을 ISIN 으로 다뤄야 하는 분석에는 이쪽이 편하다.

    ``ratio`` 는 이미 퍼센트값이다 -- 201행 전체 합이 100.0002 인 것을 확인했다.
    (앞 100행만 합치면 3.0 이 나와 소수 비율처럼 보이는데, 뒤쪽에 큰 비중이
    몰려 있어서 생긴 착시다.)

    ``n``(상품코드)과 기준일 ``d`` 는 잡 파라미터에서 찍는다 -- 응답 행에는
    ``wkdate`` 만 있고 어느 ETF 인지가 없다.

    Spring 페이징이고 page 가 0부터다. ``pageSize=100`` 에 201행짜리가 있어
    페이징이 실제로 필요하다."""

    __tablename__ = "plus_etf_pdf"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    prod_id: str = Field(index=True, max_length=20)                     # 한화 상품코드 (잡 파라미터에서)
    wkdate: Optional[str] = Field(default=None, index=True, max_length=8)  # 기준일
    num: Optional[int] = Field(default=None)                            # 순번

    jmcd: Optional[str] = Field(default=None, index=True, max_length=20)   # 구성종목 단축코드
    krjmcd: Optional[str] = Field(default=None, index=True, max_length=20)  # 구성종목 ISIN
    jmnm: Optional[str] = Field(default=None, max_length=200)           # 구성종목명
    amount: Optional[float] = Field(default=None)                       # 수량
    ratio: Optional[float] = Field(default=None)                        # 비중(%)

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class AceEtf(ResultTable, VendorRecordBase, table=True):
    """ACE_ETF_LIST (한국투자신탁운용 ``papi.aceetf.co.kr/api/funds``) -- ACE ETF
    상품 목록.

    운용사 목록 중 유일하게 **ETF 의 ISIN**(``stockcd``)을 준다. 다른 곳은
    단축코드뿐이라 mst_etf.isin 이 통째로 비어 있었는데, 여기서 처음 실물이
    들어온다. ``fundcd``(``KR5101877748``)가 구성종목 조회키다.

    ISIN 의 4~9번째 자리가 단축코드라는 것도 여기서 확인했다
    (``KR7105190003`` -> ``105190`` = ACE 200). 그 방향은 자릿수를 잘라내는
    것뿐이라 안전하지만, 반대로 단축코드에서 ISIN 을 만드는 것은 체크디지트를
    지어내는 일이라 하지 않는다.

    page 파라미터로 10건씩 12페이지."""

    __tablename__ = "ace_etf"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    fundcd: str = Field(index=True, max_length=20)                      # 조회키 (KR5101877748)
    stockcd: Optional[str] = Field(default=None, index=True, max_length=12)  # ETF ISIN
    fundnm: Optional[str] = Field(default=None, max_length=200)         # 상품명
    fundwhlnm: Optional[str] = Field(default=None, max_length=300)      # 정식명칭
    fundtypenm: Optional[str] = Field(default=None, max_length=50)      # 유형
    lstddt: Optional[str] = Field(default=None, max_length=8)           # 상장일
    stddt: Optional[str] = Field(default=None, index=True, max_length=10)  # 기준일
    nastamt: Optional[float] = Field(default=None)                      # 순자산
    clpr: Optional[float] = Field(default=None)                         # 종가

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class AceEtfPdf(ResultTable, EtfPdfDateMixin, VendorRecordBase, table=True):
    """ACE_ETF_PDF (``/api/funds/{fundCd}/pdf``) -- ACE ETF 구성종목.

    응답 봉투에 ``pdfList`` 말고 ``sectorList``/``assetList``(섹터·자산군 비중)도
    함께 오는데, 그건 구성종목에서 계산되는 값이라 받지 않는다.

    ``wg`` 는 퍼센트값, ``jm_ksc_cd`` 는 구성종목 단축코드다. ``std_dt`` 는 행마다
    오지만 표기가 ``2026-08-31`` 이라 EtfPdfDateMixin 이 숫자만 남긴다."""

    __tablename__ = "ace_etf_pdf"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)

    fundcd: str = Field(index=True, max_length=20)                      # 조회키 (잡 파라미터에서)
    std_dt: Optional[str] = Field(default=None, index=True, max_length=10)  # 기준일 (2026-08-31)
    rank: Optional[int] = Field(default=None)                           # 순위
    jm_ksc_cd: Optional[str] = Field(default=None, index=True, max_length=20)  # 구성종목 코드
    sec_nm: Optional[str] = Field(default=None, max_length=200)         # 구성종목명
    cu_item_cnt: Optional[float] = Field(default=None)                  # 수량
    val_am: Optional[float] = Field(default=None)                       # 평가금액
    wg: Optional[float] = Field(default=None)                           # 비중(%)

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))


class AmcEtfPdf(ResultTable, EtfPdfDateMixin, VendorRecordBase, table=True):
    """표 형태로만 공시하는 운용사들의 ETF 구성종목 -- TIGER, KB, NH, 키움, 타임.

    JSON 을 주는 운용사들과 달리 이쪽은 HTML 표나 스프레드시트로만 나온다.
    그런데 형식이 다를 뿐 **컬럼 구성은 사실상 같다** -- 종목코드·종목명·수량·
    평가금액·비중이 전부다. 필드명이 소스마다 다른 JSON 쪽과 달리, 여기서는
    이름을 붙이는 주체가 우리다(``response_parse_json`` 의 ``fields`` 가
    위치순으로 이름을 준다). 그래서 소스별로 표를 나눌 이유가 없다.

    다섯 운용사가 한 표를 쓰므로 ``api_id`` 가 출처를 가른다. ``amc_etf_cd`` 는
    그 운용사의 조회 코드이고 mst_etf 와 같은 값이다.

    ``kr_cd``(구성종목 ISIN)는 키움만 준다. ``rtn_1w``(1주 수익률)는 TIGER 만
    준다. 나머지는 그 자리를 비워 둔다 -- 표마다 컬럼 수가 다른 것이 아니라
    같은 표에서 어떤 소스는 채우고 어떤 소스는 안 채우는 것이다.

    값은 소스가 쓴 그대로 문자열이다. 천단위 쉼표(``1,794,888,000``), 퍼센트
    기호(``17.76%``), 엑셀이 수를 실수로 저장해 생긴 ``1.0`` 같은 표기가 섞여
    있어, 숫자로 바꾸는 일은 통합 뷰에서 소스별로 한다."""

    __tablename__ = "amc_etf_pdf"

    id: Optional[int] = Field(default=None, sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(index=True, max_length=150)
    job_id: str = Field(index=True, max_length=150)

    amc_etf_cd: str = Field(index=True, max_length=30)                  # 운용사 조회코드 (잡 파라미터에서)
    std_dt: Optional[str] = Field(default=None, index=True, max_length=10)  # 기준일

    no: Optional[str] = Field(default=None, max_length=20)              # 표의 순번
    itm_cd: Optional[str] = Field(default=None, index=True, max_length=50)  # 구성종목 코드
    itm_nm: Optional[str] = Field(default=None, max_length=200)         # 구성종목명
    kr_cd: Optional[str] = Field(default=None, max_length=20)           # 구성종목 ISIN (키움만)
    qty: Optional[str] = Field(default=None, max_length=40)             # 수량
    eval_amt: Optional[str] = Field(default=None, max_length=40)        # 평가금액
    wgt: Optional[str] = Field(default=None, max_length=20)             # 비중
    rtn_1w: Optional[str] = Field(default=None, max_length=20)          # 1주 수익률 (TIGER만)

    updated_at: Optional[datetime] = Field(default=None,
                sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))
