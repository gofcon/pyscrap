"""결과·사전·상태 표의 모델. 소스별로 모듈이 갈려 있고 여기서 전부 재노출한다.

`from app.db.models import X` 가 분할 전과 똑같이 동작하게 하는 것이 목적이다.
app.services.export._discover_table_registry 가 `vars(app.db.models)` 를 훑으므로,
재노출이 곧 그 레지스트리의 입력이기도 하다.

규약: 모든 표에서 `updated_at` 은 **마지막 필드**다. Oracle 은 컬럼 순서를
바꿀 수 없으므로 새 필드는 그 앞에 넣는다.
"""

# ruff: noqa: F401  -- 재노출이 목적이다

from app.db.models.base import (
    EtfPdfDateMixin,
    MdcRecordBase,
    VendorRecordBase,
)
from app.db.models.config import (
    ApiJob,
    ApiJobBuilder,
    ApiJobLog,
    ApiMst,
    ApiRst,
)
from app.db.models.kis import (
    FoIdxCodeMst,
    KisEtf,
    KisEtfPdf,
    KisFutoptChart,
    KisFutoptDaily,
    KisFutoptPrice,
    KisIndexDaily,
)
from app.db.models.krx import (
    KrxDerivExpyy,
    KrxDerivHist,
    KrxDerivInfo,
    KrxEtfDaily,
    KrxEtfList,
    KrxEtfPdf,
    KrxEtnDaily,
    KrxFutDaily,
    KrxIndexDaily,
    KrxOptDaily,
    KrxStockBase,
    KrxStockDaily,
)
from app.db.models.ksd import (
    KsdBondCredit,
    KsdBondInfo,
    KsdBondInfoApi,
    KsdBondInt,
    KsdBondIntApi,
    KsdBondIsin,
    KsdBondPrin,
    KsdBondRedem,
    KsdBondRedemHis,
    KsdBondStopt,
    KsdBondStoptHis,
    KsdBondStoptPrc,
    KsdBondYield,
    KsdKacdCode,
)
from app.db.models.etf import (
    AceEtf,
    AceEtfPdf,
    AmcEtfPdf,
    KodexEtf,
    KodexEtfPdf,
    PlusEtf,
    PlusEtfPdf,
    SolEtf,
    SolEtfPdf,
)
from app.db.models.reference import (
    KrxIndexMst,
    MetaFuoptInfo,
    MetaMaturity,
    MstBond,
    MstEtf,
    MstFuopt,
    MstIndex,
    MstStock,
    StockIndexHis,
    UserEtf,
)
from app.db.models.state import (
    KsdBondNoDetail,
)

__all__ = [
    "AceEtf",
    "AceEtfPdf",
    "AmcEtfPdf",
    "ApiJob",
    "ApiJobBuilder",
    "ApiJobLog",
    "ApiMst",
    "ApiRst",
    "EtfPdfDateMixin",
    "FoIdxCodeMst",
    "KisEtf",
    "KisEtfPdf",
    "KisFutoptChart",
    "KisFutoptDaily",
    "KisFutoptPrice",
    "KisIndexDaily",
    "KodexEtf",
    "KodexEtfPdf",
    "KrxDerivExpyy",
    "KrxDerivHist",
    "KrxDerivInfo",
    "KrxEtfDaily",
    "KrxEtfList",
    "KrxEtfPdf",
    "KrxEtnDaily",
    "KrxFutDaily",
    "KrxIndexDaily",
    "KrxIndexMst",
    "KrxOptDaily",
    "KrxStockBase",
    "KrxStockDaily",
    "KsdBondCredit",
    "KsdBondInfo",
    "KsdBondInfoApi",
    "KsdBondInt",
    "KsdBondIntApi",
    "KsdBondIsin",
    "KsdBondNoDetail",
    "KsdBondPrin",
    "KsdBondRedem",
    "KsdBondRedemHis",
    "KsdBondStopt",
    "KsdBondStoptHis",
    "KsdBondStoptPrc",
    "KsdBondYield",
    "KsdKacdCode",
    "MdcRecordBase",
    "MetaFuoptInfo",
    "MetaMaturity",
    "MstBond",
    "MstEtf",
    "MstFuopt",
    "MstIndex",
    "MstStock",
    "PlusEtf",
    "PlusEtfPdf",
    "SolEtf",
    "SolEtfPdf",
    "StockIndexHis",
    "UserEtf",
    "VendorRecordBase",
]
