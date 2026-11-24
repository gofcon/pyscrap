"""수집 상태 -- 결과도 사전도 아닌 것. docs/SCHEMA-V2.md 9.5 참고."""

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
    StateTable,
    VendorRecordBase,
)




class KsdBondNoDetail(StateTable, SQLModel, table=True):
    """ISINs SEIBRO has no detail for -- so the migration stops asking them.

    The ten ksd_bond_* detail builders all pick their night's work the same
    way: bonds in mst_bond that are not yet in ksd_bond_info, newest
    first_seen first, 800 of them. An empty reply is a legitimate answer
    there (a private placement, an old issue SEIBRO no longer details), and
    it leaves no row -- so the ISIN stays in that window for ever, and
    because its job already exists it is never re-run either. By 2026-10-15
    402 of the 800 slots were held by such ISINs and every type was
    collecting 398 new bonds a night instead of 800.

    This table is where "we asked and there was nothing" is written down, and
    the builders exclude it. It has to be a table rather than a view over the
    jobs: sp_purge_jobs deletes an inactive job after 90 days, so a view
    would forget and the whole set would be asked again.

    Collection state, not reference data and not a result -- which is why it
    is here and not a column on mst_bond: that table is the instrument
    master, and whether we managed to scrape something is not a property of
    the instrument. No job_id, so it stays out of TABLE_REGISTRY and out of
    the export/external-table work (see
    app.services.export._discover_table_registry).

    Written once per ISIN by sp_ksd_no_detail_mark, which runs in
    sp_run_daily_batch2 ahead of the cycle that generates the jobs. Marking
    reads api_job -- but only to write this row, once; after that the row
    stands on its own and outlives the job.

    To ask a bond again (SEIBRO published it late, say), delete its row here:
    the gate lets it back into the window the next night."""

    __tablename__ = "ksd_bond_no_detail"

    isin: str = Field(primary_key=True, max_length=12)                       # 표준코드
    # SYSDATE 기준(UTC). 감사용이라 날짜만 쓰면 되고 비교에 쓰이지 않는다.
    marked_at: Optional[datetime] = Field(default=None, sa_column=Column(DateTime))
    note: Optional[str] = Field(default=None, max_length=100)


# ---------------------------------------------------------------------------
# 운용사 ETF 구성종목(PDF)
#
# ETF 유니버스는 KRX 오픈API(krx_etf_daily)에 이미 있다. 운용사에서 받는 것은
# KRX 가 로그인 없이는 주지 않는 **구성종목** 하나뿐이라, 여기 표들은 마스터가
# 아니라 그 한 조각만 담는다.
#
# 운용사마다 표를 따로 두는 이유는 필드 이름이 제각각이어서다(삼성 secNm/itmNo/
# applyQ, KRX COMPST_ISU_NM/COMPST_ISU_CD/...). 수집 단계에서 이름을 맞추려면
# 엔진에 없는 장치가 필요하고, 소스 이름을 그대로 두고 DB 쪽에서 뷰로 합치는
# 편이 ksd_bond_info / ksd_bond_info_api 때와 같은 이유로 낫다.
#
# 컬럼명은 삼성이 쓰는 이름을 소문자로 접은 것이다(`fId` -> `fid`,
# `stkTicker` -> `stkticker`). VendorRecordBase 가 키를 소문자화하므로 그대로
# 맞아떨어지고, 밑줄을 끼워 넣으면(`f_id`) 매칭이 깨진다.
# ---------------------------------------------------------------------------
