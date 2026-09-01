CREATE OR REPLACE PROCEDURE sp_mst_stock_sync (p_merged OUT NUMBER) AS
-- krx_stock_base(거래소 주식 종목기본정보) -> mst_stock(정제 마스터) 적재.
--
-- 코드 두 개가 서로 바뀐다. 거래소는 표준코드를 isu_cd, 단축코드를 isu_srt_cd
-- 라 부르는데, 이 프로젝트의 마스터는 mst_etf 이래로 단축코드를 isu_cd 로
-- 써 왔다 -- 다른 소스에 질문을 던질 때 쓰는 코드가 그쪽이라서다. 그 맞바꿈이
-- 일어나는 곳은 여기 한 군데다.
--
-- 종목이 사라져도 지우지 않는다. 거래소의 일자별 목록은 그날 상장돼 있던 것만
-- 보여주므로, 2014년에 상장폐지된 종목은 그 시절 어느 날의 응답에만 있다.
-- 과거 이행이 그 날들을 거꾸로 훑고, 처음 보는 코드가 여기 쌓인다.
--
-- 그래서 갱신에 조건이 붙는다: 이행이 최근 -> 과거 순이라 들어오는 행은 대개
-- 여기 있는 것보다 오래된 것이다. bas_dd 가 last_seen_dd 보다 새로울 때만
-- 덮어쓰므로, 옛날 날짜는 종목을 더할 수는 있어도 지금 값을 되돌리지 못한다.
-- 이 조건이 없으면 이행이 끝날 때쯤 모든 상장주식수가 2010년 값이 된다.
--
-- description 은 건드리지 않는다. 사람이 적는 칸이다 (mst_etf 의 운용사 코드와
-- 같은 취급).
--
-- 소스가 종목당 한 행인 것은 sp_krx_stock_base_dedup 이 보장한다. 그래도 여기서
-- 한 번 더 접는 이유는 MERGE 가 소스 키 중복에 ORA-30926 으로 죽기 때문이다 --
-- 수집 직후, 접기 전에 이걸 부르면 그 상태가 된다.
BEGIN
  MERGE /*+ NO_PARALLEL */ INTO mst_stock t
  USING (
    SELECT isu_srt_cd AS isu_cd,          -- 거래소의 isu_srt_cd = 여기의 isu_cd
           isu_cd     AS isin,            -- 거래소의 isu_cd     = 여기의 isin
           isu_nm, isu_abbrv, isu_eng_nm, list_dd, mkt_tp_nm, secugrp_nm,
           sect_tp_nm, kind_stkcert_tp_nm, parval, list_shrs,
           bas_dd     AS last_seen_dd
      FROM (SELECT b.*, ROW_NUMBER() OVER (PARTITION BY b.isu_srt_cd
                                           ORDER BY b.bas_dd DESC, b.id DESC) rn
              FROM krx_stock_base b)
     WHERE rn = 1
  ) s
  ON (t.isu_cd = s.isu_cd)
  WHEN MATCHED THEN UPDATE SET
       t.isin = s.isin, t.isu_nm = s.isu_nm, t.isu_abbrv = s.isu_abbrv,
       t.isu_eng_nm = s.isu_eng_nm, t.list_dd = s.list_dd,
       t.mkt_tp_nm = s.mkt_tp_nm, t.secugrp_nm = s.secugrp_nm,
       t.sect_tp_nm = s.sect_tp_nm, t.kind_stkcert_tp_nm = s.kind_stkcert_tp_nm,
       t.parval = s.parval, t.list_shrs = s.list_shrs,
       t.last_seen_dd = s.last_seen_dd
     WHERE t.last_seen_dd IS NULL OR s.last_seen_dd > t.last_seen_dd
  WHEN NOT MATCHED THEN
    INSERT (isu_cd, isin, isu_nm, isu_abbrv, isu_eng_nm, list_dd, mkt_tp_nm,
            secugrp_nm, sect_tp_nm, kind_stkcert_tp_nm, parval, list_shrs,
            last_seen_dd)
    VALUES (s.isu_cd, s.isin, s.isu_nm, s.isu_abbrv, s.isu_eng_nm, s.list_dd,
            s.mkt_tp_nm, s.secugrp_nm, s.sect_tp_nm, s.kind_stkcert_tp_nm,
            s.parval, s.list_shrs, s.last_seen_dd);

  p_merged := SQL%ROWCOUNT;
  DBMS_OUTPUT.PUT_LINE('mst_stock: ' || TO_CHAR(p_merged, 'FM999,999,999') || ' 행 반영');
END;
