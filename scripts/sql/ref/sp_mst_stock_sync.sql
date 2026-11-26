CREATE OR REPLACE PROCEDURE sp_mst_stock_sync (p_merged OUT NUMBER) AS
-- krx_stock_base(거래소 주식 종목기본정보) -> mst_stock(정제 마스터) 적재.
--
-- 이름이 통째로 바뀌는 자리다. 왼쪽(krx_stock_base)은 거래소 어휘고,
-- 오른쪽(mst_stock)은 내부 시스템 어휘다 -- mst_* 가 층으로서 존재하는 이유가
-- 그 번역이고, 그 대응표가 적힌 곳은 여기 한 군데다.
--
-- 코드 두 개는 서로 엇갈리기까지 한다. 거래소는 표준코드를 isu_cd, 단축코드를
-- isu_srt_cd 라 부른다. 여기서는 단축코드가 short_code, 표준코드가 isin 이다 --
-- 각자 무엇인지가 이름에 드러나는 쪽으로.
--
-- 종목이 사라져도 지우지 않는다. 거래소의 일자별 목록은 그날 상장돼 있던 것만
-- 보여주므로, 2014년에 상장폐지된 종목은 그 시절 어느 날의 응답에만 있다.
-- 과거 이행이 그 날들을 거꾸로 훑고, 처음 보는 코드가 여기 쌓인다.
--
-- 그래서 갱신에 조건이 붙는다: 이행이 최근 -> 과거 순이라 들어오는 행은 대개
-- 여기 있는 것보다 오래된 것이다. bas_dd 가 last_seen_dt 보다 새로울 때만
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
    SELECT isu_srt_cd         AS short_code,   -- 거래소 isu_srt_cd = 여기 short_code
           isu_cd             AS isin,         -- 거래소 isu_cd     = 여기 isin
           isu_nm             AS prod_nm,
           isu_abbrv          AS prod_snm,
           isu_eng_nm         AS prod_enm,
           list_dd            AS list_dt,
           mkt_tp_nm          AS mkt_div,
           secugrp_nm         AS secu_grp,
           sect_tp_nm         AS secu_type,
           kind_stkcert_tp_nm AS stock_kind_type,
           parval             AS face_amt,
           list_shrs          AS list_cnt,
           bas_dd             AS last_seen_dt
      FROM (SELECT b.*, ROW_NUMBER() OVER (PARTITION BY b.isu_srt_cd
                                           ORDER BY b.bas_dd DESC, b.id DESC) rn
              FROM krx_stock_base b)
     WHERE rn = 1
  ) s
  ON (t.short_code = s.short_code)
  WHEN MATCHED THEN UPDATE SET
       t.isin = s.isin, t.prod_nm = s.prod_nm, t.prod_snm = s.prod_snm,
       t.prod_enm = s.prod_enm, t.list_dt = s.list_dt,
       t.mkt_div = s.mkt_div, t.secu_grp = s.secu_grp,
       t.secu_type = s.secu_type, t.stock_kind_type = s.stock_kind_type,
       t.face_amt = s.face_amt, t.list_cnt = s.list_cnt,
       t.last_seen_dt = s.last_seen_dt
     WHERE t.last_seen_dt IS NULL OR s.last_seen_dt > t.last_seen_dt
  WHEN NOT MATCHED THEN
    INSERT (short_code, isin, prod_nm, prod_snm, prod_enm, list_dt, mkt_div,
            secu_grp, secu_type, stock_kind_type, face_amt, list_cnt,
            last_seen_dt)
    VALUES (s.short_code, s.isin, s.prod_nm, s.prod_snm, s.prod_enm, s.list_dt,
            s.mkt_div, s.secu_grp, s.secu_type, s.stock_kind_type,
            s.face_amt, s.list_cnt, s.last_seen_dt);

  p_merged := SQL%ROWCOUNT;
  DBMS_OUTPUT.PUT_LINE('mst_stock: ' || TO_CHAR(p_merged, 'FM999,999,999') || ' 행 반영');
END;
