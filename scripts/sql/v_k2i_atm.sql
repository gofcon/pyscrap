-- 일자별 KOSPI200 종가와 그날의 ATM 행사가, 그리고 다가오는 만기일들.
--
-- 행사가는 2.5 격자이므로 종가는 대개 두 행사가 사이에 떨어짐. 반올림한
-- 행사가에서 0.7 이내면 그 행사가를 ATM 으로 확정하고, 그보다 멀면 어느
-- 쪽도 ATM 이라 하기 어려우므로 콜은 아래쪽 · 풋은 위쪽을 쓴다(각자
-- 등가에서 밀리는 방향). 격자가 2.5 라 중간점까지가 1.25 이므로 0.7~1.25
-- 구간만 이 "사이" 판정에 해당함.
--
-- 위클리 둘은 LEFT: WKM 은 2023-08 상장이라 그 이전 NULL 이 정상이고, 그
-- 사실 자체가 데이터로서 의미가 있으므로 행을 지우지 않음.
--
-- 만기일 셋은 조인 조건으로 구함: meta_maturity 의 각 행은 (직전만기, 만기]
-- 구간을 덮으므로, 그 구간에 trade_date 가 들어가는 행이 곧 "그날 기준
-- 가장 가까운 만기"임. 스칼라 서브쿼리로 쓰면 더 직관적이지만 머티리얼라이즈드
-- 뷰의 SELECT 절에서는 허용되지 않음(ORA-22818).
CREATE MATERIALIZED VIEW v_k2i_atm
  BUILD IMMEDIATE
  REFRESH COMPLETE ON DEMAND
AS
SELECT h.trade_date,
       h.close_price                                        AS k2i_close,
       ROUND(h.close_price / 2.5) * 2.5                     AS atm,
       CASE WHEN ABS(h.close_price - ROUND(h.close_price / 2.5) * 2.5) <= 0.7
            THEN ROUND(h.close_price / 2.5) * 2.5
            ELSE FLOOR(h.close_price / 2.5) * 2.5 END       AS call_atm,
       CASE WHEN ABS(h.close_price - ROUND(h.close_price / 2.5) * 2.5) <= 0.7
            THEN ROUND(h.close_price / 2.5) * 2.5
            ELSE CEIL(h.close_price / 2.5) * 2.5 END        AS put_atm,
       k.mat_date                                           AS mat_date,
       wi.mat_date                                          AS wki_date,
       wm.mat_date                                          AS wkm_date
  FROM stock_index_his h
  -- 월물만 INNER: 지수 시계열은 2006년부터지만 만기 캘린더는 옵션 데이터가
  -- 있는 2019-09부터라, 그 이전 날짜는 만기를 붙일 수 없는 빈 껍데기가 됨.
  -- 월물이 잡히는 날짜가 곧 "옵션 데이터가 있는 구간"의 시작.
  JOIN meta_maturity k       ON k.prod_type  = 'K2I'
                            AND h.trade_date >  k.prev_mat_date
                            AND h.trade_date <= k.mat_date
  LEFT JOIN meta_maturity wi ON wi.prod_type = 'WKI'
                            AND h.trade_date >  wi.prev_mat_date
                            AND h.trade_date <= wi.mat_date
  LEFT JOIN meta_maturity wm ON wm.prod_type = 'WKM'
                            AND h.trade_date >  wm.prev_mat_date
                            AND h.trade_date <= wm.mat_date
 WHERE h.mv_id = 'KI2'
