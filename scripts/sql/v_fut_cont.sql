-- 연결선물: 상품별로 날짜마다 최근월물 하나를 골라 이어 붙인 시계열.
--
-- 최근월물은 "그날 가격이 있는 월물 중 만기가 가장 이른 것". 만기일 당일까지
-- 그 월물이고(마지막 정산가까지 들어감), 다음 날부터 다음 월물이다 -- 만기가
-- 지난 월물은 원천에서 사라지므로 달력 없이 데이터만으로 정해진다. 월물은
-- 종목명 'xxx F 202609' 의 여섯 자리이고, 'SP 2309-2406' 같은 스프레드는 F 가
-- 없어 애초에 걸러진다. 정규장만 본다: 야간장은 같은 월물이 따로 한 행이다.
--
-- 가격은 정산가를 우선한다. 거래가 없는 날도 정산가는 나오므로 종가보다
-- 빈 곳이 훨씬 적다(최근월 기준 종가 NULL 9.2만 행). 다만 원천에는 값이 아닌
-- 정산가가 있다. 2015-06-11 이전에는 거래 없는 월물에 0 이 찍혀 있고(5.9만 행),
-- 미결제가 0 인 채 만기를 맞은 월물의 최종정산가 자리에는 자릿수 채우기용
-- 숫자가 온다 -- 5년국채 201506 에 0.01, 금(구) 201506·201507 에 10 (직전
-- 정산가 42,600). 세 행뿐이지만 그대로 두면 수익률 -100% 로 지수가 0 이 되어
-- 영영 안 돌아온다. 그래서 0 과, 같은 월물의 직전 가격 1/10 미만으로 떨어진
-- 값은 없는 값으로 본다 -- 지수·금리·환율·상품·주식선물 어느 것도 하루에
-- 90% 를 잃지 않는다(주식은 가격제한폭 30%). 정산가가 없으면 종가로 대신하고,
-- 둘 다 없으면 그 월물은 그날 빠지고 다음 월물이 올라온다 -- 엔·유로·돈육처럼
-- 한산했던 상품의 옛 구간에서만 일어나는 일이다. 그런 상품은 원천의 잡음도
-- 그대로 지닌다: 돈육 201307 은 미결제 0 인 채 정산가가 4,020 → 6,050 → 4,090
-- 으로 뛰는데 그건 걸러낼 근거가 없다.
--
-- 이어 붙이면 롤 하는 날 가격이 튄다: 어제 9월물 종가와 오늘 12월물 종가의
-- 차이는 시장이 움직인 게 아니라 베이시스다. 그래서 수익률은 항상 같은
-- 월물끼리 구한다 -- 오늘 최근월물의 오늘 정산가를 그 월물의 직전 정산가로
-- 나눈다. 롤 하는 날엔 12월물의 어제 정산가가 분모가 되고, 그건 어제 차근월로
-- 있던 값이다. cont_idx 는 그 수익률을 누적한 것이라 롤 자국이 없고, 첫날의
-- 최근월물 가격에서 출발해 가격 수준 그대로 읽힌다. cont_prc 는 그냥 이어
-- 붙인 원가격이라 롤 하는 날 튀는 것이 정상이며, 그 날은 rolled = 1 이다.
--
-- 원천 krx_fut_daily 는 2010-01-04 부터.
CREATE OR REPLACE VIEW v_fut_cont AS
WITH c0 AS (
    SELECT bas_dd, prod_nm, isu_cd, isu_nm,
           REGEXP_SUBSTR(isu_nm, ' F ([0-9]{6})', 1, 1, NULL, 1)   AS mat_ym,
           COALESCE(NULLIF(setl_prc, 0), NULLIF(tdd_clsprc, 0))  AS prc0,
           tdd_opnprc, tdd_hgprc, tdd_lwprc, tdd_clsprc, setl_prc, spot_prc,
           acc_trdvol, acc_trdval, acc_opnint_qty
      FROM krx_fut_daily
     WHERE mkt_nm = '정규'
       AND REGEXP_LIKE(isu_nm, ' F [0-9]{6}')
),
c AS (
    -- 직전 가격의 1/10 미만이면 자릿수 채우기용 숫자다 (위 설명).
    SELECT c0.*,
           CASE WHEN prc0 < LAG(prc0) IGNORE NULLS
                              OVER (PARTITION BY prod_nm, mat_ym ORDER BY bas_dd) / 10
                THEN NULL ELSE prc0 END                            AS prc
      FROM c0
),
p AS (
    -- 같은 월물의 직전 가격. 최근월을 고르기 전에 구해야 롤 하는 날 분모가
    -- "어제 차근월이던 그 월물" 의 값이 된다.
    SELECT c.*,
           LAG(prc) IGNORE NULLS OVER (PARTITION BY prod_nm, mat_ym ORDER BY bas_dd) AS prev_prc
      FROM c
),
f AS (
    SELECT p.*,
           ROW_NUMBER() OVER (PARTITION BY prod_nm, bas_dd ORDER BY mat_ym) AS rn
      FROM p
     WHERE prc IS NOT NULL
),
r AS (
    SELECT f.*,
           LAG(mat_ym) OVER (PARTITION BY prod_nm ORDER BY bas_dd)          AS prev_mat_ym,
           CASE WHEN prev_prc > 0 THEN prc / prev_prc END                    AS ret
      FROM f
     WHERE rn = 1
)
SELECT bas_dd,
       prod_nm,
       mat_ym,
       isu_cd,
       isu_nm,
       CASE WHEN prev_mat_ym IS NOT NULL AND mat_ym <> prev_mat_ym THEN 1 ELSE 0 END AS rolled,
       prc                                                                    AS cont_prc,
       ret - 1                                                                AS ret,
       FIRST_VALUE(prc) OVER (PARTITION BY prod_nm ORDER BY bas_dd)
         * EXP(SUM(LN(NVL(ret, 1))) OVER (PARTITION BY prod_nm ORDER BY bas_dd)) AS cont_idx,
       tdd_opnprc, tdd_hgprc, tdd_lwprc, tdd_clsprc, setl_prc, spot_prc,
       acc_trdvol, acc_trdval, acc_opnint_qty
  FROM r
