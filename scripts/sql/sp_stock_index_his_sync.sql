CREATE OR REPLACE PROCEDURE sp_stock_index_his_sync (p_merged OUT NUMBER) AS
-- 지수 일별시세 -> stock_index_his(참조 시계열) 이관. 원천이 둘이고 순서가
-- 우선순위다.
--
--   1. kis_index_daily -- 당일분을 그날 저녁에 준다. 없는 날만 넣는다(INSERT
--      전용). KRX 가 아직 내놓지 않은 당일 종가를 하루 먼저 채우는 자리다.
--   2. krx_index_daily -- 전 거래일 확정분. 이미 있는 행도 덮는다. 그래서
--      KIS 로 먼저 채운 당일 행은 다음 날 확정값으로 바뀐다.
--
-- 거래소가 주 원천인 이유는 그대로다: 코스피 계열 51개 지수를 한 요청에 주고,
-- 지수를 코드가 아니라 이름으로 식별해 마스터를 통해 풀린다. 두 소스의 값은
-- 같다 -- 29일치 OHLC 가 소수점까지 전건 일치했고, 2026-09-18~30 을 다시
-- 대조해도 종가·시가가 같다. 다른 것은 단위뿐이다(아래).
--
-- v_k2i_atm 이 이 변경의 이유다. 그 MV 는 ATM 행사가를 뽑는 데 쓰이고, 3분
-- 샤드가 어느 행사가를 받을지 정하는 밴드의 중심이다. 거래소만 읽으면 그 중심이
-- 늘 이틀 전 종가였다(D 아침에 받는 것이 D-1 분이고, MV 갱신은 그날 밤이다).
-- KIS 를 앞에 두면 하루가 줄고, ATM 추정은 조금 부정확해도 되는 값이다.
--
-- mv_id 는 krx_index_mst 에서 온다. 시세는 지수 이름만 주므로 이름으로 붙고,
-- 지수를 하나 더 따라가려면 거기 mv_id 를 채우고 mst_index 에 한 줄 넣으면
-- 된다 -- 이 프로시저는 손대지 않는다. 예전 CASE 문이 하던 일이다.
--
-- 단위는 거래소가 주는 그대로다: 거래량은 주, 거래대금과 시가총액은 원.
-- 예전에는 KIS 가 주는 천주/백만원으로 쌓였고(같은 값의 다른 표기 -- 29일치
-- 대조에서 volume = 주/1,000, trading_value = 원/1,000,000 으로 전건
-- 일치했다), 소스를 옮기면서 원 단위로 통일했다. 옛 표기의 5,094 행은
-- stock_index_his_bak_20260901 에 그대로 있다.
--
-- 거래소가 주는 것은 2010-01-04 부터다. 그 앞(2006~2009)은 KIS 표기를
-- 1,000 / 1,000,000 배 해서 옮겼으므로 반올림 오차가 남아 있다 -- 지수값
-- 자체는 두 소스가 같아서 영향이 없고, 거래량·거래대금만 그렇다.
--
-- price_change/change_rate 는 원본에 없어 종가 시계열에서 LAG 로 계산한다.
-- listed_market_cap 은 거래소의 MKTCAP 을 그대로 넣는다(원).
--
-- 끝에서 v_k2i_atm 을 갱신한다. 그 MV 는 stock_index_his 의 종가에서 그날의
-- ATM 행사가를 뽑아 놓은 것이라, 여기서 새 종가를 넣고 갱신하지 않으면 MV 는
-- 어제까지만 알고 있다. 그 상태로 내보내면 당일분이 빈 채로 나가는데, 파일이
-- 만들어지긴 하므로 배치는 성공으로 끝나고 아무도 모른다.
--
-- 배치 순서에 맡기지 않고 여기 둔 이유: 갱신해야 할 시점은 '내보내기 전' 이
-- 아니라 '원본이 바뀐 직후' 다. 그 시점을 아는 것은 이 프로시저뿐이고,
-- 호출하는 쪽에 순서를 맡기면 언젠가 한 군데서 빠진다.
--
-- ON DEMAND MV 라 COMPLETE 로 다시 만든다. 1,700 행 남짓이라 그 편이
-- 빠르고, FAST 는 로그 테이블을 요구해서 원본 쪽에 부담을 남긴다.
--
-- 병렬 DML 을 끄는 이유: MV 가 읽는 stock_index_his 를 바로 위에서 고쳤는데,
-- 그 MERGE 가 병렬로 돌면 같은 트랜잭션에서 그 테이블을 다시 읽을 수 없다
-- (ORA-12838). 커밋으로 풀 수도 있지만 이 프로시저는 커밋하지 않는다 --
-- 호출하는 쪽이 다른 작업과 묶을 수 있어야 해서다. 수십 행짜리 MERGE 라
-- 병렬로 얻을 것도 없다.
--
-- 세션 설정이라 되돌린다. 이 DB 는 병렬 DML 이 기본 활성이고(그래서 위
-- 오류가 났다), 안 되돌리면 배치의 다음 단계까지 직렬이 된다.
BEGIN
  EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML';

  -- kis_index_daily 의 중복 정리. 아래 MERGE 가 이 표를 읽으므로 (날짜,
  -- 종목코드) 가 한 건이어야 한다 -- 소스 키가 중복되면 ORA-30926 이다.
  -- 겹침이 생기는 경로: 빌더가 롤링 기간을 쓰던 때 쌓인 것과, save_mode=
  -- overwrite 가 '같은 job_id 의 이전 결과' 만 지우는 성질. 지금 빌더는
  -- 하루짜리라 새로 생기지는 않지만, 옛 행이 남아 있고 기간을 다시 넓힐
  -- 여지도 있어 접는 쪽을 둔다.
  DELETE FROM kis_index_daily
   WHERE id IN (
     SELECT id FROM (
       SELECT id, ROW_NUMBER() OVER (PARTITION BY stck_bsop_date, short_code
                                     ORDER BY updated_at DESC, id DESC) rn
         FROM kis_index_daily)
      WHERE rn > 1);

  -- 1. KIS: 없는 날만. 이미 있는 행은 건드리지 않는다 -- 거래소가 확정한 값을
  -- 덜 확정된 값으로 되돌리지 않기 위해서다. 단위를 거래소 쪽에 맞춘다: KIS 는
  -- 거래량을 천주, 거래대금을 백만원으로 준다(29일치 대조에서 각각 1,000 /
  -- 1,000,000 배로 전건 일치). 시가총액은 KIS 가 주지 않아 NULL 이고, 다음 날
  -- 거래소 MERGE 가 채운다.
  --
  -- mv_id 는 meta_fuopt_info 에서 온다. 그 표가 기초자산의 코드(KIS
  -- unas_short_code)와 시계열에서의 이름을 같은 행에 들고 있다 -- 거래소 쪽이
  -- krx_index_mst 로 이름을 푸는 것과 같은 자리다.
  MERGE INTO stock_index_his t
  USING (
    SELECT TO_DATE(k.stck_bsop_date, 'YYYYMMDD') AS trade_date,
           u.mv_id                               AS mv_id,
           k.bstp_nmix_prpr                      AS close_price,
           k.bstp_nmix_oprc                      AS open_price,
           k.bstp_nmix_hgpr                      AS high_price,
           k.bstp_nmix_lwpr                      AS low_price,
           k.acml_vol * 1000                     AS volume,        -- 천주 -> 주
           k.acml_tr_pbmn * 1000000              AS trading_value  -- 백만원 -> 원
      FROM kis_index_daily k
      JOIN (SELECT DISTINCT ul_code, mv_id FROM meta_fuopt_info WHERE mv_id IS NOT NULL) u
        ON u.ul_code = k.short_code
     WHERE k.bstp_nmix_prpr IS NOT NULL
  ) s
  ON (t.trade_date = s.trade_date AND t.mv_id = s.mv_id)
  WHEN NOT MATCHED THEN
    INSERT (trade_date, mv_id, close_price, open_price, high_price, low_price,
            volume, trading_value)
    VALUES (s.trade_date, s.mv_id, s.close_price, s.open_price, s.high_price, s.low_price,
            s.volume, s.trading_value);

  -- 2. 거래소: 확정분. 이미 있는 행도 덮는다 -- 위에서 KIS 로 채운 당일 행이
  -- 여기서 확정값으로 바뀐다.
  MERGE INTO stock_index_his t
  USING (
    SELECT TO_DATE(k.bas_dd, 'YYYYMMDD')        AS trade_date,
           m.mv_id                              AS mv_id,
           k.clsprc_idx                         AS close_price,
           k.opnprc_idx                         AS open_price,
           k.hgprc_idx                          AS high_price,
           k.lwprc_idx                          AS low_price,
           k.acc_trdvol                         AS volume,        -- 주
           k.acc_trdval                         AS trading_value, -- 원
           k.mktcap                             AS listed_market_cap
      FROM (
        -- 같은 영업일이 여러 잡에서 중복 적재될 수 있으므로 최신 1건만.
        -- MERGE 는 소스 키가 중복되면 ORA-30926 으로 실패한다.
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY idx_nm, bas_dd
                                       ORDER BY updated_at DESC NULLS LAST, id DESC) rn
          FROM krx_index_daily d
      ) k
      -- mv_id 가 붙은 지수만. 매핑 안 된 지수를 조용히 흘려보내지 않고 아예
      -- 제외한다 (mv_id NULL 은 PK 위반이 된다). 지수를 새로 따라가기 시작할
      -- 때 krx_index_mst 갱신을 잊으면 여기서 0건으로 드러난다.
      JOIN krx_index_mst m ON m.idx_nm = k.idx_nm AND m.mv_id IS NOT NULL
     WHERE k.rn = 1
       -- 지수값 없이 거래량·시총만 있는 집계 행(코스피 (외국주포함) 같은)이
       -- 섞여 있다. 종가가 없는 행은 시계열에 넣을 것이 없다.
       AND k.clsprc_idx IS NOT NULL
  ) s
  ON (t.trade_date = s.trade_date AND t.mv_id = s.mv_id)
  WHEN MATCHED THEN UPDATE SET
       t.close_price = s.close_price, t.open_price = s.open_price,
       t.high_price  = s.high_price,  t.low_price  = s.low_price,
       t.volume      = s.volume,      t.trading_value = s.trading_value,
       t.listed_market_cap = s.listed_market_cap,
       -- 종가가 실제로 바뀌면 전일대비를 비워 아래에서 다시 계산되게 한다.
       -- KIS 로 먼저 채운 행이 확정값과 다를 때를 위한 것이다. 두 소스가
       -- 소수점까지 같아서 아직 일어난 적은 없다. 다음 날 행의 전일대비까지
       -- 따라 틀어지는 것은 여기서 고치지 않는다 -- 그 경우가 실제로 생기면
       -- 그때 범위를 넓히는 편이, 지금 쓰지 않을 일반화를 넣는 것보다 낫다.
       t.price_change = CASE WHEN s.close_price <> t.close_price THEN NULL ELSE t.price_change END,
       t.change_rate  = CASE WHEN s.close_price <> t.close_price THEN NULL ELSE t.change_rate END
  WHEN NOT MATCHED THEN
    INSERT (trade_date, mv_id, close_price, open_price, high_price, low_price,
            volume, trading_value, listed_market_cap)
    VALUES (s.trade_date, s.mv_id, s.close_price, s.open_price, s.high_price, s.low_price,
            s.volume, s.trading_value, s.listed_market_cap);

  p_merged := SQL%ROWCOUNT;

  -- 전일대비/등락률: 직전 영업일 종가 대비. 이미 값이 있는 행은 건드리지 않아
  -- 다른 소스로 채운 과거분과 충돌하지 않음.
  MERGE INTO stock_index_his t
  USING (
    SELECT trade_date, mv_id,
           close_price - prev AS chg,
           ROUND((close_price - prev) / prev * 100, 2) AS rate
      FROM (SELECT trade_date, mv_id, close_price, price_change,
                   LAG(close_price) OVER (PARTITION BY mv_id ORDER BY trade_date) prev
              FROM stock_index_his)
     WHERE prev IS NOT NULL AND price_change IS NULL
  ) s
  ON (t.trade_date = s.trade_date AND t.mv_id = s.mv_id)
  WHEN MATCHED THEN UPDATE SET t.price_change = s.chg, t.change_rate = s.rate;

  DBMS_MVIEW.REFRESH('v_k2i_atm', 'C');

  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
EXCEPTION
  WHEN OTHERS THEN
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
    RAISE;
END;
