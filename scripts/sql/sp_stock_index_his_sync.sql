CREATE OR REPLACE PROCEDURE sp_stock_index_his_sync (p_merged OUT NUMBER) AS
-- krx_index_daily(거래소 지수 일별시세) -> stock_index_his(참조 시계열) 이관.
--
-- 예전에는 kis_index_daily 를 읽었다. 거래소가 같은 값을 주고(29일치 OHLC
-- 전건 일치, 소수점까지), 코스피 계열 51개 지수를 한 요청에 주며, 무엇보다
-- 지수를 코드가 아니라 이름으로 식별하는 쪽이 마스터를 통해 풀린다.
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

  -- kis_index_daily 의 중복 정리. 이 프로시저가 더는 읽지 않는 표지만, 그
  -- 수집 잡은 계속 돌고 겹침도 계속 생긴다 -- 빌더가 롤링 기간
  -- (last_month ~ last_bday)을 쓰고 save_mode=overwrite 는 '같은 job_id 의
  -- 이전 결과' 만 지우므로, 어제 잡과 오늘 잡의 겹치는 구간이 두 벌로 남는다.
  -- 기간을 하루로 좁히면 겹침은 없어지지만 하루 걸렀을 때 저절로 메워지는
  -- 성질도 같이 없어져서, 그 겹침은 자가 치유 장치로 두고 여기서 접는다.
  -- 읽지 않는 표를 청소하는 것이 어색하긴 하나, 옮기면 어디에도 없게 된다.
  DELETE FROM kis_index_daily
   WHERE id IN (
     SELECT id FROM (
       SELECT id, ROW_NUMBER() OVER (PARTITION BY stck_bsop_date, short_code
                                     ORDER BY updated_at DESC, id DESC) rn
         FROM kis_index_daily)
      WHERE rn > 1);

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
       t.listed_market_cap = s.listed_market_cap
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
