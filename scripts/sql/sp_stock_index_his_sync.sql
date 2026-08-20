CREATE OR REPLACE PROCEDURE sp_stock_index_his_sync (p_merged OUT NUMBER) AS
-- kis_index_daily(스크래핑 원본 적재분) -> stock_index_his(참조 시계열) 이관.
--
-- mv_id 를 여기서 부여함: 엔드포인트가 지수 코드 하나만 받고 그 값이 ApiMst 의
-- api_url 에 2001(KOSPI200)로 고정돼 있어서, 원본 행에는 어느 지수인지가 남지
-- 않음. 지수를 추가하려면 ApiMst 행과 이 매핑을 함께 늘려야 함.
--
-- price_change/change_rate 는 원본에 없어 종가 시계열에서 LAG 로 계산함.
-- listed_market_cap 도 이 엔드포인트가 주지 않으므로 건드리지 않고 남겨둠
-- (다른 소스로 채운 과거분을 덮어쓰지 않기 위함).
  c_mv_id CONSTANT VARCHAR2(20) := 'KI2';
BEGIN
  MERGE INTO stock_index_his t
  USING (
    SELECT TO_DATE(k.stck_bsop_date, 'YYYYMMDD') AS trade_date,
           c_mv_id AS mv_id,
           k.bstp_nmix_prpr AS close_price,
           k.bstp_nmix_oprc AS open_price,
           k.bstp_nmix_hgpr AS high_price,
           k.bstp_nmix_lwpr AS low_price,
           k.acml_vol       AS volume,
           k.acml_tr_pbmn   AS trading_value
      FROM (
        -- 같은 영업일이 여러 잡/구간에서 중복 적재될 수 있으므로 최신 1건만.
        -- MERGE 는 소스 키가 중복되면 ORA-30926 으로 실패함.
        SELECT k.*, ROW_NUMBER() OVER (PARTITION BY stck_bsop_date
                                       ORDER BY updated_at DESC NULLS LAST, id DESC) rn
          FROM kis_index_daily k
      ) k
     WHERE k.rn = 1
  ) s
  ON (t.trade_date = s.trade_date AND t.mv_id = s.mv_id)
  WHEN MATCHED THEN UPDATE SET
       t.close_price = s.close_price, t.open_price = s.open_price,
       t.high_price  = s.high_price,  t.low_price  = s.low_price,
       t.volume      = s.volume,      t.trading_value = s.trading_value
  WHEN NOT MATCHED THEN
    INSERT (trade_date, mv_id, close_price, open_price, high_price, low_price, volume, trading_value)
    VALUES (s.trade_date, s.mv_id, s.close_price, s.open_price, s.high_price, s.low_price,
            s.volume, s.trading_value);

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
END;
