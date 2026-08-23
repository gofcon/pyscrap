CREATE OR REPLACE PROCEDURE sp_stock_index_his_sync (p_merged OUT NUMBER) AS
-- kis_index_daily(스크래핑 원본 적재분) -> stock_index_his(참조 시계열) 이관.
--
-- mv_id 는 원본의 short_code(잡의 FID_INPUT_ISCD 값)를 매핑해서 부여함.
-- KIS 의 지수 코드와 이 시계열의 mv_id 가 서로 다른 체계라 변환이 필요하고,
-- 지수를 추가하려면 아래 CASE 에 한 줄만 더하면 됨.
--
-- price_change/change_rate 는 원본에 없어 종가 시계열에서 LAG 로 계산함.
-- listed_market_cap 도 이 엔드포인트가 주지 않으므로 건드리지 않고 남겨둠
-- (다른 소스로 채운 과거분을 덮어쓰지 않기 위함).
BEGIN
  MERGE INTO stock_index_his t
  USING (
    SELECT TO_DATE(k.stck_bsop_date, 'YYYYMMDD') AS trade_date,
           CASE k.short_code WHEN '2001' THEN 'K2I' END AS mv_id,
           k.bstp_nmix_prpr AS close_price,
           k.bstp_nmix_oprc AS open_price,
           k.bstp_nmix_hgpr AS high_price,
           k.bstp_nmix_lwpr AS low_price,
           k.acml_vol       AS volume,
           k.acml_tr_pbmn   AS trading_value
      FROM (
        -- 같은 영업일이 여러 잡/구간에서 중복 적재될 수 있으므로 최신 1건만.
        -- MERGE 는 소스 키가 중복되면 ORA-30926 으로 실패함.
        SELECT k.*, ROW_NUMBER() OVER (PARTITION BY short_code, stck_bsop_date
                                       ORDER BY updated_at DESC NULLS LAST, id DESC) rn
          FROM kis_index_daily k
      ) k
     -- 매핑되지 않은 지수 코드는 조용히 흘려보내지 않고 아예 제외 (mv_id NULL
     -- 은 PK 위반이 됨). 새 지수를 붙일 때 CASE 갱신을 잊으면 여기서 0건으로
     -- 드러남.
     WHERE k.rn = 1 AND k.short_code = '2001'
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
