CREATE OR REPLACE PROCEDURE sp_mst_etf_sync (p_inserted OUT NUMBER) AS
-- krx_etf_daily(거래소 유니버스)와 운용사 상품목록들에서 mst_etf(정제 마스터)를
-- 채운다. sp_mst_fuopt_sync / sp_mst_bond_sync 와 같은 방식이다: 유니버스는
-- 신규만 INSERT, 운용사 코드는 비어 있을 때만 채움. 사람이 손댄 값은 살아남는다.
--
-- 이 표가 있는 이유는 운용사마다 조회 코드가 따로라서다. 구성종목 엔드포인트가
-- KODEX 는 2ETF01, SOL 은 211096, PLUS 는 006184 를 요구하는데 셋 다 거래소
-- 데이터 어디에도 없다. 각 운용사 목록이 자기 코드 옆에 단축코드를 같이 주므로,
-- 여기서 한 번 접어 두면 구성종목 빌더들이 전부 같은 표를 보게 된다.
--
-- 순서가 중요하다: daily_batch1 이 krx_etf_daily 와 세 운용사 목록을 갱신한 뒤에
-- 돌아야 한다. 먼저 돌면 어제 목록으로 접는다.
BEGIN
  -- 1) 유니버스: 거래소에 보인 적 있는 단축코드를 신규만 넣는다.
  MERGE INTO mst_etf t
  USING (
    SELECT isu_cd,
           MIN(bas_dd) AS first_seen,
           MAX(isu_nm) KEEP (DENSE_RANK LAST ORDER BY bas_dd) AS isu_nm
      FROM krx_etf_daily
     WHERE isu_cd IS NOT NULL
     GROUP BY isu_cd
  ) s
  ON (t.isu_cd = s.isu_cd)
  WHEN NOT MATCHED THEN
    INSERT (isu_cd, isu_nm, first_seen)
    VALUES (s.isu_cd, s.isu_nm, s.first_seen);

  p_inserted := SQL%ROWCOUNT;

  -- 2) 운용사 코드: 비어 있는 것만 채운다.
  --
  -- 한 ETF 는 운용사가 하나뿐이라 소스에 단축코드가 겹칠 일이 없어야 하지만,
  -- 목록이 갱신되는 중이거나 브랜드가 옮겨가면 겹칠 수 있다. MERGE 는 소스 키가
  -- 중복되면 ORA-30926 으로 죽으므로 한 행으로 접어 둔다.
  MERGE INTO mst_etf t
  USING (
    SELECT isu_cd,
           MAX(amc)        KEEP (DENSE_RANK FIRST ORDER BY amc) AS amc,
           MAX(amc_etf_cd) KEEP (DENSE_RANK FIRST ORDER BY amc) AS amc_etf_cd
      FROM (
        SELECT stkticker AS isu_cd, 'KODEX' AS amc, fid     AS amc_etf_cd
          FROM kodex_etf WHERE stkticker IS NOT NULL
        UNION ALL
        SELECT etf_cd6,             'SOL',          fund_cd
          FROM sol_etf   WHERE etf_cd6   IS NOT NULL
        UNION ALL
        SELECT namecode,            'PLUS',         prod_id
          FROM plus_etf  WHERE namecode  IS NOT NULL
      )
     GROUP BY isu_cd
  ) s
  ON (t.isu_cd = s.isu_cd)
  WHEN MATCHED THEN
    UPDATE SET t.amc = s.amc, t.amc_etf_cd = s.amc_etf_cd
    WHERE t.amc_etf_cd IS NULL;
END;
