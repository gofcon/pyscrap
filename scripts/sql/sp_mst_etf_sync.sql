CREATE OR REPLACE PROCEDURE sp_mst_etf_sync (p_inserted OUT NUMBER) AS
-- 이름이 바뀌는 자리다. s 쪽(수집물)은 소스가 쓰는 이름 그대로고, t 쪽
-- (mst_etf)은 내부 시스템 이름이다 -- isu_cd -> short_code, isu_nm -> prod_nm.
-- mst_* 가 층으로 존재하는 이유가 그 번역이라, MERGE 의 ON 절이 매번 두 어휘를
-- 마주 놓는다. 옛 이름의 표는 mst_etf_old 로 남아 있다.
-- krx_etf_daily(거래소 유니버스)와 운용사 상품목록들에서 mst_etf(정제 마스터)를
-- 채운다. sp_mst_fuopt_sync / sp_mst_bond_sync 와 같은 방식이다: 유니버스는
-- 신규만 INSERT, 운용사 코드는 비어 있을 때만 채움. 사람이 손댄 값은 살아남는다.
--
-- 이 표가 있는 이유는 운용사마다 조회 코드가 따로라서다. 구성종목 엔드포인트가
-- KODEX 는 2ETF01, SOL 은 211096, PLUS 는 006184 를 요구하는데 셋 다 거래소
-- 데이터 어디에도 없다. 각 운용사 목록이 자기 코드 옆에 단축코드를 같이 주므로,
-- 여기서 한 번 접어 두면 구성종목 빌더들이 전부 같은 표를 보게 된다.
--
-- 순서가 중요하다: daily_batch1 이 krx_etf_daily 와 운용사 목록들을 갱신한 뒤에
-- 돌아야 한다. 먼저 돌면 어제 목록으로 접는다.
--
-- MERGE 마다 NO_PARALLEL 힌트가 붙는 이유: 이 DB 는 병렬 DML 이 기본 활성인데,
-- 같은 표를 잇달아 갱신하는 아래 문장들이 ORA-12860(형제 행 잠금 대기 중
-- 교착)으로 죽었다. sp_stock_index_his_sync 는 세션 설정으로 껐지만 여기서는
-- 못 쓴다 -- 그쪽은 중간에 REFRESH 가 커밋을 넣어 주는데, 여기는 트랜잭션이
-- 열린 채 끝나 되돌릴 때 ORA-12841 이 난다. 문장 단위 힌트는 그 제약이 없고
-- 다른 세션 상태를 건드리지도 않는다. 천여 행짜리라 병렬로 얻을 것도 없다.
BEGIN
  -- 1) 유니버스: 거래소에 보인 적 있는 단축코드를 신규만 넣는다.
  MERGE /*+ NO_PARALLEL */ INTO mst_etf t
  USING (
    SELECT isu_cd,
           MIN(bas_dd) AS first_seen,
           MAX(isu_nm) KEEP (DENSE_RANK LAST ORDER BY bas_dd) AS isu_nm
      FROM krx_etf_daily
     WHERE isu_cd IS NOT NULL
     GROUP BY isu_cd
  ) s
  ON (t.short_code = s.isu_cd)
  WHEN NOT MATCHED THEN
    INSERT (short_code, prod_nm, first_seen)
    VALUES (s.isu_cd, s.isu_nm, s.first_seen);

  p_inserted := SQL%ROWCOUNT;

  -- 2) 운용사 코드: 비어 있는 것만 채운다.
  --
  -- 소스가 둘이다. JSON 목록을 주는 넷은 수집물에서, 나머지 다섯(TIGER, KB,
  -- NH, 키움, TIME)은 user_etf 에서 온다 -- 그쪽은 목록이 화면이고 코드가 링크
  -- href 나 data- 속성, 셀 문장 속에 박혀 있어 표로 읽히지 않는다. ETF 의
  -- 운용사 코드는 상장 때 정해지면 안 바뀌므로 손으로 적어도 신규 상장분만
  -- 가끔 더하면 된다.
  --
  -- 수집물 쪽은 비어 있을 때만 채운다 -- 여느 마스터와 같은 규칙이다.
  --
  -- 한 ETF 는 운용사가 하나뿐이라 소스에 단축코드가 겹칠 일이 없어야 하지만,
  -- 목록이 갱신되는 중이거나 브랜드가 옮겨가면 겹칠 수 있다. MERGE 는 소스 키가
  -- 중복되면 ORA-30926 으로 죽으므로 한 행으로 접어 둔다.
  MERGE /*+ NO_PARALLEL */ INTO mst_etf t
  USING (
    SELECT isu_cd,
           MAX(amc)        KEEP (DENSE_RANK FIRST ORDER BY amc) AS amc,
           MAX(amc_etf_cd) KEEP (DENSE_RANK FIRST ORDER BY amc) AS amc_etf_cd
      FROM (
        SELECT stkticker AS isu_cd, 'KODEX' AS amc, fid AS amc_etf_cd
          FROM kodex_etf WHERE stkticker IS NOT NULL
        UNION ALL
        SELECT etf_cd6,             'SOL',          fund_cd
          FROM sol_etf   WHERE etf_cd6   IS NOT NULL
        UNION ALL
        SELECT namecode,            'PLUS',         prod_id
          FROM plus_etf  WHERE namecode  IS NOT NULL
        UNION ALL
        -- ACE 만 ISIN 을 주므로 단축코드는 거기서 잘라 쓴다. ISIN 의 4~9번째가
        -- 단축코드다(KR7105190003 -> 105190 = ACE 200, 실물 3건 대조 확인).
        -- 자릿수를 잘라내는 방향이라 안전하다 -- 반대로 단축코드에서 ISIN 을
        -- 만드는 것은 체크디지트를 지어내는 일이라 하지 않는다.
        SELECT SUBSTR(stockcd, 4, 6),'ACE',          fundcd
          FROM ace_etf   WHERE stockcd IS NOT NULL AND LENGTH(stockcd) = 12
      )
     GROUP BY isu_cd
  ) s
  ON (t.short_code = s.isu_cd)
  WHEN MATCHED THEN
    UPDATE SET t.amc = s.amc, t.amc_etf_cd = s.amc_etf_cd
    WHERE t.amc_etf_cd IS NULL;

  -- 3) 사람이 적은 코드: 덮어쓴다.
  --
  -- user_etf 는 수집이 닿지 않는 다섯 운용사를 손으로 적어 두는 표다. 비어
  -- 있을 때만 채우는 위 규칙과 달리 여기는 무조건 덮어쓴다 -- 표 이름 그대로
  -- '사람이 정한 값'이라, 수집된 코드가 틀렸을 때 고칠 자리가 여기 말고는
  -- 없기 때문이다. 지우면 다음 실행에서 수집물 쪽 값으로 돌아간다.
  MERGE /*+ NO_PARALLEL */ INTO mst_etf t
  USING (
    SELECT isu_cd, MAX(amc) amc, MAX(amc_etf_cd) amc_etf_cd
      FROM user_etf
     WHERE amc IS NOT NULL AND amc_etf_cd IS NOT NULL
     GROUP BY isu_cd
  ) s
  ON (t.short_code = s.isu_cd)
  WHEN MATCHED THEN
    UPDATE SET t.amc = s.amc, t.amc_etf_cd = s.amc_etf_cd;

  -- 4) ISIN: ACE 목록에서. 5) 가 붙기 전까지 유일한 소스였다.
  --
  -- 거래소 쪽 수집물에는 ETF 의 ISIN 이 없고(krx_etf_daily 에 컬럼 자체가 없다),
  -- 운용사 중에서도 한투만 stockCd 로 준다. 그래서 여기 채워지는 것은 ACE
  -- 종목뿐이다. 비어 있는 것만 채우므로 5) 가 생겨도 이 문장은 그대로 둔다 --
  -- 먼저 도는 이쪽이 채운 값을 5) 가 덮지 않는다.
  MERGE /*+ NO_PARALLEL */ INTO mst_etf t
  USING (
    SELECT SUBSTR(stockcd, 4, 6) AS isu_cd,
           MAX(stockcd) KEEP (DENSE_RANK FIRST ORDER BY fundcd) AS isin
      FROM ace_etf
     WHERE stockcd IS NOT NULL AND LENGTH(stockcd) = 12
     GROUP BY SUBSTR(stockcd, 4, 6)
  ) s
  ON (t.short_code = s.isu_cd)
  WHEN MATCHED THEN
    UPDATE SET t.isin = s.isin
    WHERE t.isin IS NULL;

  -- 5) ISIN: KRX 데이터마켓의 ETF 종목 목록에서. 이걸로 전종목이 채워진다.
  --
  -- 4) 가 기다리던 "다른 소스"가 이것이다. 로그인이 필요한 화면이라 늦게
  -- 붙었다(api_mst 의 KRX_ETF_LIST / KRX_DATA_LOGIN). 단축코드와 ISIN 을
  -- 나란히 주므로 접을 것이 없고, 처음 붙였을 때 1,051 종목이 한 번에
  -- 채워지면서 기존 ACE 값 112 건과 한 건도 어긋나지 않았다.
  --
  -- 날짜를 가리지 않고 api_rst 전체에서 접는다: 한 종목의 ISIN 은 바뀌지
  -- 않고, 상장폐지된 종목도 마지막으로 본 값이 남아야 mst_etf 의 과거 행이
  -- 비지 않는다. api_rst.api_id 에는 인덱스가 있다.
  --
  -- 이 프로시저는 daily_batch2 안에서 run_cycle 보다 *먼저* 돈다(서비스
  -- 파일의 ExecStart 순서). 그래서 여기서 보는 목록은 늘 직전 실행분이고,
  -- 갓 상장한 ETF 는 하루 뒤에 ISIN 이 채워진다 -- 그날의 구성종목 잡도
  -- 하루 늦게 생긴다. 당일부터 받으려면 KRX_ETF_LIST 를 daily_batch1 로
  -- 옮기면 되지만, 쿠키가 프로세스를 못 넘으므로 그 사이클에도 로그인 행이
  -- 하나 더 있어야 한다.
  MERGE /*+ NO_PARALLEL */ INTO mst_etf t
  USING (
    SELECT JSON_VALUE(result_json, '$.ISU_SRT_CD') AS isu_cd,
           MAX(JSON_VALUE(result_json, '$.ISU_CD')) AS isin
      FROM api_rst
     WHERE api_id = 'KRX_ETF_LIST'
       AND JSON_VALUE(result_json, '$.ISU_CD') IS NOT NULL
     GROUP BY JSON_VALUE(result_json, '$.ISU_SRT_CD')
  ) s
  ON (t.short_code = s.isu_cd)
  WHEN MATCHED THEN
    UPDATE SET t.isin = s.isin
    WHERE t.isin IS NULL;

END;
