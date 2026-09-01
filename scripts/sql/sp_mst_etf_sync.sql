CREATE OR REPLACE PROCEDURE sp_mst_etf_sync (p_inserted OUT NUMBER) AS
-- krx_etf_list(거래소 ETF 종목 목록)와 운용사 상품목록들에서 mst_etf(정제
-- 마스터)를 채운다.
--
-- 이름이 바뀌는 자리다. s 쪽(수집물)은 소스가 쓰는 이름 그대로고, t 쪽은 내부
-- 시스템 이름이다 -- isu_srt_cd -> short_code, isu_nm -> prod_nm,
-- etf_obj_idx_nm -> target_idx_nm. mst_* 가 층으로 존재하는 이유가 그 번역이라,
-- MERGE 의 ON 절이 매번 두 어휘를 마주 놓는다.
--
-- 예전에는 krx_etf_daily 가 유니버스였고, 그 표에는 단축코드와 이름과 시세밖에
-- 없어서 마스터도 딱 그만큼이었다. ISIN 은 api_rst 에 쌓이던 KRX_ETF_LIST 를
-- JSON_VALUE 로 파서 채웠는데, 그 API 가 전용 표(krx_etf_list)를 갖게 되면서
-- api_rst 쪽은 0건이 되었다 -- 즉 그 블록은 한동안 아무것도 하지 않고 있었다.
-- 이제 같은 목록을 표에서 바로 읽고, ISIN 만이 아니라 목록이 주는 것을 전부
-- 가져온다: 기초지수, 추적배수, 복제방법, 시장·자산 분류, CU 수량, 총보수,
-- 과세유형.
--
-- krx_etf_daily 는 남는다. 상장폐지된 ETF 는 목록에서 빠지지만 과거 시세에는
-- 남아 있어서, 그 표가 없으면 마스터에서 행이 사라진다. 최초관찰일도 거기서
-- 온다 -- 목록은 오늘 무엇이 있는지만 말한다.
--
-- amc 는 거래소가 적은 운용사명(삼성자산운용)이다. 예전에는 여기에 브랜드
-- 키(KODEX)를 넣었는데, 그 값의 출처는 운용사 목록을 어느 이름으로 수집했는가
-- 였을 뿐 데이터가 아니었다. 거래소가 같은 것을 이름으로 주므로 그쪽을 쓴다.
-- 둘은 1:1 로 대응한다(KODEX 241건 = 삼성자산운용 241건, 아홉 브랜드 전부
-- 어긋남 없음). 그래서 아래 3)/4) 는 amc_etf_cd 만 건드린다 -- 운용사 목록이
-- 줄 수 있는 것은 그 코드뿐이고, 운용사가 누구인지는 거래소가 더 잘 안다.
--
-- 순서가 중요하다: daily_batch 가 krx_etf_list 와 운용사 목록들을 갱신한 뒤에
-- 돌아야 한다. 먼저 돌면 어제 목록으로 접는다.
--
-- MERGE 마다 NO_PARALLEL 힌트가 붙는 이유: 이 DB 는 병렬 DML 이 기본 활성인데,
-- 같은 표를 잇달아 갱신하는 아래 문장들이 ORA-12860(형제 행 잠금 대기 중
-- 교착)으로 죽었다. sp_stock_index_his_sync 는 세션 설정으로 껐지만 여기서는
-- 못 쓴다 -- 그쪽은 중간에 REFRESH 가 커밋을 넣어 주는데, 여기는 트랜잭션이
-- 열린 채 끝나 되돌릴 때 ORA-12841 이 난다. 문장 단위 힌트는 그 제약이 없고
-- 다른 세션 상태를 건드리지도 않는다. 천여 행짜리라 병렬로 얻을 것도 없다.
BEGIN
  -- 1) 거래소 목록: 유니버스와 펀드의 사실들.
  --
  -- 신규만 넣는 다른 마스터와 달리 기존 행도 갱신한다. 여기 담긴 것 중
  -- 상장주수, 총보수, CU 수량은 움직이는 값이고, 목록이 늘 최신을 준다.
  -- 사람이 적는 칸(description)과 수집으로만 얻는 칸(amc_etf_cd), 그리고
  -- 관측 이력(first_seen)은 건드리지 않는다.
  --
  -- 잡을 여러 날 돌리면 같은 종목이 날짜별로 쌓이므로 최신 기준일 한 행만
  -- 쓴다. MERGE 는 소스 키가 중복되면 ORA-30926 으로 죽는다.
  --
  -- 상장일은 목록이 '2002/10/14' 로 준다. 이 프로젝트의 날짜는 전부
  -- YYYYMMDD 8자리이고 mst_stock.list_dt 도 그 모양이라 구분자를 뗀다 --
  -- 두 마스터의 같은 이름 컬럼이 다른 모양이면 조인할 때마다 걸린다.
  MERGE /*+ NO_PARALLEL */ INTO mst_etf t
  USING (
    SELECT isu_srt_cd, isu_cd, isu_nm, isu_abbrv, isu_eng_nm,
           REPLACE(list_dd, '/') AS list_dd,
           etf_obj_idx_nm, idx_calc_inst_nm2, etf_replica_methd_tp_cd,
           idx_mkt_clss_nm, idx_asst_clss_nm, com_abbrv,
           cu_qty, list_shrs, etf_tot_fee, tax_tp_cd
      FROM (SELECT l.*, ROW_NUMBER() OVER (PARTITION BY l.isu_srt_cd
                                           ORDER BY l.bas_dd DESC, l.id DESC) rn
              FROM krx_etf_list l WHERE l.isu_srt_cd IS NOT NULL)
     WHERE rn = 1
  ) s
  ON (t.short_code = s.isu_srt_cd)
  WHEN MATCHED THEN UPDATE SET
       t.isin = s.isu_cd, t.prod_nm = s.isu_nm, t.prod_snm = s.isu_abbrv,
       t.prod_enm = s.isu_eng_nm, t.list_dt = s.list_dd,
       t.target_idx_nm = s.etf_obj_idx_nm, t.trace_mtd = s.idx_calc_inst_nm2,
       t.replica_mtd = s.etf_replica_methd_tp_cd, t.mkt_div = s.idx_mkt_clss_nm,
       t.idx_asst_class = s.idx_asst_clss_nm, t.amc = s.com_abbrv,
       t.cu_qty = s.cu_qty, t.list_cnt = s.list_shrs,
       t.etf_tot_fee = s.etf_tot_fee, t.tax_div = s.tax_tp_cd
  WHEN NOT MATCHED THEN
    INSERT (short_code, isin, prod_nm, prod_snm, prod_enm, list_dt,
            target_idx_nm, trace_mtd, replica_mtd, mkt_div, idx_asst_class,
            amc, cu_qty, list_cnt, etf_tot_fee, tax_div)
    VALUES (s.isu_srt_cd, s.isu_cd, s.isu_nm, s.isu_abbrv, s.isu_eng_nm, s.list_dd,
            s.etf_obj_idx_nm, s.idx_calc_inst_nm2, s.etf_replica_methd_tp_cd,
            s.idx_mkt_clss_nm, s.idx_asst_clss_nm, s.com_abbrv,
            s.cu_qty, s.list_shrs, s.etf_tot_fee, s.tax_tp_cd);

  p_inserted := SQL%ROWCOUNT;

  -- 2) 시세에만 남은 종목과, 최초관찰일.
  --
  -- 목록은 오늘 상장돼 있는 것만 보여준다. 상장폐지된 ETF 는 거기서 빠지지만
  -- 과거 시세에는 남아 있으므로, 그 행을 여기서 살린다 -- 이름 말고는 채울
  -- 것이 없어 나머지는 NULL 로 남는다.
  --
  -- first_seen 은 비어 있을 때만 채운다. 1) 이 새로 넣은 행이 그렇고, 이미
  -- 값이 있는 행은 더 오래된 관측일 수 있으므로 덮지 않는다.
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
  WHEN MATCHED THEN
    UPDATE SET t.first_seen = s.first_seen WHERE t.first_seen IS NULL
  WHEN NOT MATCHED THEN
    INSERT (short_code, prod_nm, first_seen)
    VALUES (s.isu_cd, s.isu_nm, s.first_seen);

  -- 3) 운용사 코드: 비어 있는 것만 채운다.
  --
  -- 소스가 둘이다. JSON 목록을 주는 넷은 수집물에서, 나머지 다섯(TIGER, KB,
  -- NH, 키움, TIME)은 user_etf 에서 온다 -- 그쪽은 목록이 화면이고 코드가 링크
  -- href 나 data- 속성, 셀 문장 속에 박혀 있어 표로 읽히지 않는다. ETF 의
  -- 운용사 코드는 상장 때 정해지면 안 바뀌므로 손으로 적어도 신규 상장분만
  -- 가끔 더하면 된다.
  --
  -- 한 ETF 는 운용사가 하나뿐이라 소스에 단축코드가 겹칠 일이 없어야 하지만,
  -- 목록이 갱신되는 중이거나 브랜드가 옮겨가면 겹칠 수 있다. MERGE 는 소스 키가
  -- 중복되면 ORA-30926 으로 죽으므로 한 행으로 접어 둔다.
  MERGE /*+ NO_PARALLEL */ INTO mst_etf t
  USING (
    SELECT isu_cd,
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
    UPDATE SET t.amc_etf_cd = s.amc_etf_cd
    WHERE t.amc_etf_cd IS NULL;

  -- 4) 사람이 적은 코드: 덮어쓴다.
  --
  -- user_etf 는 수집이 닿지 않는 다섯 운용사를 손으로 적어 두는 표다. 비어
  -- 있을 때만 채우는 위 규칙과 달리 여기는 무조건 덮어쓴다 -- 표 이름 그대로
  -- '사람이 정한 값'이라, 수집된 코드가 틀렸을 때 고칠 자리가 여기 말고는
  -- 없기 때문이다. 지우면 다음 실행에서 수집물 쪽 값으로 돌아간다.
  --
  -- user_etf.amc 는 브랜드 키(TIGER)라 더는 마스터로 옮기지 않는다. 그 표에서
  -- 필요한 것은 코드뿐이고, 운용사명은 1) 이 거래소에서 가져온다.
  MERGE /*+ NO_PARALLEL */ INTO mst_etf t
  USING (
    SELECT isu_cd, MAX(amc_etf_cd) amc_etf_cd
      FROM user_etf
     WHERE amc IS NOT NULL AND amc_etf_cd IS NOT NULL
     GROUP BY isu_cd
  ) s
  ON (t.short_code = s.isu_cd)
  WHEN MATCHED THEN
    UPDATE SET t.amc_etf_cd = s.amc_etf_cd;

  -- 5) ISIN: ACE 목록에서, 비어 있는 것만.
  --
  -- 1) 이 전종목의 ISIN 을 주므로 평소에는 채울 것이 없다. 남겨 둔 이유는
  -- 거래소 목록이 로그인 화면이라 다른 수집물보다 잘 비고, 그때 ACE 112 종목
  -- 이라도 ISIN 을 갖게 하기 위해서다. 두 소스가 어긋난 적은 없다.
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

END;
