CREATE OR REPLACE PROCEDURE sp_meta_maturity_sync (p_inserted OUT NUMBER) AS
-- krx_deriv_info(거래소 전종목 기본정보, 매일 재적재)에서 meta_maturity(만기 달력)로
-- 새 만기만 적재. 기존 행은 건드리지 않는다.
--
-- 달력은 원래 손으로, 또는 KIS 에 마지막 거래일을 물어 채웠고 그래서 늘 뒤처졌다.
-- 2026-09 에 위클리 달력이 08-27/08-31 에서 끊긴 채로 있어 9월 위클리는 만기가
-- 안 붙었고, 만기로 조인하는 종목 선택 쿼리가 그것들을 전부 떨어뜨려 열흘간
-- 위클리가 하나도 수집되지 않았다. 거래소 목록은 종목마다 최종거래일
-- (lsttrd_dd)을 주므로, 상장되는 순간 달력도 같이 온다 -- 위클리는 한 주 앞,
-- 월물은 몇 년 앞.
--
-- sp_mst_fuopt_sync 보다 먼저 돈다. 그쪽 마지막 MERGE 가 만기 없는 종목의
-- 만기를 이 달력에서 메우므로, 달력이 먼저 늘어나 있어야 같은 날 그 자리가
-- 채워진다.
--
-- 원천은 셋이고 순서가 우선순위다.
--   1. 거래소 목록(krx_deriv_info) -- 최종거래일 그대로.
--   2. mst_fuopt -- 거래소 목록은 지금 상장된 것만 주므로, 하루라도 거르면
--      (로그인이 막혔던 2026-09-02~13 이 그랬다) 그 사이 상장되고 만기된
--      위클리는 목록에서 영영 사라진다. 마스터는 한 번 본 종목을 지우지
--      않으므로 거기 남은 만기로 그 구멍을 메운다. 거래소가 확인한 행만 쓴다:
--      KIS 마스터로 들어와 만기를 이 달력에서 받은 행을 다시 원천으로 삼으면
--      추정값이 확정값으로 둔갑한다.
--   3. KIS 마스터(fo_idx_code_mst) 의 만기코드에서 규칙으로 계산 -- 월물은 둘째
--      목요일, 목요일 위클리 'YYMMWn' 은 n번째 목요일, 월요일 위클리는 n번째
--      월요일. 거래소가 비어 있고 마스터에도 없는 만기, 즉 로그인이 막힌 동안
--      새로 상장된 위클리가 여기 온다. 휴일이면 하루 밀리는데 그건 규칙이 모른다
--      (기존 630행 중 32행이 그렇다: 추석·근로자의날·성탄절). 그래서 미확인이라
--      적어 두고, 거래소가 돌아오면 WHEN MATCHED 가 그 행의 날짜를 바로잡는다.
--      틀린 하루가 문제 되는 것은 그 만기 주의 종목 선택뿐이고, 달력에 없어서
--      계열 전체가 빠지는 것보다는 낫다.
--
--   mat_code  종목약명의 만기 부분: 'C 202609 335.0' -> 202609, 'C 2609W1 945.0' -> 2609W1
--   mat_scd   KIS 코드의 4~6번째 글자: 101W12 -> W12, A01609 -> 609, B09FCW945 -> FCW.
--             거래소 코드에서 만들 때는 월이 한 글자(9, A, B, C)라 두 자리로 되돌려야
--             한다 -- A0169000 -> 6 + 09, 101WC000 -> W + 12. 위클리는 두 코드의
--             4~5번째가 같아 거기에 'W' 만 붙인다. 만기코드(202609)에서 만들면 안
--             된다: 연도 자리가 KIS 의 글자(W)가 아니라 숫자가 되어 2026년 이전이
--             전부 틀린다. 기존 626행으로 대조해 손으로 잘못 넣은 둘 빼고 일치.
--   prev_mat_date  같은 계열의 직전 만기. (직전만기, 만기] 가 그 계약이 덮는 기간이라
--             v_k2i_atm 이 "그날 기준 가장 가까운 만기" 를 이 구간으로 찾는다.
--             목요일 위클리(WKI)는 월물 만기 주에 상장되지 않아 그 주를 건너뛰는데,
--             직전을 자기 계열에서만 구하면 월물 주도 다음 위클리의 구간에 들어가
--             그 주에도 "가장 가까운 위클리" 가 답이 된다. 달력에서 계산되는 값이라
--             매번 전체를 다시 구한다 -- 202712 의 직전이 202709 상장 전에 202706 으로
--             적힌 채 남는 식의 묵은 값을 없애기 위해서다. 다만 계열의 첫 행은 계산이
--             NULL 이므로 손으로 넣어 둔 값을 지킨다: 그게 없으면 v_k2i_atm 에서 그
--             첫 만기 이전 날짜가 전부 빠진다.
BEGIN
  MERGE /*+ NO_PARALLEL */ INTO meta_maturity t
  USING (
    SELECT prod_type, mat_code,
           -- 확정(src 1)이 하나라도 있으면 그것, 없으면 규칙값
           MIN(src) AS src,
           COALESCE(MIN(CASE WHEN src = 1 THEN mat_date END),
                    MIN(CASE WHEN src = 2 THEN mat_date END)) AS mat_date,
           MIN(mat_scd) AS mat_scd
      FROM (
        -- 거래소 코드에서: 위클리는 4~5번째+'W', 월물은 연도 글자 + 두 자리 월
        SELECT 1 AS src,
               u.prod_type,
               REGEXP_SUBSTR(k.isu_abbrv, '[0-9]{6}|[0-9]{4}W[0-9]') AS mat_code,
               TO_DATE(k.lsttrd_dd, 'YYYY/MM/DD') AS mat_date,
               CASE WHEN u.prod_type IN ('WKI','WKM')
                      THEN SUBSTR(k.isu_srt_cd, 4, 2) || 'W'
                    ELSE SUBSTR(k.isu_srt_cd, 4, 1)
                         || CASE SUBSTR(k.isu_srt_cd, 5, 1)
                              WHEN 'A' THEN '10' WHEN 'B' THEN '11' WHEN 'C' THEN '12'
                              ELSE '0' || SUBSTR(k.isu_srt_cd, 5, 1) END
               END AS mat_scd
          FROM krx_deriv_info k
          -- 어떤 prodId 가 어느 계열인지는 meta_fuopt_info 가 말한다
          JOIN (SELECT DISTINCT prod_type, krx_prod_ids FROM meta_fuopt_info) u
            ON INSTR(',' || u.krx_prod_ids || ',', ',' || k.prod_id || ',') > 0
           -- 스프레드는 만기가 둘이라 달력의 한 행이 아니다.
         WHERE SUBSTR(k.isu_srt_cd, 1, 1) NOT IN ('D', '4')
           AND k.lsttrd_dd IS NOT NULL
        UNION ALL
        -- 마스터의 거래소 코드에서, 위와 같은 규칙
        SELECT 1, f.prod_type, f.mat_code, f.mat_date,
               CASE WHEN f.prod_type IN ('WKI','WKM')
                      THEN SUBSTR(f.short_code, 4, 2) || 'W'
                    ELSE SUBSTR(f.short_code, 4, 1)
                         || CASE SUBSTR(f.short_code, 5, 1)
                              WHEN 'A' THEN '10' WHEN 'B' THEN '11' WHEN 'C' THEN '12'
                              ELSE '0' || SUBSTR(f.short_code, 5, 1) END
               END
          FROM mst_fuopt f
         WHERE f.mat_date IS NOT NULL
           AND (f.description IS NULL OR f.description NOT LIKE 'from fo_idx_code_mst%')
        UNION ALL
        -- KIS 코드에서: 월이 이미 두 자리라 4~6번째를 그대로 쓴다
        SELECT 2, r.prod_type, r.mat_code,
               CASE WHEN r.mat_code LIKE '%W%'
                    THEN NEXT_DAY(TO_DATE('20' || SUBSTR(r.mat_code, 1, 4) || '01', 'YYYYMMDD') - 1,
                                  CASE r.prod_type WHEN 'WKM' THEN 'MONDAY' ELSE 'THURSDAY' END)
                         + 7 * (TO_NUMBER(SUBSTR(r.mat_code, 6, 1)) - 1)
                    ELSE NEXT_DAY(TO_DATE(r.mat_code || '01', 'YYYYMMDD') - 1, 'THURSDAY') + 7
               END,
               SUBSTR(r.short_code, 4, 3)
          FROM (
            SELECT u.prod_type,
                   REGEXP_SUBSTR(f.kor_name, '[0-9]{6}|[0-9]{4}W[0-9]') AS mat_code,
                   f.short_code
              FROM fo_idx_code_mst f
              JOIN (SELECT DISTINCT prod_type, kis_info_types FROM meta_fuopt_info) u
                ON INSTR(',' || u.kis_info_types || ',', ',' || f.info_type || ',') > 0
             WHERE f.trade_at = (SELECT MAX(trade_at) FROM fo_idx_code_mst)
          ) r
         WHERE r.mat_code IS NOT NULL
      )
     WHERE mat_code IS NOT NULL
     GROUP BY prod_type, mat_code
  ) s
  ON (t.prod_type = s.prod_type AND t.mat_code = s.mat_code)
  -- 규칙으로 넣어 둔 행을 확정값이 바로잡는다.
  WHEN MATCHED THEN UPDATE SET t.mat_date = s.mat_date,
                               t.description = 'from krx_deriv_info (lsttrd_dd)'
                   WHERE s.src = 1 AND t.description LIKE 'derived by rule%'
  WHEN NOT MATCHED THEN
    INSERT (prod_type, mat_code, mat_date, mat_scd, description)
    VALUES (s.prod_type, s.mat_code, s.mat_date, s.mat_scd,
            CASE s.src WHEN 1 THEN 'from krx_deriv_info (lsttrd_dd)'
                       ELSE 'derived by rule from mat_code; unconfirmed (a holiday shifts it)' END);

  p_inserted := SQL%ROWCOUNT;

  -- 직전 만기를 달력 전체에서 다시 구한다 (위 설명). NO_PARALLEL 은 필수:
  -- 자동 병렬 DML 이 갱신 중인 표를 서브쿼리로 다시 읽다가 형제 슬레이브끼리
  -- 행 잠금으로 교착한다 (ORA-12860).
  UPDATE /*+ NO_PARALLEL */ meta_maturity t
     SET t.prev_mat_date = NVL((SELECT MAX(m.mat_date)
                                  FROM meta_maturity m
                                 WHERE m.prod_type = t.prod_type
                                   AND m.mat_date  < t.mat_date),
                               t.prev_mat_date)
   WHERE t.mat_date IS NOT NULL;
END;
