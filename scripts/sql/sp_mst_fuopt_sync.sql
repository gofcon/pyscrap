CREATE OR REPLACE PROCEDURE sp_mst_fuopt_sync (p_inserted OUT NUMBER) AS
-- fo_idx_code_mst(원본 마스터, 매일 전량 재적재)에서 mst_fuopt(정제 마스터)로
-- 신규 종목만 적재. 기존 short_code 는 건드리지 않음 -- mst_fuopt 에 사람이
-- 손댄 값이 있어도 덮어쓰지 않기 위함.
BEGIN
  MERGE INTO mst_fuopt t
  USING (
    SELECT b.short_code, b.prod_nm, b.prod_type, b.call_put_cd, b.ul_code, b.ul_nm,
           b.cont_mult, b.mat_code, b.strike_prc,
           m.mat_date, m.prev_mat_date AS front_date
      FROM (
        SELECT f.short_code,
               f.kor_name AS prod_nm,
               -- meta_maturity.prod_type 과 동일한 코드 체계이며, 그 조인 키의
               -- 앞부분. 어휘가 어긋나면 만기일이 안 붙음.
               --   K2I 코스피200 월물 (지수선물 + 월물 옵션)
               --   MKI 지수미니 (KIS 원본 표기)
               --   WKI 위클리 목요일 만기
               --   WKM 위클리 월요일 만기
               -- 지수선물(1)이 월물 옵션과 같은 K2I 인 것은 둘이 같은 날 만기이기
               -- 때문 -- 이 컬럼은 상품 분류가 아니라 '어느 만기 캘린더를 따르는가'.
               CASE
                 WHEN f.info_type IN ('1','5','6') THEN 'K2I'
                 WHEN f.info_type IN ('D','E')     THEN 'MKI'
                 WHEN f.info_type IN ('L','M')     THEN 'WKI'
                 WHEN f.info_type IN ('N','O')     THEN 'WKM'
               END AS prod_type,
               CASE
                 WHEN f.info_type IN ('5','L','N','D') THEN 'CALL'
                 WHEN f.info_type IN ('6','M','O','E') THEN 'PUT'
                 WHEN f.info_type = '1'                THEN 'FUT'
               END AS call_put_cd,
               f.unas_short_code AS ul_code,
               f.unas_kor_name   AS ul_nm,
               -- 거래승수: 미니 5만원, 그 외 KOSPI200 계열 25만원
               CASE WHEN f.info_type IN ('D','E') THEN 50000 ELSE 250000 END AS cont_mult,
               -- kor_name 에서 만기 토큰 추출: 'C 202609  335.0' -> 202609,
               -- '위클리C 2608W3  910.0' -> 2608W3. KIS 가 별도 배포하는 일별
               -- 마스터 CSV 의 optn_month_code 컬럼(원본 .mst 에는 없는 파생값)과
               -- 대조해 392,190건 전건 일치를 확인한 식임.
               REGEXP_SUBSTR(f.kor_name, '[0-9]{6}|[0-9]{4}W[0-9]') AS mat_code,
               NULLIF(f.acpr, 0) AS strike_prc,
               -- 원본은 short_code 에 유니크 제약이 없음. MERGE 는 소스에 키가
               -- 중복되면 ORA-30926 으로 실패하므로 여기서 1건으로 접어둠.
               ROW_NUMBER() OVER (PARTITION BY f.short_code ORDER BY f.id) AS rn
          FROM fo_idx_code_mst f
         WHERE f.info_type IN ('1','5','6','L','M','N','O','D','E')
      ) b
      -- 복합키 조인: mat_code 는 상품유형이 다르면 재사용되므로(위클리 목/월이
      -- 같은 '2308W1' 을 씀) prod_type 없이 붙이면 행이 불어남.
      LEFT JOIN meta_maturity m ON m.prod_type = b.prod_type AND m.mat_code = b.mat_code
     WHERE b.rn = 1
  ) s
  ON (t.short_code = s.short_code)
  WHEN NOT MATCHED THEN
    INSERT (short_code, prod_nm, prod_type, call_put_cd, ul_code, ul_nm,
            cont_mult, mat_code, mat_date, front_date, strike_prc)
    VALUES (s.short_code, s.prod_nm, s.prod_type, s.call_put_cd, s.ul_code, s.ul_nm,
            s.cont_mult, s.mat_code, s.mat_date, s.front_date, s.strike_prc);

  p_inserted := SQL%ROWCOUNT;
END;
