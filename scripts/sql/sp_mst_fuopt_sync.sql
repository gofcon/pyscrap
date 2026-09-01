CREATE OR REPLACE PROCEDURE sp_mst_fuopt_sync (p_inserted OUT NUMBER) AS
-- krx_deriv_info(거래소 전종목 기본정보, 매일 재적재)에서 mst_fuopt(정제 마스터)로
-- 신규 종목만 적재. 기존 행은 건드리지 않음 -- 사람이 손댄 값을 지키기 위함.
--
-- 예전에는 KIS 마스터(fo_idx_code_mst)가 유일한 소스였고, 거기 없는 만기 지난
-- 종목은 app.services.discovery 가 행사가 사다리를 훑어 코드를 복원했다.
-- mst_fuopt 62,919행 중 38,700행이 그렇게 만들어진 값이었고, 그중 1,794건은
-- 존재한 적도 없는 종목이었다(단 한 건의 시세도 돌아오지 않았다).
--
-- 거래소가 1996년치까지 종목 목록을 주므로 이제 추론하지 않는다:
--   short_code    거래소 단축코드 (기본키)
--   kis_short_cd  KIS 표기 -- KIS 로 나가는 모든 요청이 이 값을 쓴다
-- 둘은 만기 칸의 자릿수만 다르다. 아래 to_kis 식 참고.
BEGIN
  MERGE /*+ NO_PARALLEL */ INTO mst_fuopt t
  USING (
    SELECT k.isu_srt_cd AS short_code,
           -- KRX 8자 -> KIS 표기. 월물은 월을 두 자리로 되돌리고(9->09,
           -- A/B/C->10/11/12), 위클리는 만기 뒤에 'W' 를 넣고, 선물은 행사가
           -- 자리('000')를 뗀다. mst_fuopt 의 마스터 출처 21,643행으로 대조해
           -- 전건 일치를 확인한 식이다.
           CASE
             WHEN k.prod_id IN ('KRDRVOPWKI','KRDRVOPWKM')
               THEN SUBSTR(k.isu_srt_cd,1,5)||'W'||SUBSTR(k.isu_srt_cd,6,3)
             ELSE SUBSTR(k.isu_srt_cd,1,4)
                  || CASE SUBSTR(k.isu_srt_cd,5,1)
                       WHEN 'A' THEN '10' WHEN 'B' THEN '11' WHEN 'C' THEN '12'
                       ELSE '0'||SUBSTR(k.isu_srt_cd,5,1) END
                  || CASE WHEN k.rght_tp_nm = '-' THEN '' ELSE SUBSTR(k.isu_srt_cd,6,3) END
           END AS kis_short_cd,
           k.isu_abbrv AS prod_nm,
           -- meta_maturity.prod_type 과 같은 어휘. 어긋나면 만기일이 안 붙는다.
           CASE k.prod_id
             WHEN 'KRDRVFUK2I' THEN 'K2I' WHEN 'KRDRVOPK2I' THEN 'K2I'
             WHEN 'KRDRVFUMKI' THEN 'MKI' WHEN 'KRDRVOPMKI' THEN 'MKI'
             WHEN 'KRDRVOPWKI' THEN 'WKI' WHEN 'KRDRVOPWKM' THEN 'WKM'
           END AS prod_type,
           CASE k.rght_tp_nm WHEN '콜옵션' THEN 'CALL' WHEN '풋옵션' THEN 'PUT'
                             ELSE 'FUT' END AS call_put_cd,
           k.setlmult AS cont_mult,
           -- 'C 202609   335.0' -> 202609,  'C 2609W1   945.0' -> 2609W1
           REGEXP_SUBSTR(k.isu_abbrv, '[0-9]{6}|[0-9]{4}W[0-9]') AS mat_code,
           TO_DATE(k.lsttrd_dd, 'YYYY/MM/DD') AS mat_date,
           NULLIF(k.exer_prc, 0) AS strike_prc
      FROM krx_deriv_info k
     WHERE k.prod_id IN ('KRDRVFUK2I','KRDRVOPK2I','KRDRVFUMKI',
                         'KRDRVOPMKI','KRDRVOPWKI','KRDRVOPWKM')
       -- 스프레드(SP)는 제외한다. 두 만기를 한 코드에 담아 mat_code 가 하나로
       -- 정해지지 않고, KIS 쪽에도 대응이 없다.
       AND SUBSTR(k.isu_srt_cd,1,1) NOT IN ('D','4')
       AND REGEXP_SUBSTR(k.isu_abbrv, '[0-9]{6}|[0-9]{4}W[0-9]') IS NOT NULL
  ) s
  ON (t.short_code = s.short_code)
  WHEN NOT MATCHED THEN
    INSERT (short_code, kis_short_cd, prod_nm, prod_type, call_put_cd,
            cont_mult, mat_code, mat_date, strike_prc)
    VALUES (s.short_code, s.kis_short_cd, s.prod_nm, s.prod_type, s.call_put_cd,
            s.cont_mult, s.mat_code, s.mat_date, s.strike_prc);

  p_inserted := SQL%ROWCOUNT;

  -- 기초자산은 거래소 목록에 없다. KIS 마스터에서 kis_short_cd 로 붙여 채운다.
  -- 비어 있는 것만 채우므로 사람이 넣은 값은 그대로다.
  MERGE /*+ NO_PARALLEL */ INTO mst_fuopt t
  USING (
    SELECT short_code AS kis_short_cd,
           MAX(unas_short_code) KEEP (DENSE_RANK LAST ORDER BY trade_at, id) AS ul_code,
           MAX(unas_kor_name)   KEEP (DENSE_RANK LAST ORDER BY trade_at, id) AS ul_nm
      FROM fo_idx_code_mst
     WHERE info_type IN ('1','5','6','L','M','N','O','D','E')
       AND unas_short_code IS NOT NULL
     GROUP BY short_code
  ) s
  ON (t.kis_short_cd = s.kis_short_cd)
  WHEN MATCHED THEN UPDATE SET t.ul_code = s.ul_code, t.ul_nm = s.ul_nm
                   WHERE t.ul_code IS NULL;

  -- 만기 달력이 뒤늦게 채워진 종목의 만기일을 메운다.
  --
  -- 위 MERGE 는 신규만 INSERT 하고 기존 행은 손대지 않는다. 그런데 종목이
  -- meta_maturity 보다 먼저 상장되면 그 시점에 mat_date 가 NULL 로 들어가고,
  -- 나중에 달력을 채워도 그 행은 영영 NULL 로 남는다. 실제로 위클리 490 종목이
  -- 그 상태로 수집에서 통째로 빠졌다 -- 종목 선택 쿼리가 mat_date 로 조인하므로
  -- NULL 은 탈락한다.
  --
  -- 거래소에서 옮겨온 과거 종목도 여기서 만기일을 얻는다: 목록 화면은 코드와
  -- 이름만 주고 날짜를 주지 않는다.
  --
  -- NULL 인 것만 채우므로 사람이 넣은 값은 그대로다.
  MERGE /*+ NO_PARALLEL */ INTO mst_fuopt t
  USING meta_maturity m
     ON (t.prod_type = m.prod_type AND t.mat_code = m.mat_code)
  WHEN MATCHED THEN UPDATE SET t.mat_date   = m.mat_date,
                               t.front_date = m.prev_mat_date
                   WHERE t.mat_date IS NULL;
END;
