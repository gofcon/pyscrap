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
--
-- 거래소가 첫째, KIS 마스터(fo_idx_code_mst)가 둘째다. 거래소 목록은 로그인
-- 뒤에 있어 그게 막히면 비는데(2026-09-02~13 열흘), 그동안 상장된 위클리는
-- 마스터에 못 들어와 수집에서 통째로 빠졌다. KIS 마스터는 로그인이 없고 같은
-- 종목을 같은 날 주므로 그 구멍을 메운다 -- 다만 만기일이 없어, 그건 달력
-- (meta_maturity)에서 가져오고 description 에 미확인이라 적어 둔다. 거래소가
-- 돌아오면 첫 MERGE 가 그 행을 만나 만기일을 거래소 값으로 바꾸고 표시를
-- 지운다. 순서가 곧 우선순위다: 거래소 MERGE 가 먼저 넣고, KIS MERGE 는 없는
-- 것만 넣는다.
BEGIN
  MERGE /*+ NO_PARALLEL */ INTO mst_fuopt t
  USING (
    SELECT k.isu_srt_cd AS short_code,
           -- KRX 8자 -> KIS 표기. 월물은 월을 두 자리로 되돌리고(9->09,
           -- A/B/C->10/11/12), 위클리는 만기 뒤에 'W' 를 넣고, 선물은 행사가
           -- 자리('000')를 뗀다. mst_fuopt 의 마스터 출처 21,643행으로 대조해
           -- 전건 일치를 확인한 식이다.
           CASE
             WHEN u.prod_type IN ('WKI','WKM')
               THEN SUBSTR(k.isu_srt_cd,1,5)||'W'||SUBSTR(k.isu_srt_cd,6,3)
             ELSE SUBSTR(k.isu_srt_cd,1,4)
                  || CASE SUBSTR(k.isu_srt_cd,5,1)
                       WHEN 'A' THEN '10' WHEN 'B' THEN '11' WHEN 'C' THEN '12'
                       ELSE '0'||SUBSTR(k.isu_srt_cd,5,1) END
                  || CASE WHEN k.rght_tp_nm = '-' THEN '' ELSE SUBSTR(k.isu_srt_cd,6,3) END
           END AS kis_short_cd,
           k.isu_abbrv AS prod_nm,
           -- meta_maturity.prod_type 과 같은 어휘. 어떤 prodId 가 어느 계열인지는
           -- meta_fuopt_info.krx_prod_ids 가 말한다 -- 새 계열은 거기 한 행이다.
           u.prod_type,
           CASE k.rght_tp_nm WHEN '콜옵션' THEN 'CALL' WHEN '풋옵션' THEN 'PUT'
                             ELSE 'FUT' END AS call_put_cd,
           k.setlmult AS cont_mult,
           -- 'C 202609   335.0' -> 202609,  'C 2609W1   945.0' -> 2609W1
           REGEXP_SUBSTR(k.isu_abbrv, '[0-9]{6}|[0-9]{4}W[0-9]') AS mat_code,
           TO_DATE(k.lsttrd_dd, 'YYYY/MM/DD') AS mat_date,
           NULLIF(k.exer_prc, 0) AS strike_prc
      FROM krx_deriv_info k
      JOIN (SELECT DISTINCT prod_type, krx_prod_ids FROM meta_fuopt_info) u
        ON INSTR(',' || u.krx_prod_ids || ',', ',' || k.prod_id || ',') > 0
       -- 스프레드(SP)는 제외한다. 두 만기를 한 코드에 담아 mat_code 가 하나로
       -- 정해지지 않고, KIS 쪽에도 대응이 없다.
     WHERE SUBSTR(k.isu_srt_cd,1,1) NOT IN ('D','4')
       AND REGEXP_SUBSTR(k.isu_abbrv, '[0-9]{6}|[0-9]{4}W[0-9]') IS NOT NULL
  ) s
  ON (t.short_code = s.short_code)
  -- KIS 마스터로 먼저 들어온 행: 거래소가 만기일을 확인해 준다.
  WHEN MATCHED THEN UPDATE SET t.mat_date = s.mat_date, t.description = NULL
                   WHERE t.description LIKE 'from fo_idx_code_mst%'
  WHEN NOT MATCHED THEN
    INSERT (short_code, kis_short_cd, prod_nm, prod_type, call_put_cd,
            cont_mult, mat_code, mat_date, strike_prc)
    VALUES (s.short_code, s.kis_short_cd, s.prod_nm, s.prod_type, s.call_put_cd,
            s.cont_mult, s.mat_code, s.mat_date, s.strike_prc);

  p_inserted := SQL%ROWCOUNT;

  -- 거래소 목록에 없는 종목을 KIS 마스터의 최신 스냅샷에서 넣는다. 거래소가
  -- 비었으면(로그인 실패로 재적재가 안 된 날) 그날의 신규가 전부 여기로 온다.
  -- KIS 코드 -> 거래소 단축코드는 위 to_kis 의 역: 월물은 두 자리 월을 한 자리
  -- (10/11/12 -> A/B/C)로, 위클리는 'W' 를 빼고, 선물은 행사가 자리에 '000'.
  -- 양쪽에 다 있는 8,363 종목으로 대조해 전건 일치를 확인한 식이다.
  MERGE /*+ NO_PARALLEL */ INTO mst_fuopt t
  USING (
    SELECT b.*, m.mat_date, m.prev_mat_date AS front_date
      FROM (
        SELECT CASE WHEN u.prod_type IN ('WKI','WKM')
                      THEN SUBSTR(f.short_code, 1, 5) || SUBSTR(f.short_code, 7, 3)
                    ELSE SUBSTR(f.short_code, 1, 4)
                         || CASE SUBSTR(f.short_code, 5, 2)
                              WHEN '10' THEN 'A' WHEN '11' THEN 'B' WHEN '12' THEN 'C'
                              ELSE SUBSTR(f.short_code, 6, 1) END
                         || CASE WHEN SUBSTR(f.short_code, 1, 1) = 'A' THEN '000'
                                 ELSE SUBSTR(f.short_code, 7, 3) END
               END AS short_code,
               f.short_code AS kis_short_cd,
               f.kor_name   AS prod_nm,
               u.prod_type,
               -- KIS 마스터의 콜/풋/선물은 info_type 이 아니라 코드 첫 글자가 말한다:
               -- B 콜, C 풋, A 선물 (D 는 스프레드, 계열 목록에 없어 이미 빠짐)
               CASE SUBSTR(f.short_code, 1, 1) WHEN 'B' THEN 'CALL' WHEN 'C' THEN 'PUT'
                                              ELSE 'FUT' END AS call_put_cd,
               u.cont_mult,
               REGEXP_SUBSTR(f.kor_name, '[0-9]{6}|[0-9]{4}W[0-9]') AS mat_code,
               NULLIF(f.acpr, 0) AS strike_prc,
               ROW_NUMBER() OVER (PARTITION BY f.short_code ORDER BY f.id DESC) AS rn
          FROM fo_idx_code_mst f
          JOIN (SELECT DISTINCT prod_type, kis_info_types, cont_mult FROM meta_fuopt_info) u
            ON INSTR(',' || u.kis_info_types || ',', ',' || f.info_type || ',') > 0
         WHERE f.trade_at = (SELECT MAX(trade_at) FROM fo_idx_code_mst)
      ) b
      LEFT JOIN meta_maturity m ON m.prod_type = b.prod_type AND m.mat_code = b.mat_code
     WHERE b.rn = 1 AND b.mat_code IS NOT NULL
  ) s
  ON (t.short_code = s.short_code)
  WHEN NOT MATCHED THEN
    INSERT (short_code, kis_short_cd, prod_nm, prod_type, call_put_cd,
            cont_mult, mat_code, mat_date, front_date, strike_prc, description)
    VALUES (s.short_code, s.kis_short_cd, s.prod_nm, s.prod_type, s.call_put_cd,
            s.cont_mult, s.mat_code, s.mat_date, s.front_date, s.strike_prc,
            'from fo_idx_code_mst; expiry unconfirmed');

  p_inserted := p_inserted + SQL%ROWCOUNT;

  -- 기초자산은 지수 계열에서는 상품 계열의 성질이다: 코스피200 계열은 전부
  -- KOSPI200 위에 있다. 그래서 meta_fuopt_info 에서 prod_type 으로 붙인다 --
  -- 만기가 meta_maturity 에서 오는 것과 같은 길이다. 예전엔 KIS 마스터에서
  -- 종목마다 옮겨 적어, 같은 사실이 6만 번 반복되고 마스터에 없던 4만 3천
  -- 종목(만기 지난 과거분)은 비어 있었다.
  --
  -- 기초자산이 하나뿐인 계열만 이렇게 찍는다(HAVING). 개별주식 선물·옵션은
  -- 한 계열에 기초자산이 수백이라 계약마다 원천에서 와야 하고, 이 표는 그때
  -- (prod_type, ul_code) 로 이름과 승수를 줄 뿐이다. 비어 있는 것만 채우므로
  -- 사람이 넣은 값은 그대로다.
  MERGE /*+ NO_PARALLEL */ INTO mst_fuopt t
  USING (SELECT prod_type, MIN(ul_code) AS ul_code, MIN(ul_nm) AS ul_nm
           FROM meta_fuopt_info
          GROUP BY prod_type
         HAVING COUNT(*) = 1) u
     ON (t.prod_type = u.prod_type)
  WHEN MATCHED THEN UPDATE SET t.ul_code = u.ul_code, t.ul_nm = u.ul_nm
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
  -- NULL 인 것만 채우므로 사람이 넣은 값은 그대로다. KIS 마스터로 들어와
  -- 아직 거래소가 확인하지 않은 행은 예외로, 달력이 바뀌면 따라간다 -- 그
  -- 달력 행이 규칙으로 추정한 것이었다가 거래소 값으로 바로잡히는 경우다.
  MERGE /*+ NO_PARALLEL */ INTO mst_fuopt t
  USING meta_maturity m
     ON (t.prod_type = m.prod_type AND t.mat_code = m.mat_code)
  WHEN MATCHED THEN UPDATE SET t.mat_date   = m.mat_date,
                               t.front_date = m.prev_mat_date
                   WHERE t.mat_date IS NULL
                      OR t.description LIKE 'from fo_idx_code_mst%';
END;
