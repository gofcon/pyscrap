-- SCRAP 계정: response_type='html_block' 행을 읽는 뷰. 손으로 적는다.
--
-- 왜 생성물이 아닌가. 04_scrap_views.sql 은 모델에서 컬럼을 뽑아 만든다 --
-- "지금은 api_rst 에 있지만 언제든 타입 테이블로 승격할 수 있는" 행들이고,
-- 그래서 모델이 그 모양의 정본이다.
--
-- html_block 행은 그 종류가 아니다. 저장하는 것이 블록의 마크업 자체이고
-- 해석은 아래층 몫이라, 승격할 타입 테이블이라는 것이 없다. 컬럼 셋은 이
-- 모드가 항상 만드는 셋(block_index · label · html) + row_stamp_json 이
-- 찍는 것뿐이고 소스마다 달라지지 않는다. 그래서 모델을 만들지 않고 여기
-- 적는다.
--
-- ===========================================================================
-- html 은 반드시 RETURNING CLOB
-- ===========================================================================
-- VARCHAR2(4000) 으로 두면 4000자를 넘는 순간 **에러 없이 NULL** 이다.
-- JSON_VALUE 의 기본이 NULL ON ERROR 라서 질의가 성공하고 값만 사라진다.
-- 실측:
--
--   RETURNING CLOB                        -> 7,703자 정상
--   RETURNING VARCHAR2(4000)              -> NULL (조용히)
--   RETURNING VARCHAR2(4000) ERROR ON ERROR -> ORA-61723: exceeds maximum (4000)
--
-- 블록 하나가 400~1,100자라 지금은 4000 안에 들어오지만, 더 큰 블록을 담는
-- 설정이 생기면 그 행만 조용히 비어 보인다. 처음부터 CLOB 으로 둔다.

CREATE OR REPLACE VIEW v_isin_bond_info AS
SELECT api_id, job_id,
       JSON_VALUE(result_json, '$.isin'        RETURNING VARCHAR2(12 CHAR)) AS isin,
       -- 블록 순번. **종목마다 같은 섹션을 가리키지 않는다** -- parse_json 의
       -- 셀렉터가 빈 섹션을 걸러내므로(:has(td:not(:empty))), 주식관련사채가
       -- 있는 채권은 그 섹션이 남아 뒤 번호가 밀린다. 섹션을 찾을 때는
       -- label 을 쓴다.
       JSON_VALUE(result_json, '$.block_index' RETURNING NUMBER)            AS block_index,
       -- 블록의 첫 th 텍스트. '발행기관코드' · 'CFI' · 'Call 주체' 같은 것이고
       -- 이것이 섹션의 안정적인 이름이다.
       JSON_VALUE(result_json, '$.label'       RETURNING VARCHAR2(60 CHAR)) AS label,
       JSON_VALUE(result_json, '$.html'        RETURNING CLOB)              AS html
  FROM api_rst
 WHERE api_id IN ('ISIN_BOND_INFO');

COMMENT ON TABLE v_isin_bond_info IS
  '표준코드 채권 상세. html 은 블록 마크업 그대로 -- 해석은 읽는 쪽에서 한다';

-- 쓰는 법
-- ---------------------------------------------------------------------------
-- 한 종목의 모든 블록:
--   SELECT block_index, label, html FROM v_isin_bond_info
--    WHERE isin = 'KR6067571G13' ORDER BY block_index;
--
-- 한 섹션만 (번호가 아니라 이름으로):
--   SELECT isin, html FROM v_isin_bond_info WHERE label = '발행기관코드';
--
-- 인덱스가 필요해지면 뷰에 적힌 식을 **그대로** 베껴야 한다. isin 으로
-- 찾는 질의가 전체 스캔으로 떨어지면 먼저 이것을 본다:
--   CREATE INDEX ix_api_rst_isin ON api_rst
--     (JSON_VALUE(result_json, '$.isin' RETURNING VARCHAR2(12 CHAR))) LOCAL;

-- ---------------------------------------------------------------------------
-- v_krx_isin_srch -- 과거 표준코드 (2026-11-27)
-- ---------------------------------------------------------------------------
-- KRX 표준코드 조회의 결과. 이 소스의 값은 **상환·만기된 코드까지** 있다는
-- 것이다 -- KSD(ksd_bond_isin)는 살아 있는 종목만이다. 실측 적재:
-- 비상장 402,519 · 상장폐지 59,286 · 상장 18,269.
--
-- **ISIN 하나당 한 행으로 접는다.** 검색어(#isur_nm1)가 표준코드와 한글종목명을
-- 둘 다 훑기 때문에 접두사 축이 서로 겹친다 -- 'KRX' 로 검색하면 코드가
-- KRD... 인 "KRX 300 선물 인버스 -2X지수" 가 걸리고, 그 행은 KRD 축에서도
-- 온다. 실측으로 480,092행 중 고유 ISIN 이 479,810개이고, 같은 ISIN 의
-- 부여일이 다른 경우는 0이다 -- 즉 겹친 것은 같은 행이므로 접어도 잃지 않는다.
--
-- 축 별 오염(코드가 그 접두사로 시작하지 않는 행): KRX 478/478 · KR0 10/10 ·
-- KR9 10/10 은 전부 이름 매칭이고, KR6·KR3·KR2·KR5·KR1·KR8 은 각 10행씩이다.
-- 그래도 버리지 않는다 -- 그중 일부는 축에 없는 접두사(KR4·KR7·KRA 제외분)의
-- 코드라, 이름으로 걸려 들어온 것이 유일한 경로다.
--
-- code 는 그 행을 가져온 축 값이다(row_stamp_json). 어느 검색어로 왔는지는
-- 진단에 쓰고, 상품 분류로 읽으면 안 된다 -- 위의 이름 매칭 때문이다.
CREATE OR REPLACE VIEW v_krx_isin_srch AS
SELECT isin, prod_type, kor_name, issuer, issue_dd, mat_dd,
       list_yn, list_dd, grant_dd, code, job_id
  FROM (SELECT JSON_VALUE(result_json, '$.isin'      RETURNING VARCHAR2(12 CHAR))  AS isin,
               JSON_VALUE(result_json, '$.prod_type' RETURNING VARCHAR2(40 CHAR))  AS prod_type,
               JSON_VALUE(result_json, '$.kor_name'  RETURNING VARCHAR2(200 CHAR)) AS kor_name,
               JSON_VALUE(result_json, '$.issuer'    RETURNING VARCHAR2(200 CHAR)) AS issuer,
               JSON_VALUE(result_json, '$.issue_dd'  RETURNING VARCHAR2(10 CHAR))  AS issue_dd,
               JSON_VALUE(result_json, '$.mat_dd'    RETURNING VARCHAR2(10 CHAR))  AS mat_dd,
               JSON_VALUE(result_json, '$.list_yn'   RETURNING VARCHAR2(20 CHAR))  AS list_yn,
               JSON_VALUE(result_json, '$.list_dd'   RETURNING VARCHAR2(10 CHAR))  AS list_dd,
               JSON_VALUE(result_json, '$.grant_dd'  RETURNING VARCHAR2(10 CHAR))  AS grant_dd,
               JSON_VALUE(result_json, '$.code'      RETURNING VARCHAR2(8 CHAR))   AS code,
               job_id,
               -- 같은 ISIN 이 둘 이상이면 하나만. 행 내용이 같으므로 어느
               -- 것을 남겨도 되지만, 결정적이어야 다시 돌려도 같은 답이 난다.
               ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(result_json, '$.isin')
                                      ORDER BY job_id) AS rn
          FROM api_rst
         WHERE api_id = 'KRX_ISIN_SRCH')
 WHERE rn = 1;

COMMENT ON TABLE v_krx_isin_srch IS
  'KRX 표준코드 조회 결과. 상환/만기된 코드까지 포함한다(KSD 는 생존 종목만). ISIN 하나당 한 행 -- 검색어가 코드와 종목명을 둘 다 훑어 접두사 축이 겹친다';
