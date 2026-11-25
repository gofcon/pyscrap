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
