-- 코드 레지스트리: SRC_CODE (SCRAP) · MST_CODE · USER_CODE_MAP (REF)
--
-- docs/SCHEMA-V2.md 6.5 참고. 계정 이름은 적지 않는다 -- &scrap_user / &ref_user
-- 를 채워 돌린다 (06_grants_synonyms.sql 과 같은 방식).
--
-- 무엇을 푸는가: 코드성 값이 결과 테이블의 컬럼으로 들어온다(103개 컬럼,
-- KSD 53 · KRX 23 · KIS 14 · AMC 13). 원천이 코드표를 공시하는 경우도 있고
-- 아닌 경우도 있어서, 코드 목록 API 를 수집하는 것으로는 덮이지 않는다.
-- 그래서 **들어온 값에서 거꾸로 수확**한다 -- 실제로 온 값이 곧 어휘다.
--
-- 세 표가 하는 일:
--   SRC_CODE       원천이 보낸 값 목록. 프로시저가 수확한다. 수집 상태다
--   USER_CODE_MAP  그 값이 우리 어휘의 무엇인가. 사람이 적는다
--   MST_CODE       우리 어휘. 사람이 적는다
--
-- 왜 셋인가: 카디널리티가 다르다. 원천 값 여러 개가 우리 코드 하나로 모인다
-- (KSD 의 'A' 와 'A0' 가 CREDIT_GRD.A 하나로). MST_CODE 가 없으면 "우리는
-- 이걸 뭐라 부르는가" 를 적을 곳이 없고, SRC_CODE 가 없으면 원천이 보냈지만
-- 아직 매핑 안 한 값을 알 수 없다.

-- ===========================================================================
-- SCRAP 으로 접속해서 실행
-- ===========================================================================

-- 수확물. 결과 테이블에서 뽑은 값이라 수집 상태이고, 그래서 이 계정이다
-- (ksd_bond_no_detail 과 같은 분류 -- docs/SCHEMA-V2.md 9.5 "경계를 어디로").
CREATE TABLE src_code (
  -- 키가 (표, 컬럼, 값) 인 것이 요점이다. 값만으로 키를 잡으면 뜻이 다른 것이
  -- 합쳐진다 -- 측정해 보니 둘 이상의 컬럼에 나오는 값 109개가 거의 다 그런
  -- 경우였다. '2' 는 ATM구분에서도 2, 전일대비부호에서도 2 인데 뜻이 전혀
  -- 다르고, 날짜도 여섯 컬럼에 겹쳐 나온다. 컬럼 기준으로 두면 매핑이 1.40배
  -- (615 -> 862줄) 늘지만 그 늘어남이 곧 "이 둘은 같은 뜻" 이라는 판단이다.
  table_name    VARCHAR2(128 CHAR) NOT NULL,
  column_name   VARCHAR2(128 CHAR) NOT NULL,
  code_value    VARCHAR2(400 CHAR) NOT NULL,

  -- api_mst.source_name 과 같은 어휘이고 같은 이름을 쓴다. 한 개념에
  -- 이름이 둘이면 조인할 때마다 어느 쪽인지 생각해야 한다.
  source_name   VARCHAR2(20 CHAR),    -- KRX KSD KIS AMC. 표 이름에서 끌어낸다
  row_cnt       NUMBER,               -- 그 값이 나온 행 수
  distinct_cnt  NUMBER,               -- 그 컬럼의 고유값 수 (후보인지 판단)

  -- 새 값이 언제 처음 나타났나. 지금은 원천이 새 값을 보내면 조용히 들어오는데
  -- 여기 새 행이 생기는 것으로 드러난다.
  first_seen    DATE DEFAULT SYSDATE NOT NULL,
  -- 사라진 값은 지우지 않는다. last_seen 이 멈추는 것으로 알 수 있고, 과거
  -- 데이터에는 그 값이 아직 남아 있다.
  last_seen     DATE DEFAULT SYSDATE NOT NULL,

  updated_at    DATE DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT pk_src_code PRIMARY KEY (table_name, column_name, code_value)
);

CREATE INDEX ix_src_code_col   ON src_code (table_name, column_name);
CREATE INDEX ix_src_code_first ON src_code (first_seen);

COMMENT ON TABLE  src_code IS '결과 컬럼에서 수확한 원천 코드 값. sp_src_code_sync 가 채운다';
COMMENT ON COLUMN src_code.last_seen IS '멈춘 값 = 원천이 더는 보내지 않는 값. 지우지 않는다';

-- WITH GRANT OPTION 이 필요하다. REF 의 v_code_unmapped 가 이 표를 읽고, 그
-- 뷰를 다시 SCRAP 에 넘겨주려면 원본에 대한 전달 권한이 있어야 한다. 없으면
-- 뷰를 만들 때가 아니라 **넘겨줄 때** ORA-01720 이 난다 -- 뷰는 멀쩡히
-- 만들어지고 GRANT 에서 막히므로 원인이 한 겹 숨는다.
GRANT SELECT ON src_code TO &ref_user WITH GRANT OPTION;

-- ===========================================================================
-- REF 으로 접속해서 실행
-- ===========================================================================

-- 우리 어휘. 손으로 적는다.
CREATE TABLE mst_code (
  code_group  VARCHAR2(40 CHAR)  NOT NULL,   -- BOND_TYPE MKT CREDIT_GRD …
  code        VARCHAR2(40 CHAR)  NOT NULL,   -- GOVT SPECIAL …
  code_nm     VARCHAR2(200 CHAR) NOT NULL,   -- 우리가 부르는 이름
  -- 계층이 필요한 어휘를 위해서다(KSD 의 KACD 가 2단이다). depth 를 따로
  -- 두지 않는다 -- CONNECT BY 로 나오고, 두 군데 적으면 어긋난다.
  parent_code VARCHAR2(40 CHAR),
  sort_order  NUMBER,
  description VARCHAR2(400 CHAR),
  updated_at  DATE DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT pk_mst_code PRIMARY KEY (code_group, code)
);

CREATE INDEX ix_mst_code_parent ON mst_code (code_group, parent_code);

-- 원천 값 → 우리 코드. 손으로 적는다. user_etf 와 같은 성질이다:
-- 다시 만들 수 없고, 수집이 덮지 않는다.
CREATE TABLE user_code_map (
  -- SRC_CODE 와 같은 키다. 외래키를 걸지 않는 이유 둘: 원천이 아직 안 보낸
  -- 값을 미리 매핑해 둘 수 있어야 하고(MBS·SLBS 가 그 경우 -- KSD 코드표에
  -- 없다), 수확이 SRC_CODE 를 다시 만들 때 제약이 걸린다.
  table_name  VARCHAR2(128 CHAR) NOT NULL,
  column_name VARCHAR2(128 CHAR) NOT NULL,
  code_value  VARCHAR2(400 CHAR) NOT NULL,

  code_group  VARCHAR2(40 CHAR)  NOT NULL,
  code        VARCHAR2(40 CHAR)  NOT NULL,

  note        VARCHAR2(400 CHAR),
  updated_at  DATE DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT pk_user_code_map PRIMARY KEY (table_name, column_name, code_value)
);

-- 이 인덱스가 "코드 컬럼 목록" 역할을 한다. 별도 선언 표를 두지 않는 이유가
-- 그것이다 -- USER_CODE_MAP 에 어떤 (표, 컬럼) 이 등장하면 그 자체가 "이
-- 컬럼은 코드 컬럼이고 이 어휘를 쓴다" 는 선언이다.
CREATE INDEX ix_user_code_map_col   ON user_code_map (table_name, column_name);
CREATE INDEX ix_user_code_map_group ON user_code_map (code_group, code);

GRANT SELECT ON mst_code      TO &scrap_user;
GRANT SELECT ON user_code_map TO &scrap_user;

-- 조인해서 쓰는 뷰. 질의마다 두 표를 잇지 않게 한다.
--
-- mst_code 를 LEFT 로 잇는 이유: 매핑이 가리키는 코드가 mst_code 에 없으면
-- 그것은 데이터 오류이고, INNER 로 두면 그 행이 조용히 사라져서 오류가
-- 안 보인다. code_nm 이 NULL 로 나오는 쪽이 낫다.
CREATE OR REPLACE VIEW v_code_map AS
SELECT p.table_name,
       p.column_name,
       p.code_value,
       p.code_group,
       p.code,
       m.code_nm,
       m.parent_code,
       m.sort_order
  FROM user_code_map p
  LEFT JOIN mst_code m
         ON m.code_group = p.code_group
        AND m.code       = p.code;

COMMENT ON TABLE v_code_map IS '원천 값 -> 우리 코드/이름. 결과 테이블에 LEFT JOIN 해서 쓴다';

-- 아직 매핑 안 한 값. 운영 중에 가장 자주 볼 질의라 뷰로 둔다.
-- src_code 는 SCRAP 소유이고 여기서는 synonym 으로 닿는다 -- 그 GRANT 에
-- WITH GRANT OPTION 이 붙어 있어야 이 뷰를 SCRAP 에 되돌려 줄 수 있다.
CREATE OR REPLACE VIEW v_code_unmapped AS
SELECT s.source_name, s.table_name, s.column_name, s.code_value,
       s.row_cnt, s.distinct_cnt, s.first_seen, s.last_seen
  FROM src_code s
 WHERE NOT EXISTS (SELECT 1
                     FROM user_code_map p
                    WHERE p.table_name  = s.table_name
                      AND p.column_name = s.column_name
                      AND p.code_value  = s.code_value);

COMMENT ON TABLE v_code_unmapped IS '수확됐지만 매핑 안 된 값. row_cnt 큰 것부터 보면 된다';

GRANT SELECT ON v_code_map      TO &scrap_user;
GRANT SELECT ON v_code_unmapped TO &scrap_user;

-- ===========================================================================
-- synonym (각 계정에서)
-- ===========================================================================
-- REF 에서 (위 v_code_unmapped 보다 먼저 만들어야 한다):
CREATE SYNONYM src_code FOR &scrap_user..src_code;

-- SCRAP 에서:
-- CREATE SYNONYM mst_code        FOR &ref_user..mst_code;
-- CREATE SYNONYM user_code_map   FOR &ref_user..user_code_map;
-- CREATE SYNONYM v_code_map      FOR &ref_user..v_code_map;
-- CREATE SYNONYM v_code_unmapped FOR &ref_user..v_code_unmapped;

-- ===========================================================================
-- 쓰는 법
-- ===========================================================================
-- 조인은 하나다. 매핑 안 된 값이 행을 떨어뜨리지 않고 NULL 로 드러나야 하므로
-- LEFT 다 -- 조용히 사라지면 매핑이 빠진 것을 알 수 없다.
--
--   SELECT b.isin, c.code_nm AS bond_type
--     FROM ksd_bond_isin b
--     LEFT JOIN v_code_map c
--            ON c.table_name  = 'ksd_bond_isin'
--           AND c.column_name = 'codevalue_nm'
--           AND c.code_value  = b.codevalue_nm;
--
-- 한 표에 코드 컬럼이 여럿이면 뷰를 여러 번 잇는다. 별칭만 다르다:
--
--   LEFT JOIN v_code_map c1 ON c1.table_name='ksd_bond_credit'
--                          AND c1.column_name='secn_kanm'
--                          AND c1.code_value=x.secn_kanm
--   LEFT JOIN v_code_map c2 ON c2.table_name='ksd_bond_credit'
--                          AND c2.column_name='valat_org_nm'
--                          AND c2.code_value=x.valat_org_nm
--
-- 매핑 밀린 것:
--
--   SELECT * FROM v_code_unmapped ORDER BY row_cnt DESC FETCH FIRST 30 ROWS ONLY;
--
-- 어제 새로 나타난 값:
--
--   SELECT * FROM src_code WHERE first_seen >= TRUNC(SYSDATE) - 1;
