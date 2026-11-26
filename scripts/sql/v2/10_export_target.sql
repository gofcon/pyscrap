-- SCRAP 계정: 내보내기 경로. **v2 에만 만든다.**
--
-- v1 과 같은 버킷을 보면서 같은 경로에 쓰면, sp_export_parquet 가 쓰기 전에
-- 그 날짜 폴더를 비우므로 두 환경이 서로의 파일을 지운다 (fn_export_base
-- 주석 참고). 이 표가 있는 계정만 다른 뿌리를 쓰고, 없는 계정(v1)은 함수가
-- 기존 상수로 떨어진다.
--
-- SCRAP 으로 접속해서 실행한다.

CREATE TABLE export_target (
  -- 한 행만 둔다. id 를 1 로 못 박는 이유: fn_export_base 가 id = 1 을 읽고,
  -- 두 행이 되면 어느 쪽이 쓰이는지가 조회 계획에 달린다 -- 경로가 조용히
  -- 바뀌는 종류의 사고다.
  id         NUMBER       DEFAULT 1 NOT NULL,
  base_uri   VARCHAR2(400) NOT NULL,
  note       VARCHAR2(400),
  updated_at DATE DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT pk_export_target PRIMARY KEY (id),
  CONSTRAINT ck_export_target_one CHECK (id = 1)
);

COMMENT ON TABLE  export_target IS '내보내기 객체 경로의 뿌리. 없으면 fn_export_base 가 기본값을 쓴다';
COMMENT ON COLUMN export_target.base_uri IS '슬래시로 끝나야 한다 -- 뒤에 <표>/<날짜>/ 가 붙는다';

-- v2 는 같은 버킷의 'v2/' 아래로 쓴다. 버킷을 따로 만들지 않은 이유: 자격
-- (BUCKETAUTH_NEW)과 분석 DB 의 외부 테이블 설정이 이 버킷에 묶여 있어,
-- 접두사만 가르면 분석 쪽을 건드리지 않고 갈 수 있다.
--
-- 전환할 때 고칠 것은 이 표의 한 행뿐이다. 수집 계정의
-- sp_create_external_tables 는 2026-11-26 에 내렸으므로 옮길 경로가 더 없다
-- (docs/SCHEMA-V2.md 7.6).
INSERT INTO export_target (id, base_uri, note)
VALUES (1, 'https://objectstorage.ap-chuncheon-1.oraclecloud.com/n/axtl8qsnlcns/b/bucket-20260410-1831/o/v2/',
        'v1 과 같은 버킷을 공유하는 동안 접두사로 가른다. 전환하면 이 행을 지우거나 기본 경로로 바꾼다');

COMMIT;

-- 확인
--   SELECT fn_export_base FROM dual;      -- ...o/v2/ 로 끝나야 한다
