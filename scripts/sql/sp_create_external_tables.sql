CREATE OR REPLACE PROCEDURE sp_create_external_tables (
  p_target IN VARCHAR2 DEFAULT NULL   -- 이 대상만 다시 걸기 (기본: 전부)
) AS
-- 오브젝트 스토리지의 Parquet 을 테이블처럼 읽도록 외부 테이블을 건다.
-- sp_run_export 가 내보낸 대상마다 하나씩, 이름은 xt_<대상>.
--
-- 분석용 DB 에 두는 프로시저다. 수집 DB 에 만들면 원본 테이블 옆에 같은
-- 내용의 외부 테이블이 생기는데(이름이 xt_ 로 갈리니 충돌은 안 난다) 굳이
-- 그럴 이유는 없다 -- 원본을 직접 읽으면 된다. 두 DB 를 나눈 목적이 수집과
-- 분석의 분리이고, 이것이 그 경계다: 분석 쪽은 DB 링크도 계정도 아닌 버킷
-- 하나만 보면 된다.
--
-- 먼저 그 DB 에 버킷을 읽을 크리덴셜이 있어야 한다:
--   BEGIN DBMS_CLOUD.CREATE_CREDENTIAL('BUCKETAUTH_NEW', '<user>', '<auth token>'); END;
--
-- 다시 부를 일: 대상이 늘었을 때, 그리고 내보내기 쿼리의 컬럼이 바뀌었을 때.
-- 컬럼은 만들 때 Parquet 에서 읽어 확정되므로, 새 컬럼이 담긴 파일이 쌓여도
-- 기존 테이블은 그것을 모른다. 반대로 날짜가 늘어나는 것은 다시 걸 필요가
-- 없다 -- 경로가 와일드카드라 새 폴더가 저절로 들어온다.
--
-- 경로는 <대상>/<날짜>/<파일> 이고, 와일드카드 하나로 전 기간을 한 테이블로
-- 읽는다. 날짜 폴더는 테이블에 드러나지 않는다 -- 날짜로 거르려면 데이터
-- 안의 날짜 컬럼(stck_bsop_date, trade_date 등)을 쓰면 되고, 그 조건은 파일을
-- 골라내지는 못하고 연 다음에 걸러낸다. 즉 하루를 묻는 질의도 그 대상의 모든
-- 날을 훑는다. 폴더 이름을 'day=20260813' 으로 두면
-- DBMS_CLOUD.CREATE_EXTERNAL_PART_TABLE 이 그것을 파티션으로 삼아 그날 파일만
-- 열지만, 경로 모양이 바뀐다. 지금은 경로를 택했다.
--
-- 컬럼 목록을 적지 않는 이유는 그것이 곧 내보낸 쿼리의 SELECT 목록이어서,
-- 여기 옮겨 적으면 쿼리를 고칠 때마다 두 군데를 맞춰야 하기 때문이다. 다만
-- 타입은 Parquet 을 거치며 옮겨간다 -- NUMBER 는 BINARY_DOUBLE, DATE 는
-- TIMESTAMP(3) 으로 돌아온다.
  c_cred CONSTANT VARCHAR2(30)  := 'BUCKETAUTH_NEW';
  c_base CONSTANT VARCHAR2(200) :=
    'https://objectstorage.ap-chuncheon-1.oraclecloud.com/n/axtl8qsnlcns/b/bucket-20260410-1831/o/';

  -- 대상 하나를 걸고 결과를 한 줄 남긴다. 호출부에 남는 것은 대상 이름뿐이라
  -- sp_run_export 의 목록과 나란히 읽힌다.
  PROCEDURE attach (p_name IN VARCHAR2) IS
    v_table VARCHAR2(128) := 'xt_' || LOWER(p_name);
    v_files NUMBER;
    v_cols  NUMBER;
  BEGIN
    IF p_target IS NOT NULL AND LOWER(p_target) != LOWER(p_name) THEN
      RETURN;
    END IF;

    -- 아직 한 번도 안 나간 대상은 건너뛴다. 파일이 없으면 컬럼을 알아낼
    -- 방법이 없어 생성 자체가 실패하고, 그러면 뒤의 대상까지 못 걸린다.
    SELECT COUNT(*) INTO v_files
      FROM DBMS_CLOUD.LIST_OBJECTS(c_cred, c_base || LOWER(p_name) || '/');
    IF v_files = 0 THEN
      DBMS_OUTPUT.PUT_LINE(RPAD(v_table, 26) || 'skipped (no objects yet)');
      RETURN;
    END IF;

    BEGIN
      EXECUTE IMMEDIATE 'DROP TABLE ' || v_table || ' PURGE';
    EXCEPTION
      WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;   -- -942: 아직 없음
    END;

    -- '<대상>/*/*.parquet' -- 날짜 폴더 한 겹을 와일드카드로 넘긴다. 대상 폴더
    -- 바로 밑에 놓인 파일(옛 방식의 잔재 같은)은 이 패턴에 안 걸리므로 저절로
    -- 빠진다.
    DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
      table_name      => v_table,
      credential_name => c_cred,
      file_uri_list   => c_base || LOWER(p_name) || '/*/*.parquet',
      format          => JSON_OBJECT('type' VALUE 'parquet'));

    SELECT COUNT(*) INTO v_cols FROM user_tab_columns WHERE table_name = UPPER(v_table);
    DBMS_OUTPUT.PUT_LINE(RPAD(v_table, 26) || v_files || ' file(s), ' || v_cols || ' column(s)');
  END;

BEGIN
  attach('kis_futopt_price');
  attach('kis_futopt_chart');
  attach('kis_futopt_daily');
  attach('kis_index_daily');
  attach('kis_futopt_price1');
  attach('v_k2i_atm');
END;
