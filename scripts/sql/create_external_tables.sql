-- 오브젝트 스토리지의 Parquet 을 테이블처럼 읽도록 외부 테이블을 건다.
-- sp_run_export 가 내보낸 대상마다 하나씩, 이름은 xt_<대상>.
--
-- 분석용 DB 에서 돌리는 스크립트다. 수집 DB 에서 돌리면 원본 테이블 옆에
-- 같은 내용의 외부 테이블이 생기는데(이름이 xt_ 로 갈리니 충돌은 안 난다)
-- 굳이 그럴 이유는 없다 -- 원본을 직접 읽으면 된다. 두 DB 를 나눈 목적이
-- 수집과 분석의 분리이고, 이 스크립트가 그 경계다: 분석 쪽은 DB 링크도
-- 계정도 아닌 버킷 하나만 보면 된다.
--
-- 먼저 그 DB 에 버킷을 읽을 크리덴셜이 있어야 한다:
--   BEGIN DBMS_CLOUD.CREATE_CREDENTIAL('BUCKETAUTH_NEW', '<user>', '<auth token>'); END;
--
-- 날짜는 파티션이 된다. 경로가 'day=20260813' 이라 (하이브 규약)
-- CREATE_EXTERNAL_PART_TABLE 이 경로만 보고 day 컬럼과 파티션을 만들어 내고,
-- 하루를 묻는 질의는 그날 파일만 연다 -- 실행계획에서 Pstart/Pstop 이 한
-- 파티션으로 좁혀지는 것을 확인했다. 파티션 목록은 만든 시점의 스냅샷이라,
-- 새 날짜가 쌓이면 이 스크립트를 다시 돌려야 보인다.
--
-- 컬럼은 Parquet 에서 읽어 온다. 목록을 적지 않는 이유는 그것이 곧 내보낸
-- 쿼리의 SELECT 목록이어서, 여기 옮겨 적으면 쿼리를 고칠 때마다 두 군데를
-- 맞춰야 하기 때문이다. 다만 타입은 Parquet 을 거치며 옮겨간다 -- NUMBER 는
-- BINARY_DOUBLE 로 돌아온다.
SET SERVEROUTPUT ON
DECLARE
  c_cred CONSTANT VARCHAR2(30)  := 'BUCKETAUTH_NEW';
  c_base CONSTANT VARCHAR2(200) :=
    'https://objectstorage.ap-chuncheon-1.oraclecloud.com/n/axtl8qsnlcns/b/bucket-20260410-1831/o/';

  -- 대상 하나를 걸고 결과를 한 줄 남긴다. 호출부에 남는 것은 대상 이름뿐이라
  -- sp_run_export 의 목록과 나란히 읽힌다.
  PROCEDURE attach (p_target IN VARCHAR2) IS
    v_table VARCHAR2(128) := 'xt_' || LOWER(p_target);
    v_files NUMBER;
    v_stray NUMBER;
    v_parts NUMBER;
  BEGIN
    -- 아직 한 번도 안 나간 대상은 건너뛴다. 파일이 없으면 컬럼을 알아낼
    -- 방법이 없어 생성 자체가 실패하고, 그러면 뒤의 대상까지 못 걸린다.
    SELECT COUNT(*), COUNT(CASE WHEN INSTR(object_name, '/') = 0 THEN 1 END)
      INTO v_files, v_stray
      FROM DBMS_CLOUD.LIST_OBJECTS(c_cred, c_base || LOWER(p_target) || '/');
    IF v_files = 0 THEN
      DBMS_OUTPUT.PUT_LINE(RPAD(v_table, 26) || 'skipped (no objects yet)');
      RETURN;
    END IF;

    -- day= 폴더 밖에 놓인 파일이 있으면 멈춘다. 파티션 값은 파일 바로 위
    -- 폴더 이름에서 나오므로, 대상 폴더 바로 밑에 있는 파일은 대상 이름
    -- 자체를 하루로 삼는다 -- day 가 'kis_futopt_price' 인 파티션이 생기고,
    -- varchar2(8) 에 안 들어가 ORA-14036 으로 죽는다. 타입을 늘리면 생성은
    -- 되지만 날짜가 아닌 파티션이 섞여 더 나쁘다. 옛 방식으로 내보낸
    -- 잔재이니 지우고 다시 부르는 편이 맞다.
    IF v_stray > 0 THEN
      DBMS_OUTPUT.PUT_LINE(RPAD(v_table, 26) || 'ABORTED -- ' || v_stray
        || ' file(s) sit outside a day= folder under ' || LOWER(p_target) || '/');
      RETURN;
    END IF;

    BEGIN
      EXECUTE IMMEDIATE 'DROP TABLE ' || v_table || ' PURGE';
    EXCEPTION
      WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;   -- -942: 아직 없음
    END;

    DBMS_CLOUD.CREATE_EXTERNAL_PART_TABLE(
      table_name      => v_table,
      credential_name => c_cred,
      file_uri_list   => c_base || LOWER(p_target) || '/*.parquet',
      format          => JSON_OBJECT('type' VALUE 'parquet',
                           'partition_columns' VALUE JSON_ARRAY(
                             JSON_OBJECT('name' VALUE 'day', 'type' VALUE 'varchar2(8)'))));

    SELECT COUNT(*) INTO v_parts FROM user_tab_partitions WHERE table_name = UPPER(v_table);
    DBMS_OUTPUT.PUT_LINE(RPAD(v_table, 26) || v_files || ' file(s), ' || v_parts || ' day partition(s)');
  END;

BEGIN
  attach('kis_futopt_price');
  attach('kis_futopt_chart');
  attach('kis_futopt_daily');
  attach('kis_index_daily');
  attach('kis_futopt_price1');
  attach('v_k2i_atm');
END;
/
