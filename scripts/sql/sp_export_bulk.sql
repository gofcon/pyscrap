CREATE OR REPLACE PROCEDURE sp_export_bulk (
  p_name    IN  VARCHAR2,                 -- 내보낼 대상 (프리픽스 이름 겸 기본 소스)
  p_to      IN  VARCHAR2,                 -- 마지막 영업일 (YYYYMMDD, 포함)
  p_from    IN  VARCHAR2,                 -- 시작 영업일 (포함)
  p_query   IN  CLOB     DEFAULT NULL,    -- 사용자 지정 쿼리 (:FROM, :TO 치환)
  p_rows    OUT NUMBER                    -- 내보낸 행 수
) AS
-- 한 대상의 지정 기간 전체를 Parquet 파일 하나로 내보낸다.
--
-- sp_export_parquet 과 나눈 이유: 날짜별로 쪼개는 것은 일 배치의 필요다. 그날
-- 것만 지우고 다시 쓸 수 있어야 재실행이 안전하고, 그래서 하루가 폴더 하나다.
-- 과거 전체를 한 번 뽑는 작업에는 그 필요가 없고, 쪼개면 손해만 남는다 --
-- v_k2i_atm 은 하루 1행이라 2.5KB 짜리 객체가 1,702개 생기고, Parquet 은 파일
-- 마다 스키마와 압축 사전을 다시 만든다.
--
-- p_query 로 부를 때는 ORDER BY 를 쿼리에 직접 넣어야 한다. 기본 쿼리는 날짜
-- 컬럼으로 정렬해서 내보내는데(아래 참고), 사용자 쿼리에는 그럴 자리를 짐작해
-- 끼워넣을 수 없다 -- 무엇으로 정렬하는 게 맞는지는 그 쿼리를 쓴 사람만 안다.
--
-- 쿼리 계약도 다르다. 날짜별은 :DAY 로 그날을 좁히지만 여기서는 기간이 통째로
-- 한 파일이므로 :FROM 과 :TO 를 쓴다. 같은 프로시저에 인자로 모드를 두면 어느
-- 자리표시자가 유효한지가 인자에 따라 달라지는데, 그건 시그니처가 말해주지
-- 않는 규칙이라 부르는 쪽이 틀리기 쉽다.
--
-- 파일은 <대상>/_bulk/<시작>_<끝>_part_... 로 나간다. 범위를 파일 이름이 들고
-- 있어서, 날짜별 내보내기가 겹침을 판단할 때 볼 것이 버킷 하나뿐이다
-- (sp_export_parquet 의 -20002 참고). 폴더를 따로 두는 것은 그 조회를 싸게
-- 하기 위해서다 -- LIST_OBJECTS 는 폴더 경계에서만 매칭한다.
--
-- 외부 테이블은 <대상>/*/*.parquet 로 읽으므로 _bulk 도 날짜 폴더도 같이
-- 잡힌다. 아래에서 겹치는 날짜 폴더를 비우므로 둘의 기간은 겹치지 않고,
-- 한 테이블로 읽어도 정확히 한 벌이다.
  c_cred     CONSTANT VARCHAR2(30)  := 'BUCKETAUTH_NEW';
  c_base     CONSTANT VARCHAR2(200) :=
    'https://objectstorage.ap-chuncheon-1.oraclecloud.com/n/axtl8qsnlcns/b/bucket-20260410-1831/o/';
  v_root     VARCHAR2(400) := c_base || LOWER(p_name) || '/';
  v_prefix   VARCHAR2(400) := c_base || LOWER(p_name) || '/_bulk/';
  v_query    CLOB;
  v_rows     NUMBER;
  v_cleared  NUMBER := 0;
BEGIN
  IF p_from > p_to THEN
    raise_application_error(-20003, 'p_from ' || p_from || ' is after p_to ' || p_to);
  END IF;

  IF p_query IS NULL THEN
    -- 날짜순으로 정렬해서 내보낸다. 값이 파일 안에 어떤 순서로 놓이느냐가
    -- 읽는 쪽 비용을 정한다: Parquet 은 로우그룹마다 컬럼 min/max 를 들고
    -- 있어서, 찾는 값이 그 범위 밖이면 그룹을 통째로 건너뛴다.
    --
    -- 정렬 없이 내보내면 테이블 순서(= 수집 잡 순서 = 종목별)로 나가고, 그러면
    -- 로우그룹 하나가 3개월치를 담은 채 서로 겹친다 -- 실제로 그랬다:
    --   rg0 20250814~20251113 / rg1 20251010~20251211 / rg2 20251114~20260212
    -- 이러면 하루를 물어도 건너뛸 그룹이 없어 매번 전량을 읽는다. 외부 테이블
    -- 질의가 무엇을 묻든 1.1초로 같았던 이유다.
    --
    -- 파일 크기를 키우는 쪽이 아니라 이쪽이 손댈 수 있는 부분이다.
    -- EXPORT_DATA 는 약 45MB 에서 자체적으로 파일을 끊고 maxfilesize 로 그
    -- 상한을 올릴 수 없다(512MB, 1GB 를 줘도 4개, 최대 45.5MB 그대로였다).
    v_query := 'SELECT * FROM ' || p_name || ' WHERE ' || fn_export_day_col(p_name)
               || ' BETWEEN ''' || p_from || ''' AND ''' || p_to || ''''
               || ' ORDER BY ' || fn_export_day_col(p_name);
  ELSE
    -- :DAY 를 쓴 쿼리는 날짜별용이다. 그대로 두면 치환되지 않은 채 실행돼
    -- 바인드 변수 오류로 죽거나, 더 나쁘게는 기간 조건 없이 전량이 나간다.
    IF REGEXP_LIKE(p_query, ':DAY(\W|$)') THEN
      raise_application_error(-20004,
        'bulk export substitutes :FROM and :TO, not :DAY');
    END IF;
    -- 뒤따르는 한 글자를 붙잡아 되돌려 놓는다 -- sp_export_parquet 의 :DAY 와
    -- 같은 이유로, 그러지 않으면 "BETWEEN :FROM AND :TO" 의 공백이 사라진다.
    v_query := REGEXP_REPLACE(p_query,  ':FROM(\W|$)', '''' || p_from || '''\1');
    v_query := REGEXP_REPLACE(v_query,  ':TO(\W|$)',   '''' || p_to   || '''\1');
  END IF;

  EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM (' || v_query || ')' INTO v_rows;
  p_rows := v_rows;
  IF v_rows = 0 THEN
    DBMS_OUTPUT.PUT_LINE(RPAD(LOWER(p_name), 20) || p_from || '..' || p_to || '  0 rows, nothing written');
    RETURN;
  END IF;

  -- 하루가 파일 하나가 되도록 병렬 질의를 끈다 -- sp_export_parquet 과 같은
  -- 이유이고, 여기서는 기간 전체가 파일 하나가 된다.
  EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL QUERY';

  -- 이 대상의 기존 통짜를 비운다. 범위가 달라지면 파일 이름도 달라지므로,
  -- 지우지 않으면 두 벌이 남아 외부 테이블이 겹쳐 읽는다.
  FOR o IN (SELECT object_name FROM DBMS_CLOUD.LIST_OBJECTS(c_cred, v_prefix)) LOOP
    DBMS_CLOUD.DELETE_OBJECT(c_cred, v_prefix || o.object_name);
  END LOOP;

  -- 범위에 걸친 날짜 폴더도 비운다. 몇 달 일 배치로 쌓인 뒤에 통짜를 돌리면
  -- 그것들이 통짜 하나로 합쳐진다. object_name 은 프리픽스 기준 상대경로라
  -- 날짜 폴더는 '20260821/...' 꼴이고, '_bulk/...' 는 9번째가 '/' 가 아니라
  -- 걸리지 않는다.
  FOR o IN (SELECT object_name FROM DBMS_CLOUD.LIST_OBJECTS(c_cred, v_root)) LOOP
    IF INSTR(o.object_name, '/') = 9
       AND SUBSTR(o.object_name, 1, 8) BETWEEN p_from AND p_to THEN
      DBMS_CLOUD.DELETE_OBJECT(c_cred, v_root || o.object_name);
      v_cleared := v_cleared + 1;
    END IF;
  END LOOP;

  DBMS_CLOUD.EXPORT_DATA(
    credential_name => c_cred,
    file_uri_list   => v_prefix || p_from || '_' || p_to || '_part',
    format          => JSON_OBJECT('type' VALUE 'parquet'),
    query           => v_query);

  DBMS_OUTPUT.PUT_LINE(RPAD(LOWER(p_name), 20) || p_from || '..' || p_to || '  '
    || TO_CHAR(v_rows, 'FM999,999,999') || ' rows'
    || CASE WHEN v_cleared > 0 THEN ', ' || v_cleared || ' day file(s) folded in' END);

  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL QUERY';
EXCEPTION
  WHEN OTHERS THEN
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL QUERY';
    RAISE;
END;
