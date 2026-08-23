CREATE OR REPLACE PROCEDURE sp_export_parquet (
  p_name    IN  VARCHAR2,                 -- 내보낼 대상 (프리픽스 이름 겸 기본 소스)
  p_from    IN  VARCHAR2,                 -- 시작 영업일 (YYYYMMDD, 포함)
  p_to      IN  VARCHAR2 DEFAULT NULL,    -- 종료 영업일 (기본: p_from 과 같은 날)
  p_rows    OUT NUMBER,                   -- 내보낸 행 수 (범위면 합계)
  p_query   IN  CLOB     DEFAULT NULL     -- 사용자 지정 쿼리 (:DAY 가 그날로 치환됨)
) AS
-- 결과 테이블(또는 뷰, 또는 임의의 쿼리)을 날짜별 Parquet 로 오브젝트
-- 스토리지에 내보낸다.
--
-- 소스가 쿼리라는 점이 핵심이다. 수집 시점의 CSV 버퍼를 변환하던 기존 경로는
-- 그 버퍼가 언제 finalize 되었느냐에 결과가 좌우돼서, 하루가 두 번에 나뉘어
-- 처리되면 뒤엣것이 앞엣것을 덮어썼고, 버퍼를 거치지 않은 백필분은 아예
-- 내보낼 방법이 없었다. 테이블을 직접 읽으면 그 상태가 사라진다 -- 같은
-- 날짜로 몇 번을 돌리든 결과가 같고, 언제 적재됐는지와 무관하다.
--
-- p_query 를 주면 그것이 소스가 되고 p_name 은 프리픽스 이름으로만 쓰인다.
-- 컬럼 선별, 마스터 조인, 뷰, 집계 -- SELECT 로 표현되는 것이면 무엇이든
-- 되고, Parquet 의 컬럼 구성은 SELECT 목록이 그대로 결정한다. 다만 :DAY 로
-- 그날을 좁혀야 한다: 프리픽스가 날짜별이라 쿼리가 다른 날을 담으면 경로와
-- 내용이 어긋나고, 재실행 시 프리픽스 단위로 비우는 정리도 어긋난다.
--
-- :DAY 는 따옴표까지 포함해 치환되므로 쿼리에서는 감싸지 않는다 --
-- "WHERE d = :DAY" 나 "TO_DATE(:DAY,'YYYYMMDD')" 처럼 쓴다. PL/SQL 리터럴
-- 안에서 따옴표가 두 겹이 되는 걸 피하려는 것이고, 바인드 변수처럼 보이지만
-- 실제로는 EXPORT_DATA 에 넘기기 전에 끝나는 문자열 치환이다(EXPORT_DATA 의
-- query 는 정적 문자열이라 바인드를 받지 않는다). 뒤에 글자가 이어지는
-- :DAYS 같은 이름은 건드리지 않는다.
--
-- 날짜 컬럼은 테이블마다 이름이 달라 기본 쿼리용으로 여기 매핑해 둔다. 새
-- 결과 테이블을 기본 경로로 내보내려면 이 CASE 에 한 줄을 더하면 되고,
-- 매핑이 없는 대상(뷰 등)은 p_query 로 부르면 된다.
--
-- 파일명은 Oracle 이 정한다(접두사 뒤에 워커 id 와 타임스탬프가 붙는다).
-- 그래서 재내보내기는 덮어쓰지 않고 쌓이므로, 먼저 해당 프리픽스를 비운다.
--
-- 돌려주는 값이 파일 수가 아니라 행 수인 이유: 파일 수는 Oracle 이 병렬
-- 워커를 몇 개 썼는지일 뿐 같은 데이터가 2개도 4개도 되므로 호출자에게
-- 알려주는 바가 없고, 세자면 LIST_OBJECTS 를 한 번 더 불러야 한다. 행 수는
-- 빈 날을 건너뛰려고 어차피 구해 두는 값이라 공짜다.
  c_cred     CONSTANT VARCHAR2(30)  := 'BUCKETAUTH_NEW';
  c_base     CONSTANT VARCHAR2(200) :=
    'https://objectstorage.ap-chuncheon-1.oraclecloud.com/n/axtl8qsnlcns/b/bucket-20260410-1831/o/';
  v_col      VARCHAR2(60);
  v_to       VARCHAR2(8) := NVL(p_to, p_from);
  v_day      VARCHAR2(8);
  v_prefix   VARCHAR2(400);
  v_query    CLOB;
  v_rows     NUMBER;
BEGIN
  IF p_query IS NULL THEN
    v_col := CASE LOWER(p_name)
               WHEN 'kis_futopt_chart' THEN 'stck_bsop_date'
               WHEN 'kis_futopt_daily' THEN 'stck_bsop_date'
               WHEN 'kis_index_daily'  THEN 'stck_bsop_date'
               WHEN 'kis_futopt_price' THEN 'SUBSTR(trade_at, 1, 8)'
             END;
    IF v_col IS NULL THEN
      raise_application_error(-20001,
        'no date column mapped for ' || p_name || '; pass p_query instead');
    END IF;
  END IF;

  p_rows := 0;
  v_day := p_from;
  WHILE v_day <= v_to LOOP
    v_prefix := c_base || LOWER(p_name) || '/' || v_day || '/';

    IF p_query IS NULL THEN
      v_query := 'SELECT * FROM ' || p_name || ' WHERE ' || v_col || ' = ''' || v_day || '''';
    ELSE
      -- 뒤따르는 한 글자를 붙잡아 되돌려 놓는다. 그러지 않으면
      -- "TO_DATE(:DAY,'YYYYMMDD')" 의 쉼표가 치환과 함께 사라진다.
      v_query := REGEXP_REPLACE(p_query, ':DAY(\W|$)', '''' || v_day || '''\1');
    END IF;

    -- 행이 없으면 내보내지 않는다. EXPORT_DATA 는 0건이어도 워커마다 빈 파일을
    -- 남기므로, 휴장일이 섞인 범위를 한 번 돌리면 버킷이 읽을 것 없는 객체로
    -- 채워진다.
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM (' || v_query || ')' INTO v_rows;

    IF v_rows > 0 THEN
      -- 이 날짜의 기존 객체 제거. 재실행이 두 벌을 남기지 않게 하는 유일한
      -- 지점이므로, 내보내기 직전에 한다.
      FOR o IN (SELECT object_name FROM DBMS_CLOUD.LIST_OBJECTS(c_cred, v_prefix)) LOOP
        DBMS_CLOUD.DELETE_OBJECT(c_cred, v_prefix || o.object_name);
      END LOOP;

      DBMS_CLOUD.EXPORT_DATA(
        credential_name => c_cred,
        file_uri_list   => v_prefix || 'part',
        format          => JSON_OBJECT('type' VALUE 'parquet'),
        query           => v_query);

      p_rows := p_rows + v_rows;
    END IF;

    v_day := TO_CHAR(TO_DATE(v_day, 'YYYYMMDD') + 1, 'YYYYMMDD');
  END LOOP;
END;
