CREATE OR REPLACE PROCEDURE sp_export_parquet (
  p_table   IN  VARCHAR2,                 -- 내보낼 결과 테이블
  p_from    IN  VARCHAR2,                 -- 시작 영업일 (YYYYMMDD, 포함)
  p_to      IN  VARCHAR2 DEFAULT NULL,    -- 종료 영업일 (기본: p_from 과 같은 날)
  p_files   OUT NUMBER                    -- 생성된 객체 수
) AS
-- 결과 테이블을 날짜별 Parquet 로 오브젝트 스토리지에 내보낸다.
--
-- 소스가 쿼리라는 점이 핵심이다. 수집 시점의 CSV 버퍼를 변환하던 기존 경로는
-- 그 버퍼가 언제 finalize 되었느냐에 결과가 좌우돼서, 하루가 두 번에 나뉘어
-- 처리되면 뒤엣것이 앞엣것을 덮어썼고, 버퍼를 거치지 않은 백필분은 아예
-- 내보낼 방법이 없었다. 테이블을 직접 읽으면 그 상태가 사라진다 -- 같은
-- 날짜로 몇 번을 돌리든 결과가 같고, 언제 적재됐는지와 무관하다.
--
-- 날짜 컬럼은 테이블마다 이름이 달라 여기서 매핑한다. 새 결과 테이블을
-- 내보내려면 이 CASE 에 한 줄을 더하면 된다.
--
-- 파일명은 Oracle 이 정한다(접두사 뒤에 워커 id 와 타임스탬프가 붙는다).
-- 그래서 재내보내기는 덮어쓰지 않고 쌓이므로, 먼저 해당 프리픽스를 비운다.
  c_cred     CONSTANT VARCHAR2(30)  := 'BUCKETAUTH_NEW';
  c_base     CONSTANT VARCHAR2(200) :=
    'https://objectstorage.ap-chuncheon-1.oraclecloud.com/n/axtl8qsnlcns/b/bucket-20260410-1831/o/';
  v_col      VARCHAR2(30);
  v_to       VARCHAR2(8) := NVL(p_to, p_from);
  v_day      VARCHAR2(8);
  v_prefix   VARCHAR2(400);
  v_query    CLOB;
  v_deleted  NUMBER := 0;
  v_rows     NUMBER;
BEGIN
  v_col := CASE LOWER(p_table)
             WHEN 'kis_futopt_chart' THEN 'stck_bsop_date'
             WHEN 'kis_futopt_daily' THEN 'stck_bsop_date'
             WHEN 'kis_index_daily'  THEN 'stck_bsop_date'
             WHEN 'kis_futopt_price' THEN 'SUBSTR(trade_at, 1, 8)'
           END;
  IF v_col IS NULL THEN
    raise_application_error(-20001, 'no date column mapped for table ' || p_table);
  END IF;

  p_files := 0;
  v_day := p_from;
  WHILE v_day <= v_to LOOP
    v_prefix := c_base || LOWER(p_table) || '/' || v_day || '/';

    -- 이 날짜의 기존 객체 제거. 재실행이 두 벌을 남기지 않게 하는 유일한
    -- 지점이므로, 내보내기 직전에 한다.
    FOR o IN (SELECT object_name FROM DBMS_CLOUD.LIST_OBJECTS(c_cred, v_prefix)) LOOP
      DBMS_CLOUD.DELETE_OBJECT(c_cred, v_prefix || o.object_name);
      v_deleted := v_deleted + 1;
    END LOOP;

    -- 행이 없으면 내보내지 않는다. EXPORT_DATA 는 0건이어도 워커마다 빈 파일을
    -- 남기므로, 휴장일이 섞인 범위를 한 번 돌리면 버킷이 읽을 것 없는 객체로
    -- 채워진다.
    EXECUTE IMMEDIATE
      'SELECT COUNT(*) FROM ' || p_table || ' WHERE ' || v_col || ' = :d'
      INTO v_rows USING v_day;

    IF v_rows > 0 THEN
      v_query := 'SELECT * FROM ' || p_table || ' WHERE ' || v_col || ' = ''' || v_day || '''';
      DBMS_CLOUD.EXPORT_DATA(
        credential_name => c_cred,
        file_uri_list   => v_prefix || 'part',
        format          => JSON_OBJECT('type' VALUE 'parquet'),
        query           => v_query);

      FOR o IN (SELECT object_name FROM DBMS_CLOUD.LIST_OBJECTS(c_cred, v_prefix)) LOOP
        p_files := p_files + 1;
      END LOOP;
    END IF;

    v_day := TO_CHAR(TO_DATE(v_day, 'YYYYMMDD') + 1, 'YYYYMMDD');
  END LOOP;
END;
