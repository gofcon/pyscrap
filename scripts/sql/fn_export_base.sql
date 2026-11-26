CREATE OR REPLACE FUNCTION fn_export_base RETURN VARCHAR2 AS
-- 내보내기 객체 경로의 뿌리. **환경마다 다르다.**
--
-- 왜 상수가 아닌가: v1 과 v2 가 같은 버킷을 보는데, sp_export_parquet 는 쓰기
-- 전에 그 날짜 폴더의 객체를 전부 지운다. 둘이 같은 경로를 쓰면 같은 날을
-- 두 쪽이 지우고 써서 나중 것만 남고, 겹치면 섞인 파일 집합이 남는다. 분석
-- DB 의 외부 테이블은 <대상>/*/*.parquet 하나로 읽어 그 상태를 구별하지
-- 못한다.
--
-- 그래서 코드가 아니라 데이터로 둔다. 이 저장소가 수집 설정을 api_mst 에
-- 두는 것과 같은 결이다 -- 같은 코드가 환경마다 다른 곳에 쓴다.
--
-- **표가 없으면 기존 상수로 떨어진다.** 그게 이 함수의 요점이다: v1 에는
-- export_target 을 만들지 않으므로, 이 코드를 v1 에 재배포해도 동작이
-- 지금과 똑같다. v1 은 운영이라 건드리지 않는다.
--
-- 바꾸려면 그 계정의 export_target 한 행을 고친다. 재컴파일은 필요 없다 --
-- 호출마다 읽는다(내보내기는 하루 한 번이라 그 비용이 없다).
  c_default CONSTANT VARCHAR2(200) := 'https://objectstorage.ap-chuncheon-1.oraclecloud.com/n/axtl8qsnlcns/b/bucket-20260410-1831/o/';
  v_base    VARCHAR2(400);
BEGIN
  -- 동적 SQL 이라 export_target 이 없는 계정에서도 **컴파일된다**. 정적
  -- 참조로 두면 그 계정에서 이 함수와, 이 함수를 부르는 프로시저 셋이 전부
  -- INVALID 가 된다.
  EXECUTE IMMEDIATE 'SELECT base_uri FROM export_target WHERE id = 1' INTO v_base;
  RETURN NVL(v_base, c_default);
EXCEPTION
  -- 표가 없거나(ORA-00942) 행이 없으면 기존 경로. 그 둘만 삼킨다 --
  -- WHEN OTHERS 로 전부 받으면 컬럼 이름이 틀린 것도 조용히 기존 경로로
  -- 떨어져서, 설정을 고쳤는데 안 먹는 상황이 원인 없이 남는다.
  WHEN NO_DATA_FOUND THEN
    RETURN c_default;
  WHEN OTHERS THEN
    IF SQLCODE = -942 THEN
      RETURN c_default;
    END IF;
    RAISE;
END;
