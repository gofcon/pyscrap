CREATE OR REPLACE PROCEDURE sp_compact_exported (
  p_name    IN  VARCHAR2,                 -- 내보낼 대상 (프리픽스 이름 겸 기본 소스)
  p_to      IN  VARCHAR2,                 -- 마지막 영업일 (YYYYMMDD, 포함)
  p_from    IN  VARCHAR2,                 -- 시작 영업일 (포함)
  p_query   IN  CLOB     DEFAULT NULL,    -- 사용자 지정 쿼리 (:FROM, :TO 치환)
  p_rows    OUT NUMBER                    -- 내보낸 행 수
) AS
-- 한 대상의 지정 기간을 Parquet 파일 하나로 내보내고, 그 기간의 날짜별 파일을
-- 접어 넣는다. 버킷만 다룬다 -- DB 에서 행을 지우지 않는다.
--
-- 이름이 compact 인 이유. 이 프로시저(옛 sp_export_bulk)와
-- sp_archive_exported 의 내보내기 절반은 **같은 물건을 만들고 있었다** --
-- 기간 하나를 날짜순으로 정렬해 내보낸 Parquet. 그런데 이름이 _bulk 와 _arch
-- 로 둘이었고, 보관 규칙이 달랐고(전자는 대상당 하나만 남기고 전부 지움,
-- 후자는 달마다 하나씩 쌓임), 날짜 폴더를 접는 코드가 양쪽에 복사돼 있었다.
-- 하나로 합치고 _arch 를 쓴다.
--
-- 그래서 두 가지 쓰임이 한 프로시저다:
--   * 과거 전체를 한 번 뽑기 (옛 sp_export_bulk 의 일)
--   * 매월 1일 직전월 접기 (sp_archive_exported 가 이것을 부른다)
-- 둘의 차이는 기간을 누가 정하느냐뿐이다.
--
-- sp_export_parquet 과 나눈 이유는 그대로다. 날짜별로 쪼개는 것은 일 배치의
-- 필요다 -- 그날 것만 지우고 다시 쓸 수 있어야 재실행이 안전하고, 그래서
-- 하루가 폴더 하나다. 기간을 한 번 뽑는 작업에는 그 필요가 없고, 쪼개면
-- 손해만 남는다: v_k2i_atm 은 하루 1행이라 2.5KB 짜리 객체가 1,702개 생기고,
-- Parquet 은 파일마다 스키마와 압축 사전을 다시 만든다.
--
-- 접는 것이 용량 때문은 아니다. 실측으로 2일을 한 파일로 합쳐 3.3% 줄었다
-- (9,824,374 -> 9,499,104 바이트, kis_futopt_price 219,666행). 값은 객체 수와
-- 조회 지연에 있다 -- 외부 테이블이 <대상>/*/*.parquet 로 읽으므로 질의마다
-- 해당 객체를 전부 나열하고 연다. 날짜별이면 대상당 연 250개, 기간이면 12개다.
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
-- 파일은 <대상>/_arch/<시작>_<끝>_part_... 로 나간다. 범위를 파일 이름이 들고
-- 있어서, 겹침을 판단할 때 볼 것이 버킷 하나뿐이다 -- 어디까지 나갔는지를
-- 별도 표에 적어 두면 누가 버킷에서 파일을 지웠을 때 그 기록만 남아 어긋난다.
-- 판정은 fn_export_obj_kind / fn_export_obj_overlaps 가 한다.
--
-- 소스는 DB 다. 버킷의 날짜별 Parquet 을 읽어 합치는 쪽이 더 독립적이겠지만
-- (행을 지운 뒤에도 접을 수 있다) Parquet 을 다시 읽으면 타입이 한 번 더
-- 옮겨간다(NUMBER -> BINARY_DOUBLE, DATE -> TIMESTAMP(3)). 날짜별 파일과
-- 기간 파일을 외부 테이블 하나로 읽는 구조에서 그 둘의 스키마가 갈리면
-- 읽는 쪽에서 알아채기 어렵다. 재어 보지 않은 것을 운영 경로에 넣지 않는다.
  c_cred     CONSTANT VARCHAR2(30)  := 'BUCKETAUTH_NEW';
  -- 경로의 뿌리는 fn_export_base 가 준다 -- 환경마다 다르고 export_target
  -- 한 행으로 정한다. 그 표가 없는 계정(v1)에서는 함수가 기존 상수로
  -- 떨어지므로 동작이 바뀌지 않는다. 배포 순서: fn_export_base 를 먼저.
  c_base     CONSTANT VARCHAR2(400) := fn_export_base;
  v_root     VARCHAR2(400) := c_base || LOWER(p_name) || '/';
  v_prefix   VARCHAR2(400) := c_base || LOWER(p_name) || '/_arch/';
  v_query    CLOB;
  v_rows     NUMBER;
  v_folded   NUMBER := 0;
  v_dropped  NUMBER := 0;
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
        'range export substitutes :FROM and :TO, not :DAY');
    END IF;
    -- 뒤따르는 한 글자를 붙잡아 되돌려 놓는다 -- sp_export_parquet 의 :DAY 와
    -- 같은 이유로, 그러지 않으면 "BETWEEN :FROM AND :TO" 의 공백이 사라진다.
    v_query := REGEXP_REPLACE(p_query,  ':FROM(\W|$)', '''' || p_from || '''\1');
    v_query := REGEXP_REPLACE(v_query,  ':TO(\W|$)',   '''' || p_to   || '''\1');
  END IF;

  EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM (' || v_query || ')' INTO v_rows;
  p_rows := v_rows;
  IF v_rows = 0 THEN
    DBMS_OUTPUT.PUT_LINE(RPAD(LOWER(p_name), 20) || p_from || '..' || p_to
                         || '  0 rows, nothing written');
    RETURN;
  END IF;

  -- 기간 전체가 파일 하나가 되도록 병렬 질의를 끈다. EXPORT_DATA 는 병렬
  -- 워커마다 자기 파일을 쓰므로 파일 수는 곧 워커 수인데, 그 수는 파싱 시점의
  -- auto DOP 가 정하고 문장마다 달라진다. 행이 없는 워커도 스키마와 푸터만 든
  -- 1.4KB 파일을 남긴다 (sp_export_parquet 에 측정값이 있다).
  EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL QUERY';

  -- 겹치는 기간 파일을 비운다. **겹치는 것만** 비우는 것이 옛 sp_export_bulk
  -- 와 달라진 점이다. 그쪽은 대상의 _bulk 폴더를 통째로 비웠는데, 그러면 달마다
  -- 하나씩 쌓이는 쓰임이 불가능하다 -- 지난달 아카이브가 이번 달 접기에 지워진
  -- 다. 보관 규칙을 "겹치면 교체" 하나로 두면 두 쓰임이 같은 코드로 된다.
  --
  -- 비우지 않으면 두 벌이 남는다: EXPORT_DATA 는 파일 이름에 고유한 꼬리를
  -- 붙이므로 같은 프리픽스로 또 쓰면 덮지 않고 더한다. 2026-10-01 에 04:00
  -- 실행이 멈춘 뒤 손으로 다시 부르니 외부 테이블이 그 달을 정확히 두 벌
  -- (620,657 x 2) 읽었다.
  FOR o IN (SELECT object_name FROM DBMS_CLOUD.LIST_OBJECTS(c_cred, v_prefix)
             WHERE fn_export_obj_overlaps(object_name, p_from, p_to) = 'Y') LOOP
    DBMS_CLOUD.DELETE_OBJECT(c_cred, v_prefix || o.object_name);
    v_dropped := v_dropped + 1;
  END LOOP;

  DBMS_CLOUD.EXPORT_DATA(
    credential_name => c_cred,
    file_uri_list   => v_prefix || p_from || '_' || p_to || '_part',
    format          => JSON_OBJECT('type' VALUE 'parquet'),
    query           => v_query);

  -- 범위에 걸친 날짜 폴더를 접는다. 몇 달 일 배치로 쌓인 뒤에 돌리면 그것들이
  -- 기간 파일 하나로 합쳐진다. 내보내기 **뒤**에 지우는 순서가 중요하다 --
  -- 먼저 지우고 내보내다 실패하면 둘 다 없는 상태가 된다.
  FOR o IN (SELECT object_name FROM DBMS_CLOUD.LIST_OBJECTS(c_cred, v_root)
             WHERE fn_export_obj_kind(object_name) = 'DAY'
               AND fn_export_obj_overlaps(object_name, p_from, p_to) = 'Y') LOOP
    DBMS_CLOUD.DELETE_OBJECT(c_cred, v_root || o.object_name);
    v_folded := v_folded + 1;
  END LOOP;

  DBMS_OUTPUT.PUT_LINE(RPAD(LOWER(p_name), 20) || p_from || '..' || p_to || '  '
    || TO_CHAR(v_rows, 'FM999,999,999') || ' rows -> _arch'
    || CASE WHEN v_dropped > 0 THEN ', ' || v_dropped || ' range file(s) replaced' END
    || CASE WHEN v_folded  > 0 THEN ', ' || v_folded  || ' day file(s) folded in' END);

  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL QUERY';
EXCEPTION
  WHEN OTHERS THEN
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL QUERY';
    RAISE;
END;
