CREATE OR REPLACE PROCEDURE sp_archive_exported (
  p_month     IN  VARCHAR2 DEFAULT NULL,  -- 대상 월 YYYYMM (NULL 이면 직전월)
  p_deleted   OUT NUMBER                  -- 지운 행 수 합계
) AS
-- 한 달치 수집분을 Parquet 으로 옮기고 원본에서 지운다. 매월 1일에 도는 작업이다.
--
-- 왜 필요한가: kis_futopt_price 는 하루 87,000 행, kis_futopt_chart 는 33,000
-- 행씩 쌓인다. 한 달이면 각각 1GB, 0.5GB 다. 값은 Parquet 으로 이미 나가 있고
-- 외부 테이블로 그대로 읽히므로, DB 에 두 벌을 들고 있을 이유가 없다 --
-- 2026-09-01 에 이 셋을 손으로 비워 4.48GB 를 0.85GB 로 줄였고, 그 절차를
-- 그대로 옮겨 놓은 것이 이 프로시저다.
--
-- 기준이 '며칠 지난 것' 이 아니라 '어느 달' 이다. 1일에 돌면서 직전월을
-- 통째로 가져가므로 아카이브 파일 하나가 달력의 한 달과 정확히 맞고, 언제
-- 돌리든 -- 다시 돌리든 -- 같은 구간을 본다. 일수로 자르면 실행 시각에 따라
-- 경계가 흔들려 한 달이 두 파일에 걸친다.
--
-- 그 대신 못 돌고 지나간 달은 저절로 따라잡히지 않는다. 두 달을 건너뛰었다면
-- p_month 에 그 달을 주고 한 번 더 부른다. 자동으로 '남은 것 전부' 를 쓸어가게
-- 하지 않는 이유는, 그 동작이 필요한 상황이 곧 뭔가 잘못된 상황이라서다 --
-- 사람이 보고 부르는 편이 낫다.
--
-- sp_export_bulk 를 쓰지 않는다. 그쪽은 대상의 _bulk 폴더를 통째로 비우고 다시
-- 쓰는데, DB 에 최근 한 달치만 남은 상태에서 그러면 지난달까지의 아카이브가
-- 사라진다. 여기서는 달마다 <대상>/_arch/<YYYYMM>_part... 로 새 폴더를 하나씩
-- 더한다. 외부 테이블이 '<대상>/*/*.parquet' 로 읽으므로 -- 날짜 폴더든 _bulk
-- 든 _arch 든 한 겹은 와일드카드에 걸린다 -- 새 폴더는 만들자마자 읽힌다.
--
-- 지우기 전에 반드시 확인한다: 내보낸 구간을 외부 테이블에서 세어 원본과 같은
-- 수인지 본다. 이 확인이 통과하지 못하면 아무것도 지우지 않고 오류를 낸다.
-- 파일이 써졌다는 것과 읽힌다는 것은 다른 얘기고, 지우고 나면 되돌릴 수 없다.
--
-- 대상은 외부 테이블(xt_<대상>)이 있는 것으로 한정한다. 그것이 곧 "Parquet 으로
-- 읽을 수 있다"의 정의이고, 없는 표를 지우면 데이터가 사라진다.
  c_cred  CONSTANT VARCHAR2(30)  := 'BUCKETAUTH_NEW';
  c_base  CONSTANT VARCHAR2(200) :=
    'https://objectstorage.ap-chuncheon-1.oraclecloud.com/n/axtl8qsnlcns/b/bucket-20260410-1831/o/';
  TYPE t_names IS TABLE OF VARCHAR2(30);
  -- 손으로 적는다. 자동 발견(외부 테이블이 있는 표 전부)으로 하면 언젠가
  -- 의도치 않은 표가 목록에 들어오고, 그 사고의 결과가 '데이터 삭제' 다.
  v_targets t_names := t_names('kis_futopt_chart', 'kis_futopt_price', 'kis_futopt_daily');
  v_name    VARCHAR2(30);
  v_col     VARCHAR2(200);
  v_month   VARCHAR2(6);
  v_from    VARCHAR2(8);
  v_to      VARCHAR2(8);
  v_min     VARCHAR2(8);
  v_max     VARCHAR2(8);
  v_src     NUMBER;
  v_xt      NUMBER;
  v_del     NUMBER;
BEGIN
  p_deleted := 0;
  -- KST 기준. 이 프로젝트가 모으는 것은 전부 한국 시장이고, 인스턴스는 UTC 다.
  v_month := NVL(p_month, TO_CHAR(ADD_MONTHS(TRUNC(SYSDATE + 9/24, 'MM'), -1), 'YYYYMM'));
  -- 날짜 컬럼은 전부 YYYYMMDD 문자열이라 문자 비교로 한 달이 잘린다. '31' 은
  -- 그 달에 31일이 있든 없든 상한으로 맞는다 -- 없는 날짜는 데이터에도 없다.
  v_from  := v_month || '01';
  v_to    := v_month || '31';

  FOR i IN 1 .. v_targets.COUNT LOOP
    v_name := v_targets(i);
    v_col  := fn_export_day_col(v_name);

    EXECUTE IMMEDIATE 'SELECT MIN(' || v_col || '), MAX(' || v_col || '), COUNT(*) FROM '
                      || v_name || ' WHERE ' || v_col || ' BETWEEN :1 AND :2'
      INTO v_min, v_max, v_src USING v_from, v_to;

    IF v_src = 0 THEN
      DBMS_OUTPUT.PUT_LINE(RPAD(v_name, 20) || v_month || '  없음, 건너뜀');
      CONTINUE;
    END IF;

    -- 하나의 파일로 나가도록 병렬 질의를 끈다 (sp_export_bulk 와 같은 이유).
    EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL QUERY';
    DBMS_CLOUD.EXPORT_DATA(
      credential_name => c_cred,
      file_uri_list   => c_base || LOWER(v_name) || '/_arch/' || v_month || '_part',
      format          => JSON_OBJECT('type' VALUE 'parquet'),
      query           => 'SELECT * FROM ' || v_name
                         || ' WHERE ' || v_col || ' BETWEEN ''' || v_from || ''' AND ''' || v_to || ''''
                         || ' ORDER BY ' || v_col);
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL QUERY';

    -- 읽히는지 확인한다. 외부 테이블은 방금 쓴 폴더까지 포함해서 센다.
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM xt_' || v_name
                      || ' WHERE ' || v_col || ' BETWEEN :1 AND :2'
      INTO v_xt USING v_from, v_to;

    IF v_xt < v_src THEN
      raise_application_error(-20010,
        v_name || ': 외부 테이블이 ' || v_xt || ' 행만 읽는데 원본은 ' || v_src
        || ' 행이다 (' || v_month || '). 삭제하지 않았다.');
    END IF;

    EXECUTE IMMEDIATE 'DELETE /*+ NO_PARALLEL */ FROM ' || v_name
                      || ' WHERE ' || v_col || ' BETWEEN :1 AND :2' USING v_from, v_to;
    v_del := SQL%ROWCOUNT;
    p_deleted := p_deleted + v_del;

    DBMS_OUTPUT.PUT_LINE(RPAD(v_name, 20) || v_month || ' (' || v_min || '..' || v_max || ')  '
      || TO_CHAR(v_src, 'FM999,999,999') || ' rows -> _arch, '
      || TO_CHAR(v_del, 'FM999,999,999') || ' deleted (xt reads '
      || TO_CHAR(v_xt, 'FM999,999,999') || ')');
  END LOOP;
EXCEPTION
  WHEN OTHERS THEN
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL QUERY';
    RAISE;
END;
