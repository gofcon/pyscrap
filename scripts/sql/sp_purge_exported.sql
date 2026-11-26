CREATE OR REPLACE PROCEDURE sp_purge_exported (
  p_name    IN  VARCHAR2,                 -- 비울 대상
  p_to      IN  VARCHAR2,                 -- 마지막 영업일 (YYYYMMDD, 포함)
  p_from    IN  VARCHAR2,                 -- 시작 영업일 (포함)
  p_deleted OUT NUMBER                    -- 지운 행 수
) AS
-- 그 기간이 버킷에서 읽히는 것을 확인하고 DB 에서 지운다. 버킷을 건드리지
-- 않는다 -- 쓰지도, 지우지도 않는다.
--
-- 왜 가르는가. 옛 sp_archive_exported 는 '내보내기' 와 'DB 자리 비우기' 를
-- 한 몸에 하고 있었고, 그 용접이 나머지 복잡함의 원인이었다. 내보낸 직후엔
-- 날짜별과 기간 파일이 **둘 다** 있는 중간 상태가 생기고, 그래서 검증이 두
-- 단계여야 했다 -- 먼저 "외부가 원본 이상을 읽나"(중복 허용), 접은 뒤 다시
-- "정확히 같나". 쓰는 일을 떼어내면 그 중간 상태가 없어지고 검증이 한 번이다.
--
-- 그래서 이쪽은 DELETE 를 하고 버킷에 손대지 않고, sp_compact_exported 는
-- 버킷에 쓰고 DB 에 손대지 않는다. 실패 모드가 각각 하나다.
--
-- 둘은 서로를 요구하지 않는다. 여기서 세는 것은 "그 날짜들이 버킷에서 읽히나"
-- 이고, 날짜별 파일로 읽히든 기간 파일로 읽히든 같다. 접기를 안 한 달도
-- 그대로 비울 수 있다 -- 접기는 객체 수와 조회 지연을 위한 별개의 정리다.
--
-- 검증용 외부 테이블을 **자기가 만들고 자기가 지운다.** 옛 코드는 미리 걸려
-- 있는 xt_<대상> 에 기댔고, 그래서 2026-11-26 에 수집 계정에서 외부 테이블을
-- 내렸을 때 조용히 깨졌다 -- 참조가 EXECUTE IMMEDIATE 라 user_dependencies
-- 에도 안 보였다. 필요한 창을 자기가 열고 닫으면 자격만 있는 어느 계정에서든
-- 돌고, 의존이 코드 안에 보인다.
--
-- 세는 범위가 '그 달' 이 아니라 '원본에 있는 날짜들' 이다. 달로 세면 기간
-- 파일이 그 달의 일부를 덮고 있을 때 어긋난다 -- 2026-10-01 첫 실행이 그래서
-- 멈췄다: kis_futopt_chart 의 기간 파일은 20250814..20260901 이고 09-01 은
-- 이미 전달에 비워져 DB 에 없으니, 외부가 12,735 행 더 읽혀 '같지 않다' 가
-- 됐다. 날짜 목록으로 세면 기간 파일이 과거 어디를 덮든 상관없고, 같은 날이
-- 두 벌이면 그 날의 수가 늘어나므로 중복 검출력은 그대로다.
--
-- 외부가 원본보다 적으면 멈춘다(내보내지 않은 날이 있다). 많아도 멈춘다 --
-- 그 기간이 버킷에 두 벌 있다는 뜻이고, 지우고 나면 중복만 남는다. 어느
-- 쪽이든 한 행도 지우지 않는다.
  c_cred    CONSTANT VARCHAR2(30)  := 'BUCKETAUTH_NEW';
  c_base    CONSTANT VARCHAR2(400) := fn_export_base;
  c_batch   CONSTANT NUMBER := 50000;   -- 한 번에 지울 행 수 (undo 한도)
  v_table   VARCHAR2(128) := 'xtv_' || LOWER(p_name);
  v_col     VARCHAR2(200);
  v_src     NUMBER;
  v_xt      NUMBER;
  v_del     NUMBER := 0;
  v_min     VARCHAR2(8);
  v_max     VARCHAR2(8);
  v_days    VARCHAR2(4000);   -- 원본에 있는 그 기간의 날짜들, IN 절에 넣을 꼴

  PROCEDURE drop_window IS
  BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE ' || v_table || ' PURGE';
  EXCEPTION
    WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;   -- -942: 없음
  END;
BEGIN
  p_deleted := 0;

  IF p_from > p_to THEN
    raise_application_error(-20003, 'p_from ' || p_from || ' is after p_to ' || p_to);
  END IF;

  v_col := fn_export_day_col(p_name);

  EXECUTE IMMEDIATE 'SELECT MIN(' || v_col || '), MAX(' || v_col || '), COUNT(*) FROM '
                    || p_name || ' WHERE ' || v_col || ' BETWEEN :1 AND :2'
    INTO v_min, v_max, v_src USING p_from, p_to;

  IF v_src = 0 THEN
    DBMS_OUTPUT.PUT_LINE(RPAD(LOWER(p_name), 20) || p_from || '..' || p_to
                         || '  없음, 건너뜀');
    RETURN;
  END IF;

  -- 안쪽에서 별명을 붙인다: 날짜 컬럼이 식인 대상(kis_futopt_price 의
  -- SUBSTR(trade_at,1,8))에서는 바깥이 원래 컬럼을 볼 수 없다.
  EXECUTE IMMEDIATE 'SELECT LISTAGG('''''''' || d || '''''''', '','') FROM '
                    || '(SELECT DISTINCT ' || v_col || ' AS d FROM ' || p_name
                    || ' WHERE ' || v_col || ' BETWEEN :1 AND :2 ORDER BY 1)'
    INTO v_days USING p_from, p_to;

  -- 검증용 창을 연다. 글롭이 <대상>/*/*.parquet 이라 날짜별과 기간 파일을
  -- 같이 읽는다 -- 그래서 위 날짜 목록으로 좁혀 센다. 컬럼은 Parquet 에서
  -- 읽어오므로 목록을 적지 않는다.
  drop_window;
  DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
    table_name      => v_table,
    credential_name => c_cred,
    file_uri_list   => c_base || LOWER(p_name) || '/*/*.parquet',
    format          => JSON_OBJECT('type' VALUE 'parquet'));

  EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_table
                    || ' WHERE ' || v_col || ' IN (' || v_days || ')'
    INTO v_xt;

  IF v_xt != v_src THEN
    drop_window;
    raise_application_error(-20010,
      LOWER(p_name) || ': 버킷이 ' || TO_CHAR(v_xt, 'FM999,999,999')
      || ' 행을 읽는데 원본은 ' || TO_CHAR(v_src, 'FM999,999,999') || ' 행이다 ('
      || p_from || '..' || p_to || '). '
      || CASE WHEN v_xt < v_src THEN '내보내지 않은 날이 있다 -- sp_export_parquet 를 먼저 돌린다.'
              ELSE '버킷에 두 벌이 있다 -- sp_compact_exported 로 접는다.' END
      || ' 삭제하지 않았다.');
  END IF;

  drop_window;

  -- 5만 행씩 지우고 사이사이 커밋한다. 한 문장으로 지우면 undo 가 모자란다 --
  -- kis_futopt_price 는 한 달이 284만 행이고, 2026-10-01 에 ORA-30036 으로
  -- 그렇게 멈췄다 (sp_krx_stock_base_dedup 이 같은 이유로 같은 모양이다).
  --
  -- 커밋하는 것이 맞다: 지우는 행은 방금 Parquet 으로 확인된 것들이고, 중간에
  -- 멈춘 실행은 다음 실행이 이어 간다 -- 남은 것만 다시 확인하고 지운다.
  LOOP
    EXECUTE IMMEDIATE 'DELETE /*+ NO_PARALLEL */ FROM ' || p_name
                      || ' WHERE ' || v_col || ' BETWEEN :1 AND :2 AND ROWNUM <= ' || c_batch
      USING p_from, p_to;
    EXIT WHEN SQL%ROWCOUNT = 0;
    v_del := v_del + SQL%ROWCOUNT;
    COMMIT;
  END LOOP;
  p_deleted := v_del;

  -- 자리를 실제로 반납한다. DELETE 는 고수위선을 내리지 않아서, 10만 행이
  -- 남은 kis_futopt_price 가 표와 인덱스로 1,206MB 를 들고 있었다 (2026-10-01).
  -- 이 작업의 목적이 용량이므로 지우는 것으로 끝낼 수 없다.
  --
  -- MOVE ONLINE 이다. SHRINK SPACE 는 이 Autonomous DB 에서 아무 일도 하지
  -- 않았다 -- COMPACT 와 CASCADE 를 따로 불러도 0초에 끝나고 세그먼트가 그대로
  -- 였다(ROW MOVEMENT 를 켠 뒤에도). MOVE ONLINE 은 세그먼트를 새로 만들어
  -- 1,206MB 를 45MB 로 줄였고 5초가 걸렸으며, 인덱스도 함께 다시 만들어져
  -- VALID 로 남는다. ONLINE 이라 도는 동안 DML 도 막지 않는다.
  EXECUTE IMMEDIATE 'ALTER TABLE ' || p_name || ' MOVE ONLINE';

  DBMS_OUTPUT.PUT_LINE(RPAD(LOWER(p_name), 20) || p_from || '..' || p_to
    || ' (' || v_min || '..' || v_max || ')  '
    || TO_CHAR(v_del, 'FM999,999,999') || ' rows deleted (버킷 '
    || TO_CHAR(v_xt, 'FM999,999,999') || ' 행 확인)');
EXCEPTION
  WHEN OTHERS THEN
    drop_window;
    RAISE;
END;
