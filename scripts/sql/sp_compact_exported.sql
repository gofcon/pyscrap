CREATE OR REPLACE PROCEDURE sp_compact_exported (
  p_name     IN  VARCHAR2,                -- 대상 (버킷 프리픽스 이름)
  p_to       IN  VARCHAR2,                -- 마지막 영업일 (YYYYMMDD, 포함)
  p_from     IN  VARCHAR2,                -- 시작 영업일 (포함)
  p_order_by IN  VARCHAR2 DEFAULT NULL,   -- 정렬 컬럼/식 (기본: fn_export_day_col)
  p_rows     OUT NUMBER                   -- 합친 행 수
) AS
-- 버킷에 이미 있는 Parquet 을 읽어 기간 파일 하나로 합친다.
-- <대상>/_arch/<시작>_<끝>_part_... 로 쓰고, 재료가 된 객체를 지운다.
--
-- **DB 를 읽지 않는다.** 이것이 요점이다. 그래서:
--
--   * purge 가 끝난 구간도 합칠 수 있다. 월별 파일 열두 개를 연별 하나로
--     접는 것 -- 손으로 하고 싶은 바로 그 일 -- 이 DB 에 행이 없어도 된다.
--   * DB 데이터를 잃을 경로가 없다. DELETE 문이 없다.
--   * **커버리지가 구조적으로 보존된다.** 지울 객체들을 읽어서 새 파일을
--     만들므로, 새 파일은 지워지는 것들이 담고 있던 것을 그대로 담는다.
--
-- 마지막 항목이 중요하다. 2026-11-26 에 DB 를 소스로 만들어 봤다가 실측으로
-- 데이터 손실을 찾았다: 1월을 purge 한 뒤 1~2월을 합치면 DB 에 2월만 있으므로
-- 2월 400행만 내보내고 월별 파일 둘을 지웠다. 남은 파일 이름은
-- '20260101_20260228' 이라 1월을 담았다고 **주장했다** -- 그 뒤 겹침 판정이
-- 1월은 커버됐다고 믿는다. 버킷을 소스로 두면 이 상태가 만들어질 수 없다.
--
-- 왜 처음부터 이러지 않았나: Parquet 을 다시 읽으면 타입이 또 옮겨간다고
-- 추측했다. 재어 보니 아니었다 -- kis_futopt_daily 1,617행 13컬럼으로 왕복해
-- A 와 B 의 스키마가 이름·타입·정밀도·스케일·길이까지 전부 같고 숫자 7컬럼
-- 합도 일치했다. 타입이 옮겨가는 것은 첫 홉(DB -> Parquet)뿐이고
-- (NUMBER/FLOAT -> BINARY_DOUBLE, DATE -> TIMESTAMP(3)) 그것은 일별 내보내기가
-- 이미 하는 일이다. 두 번째 홉은 double -> double 이라 고정이다.
--
-- 접는 값이 어디서 오나. 용량은 아니다 -- 2일을 한 파일로 합쳐 3.3% 였다
-- (9,824,374 -> 9,499,104 바이트). 값은 객체 수와 조회 지연이다. 외부 테이블이
-- <대상>/*/*.parquet 로 읽으므로 질의마다 해당 객체를 전부 나열하고 연다.
-- 다만 EXPORT_DATA 가 파일을 자체적으로 끊어서(실측 평균 46MB, 최대 72.8MB)
-- 큰 대상은 연별로 합쳐도 객체 수가 그대로다:
--   kis_futopt_price   월 2파일 -> 연 24파일   이득 없음
--   kis_futopt_chart   월 1파일 -> 연  3파일
--   krx_opt_daily      월 1파일 -> 연  2파일
--   kis_futopt_daily   월 1파일 -> 연  1파일   가장 큼
-- 그래서 이 프로시저는 자동으로 돌지 않는다. 월별 접기는
-- sp_run_monthly_archive 가 부르고, 그 위로 더 합치는 것은 값이 있는 대상에만
-- 손으로 한다.
--
-- p_query 가 없다. 옛 판(sp_export_bulk)에는 있었지만 그건 소스가 DB 였기
-- 때문이다. 지금은 소스가 "그 범위의 버킷 객체" 로 정해져 있고, 거기에 임의
-- 쿼리를 끼우면 새 파일이 지워지는 것들을 덮는다는 보장이 깨진다.
-- 뷰·조인·집계를 내보내는 일은 sp_export_parquet 의 p_query 가 그대로 한다.
--
-- p_order_by 는 정렬 대상이다. 기본은 fn_export_day_col 이고, 매핑이 없는
-- 프리픽스(kis_futopt_price1 처럼 이름이 뷰 이름과 다른 것)에는 직접 준다.
-- 정렬을 거를 수는 없다 -- 정렬이 이 작업의 조회 이득 절반이다.
  c_cred    CONSTANT VARCHAR2(30)  := 'BUCKETAUTH_NEW';
  c_base    CONSTANT VARCHAR2(400) := fn_export_base;
  v_root    VARCHAR2(400) := c_base || LOWER(p_name) || '/';
  v_prefix  VARCHAR2(400) := c_base || LOWER(p_name) || '/_arch/';
  v_window  VARCHAR2(128) := 'xtc_' || LOWER(p_name);
  v_col     VARCHAR2(200);
  v_uris    CLOB;
  v_src     NUMBER;
  v_out     NUMBER;
  v_kind    VARCHAR2(5);
  TYPE t_names IS TABLE OF VARCHAR2(512);
  v_objs    t_names := t_names();         -- 지울 것 (v_root 기준 상대경로)
  v_fresh   t_names := t_names();         -- 방금 쓴 것 (v_prefix 기준)
  TYPE t_seen IS TABLE OF PLS_INTEGER INDEX BY VARCHAR2(512);
  v_before  t_seen;
  v_days    NUMBER := 0;
  v_ranges  NUMBER := 0;

  PROCEDURE drop_window IS
  BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE ' || v_window || ' PURGE';
  EXCEPTION
    WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;   -- -942: 없음
  END;
BEGIN
  IF p_from > p_to THEN
    raise_application_error(-20003, 'p_from ' || p_from || ' is after p_to ' || p_to);
  END IF;

  v_col  := NVL(p_order_by, fn_export_day_col(p_name));
  p_rows := 0;

  -- 1) 재료를 모은다. 겹치는 객체 전부 -- 날짜별이든 기간이든.
  --
  -- 기간 객체는 **요청 범위 안에 들어와야 한다.** 밖으로 나가는 것을 재료로
  -- 쓰면 새 파일이 그 바깥 날짜까지 담는데 이름은 [from,to] 라 이름이
  -- 실제보다 좁게 말한다. 조용히 범위를 넓히지 않고 거절하고, 어떻게 부르면
  -- 되는지 알려 준다 -- v1 의 kis_futopt_chart 가 20250814..20260901 짜리
  -- 기간 파일을 들고 있는 것이 정확히 이 경우다.
  FOR o IN (SELECT object_name FROM DBMS_CLOUD.LIST_OBJECTS(c_cred, v_root)
             WHERE fn_export_obj_overlaps(object_name, p_from, p_to) = 'Y'
             ORDER BY object_name) LOOP
    v_kind := fn_export_obj_kind(o.object_name);

    IF v_kind = 'RANGE' THEN
      DECLARE
        v_rel VARCHAR2(512) := SUBSTR(o.object_name, 7);   -- '_arch/' 뗀 것
      BEGIN
        IF SUBSTR(v_rel, 1, 8) < p_from OR SUBSTR(v_rel, 10, 8) > p_to THEN
          raise_application_error(-20005,
            LOWER(p_name) || ': 기간 파일 ' || SUBSTR(v_rel, 1, 17)
            || ' 이 요청 범위 ' || p_from || '..' || p_to || ' 밖으로 나간다. '
            || '그 파일을 덮는 범위로 다시 부른다 -- 예: p_from => '''
            || LEAST(SUBSTR(v_rel, 1, 8), p_from) || ''', p_to => '''
            || GREATEST(SUBSTR(v_rel, 10, 8), p_to) || '''.');
        END IF;
      END;
      v_ranges := v_ranges + 1;
    ELSE
      v_days := v_days + 1;
    END IF;

    v_objs.EXTEND;
    v_objs(v_objs.COUNT) := o.object_name;
    v_uris := CASE WHEN v_uris IS NULL THEN TO_CLOB('') ELSE v_uris || ',' END
              || v_root || o.object_name;
  END LOOP;

  IF v_objs.COUNT = 0 THEN
    DBMS_OUTPUT.PUT_LINE(RPAD(LOWER(p_name), 20) || p_from || '..' || p_to
                         || '  합칠 객체 없음');
    RETURN;
  END IF;

  IF v_objs.COUNT = 1 AND v_ranges = 1 THEN
    -- 이미 기간 파일 하나다. 다시 쓰면 바이트만 바뀌고 얻는 것이 없다.
    DBMS_OUTPUT.PUT_LINE(RPAD(LOWER(p_name), 20) || p_from || '..' || p_to
                         || '  이미 기간 파일 하나, 그대로 둠');
    RETURN;
  END IF;

  -- 2) 그 객체들만으로 창을 연다. 글롭이 아니라 URI 목록이다 -- 읽는 것과
  --    지울 것이 정확히 같은 집합이어야 커버리지가 보존된다.
  drop_window;
  DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
    table_name      => v_window,
    credential_name => c_cred,
    file_uri_list   => v_uris,
    format          => JSON_OBJECT('type' VALUE 'parquet'));

  EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_window INTO v_src;

  -- 3) 한 파일로 나가도록 병렬 질의를 끈다. EXPORT_DATA 는 워커마다 자기
  --    파일을 쓰고 그 수는 파싱 시점의 auto DOP 가 정한다.
  --
  --    정렬해서 내보낸다. Parquet 은 로우그룹마다 컬럼 min/max 를 들고 있어
  --    정렬돼 있으면 하루를 묻는 질의가 그 그룹만 연다. 정렬 없이 나가면
  --    로우그룹 하나가 몇 달치를 담은 채 서로 겹쳐(실측: rg0 20250814~20251113,
  --    rg1 20251010~20251211) 무엇을 물어도 전량을 읽는다.
  EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL QUERY';

  -- 쓰기 전에 _arch 에 무엇이 있었는지 적어 둔다. 같은 범위를 다시 접으면
  -- 옛 파일과 새 파일의 이름 앞부분이 같아서(<from>_<to>_part) 이름으로는
  -- 구분되지 않는다 -- 차집합으로 가른다.
  FOR o IN (SELECT object_name FROM DBMS_CLOUD.LIST_OBJECTS(c_cred, v_prefix)) LOOP
    v_before(o.object_name) := 1;
  END LOOP;

  EXECUTE IMMEDIATE
    'BEGIN DBMS_CLOUD.EXPORT_DATA(credential_name => :1, file_uri_list => :2,'
    || ' format => :3, query => :4); END;'
    USING c_cred,
          v_prefix || p_from || '_' || p_to || '_part',
          JSON_OBJECT('type' VALUE 'parquet'),
          'SELECT * FROM ' || v_window || ' ORDER BY ' || v_col;

  -- 4) 방금 쓴 것만 다시 읽어 행수를 맞춘다. 맞기 전에는 재료를 지우지
  --    않는다 -- 지우고 나서 틀린 것을 알면 되돌릴 데가 없다.
  FOR o IN (SELECT object_name FROM DBMS_CLOUD.LIST_OBJECTS(c_cred, v_prefix)) LOOP
    IF NOT v_before.EXISTS(o.object_name) THEN
      v_fresh.EXTEND;
      v_fresh(v_fresh.COUNT) := o.object_name;
    END IF;
  END LOOP;

  v_uris := NULL;
  FOR i IN 1 .. v_fresh.COUNT LOOP
    v_uris := CASE WHEN v_uris IS NULL THEN TO_CLOB('') ELSE v_uris || ',' END
              || v_prefix || v_fresh(i);
  END LOOP;

  drop_window;
  DBMS_CLOUD.CREATE_EXTERNAL_TABLE(
    table_name      => v_window,
    credential_name => c_cred,
    file_uri_list   => v_uris,
    format          => JSON_OBJECT('type' VALUE 'parquet'));
  EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_window INTO v_out;

  IF v_out != v_src THEN
    -- 새로 쓴 것을 치운다. 두면 재료와 새 파일이 겹쳐 외부 테이블이 두 벌을
    -- 읽는다 -- 읽는 쪽에서 알아채기 어려운 상태다.
    FOR i IN 1 .. v_fresh.COUNT LOOP
      DBMS_CLOUD.DELETE_OBJECT(c_cred, v_prefix || v_fresh(i));
    END LOOP;
    drop_window;
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL QUERY';
    raise_application_error(-20006,
      LOWER(p_name) || ': 합친 파일이 ' || TO_CHAR(v_out, 'FM999,999,999')
      || ' 행인데 재료는 ' || TO_CHAR(v_src, 'FM999,999,999') || ' 행이었다 ('
      || p_from || '..' || p_to || '). 쓴 것을 지웠고 재료는 그대로 있다.');
  END IF;

  drop_window;

  -- 5) 재료를 지운다. 여기까지 오면 새 파일이 그것들을 담고 있다.
  FOR i IN 1 .. v_objs.COUNT LOOP
    DBMS_CLOUD.DELETE_OBJECT(c_cred, v_root || v_objs(i));
  END LOOP;

  p_rows := v_src;

  DBMS_OUTPUT.PUT_LINE(RPAD(LOWER(p_name), 20) || p_from || '..' || p_to || '  '
    || TO_CHAR(v_src, 'FM999,999,999') || ' rows -> _arch ('
    || v_fresh.COUNT || ' file(s)), 접어 넣은 것: 날짜별 ' || v_days
    || ' + 기간 ' || v_ranges);

  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL QUERY';
EXCEPTION
  WHEN OTHERS THEN
    drop_window;
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL QUERY';
    RAISE;
END;
