CREATE OR REPLACE PROCEDURE sp_src_code_sync (
  p_max_distinct IN  NUMBER   DEFAULT 40,     -- 이 수 이하인 컬럼만 수확
  p_table        IN  VARCHAR2 DEFAULT NULL,   -- 한 표만 (기본: 전부)
  p_rows         OUT NUMBER                   -- 수확한 값의 수
) AS
-- 결과 테이블의 컬럼에서 코드성 값을 뽑아 src_code 에 적는다.
--
-- 원천이 코드표를 공시하는지와 무관하게 동작하는 것이 요점이다. KSD 는 KACD
-- 목록을 주지만(ksd_kacd_code, 61종) 그 표에 MBS·SLBS 가 없고,
-- ksd_bond_isin 의 코드 컬럼은 1,870,025 행 전부 NULL 이다 -- 전체 목록
-- API 는 이름만 주고 코드는 그 코드로 조회할 때만 알 수 있어서다. KRX 는
-- mkt_nm('정규'/'야간')에 코드를 아예 주지 않는다.
--
-- 그래서 거꾸로 간다: **실제로 들어온 값이 곧 어휘다.**
--
-- 후보는 통계로 고른다. user_tab_columns.num_distinct 가 555개 문자 컬럼
-- 전부에 채워져 있고 저카디널리티 컬럼에서는 실측과 정확히 맞는다(측정:
-- codevalue_nm 11=11, mkt_id 6=6, mkt_nm 2=2). 그래서 COUNT(DISTINCT) 를
-- 418개 컬럼에 돌리는 전수 스캔 없이 165개만 GROUP BY 하면 된다. 통계가
-- 낡으면 후보가 조금 틀릴 뿐이고, 다음 실행이 바로잡는다.
--
-- GROUP BY 비용은 실측으로 큰 표에서 1~2초다(krx_etf_pdf 4.5M행 1.63초).
-- 날짜 컬럼으로 좁히면 0.2~0.3초지만 증분으로 하지 않는다 -- 사라진 값의
-- row_cnt 까지 맞추려면 전수가 맞고, 전체가 몇 분이면 하루 한 번으로 족하다.
--
-- 지우지 않는다. 원천이 더는 보내지 않는 값은 last_seen 이 멈추는 것으로
-- 드러나고, 과거 데이터에는 그 값이 아직 남아 있다.
--
-- 이름이 날짜처럼 생긴 컬럼은 뺀다. bas_dd(17종)처럼 임계값을 통과하지만
-- 코드가 아니고, 그냥 두면 거래일마다 src_code 에 행이 하나씩 쌓인다.
-- 값으로 판정(8자리 숫자)하지 않는 이유는 8자리 숫자 코드가 있을 수 있어서다
-- -- 이름으로 거르는 것이 예측 가능하다.
--
-- sp_run_daily_batch2 의 늦은 단계에 둔다. 그날 수집이 끝난 뒤여야 새 값이
-- 보인다.
  v_n    PLS_INTEGER;
  v_sql  VARCHAR2(4000);
BEGIN
  p_rows := 0;

  FOR c IN (
    SELECT c.table_name, c.column_name, c.num_distinct,
           CASE
             WHEN c.table_name LIKE 'KRX%' THEN 'KRX'
             WHEN c.table_name LIKE 'KSD%' THEN 'KSD'
             WHEN c.table_name LIKE 'KIS%' OR c.table_name LIKE 'FO!_%' ESCAPE '!'
                                           THEN 'KIS'
             ELSE 'AMC'
           END AS source_name
      FROM user_tab_columns c
      JOIN user_tab_columns j
        ON j.table_name = c.table_name AND j.column_name = 'JOB_ID'
     WHERE c.data_type LIKE 'VARCHAR%'
       -- 외부 테이블은 뺀다. 같은 데이터를 버킷에서 다시 읽는 사본이라
       -- 수확하면 값이 두 벌로 들어오고, 스캔이 오브젝트 스토리지를 타서
       -- 느리다(검증: 45초 -> 144초).
       --
       -- user_tables 로 조인해서는 걸러지지 않는다 -- Oracle 은 외부 테이블도
       -- user_tables 에 넣는다. 이름 규칙(XT_)에 기대지 않고 사전을 본다.
       AND NOT EXISTS (SELECT 1 FROM user_external_tables x
                        WHERE x.table_name = c.table_name)
       -- 엔진 부기는 코드 어휘가 아니다. 그 값은 원천이 보낸 것이 아니라
       -- 우리가 정한 것이다(save_mode, status, schedule_tag …). JOB_ID 가
       -- 있으면 결과 테이블이라는 판정이 이것들을 잡아서 명시로 뺀다.
       AND c.table_name NOT IN ('API_MST', 'API_JOB_BUILDER', 'API_JOB',
                                'API_JOB_LOG', 'SRC_CODE')
       AND c.num_distinct BETWEEN 2 AND p_max_distinct
       -- 엔진 부기와 자유 서술 컬럼
       AND c.column_name NOT IN ('API_ID', 'JOB_ID', 'DESCRIPTION', 'NOTE')
       -- 날짜처럼 생긴 이름 (위 주석 참고)
       AND SUBSTR(c.column_name, -3) NOT IN ('_DD', '_DT', '_AT')
       AND SUBSTR(c.column_name, -4) NOT IN ('DATE', '_YMD', 'YYMM')
       AND (p_table IS NULL OR c.table_name = UPPER(p_table))
     ORDER BY c.table_name, c.column_name
  ) LOOP
    -- 값이 긴 것은 키에 못 들어간다. 400자를 넘기면 그 컬럼은 건너뛴다 --
    -- 코드성 값이 그만큼 길 일은 없고, 넘긴다면 코드 컬럼이 아니다.
    v_sql :=
      'MERGE /*+ NO_PARALLEL */ INTO src_code t'                            || CHR(10) ||
      'USING (SELECT ''' || c.table_name  || ''' table_name,'               || CHR(10) ||
      '              ''' || c.column_name || ''' column_name,'              || CHR(10) ||
      '              ' || c.column_name || ' code_value,'                   || CHR(10) ||
      '              COUNT(*) row_cnt'                                      || CHR(10) ||
      '         FROM ' || c.table_name                                      || CHR(10) ||
      '        WHERE ' || c.column_name || ' IS NOT NULL'                   || CHR(10) ||
      '          AND LENGTH(' || c.column_name || ') <= 400'                || CHR(10) ||
      '        GROUP BY ' || c.column_name || ') s'                         || CHR(10) ||
      '   ON (t.table_name  = s.table_name'                                 || CHR(10) ||
      '   AND t.column_name = s.column_name'                                || CHR(10) ||
      '   AND t.code_value  = s.code_value)'                                || CHR(10) ||
      ' WHEN MATCHED THEN UPDATE SET t.row_cnt = s.row_cnt,'                || CHR(10) ||
      '      t.distinct_cnt = ' || c.num_distinct || ', t.last_seen = SYSDATE' || CHR(10) ||
      ' WHEN NOT MATCHED THEN INSERT (table_name, column_name, code_value,' || CHR(10) ||
      '      source_name, row_cnt, distinct_cnt, first_seen, last_seen)'         || CHR(10) ||
      ' VALUES (s.table_name, s.column_name, s.code_value, '''
             || c.source_name || ''', s.row_cnt, ' || c.num_distinct
             || ', SYSDATE, SYSDATE)';

    BEGIN
      EXECUTE IMMEDIATE v_sql;
      v_n := SQL%ROWCOUNT;
      p_rows := p_rows + v_n;
      IF v_n > 0 THEN
        DBMS_OUTPUT.PUT_LINE(RPAD(LOWER(c.table_name) || '.'
                             || LOWER(c.column_name), 44) || v_n || ' value(s)');
      END IF;
    EXCEPTION
      -- 한 컬럼이 깨져도 나머지를 멈추지 않는다. 수확은 보조적인 일이고,
      -- 어느 컬럼이 안 됐는지는 출력에 남는다.
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(RPAD(LOWER(c.table_name) || '.'
                             || LOWER(c.column_name), 44) || 'skipped: ' || SQLERRM);
    END;
  END LOOP;

  DBMS_OUTPUT.PUT_LINE('src_code: ' || TO_CHAR(p_rows, 'FM999,999,999') || ' value(s)');
END;
