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
-- **통계가 낡으면 수확량이 크게 준다.** 처음엔 "조금 틀릴 뿐" 이라고 적었는데
-- 재 보니 아니었다: v1 에서 샘플을 떠 넣은 직후(통계 없음) 표 후보가 22개이고
-- 수확이 100건이었는데, GATHER_SCHEMA_STATS 를 돌린 뒤에는 후보 112개에
-- 수확 773건이었다. 적재 직후에 돌리면 그 소스의 어휘가 통째로 빠진다.
--
-- 평소에는 Oracle 의 자동 통계 수집이 따라오므로 하루 늦는 정도다. 그러나
-- 대량 적재나 새 소스의 첫날에는 먼저 통계를 모아야 한다 -- 증상이 "0건" 이
-- 아니라 "적게" 라서 눈에 안 띈다.
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
-- **그래서 이 필터는 날짜 컬럼의 이름 관례에 기대고 있다**: _dd · _dt · _at ·
-- date · _ymd · yymm. 관례를 벗어난 이름은 조용히 통과한다 -- api_rst 와
-- api_job_log 의 날짜 컬럼이 처음에 target_day 였고, _day 가 목록에 없어서
-- 후보로 들어올 수 있었다(값이 전부 NULL 이라 드러나지 않았을 뿐이다).
-- 목록에 _day 를 더하는 대신 컬럼을 target_date 로 고쳤다: 저장소의 다른
-- 날짜 컬럼이 전부 _date/_dd/_dt 이고, 관례를 지키는 쪽이 목록을 늘리는
-- 쪽보다 다음 컬럼에도 맞는다. 새 날짜 컬럼을 만들 때 이 관례를 따른다.
--
-- sp_run_daily_batch2 의 늦은 단계에 둔다. 그날 수집이 끝난 뒤여야 새 값이
-- 보인다.
--
-- 뷰 쪽 비용은 api_rst 가 커지면 같이 커진다. 표는 통계로 후보를 좁혀서
-- 저카디널리티 컬럼만 훑지만, 뷰는 통계가 없어 **후보를 가리기 위해 일단
-- 훑는다** -- 고카디널리티 컬럼(v_isin_bond_info.isin 같은 것)은 전수 GROUP BY
-- 를 하고 나서 임계값 초과로 버려진다. 파티션 프루닝이 걸려 한 소스 몫만
-- 읽는 것이 완충이지만(실측: 뷰 질의가 그 api_id 파티션만 읽는다), ISIN 채권
-- 상세가 38만 행쯤 쌓이면 재 봐야 한다. 그때 손잡이는 p_table 로 범위를
-- 좁히는 것이다.
--
-- html_block 이 만드는 block_index · label · html 은 뺀다(위 부기 목록).
-- 원천이 보낸 값이 아니라 그 모드가 붙인 것이고, label 은 화면 섹션
-- 이름이라 매핑할 대상이 없다.
  v_n    PLS_INTEGER;
  v_sql  VARCHAR2(8000);
BEGIN
  p_rows := 0;

  FOR c IN (
    -- 표: 통계로 후보를 좁힌다 (아래 "후보는 통계로 고른다" 참고).
    SELECT c.table_name, c.column_name, 'N' AS is_view,
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
       -- 엔진 부기와 자유 서술 컬럼. 뒤의 셋은 response_type='html_block' 이
       -- 만드는 것이지 원천이 보낸 값이 아니다 -- api_id/job_id 와 같은
       -- 근거로 뺀다 (확인: 이 셋은 v1 에도, 어느 모델에도 없고 그 모드가
       -- 쓰는 뷰에만 나타난다).
       --
       -- block_index(NUMBER)와 html(CLOB)은 VARCHAR 필터에 이미 안 걸리지만
       -- 셋을 같이 적는다. 이유를 한 자리에 두면 나중에 타입이 바뀌어도
       -- 판단이 남는다. label 은 화면 섹션 이름('발행기관코드' · 'CFI' ·
       -- 'Call 주체' 16종)이라 매핑할 대상이 없다.
       AND c.column_name NOT IN ('API_ID', 'JOB_ID', 'DESCRIPTION', 'NOTE',
                                 'BLOCK_INDEX', 'LABEL', 'HTML')
       -- 날짜처럼 생긴 이름 (위 주석 참고)
       AND SUBSTR(c.column_name, -3) NOT IN ('_DD', '_DT', '_AT')
       AND SUBSTR(c.column_name, -4) NOT IN ('DATE', '_YMD', 'YYMM')
       AND (p_table IS NULL OR c.table_name = UPPER(p_table))

    UNION ALL

    -- api_rst 위의 뷰. 두 층으로 내린 소스가 거기 있고, 그 값은 result_json
    -- (JSON 타입) 안이라 표를 훑는 위 분기가 **보지 못한다** -- data_type LIKE
    -- 'VARCHAR%' 에 안 걸린다. 그래서 두 층 이전이 수확을 깎았다: 운용사 ETF
    -- 표의 코드 컬럼 11개(FUNDTYPENM·TYPENM·THEME_TAG·PENSION_TAG …)가
    -- 조용히 빠져 있었다.
    --
    -- 뷰 컬럼은 타입과 이름이 사전에 그대로 올라온다(RETURNING 타입이
    -- data_type/data_length 가 된다) 므로 위와 같은 필터가 다 먹는다. 단
    -- **통계가 없다** -- 10개 뷰의 모든 컬럼이 num_distinct IS NULL 이다.
    -- 그래서 여기서는 그 조건을 걸지 않고, 카디널리티 판정을 아래 MERGE 의
    -- GROUP BY 안에서 한다 (COUNT(*) OVER ()).
    --
    -- api_rst 위의 뷰로 한정하는 이유: user_tab_columns 는 모든 뷰를
    -- 포함하므로, 타입 표 위의 뷰까지 들어오면 같은 어휘가 table_name 두
    -- 개로 쌓인다(src_code PK 가 (table_name, column_name, code_value)다).
    -- 그러면 "이 둘은 같은 뜻" 을 사람이 다시 판단해야 한다.
    SELECT c.table_name, c.column_name, 'Y' AS is_view,
           -- 이름에서 소스를 끌어낼 때 'V_' 를 떼야 한다. 떼지 않으면
           -- V_KRX_* 가 KRX 로 안 잡히고 전부 AMC 로 떨어진다.
           CASE
             WHEN SUBSTR(c.table_name, 3) LIKE 'KRX%' THEN 'KRX'
             WHEN SUBSTR(c.table_name, 3) LIKE 'KSD%' THEN 'KSD'
             WHEN SUBSTR(c.table_name, 3) LIKE 'KIS%'
               OR SUBSTR(c.table_name, 3) LIKE 'FO!_%' ESCAPE '!' THEN 'KIS'
             WHEN SUBSTR(c.table_name, 3) LIKE 'ISIN%' THEN 'KRX'
             ELSE 'AMC'
           END AS source_name
      FROM user_tab_columns c
      JOIN user_tab_columns j
        ON j.table_name = c.table_name AND j.column_name = 'JOB_ID'
     WHERE c.data_type LIKE 'VARCHAR%'
       AND c.table_name IN (SELECT d.name FROM user_dependencies d
                             WHERE d.type = 'VIEW' AND d.referenced_name = 'API_RST')
       -- 표 분기와 같은 목록 (위 주석 참고). html_block 이 만드는 셋이
       -- 여기로 들어오는 자리라, 빠지는 것은 사실상 이 분기에서다.
       AND c.column_name NOT IN ('API_ID', 'JOB_ID', 'DESCRIPTION', 'NOTE',
                                 'BLOCK_INDEX', 'LABEL', 'HTML')
       AND SUBSTR(c.column_name, -3) NOT IN ('_DD', '_DT', '_AT')
       AND SUBSTR(c.column_name, -4) NOT IN ('DATE', '_YMD', 'YYMM')
       AND (p_table IS NULL OR c.table_name = UPPER(p_table))
     ORDER BY 1, 2
  ) LOOP
    -- 값이 긴 것은 키에 못 들어간다. 400자를 넘기면 그 값은 건너뛴다 --
    -- 코드성 값이 그만큼 길 일은 없고, 넘긴다면 코드 컬럼이 아니다.
    --
    -- 카디널리티 판정이 **이 질의 안에** 있다. COUNT(*) OVER () 가 그룹 수를
    -- 주므로, 값 목록을 뽑는 그 한 번의 GROUP BY 로 "후보인가" 까지 끝난다.
    -- 뷰에는 통계가 없어서 이 방법이 필요했고, 표에도 같이 쓴다:
    --
    --   * distinct_cnt 가 추정이 아니라 **실제 값**이 된다. 통계가 낡으면
    --     전에는 틀린 수가 들어갔다.
    --   * 통계가 틀렸을 때의 안전장치가 된다. 전에는 통계가 10이라고 하면
    --     실제로 5,000종이어도 그 5,000행을 다 넣었다. 이제 grp_cnt 가
    --     임계값을 넘으면 아무것도 넣지 않는다.
    --   * 세는 대상이 저장하는 대상과 같아진다 -- 400자 넘는 값은 양쪽에서
    --     똑같이 빠진다(num_distinct 는 그것까지 세고 있었다).
    --
    -- 표의 후보 선정은 그대로 통계를 쓴다. 그게 418개 컬럼을 다 훑지 않는
    -- 이유이고, 여기 판정은 이미 좁혀진 후보에만 걸린다.
    v_sql :=
      'MERGE /*+ NO_PARALLEL */ INTO src_code t'                            || CHR(10) ||
      'USING (SELECT ''' || c.table_name  || ''' table_name,'               || CHR(10) ||
      '              ''' || c.column_name || ''' column_name,'              || CHR(10) ||
      '              code_value, row_cnt, grp_cnt'                          || CHR(10) ||
      '         FROM (SELECT ' || c.column_name || ' code_value,'           || CHR(10) ||
      '                      COUNT(*) row_cnt,'                             || CHR(10) ||
      '                      COUNT(*) OVER () grp_cnt'                      || CHR(10) ||
      '                 FROM ' || c.table_name                              || CHR(10) ||
      '                WHERE ' || c.column_name || ' IS NOT NULL'           || CHR(10) ||
      '                  AND LENGTH(' || c.column_name || ') <= 400'        || CHR(10) ||
      '                GROUP BY ' || c.column_name || ')'                   || CHR(10) ||
      '        WHERE grp_cnt BETWEEN 2 AND ' || p_max_distinct || ') s'     || CHR(10) ||
      '   ON (t.table_name  = s.table_name'                                 || CHR(10) ||
      '   AND t.column_name = s.column_name'                                || CHR(10) ||
      '   AND t.code_value  = s.code_value)'                                || CHR(10) ||
      ' WHEN MATCHED THEN UPDATE SET t.row_cnt = s.row_cnt,'                || CHR(10) ||
      '      t.distinct_cnt = s.grp_cnt, t.last_seen = SYSDATE'             || CHR(10) ||
      ' WHEN NOT MATCHED THEN INSERT (table_name, column_name, code_value,' || CHR(10) ||
      '      source_name, row_cnt, distinct_cnt, first_seen, last_seen)'    || CHR(10) ||
      ' VALUES (s.table_name, s.column_name, s.code_value, '''
             || c.source_name || ''', s.row_cnt, s.grp_cnt'
             || ', SYSDATE, SYSDATE)';

    BEGIN
      EXECUTE IMMEDIATE v_sql;
      v_n := SQL%ROWCOUNT;
      p_rows := p_rows + v_n;
      IF v_n > 0 THEN
        DBMS_OUTPUT.PUT_LINE(RPAD(LOWER(c.table_name) || '.'
                             || LOWER(c.column_name), 44) || v_n || ' value(s)'
                             || CASE WHEN c.is_view = 'Y' THEN '  (view)' END);
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
