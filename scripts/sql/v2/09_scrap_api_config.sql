-- SCRAP 계정: v1 에 없는 설정. 이행 스크립트가 다루지 않는 몫이다.
--
-- 왜 이 파일이 있나. api_mst/api_job_builder 의 내용은 데이터이고, 58+95행은
-- scripts/migrate_v1_to_v2.py 가 v1 에서 옮겼다. 그 스크립트는 **이행만**
-- 한다 -- v1 에 없는 설정을 그쪽에 적으면 같은 설정이 두 곳에 살면서 어느
-- 쪽이 정본인지 흐려진다(그 파일의 apply_to_v2 주석 참고).
--
-- 그러면 v2 에서 새로 정한 설정은 git 에 남는 곳이 없어진다. 여기가 그
-- 자리다. MERGE 라 여러 번 돌려도 같다.
--
-- 계정 이름은 적지 않는다. SCRAP 으로 접속해서 실행한다.

-- ===========================================================================
-- ISIN_BOND_INFO -- KRX 표준코드 채권 상세 (2026-11-25)
-- ===========================================================================
-- v1 의 ISIN_INFO 에서 이름과 domain 을 고치고 html_block 설정을 채운 행이다.
-- 받아 보니 채권 상세여서 domain 이 STOCK -> BOND 가 됐다.
--
-- response_type='html_block' 은 자르기만 하고 읽지 않는다. 이 페이지가 수집기
-- 가 해석할 모양이 아니기 때문이다 -- <table> 17개 중 하나는 래퍼이고, 안쪽
-- 16개는 라벨/값 격자다. html_table 은 사각형을 가정해 위치로 맞추므로 라벨이
-- 데이터 자리에 들어간다 (app.scrapers.dynamic._parse_html_block).

MERGE INTO api_mst t
USING (SELECT 'ISIN_BOND_INFO' api_id FROM dual) s
   ON (t.api_id = s.api_id)
 WHEN NOT MATCHED THEN INSERT (
        api_id, api_name, source_name, domain, protocol, method, api_url,
        response_type, parse_json, record_map_json,
        job_key_params, row_stamp_json, is_repeated, note)
      VALUES (
        'ISIN_BOND_INFO', '표준코드 채권 상세정보', 'KRX', 'BOND', 'HTTP', 'GET',
        'https://isin.krx.co.kr/srch/srch.do?method=srchPopup3&std_cd=:ISIN',
        'html_block',
        -- 자르는 규칙이 전부 여기 있다. 래퍼 표를 버리고(:not(:has(table)))
        -- 값이 있는 것만 남긴다(:has(td:not(:empty))). 실측으로 셀렉터 판정과
        -- 텍스트 기준 판정이 정확히 일치한다(채권 13 vs 13, 주식 4 vs 4).
        --
        -- label 은 블록을 번호가 아니라 **이름**으로 찾기 위한 것이다. 빈
        -- 섹션이 걸러지면 block_index 가 종목마다 다른 섹션을 가리킨다 --
        -- 주식관련사채 섹션이 비어 빠지는 채권과 그게 있는 채권이 다르다.
        JSON('{"blocks": "table:not(:has(table)):has(td:not(:empty))",
                "label": "th"}'),
        -- 두 층의 아래쪽. 라벨 125종 중 한 종목에 값이 있는 것은 50종 안팎이고
        -- 채권 종류마다 채워지는 칸이 달라, 타입 테이블로 두면 대부분 NULL 이
        -- 된다. 조회는 v_isin_bond_info (08_scrap_block_views.sql).
        JSON('{"detail": "api_rst"}'),
        JSON('["ISIN"]'), JSON('{"ISIN": "isin"}'), FALSE,
        -- header_json 을 비워 두는 것이 값에 영향을 준다. 실측:
        --   Accept-Language: ko  -> 발행금액 '20,000,000,000'
        --   그 헤더 없음         -> 발행금액 '20000000000'
        -- 쉼표 없는 쪽이 아래층에 낫다. 헤더를 추가하면 저장되는 값의 모양이
        -- 조용히 바뀐다.
        '표준코드 상세 팝업을 블록째로 보관한다. 파싱은 아래층 몫 -- html 은 '
        || 'JSON_VALUE(... RETURNING CLOB) 로 읽어야 한다(4000자 넘으면 '
        || 'VARCHAR2 는 조용히 NULL). header_json 을 비워 두면 숫자에 천 단위 '
        || '쉼표가 없다 -- Accept-Language 를 넣으면 생긴다');

-- ---------------------------------------------------------------------------
-- 빌더: 유형 분리 + 하루 몫
-- ---------------------------------------------------------------------------
-- **축이 mst_bond 인 것이 유형 분리다.** srchPopup3 은 채권 화면이고, 주식
-- ISIN 을 넣어도 HTTP 200 에 블록 4개가 저장된다 -- 값은 '채권분류 = ()' 와
-- '데이터가 없습니다.' 뿐이다. 0건이 아니므로 empty_is_pending 도 안 걸리고
-- record_count 4 로 성공 처리된다. 축을 유형으로 가르는 것이 그것을 막는
-- 유일하게 공짜인 방법이다 (CSS 로는 "값 있는 td 가 2개 이상" 을 쓸 수 없다).
--
-- 마스터 표가 이미 유형을 가른다 -- 실측으로 mst_bond / mst_stock / mst_etf
-- 사이 ISIN 겹침이 0이고, ISIN 3번째 글자도 갈린다(채권 1·2·3·6·B·C·Z,
-- 주식 0·7·8·G, ETF 7).
--
-- 하루 몫과 NOT EXISTS 는 KSD 채권 상세 이행 9개 빌더와 같은 모양이다.
-- NOT EXISTS 가 없으면 ROWNUM 이 매일 같은 앞 2,000개를 돌려주고 생성이 전부
-- 중복으로 걸러져 새 잡이 하나도 안 생긴다.
--
-- 2,000건/일 = 약 16분(실측 주기 470ms), 31,876건에 16일. 바꾸려면 ROWNUM 의
-- 수만 고친다 -- 800 이면 40일에 6분, 자르지 않으면 한 밤에 4.2시간이고 그것은
-- daily_batch2 의 다른 작업을 뒤로 밀어낸다.

MERGE INTO api_job_builder t
USING (SELECT 'ISIN_BOND_INFO_DAILY' build_id FROM dual) s
   ON (t.build_id = s.build_id)
 WHEN NOT MATCHED THEN INSERT (
        build_id, api_id, cartesian_param_json, is_active, save_mode,
        schedule_tag, note)
      VALUES (
        'ISIN_BOND_INFO_DAILY', 'ISIN_BOND_INFO',
        JSON(q'[{"ISIN": {"db": "SELECT isin FROM ( SELECT m.isin FROM mst_bond m WHERE m.isin IS NOT NULL AND NOT EXISTS (SELECT 1 FROM api_job j WHERE j.job_id = 'ISIN_BOND_INFO_' || m.isin) ORDER BY m.first_seen DESC, m.issu_dt DESC NULLS LAST) WHERE ROWNUM <= 2000"}}]'),
        TRUE,
        -- 발행 조건은 바뀌지 않으므로 한 번 받으면 끝이다. 성공한 one-shot
        -- 잡은 is_active=False 로 닫히고, 다음 생성은 안 받은 것만 집는다.
        'overwrite', 'daily_batch2',
        '유형 분리: mst_bond 만. 하루 2000건, 약 16분. 31,876건 전부에 16일');

COMMIT;

-- ===========================================================================
-- empty_keeps_previous -- 0행 성공이 지우지 않게 (2026-11-26)
-- ===========================================================================
-- save_mode='overwrite' 는 받기 전에 그 job_id 의 행을 지운다. 받은 것이
-- 0행이어도 그 삭제가 성공으로 커밋되므로, 대신 들어온 것 없이 옛것이
-- 사라진다. 실측으로 재현했다 -- PLUS_ETF_LIST 가 빈 응답을 받은 실행에서
-- 87행이 0행이 됐다.
--
-- _clear_previous_results 의 주석이 이미 원칙을 적고 있었다: 실패했을 때
-- 어제 행이 남는 것이 "overwrite 의 뜻 -- 교체되거나, 손대지 않거나". 그것은
-- 2026-10-02 에 KRX 포털이 공지 페이지를 돌려줘 27개 잡이 전부 실패하고
-- krx_deriv_info 23,000행이 비었던 뒤에 넣은 것이다. 실패는 막혔고 0행
-- 성공은 안 막혀 있었다.
--
-- empty_is_pending 으로는 덮이지 않는다 -- 그 판정이 not is_repeated 를
-- 요구하는데 아래 다섯은 전부 is_repeated 다(job_id 가 고정이라 매 실행이 같은
-- 행을 덮는다). 날짜가 job_id 에 있는 행은 새 날짜에 지울 것이 없어 애초에
-- 노출되지 않는다.
--
-- 전역 가드로 하지 않은 이유: 0행이 **진짜 답**인 행이 있다. KSD 채권 상세는
-- 상세가 없는 ISIN 에 빈 응답을 주고 그것을 ksd_bond_no_detail 이 기록한다.
-- 전역이면 그 행들에서 옛 데이터를 붙잡아 둔다. empty_is_pending 을 행 단위로
-- 둔 것과 같은 결이다.
--
-- 노출된 다섯 (is_repeated + save_mode=overwrite):
--   ACE/KODEX/PLUS/SOL_ETF_LIST  운용사 ETF 목록 (각 잡 1개)
--   KRX_DERIV_INFO               파생 전종목 기본정보 (잡 27개) -- 만기 달력과
--                                선물옵션 마스터가 이것으로 만들어진다.
--                                2026-10-02 에 비었던 그 표다.
--
-- JSON_TRANSFORM 으로 키만 더한다 -- 나중에 다른 스위치가 생겨도 덮지 않는다.

UPDATE api_mst
   SET options_json = JSON_TRANSFORM(NVL(options_json, JSON('{}')),
                                     SET '$.empty_keeps_previous' = TRUE)
 WHERE api_id IN ('ACE_ETF_LIST', 'KODEX_ETF_LIST', 'PLUS_ETF_LIST',
                  'SOL_ETF_LIST', 'KRX_DERIV_INFO');

COMMIT;

-- 확인
-- ---------------------------------------------------------------------------
-- 축이 채권만 돌려주나 (주식 0·7·8·G, ETF 7 이 섞이면 설정이 틀렸다):
--   SELECT SUBSTR(isin,3,1) ch, COUNT(*) FROM (<위 축 쿼리>) GROUP BY SUBSTR(isin,3,1);
--
-- 받은 것과 남은 것:
--   SELECT COUNT(*) done FROM api_job WHERE api_id = 'ISIN_BOND_INFO';
--   SELECT COUNT(*) left_ FROM mst_bond m
--    WHERE m.isin IS NOT NULL
--      AND NOT EXISTS (SELECT 1 FROM api_job j
--                       WHERE j.job_id = 'ISIN_BOND_INFO_' || m.isin);

-- ===========================================================================
-- KRX_ISIN_SRCH -- 코드부여일 창 + 과거 채우기 (2026-11-26)
-- ===========================================================================
-- 이 화면이 KSD 에 없는 것을 갖고 있다: **상환·만기된 코드까지** 있다
-- (실측 샘플의 list_yn 이 '상장폐지'다). KSD(ksd_bond_isin)는 살아 있는 종목만
-- 이므로, 과거 ISIN 을 찾으려면 여기가 유일한 경로다.
--
-- 창은 필수다. 날짜를 비우면 0건이고, 지금까지 0건이 아니었던 이유는 페이지가
-- 열릴 때 dateSetting() 이 최근 1년을 기본값으로 넣고 폼이 그것을 같이 보내기
-- 때문이다 -- 이 행은 **조용히 '최근 1년' 만 긁고 있었다**(KR2 로 271건).
--
-- 실측으로 정한 것들:
--
--   * 필드는 std_cd_grnt_start_dd / std_cd_grnt_end_dd 이고 **코드부여일**이다.
--     화면의 searchDateRadio(발행일/만기일/상장일/코드부여일)는 결과에 영향이
--     없다 -- 2010년으로 세 변형 모두 229건이었다. 다운로드 버튼만 가른다.
--
--   * **형식은 YYYY-MM-DD 여야 한다.** 입력란의 onblur Chk_Date2 가 10자리를
--     요구하고, 8자리를 넣으면 경고창 두 개로 거절한다 -- "입력한 '코드부여
--     시작일' 은(는) 10자리가 아닙니다", 이어서 "3년 이내의 기간을 입력하셔야
--     합니다"(날짜가 비워졌으므로). 서버는 8자리도 받지만 화면은 아니다.
--
--   * **3년을 넘으면 결과가 아니라 알림 페이지가 온다.** 1095일은 793건으로
--     되고 1096일은 안 된다. 그때 응답은 HTTP 200 에 #tbody 가 없는
--     var msg = '3년 이내의 기간을 입력하셔야 합니다.' 다. 1년 창이라 안 닿는다.
--
--   * **pageSize 는 화면에 없는 파라미터인데 서버가 받는다.** 5행 -> 1000행,
--     200배다. pageIndex 와 같이 정확히 돈다(1·2·3페이지 각 1000행, 연속,
--     중복 없음). 비문서 파라미터라 어느 날 안 먹으면 페이지 수가 200배로
--     뛰고 max_pages 에서 **조용히 잘린다** -- 엔진이 천장에 닿은 것을 자연
--     종료와 구분하지 않는다. 그 경보는 아직 없다.
--
--   * 2011 부터다. 더 과거도 조회된다 -- 달력 위젯의 yearRange 가 "2011:+30"
--     이라 연도 드롭다운만 2011부터이고 minDate 는 없다. KR6 는 1996년부터
--     있고 1993~1995 는 0건이다. 필요할 때 CONNECT BY 의 수만 늘리면 된다.
--
-- 접두사는 3번째 글자이고 17개가 있다. 채권이 아닌 셋을 뺀 14개와 2011~2026
-- 건수(실측): KR6 446,883 · KR3 23,328 · KR2 4,110 · KRD 3,098 · KR5 1,518 ·
-- KR1 963 · KRG 921 · KRX 478 · KRC 254 · KRB 90 · KRF 38 · KR8 29 · KR0 10 ·
-- KR9 10 = 481,730. 뺀 셋: KR4 선물옵션 · KR7 주식 · KRA ELW.
--
-- 실측 처리량: KR2 2011 이 237행 6.6초(2페이지), KR6 2011 이 20,701행
-- 82.5초(22페이지). 전체 약 32분이다. 엔진의 extract 를 한 번의 evaluate 로
-- 바꾸기 전에는 같은 일이 19시간이었다.

UPDATE api_mst
   SET behavior_json   = JSON('[{"action":"fill","selector":"#isur_nm1","value":":CODE"},{"action":"fill","selector":"#std_cd_grnt_start_dd","value":":GRNT_FROM"},{"action":"fill","selector":"#std_cd_grnt_end_dd","value":":GRNT_TO"},{"action":"eval","script":"$(''form[name=JLDINF20000]'').append(\"<input type=''hidden'' name=''pageIndex'' value='':PAGE''>\").append(\"<input type=''hidden'' name=''pageSize'' value=''1000''>\"); fn_search(2);","expect_navigation":true,"wait_until":"domcontentloaded"}]'),
       pagination_json = JSON('{"mode":"page","param":"PAGE","start":1,"max_pages":150}'),
       job_key_params  = JSON('["CODE","GRNT_FROM","GRNT_TO"]'),
       note            = '표준코드 목록. 코드부여일 창(:GRNT_FROM~:GRNT_TO, YYYYMMDD)이 필수다 -- 비우면 0건, 3년 넘으면 알림 페이지가 와서 엔진에는 0건으로 보인다. searchDateRadio 는 결과에 영향 없음(다운로드 버튼 전용). 한 페이지 5행'
 WHERE api_id = 'KRX_ISIN_SRCH';

-- 과거 채우기 빌더. 접두사 14 x 16년 = 224 잡을 한 번에 만든다.
--
-- **세 파라미터가 한 축이다.** 콤마 키는 한 쿼리의 컬럼들을 짝으로 유지한다 --
-- 따로 축으로 두면 14 x 16 x 16 조합이 되고 창의 시작과 끝이 다른 해가 된다.
--
-- schedule_tag 가 'backfill' 이고 **그 태그를 쓰는 타이머가 없다.** 생성도
-- 실행도 손으로만 된다 (generate-jobs/run-cycle 둘 다 태그 한정이다):
--   python -m app.cli generate-jobs backfill
--   python -m app.cli run-cycle    backfill
--
-- NOT EXISTS 는 남겨 둔다 -- 다시 생성해도 있는 것은 건너뛰므로 멱등하고,
-- 중간에 멈춘 뒤 이어 받을 때 그대로 쓴다. ROWNUM 을 줄이면 조금씩 받는다.
UPDATE api_job_builder
   SET cartesian_param_json = JSON('{"CODE,GRNT_FROM,GRNT_TO":{"db":"SELECT code, grnt_from, grnt_to FROM ( SELECT p.code, y.yr || ''-01-01'' AS grnt_from, y.yr || ''-12-31'' AS grnt_to FROM (SELECT COLUMN_VALUE AS code FROM TABLE(sys.odcivarchar2list(''KR0'',''KR1'',''KR2'',''KR3'',''KR5'',''KR6'',''KR8'',''KR9'',''KRB'',''KRC'',''KRD'',''KRF'',''KRG'',''KRX''))) p CROSS JOIN (SELECT TO_CHAR(2010 + LEVEL) AS yr FROM dual CONNECT BY LEVEL <= 16) y WHERE NOT EXISTS (SELECT 1 FROM api_job j WHERE j.job_id = ''KRX_ISIN_SRCH_'' || p.code || ''_'' || y.yr || ''-01-01_'' || y.yr || ''-12-31'') ORDER BY y.yr, p.code ) WHERE ROWNUM <= 400"}}'),
       note = '과거 표준코드. 접두사 14 x 2011~2026 = 224 잡을 한 번에 생성. KR4 선물옵션/KR7 주식/KRA ELW 제외. 타이머 없음 -- 손으로 돌린다'
 WHERE build_id = 'KRX_ISIN_SRCH_HIST';

COMMIT;
