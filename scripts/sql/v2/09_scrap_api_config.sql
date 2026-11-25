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
