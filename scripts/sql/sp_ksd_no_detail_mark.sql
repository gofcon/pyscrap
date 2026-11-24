CREATE OR REPLACE PROCEDURE sp_ksd_no_detail_mark (p_marked OUT NUMBER) AS
-- 세이브로가 상세를 주지 않는 ISIN 을 ksd_bond_no_detail 에 적는다. 한 종목당
-- 한 번만 적히고, 그 뒤로는 ksd_bond_* 빌더 열 곳이 그 종목을 후보에서 뺀다.
--
-- 왜 필요하냐면: 빌더는 밤마다 "mst_bond 에 있는데 ksd_bond_info 에 없는"
-- 채권을 first_seen 최신순 800개 뽑는다. 그런데 빈 응답도 정당한 답이다 --
-- 사모 발행이나 세이브로가 더는 상세를 주지 않는 옛 종목이 그렇다. 빈 응답은
-- 행을 남기지 않으니 그 ISIN 은 창에 영원히 남고, 잡은 이미 있어서 다시 돌지도
-- 않는다. 2026-10-15 에 800칸 중 402칸이 그런 ISIN 이었고, 열 타입 모두 밤마다
-- 800 이 아니라 398 건씩 받고 있었다. 창 맨 앞이 막혀 있어서다(정렬이
-- first_seen DESC 라 최근에 물어본 것이 앞에 온다).
--
-- 표여야 하는 이유: 근거는 "INFO 잡이 0행으로 끝났다" 인데 그 로그는
-- sp_purge_job_logs 가 지운다. 잡 위에 올린 뷰는 그때 기억을 잃고 전부 다시 묻기
-- 시작한다. 그래서 지워지지 않는 곳에 한 번 적어 둔다. mst_bond 의 칼럼이
-- 아닌 이유는 그 표가 종목 마스터이고, 우리가 긁는 데 성공했는지는 종목의
-- 성질이 아니기 때문이다.
--
-- 판정은 실행 로그 한 줄이다: 그 잡이 0행으로 성공한 기록이 있는가.
--
-- v1 에서는 api_job.description 을 문자열로 훑어야 했다
-- (LIKE 'ksd_bond_info: 0%'). 적재 건수가 거기 문자열로 들어갔기 때문인데,
-- 그 칸은 사람이 적는 설명과 같은 자리였고 다음 실행이 덮어썼다. 그래서
-- is_active = 0 을 함께 봐야 했다 -- 손으로 비활성화한 잡(KB_ETF_PDF 때처럼)이
-- 섞이지 않게 하려고.
--
-- v2 는 api_job_log.record_count 가 숫자로 있다. 0행으로 성공한 기록이 있다는
-- 것 자체가 근거이고, 손으로 비활성화한 잡에는 그런 기록이 없다. is_active 를
-- 볼 이유가 없어진다. status 를 둘로 받는 이유는 엔진이 0행을 SUCCESS 로
-- 남길지 EMPTY 로 남길지에 이 프로시저가 걸리지 않게 하려는 것이다.
--
-- 다시 묻고 싶으면(세이브로가 뒤늦게 공시한 경우) 그 행을 지우면 된다. 다음
-- 밤 창에 다시 들어온다.
--
-- sp_run_daily_batch2 에서, 잡을 생성하는 사이클 앞에 부른다. 멱등이다 --
-- 이미 적힌 ISIN 은 건너뛴다.
BEGIN
  INSERT INTO ksd_bond_no_detail (isin, marked_at, note)
  SELECT /*+ NO_PARALLEL */ m.isin, SYSDATE, 'KSD_BOND_INFO_web closed with 0 rows'
    FROM mst_bond m
   WHERE NOT EXISTS (SELECT 1 FROM ksd_bond_info b WHERE b.isin = m.isin)
     AND NOT EXISTS (SELECT 1 FROM ksd_bond_no_detail n WHERE n.isin = m.isin)
     AND EXISTS (SELECT 1 FROM api_job_log l
                  WHERE l.job_id = 'KSD_BOND_INFO_web_' || m.isin
                    AND l.status IN ('SUCCESS', 'EMPTY')
                    AND l.record_count = 0);
  p_marked := SQL%ROWCOUNT;

  DBMS_OUTPUT.PUT_LINE('  ISIN(s) with no SEIBRO detail: ' || p_marked);
END;
