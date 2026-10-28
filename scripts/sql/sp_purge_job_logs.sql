CREATE OR REPLACE PROCEDURE sp_purge_job_logs (
  p_ok_days   IN  NUMBER DEFAULT 30,    -- 성공 로그 보존 일수
  p_fail_days IN  NUMBER DEFAULT 365,   -- 실패 로그 보존 일수
  p_deleted   OUT NUMBER                -- 지운 로그 행 수
) AS
-- api_job_log 을 나이로 정리한다. 성공은 짧게, 실패는 길게 둔다.
--
-- 왜 sp_purge_jobs 와 따로 필요하냐면: 그쪽은 잡을 지우면서 그 잡의 로그를
-- 함께 지우고, 조건이 is_active = 0 AND updated_at < 90일이다. 그래서 두 가지가
-- 손에 닿지 않는다.
--   1) 활성 잡의 로그. 반복 잡(KIS_ETF_PDF, KRX_DERIV_INFO 등)은 영원히 활성이라
--      그 로그는 영원히 안 지워진다. 2026-10-29 기준 110만 행(전체의 16%)이다.
--   2) 아직 잡이 90일을 못 넘긴 로그. 엔진이 2026-08-31 에 떴으니 그 시점까지는
--      sp_purge_jobs 가 한 건도 지우지 않는다(첫 발동 2026-11 말).
-- 로그는 잡보다 훨씬 빨리 는다 -- 영업일당 12.4만 행, 월 390 MB -- 그러니 로그의
-- 보존을 잡의 보존에서 떼어낸다.
--
-- 지워도 되는 이유: 결과는 결과 테이블에 있고 로그 행은 '언제 무엇을 요청했고
-- 어떻게 끝났나' 라는 부기다. 분석 DB 로도 나가지 않는다 --
-- app.services.export._BOOKKEEPING_TABLES 가 api_job_log 를 외부표·parquet
-- 대상에서 빼므로 하류 의존이 없다.
--
-- ** p_ok_days 의 하한은 7일이 아니라, 7일에 여유를 둔 값이어야 한다 **
-- sp_retire_expired_jobs 의 규칙 4(죽은 소스 은퇴)가 최근 7일 로그에 성공이
-- 하나도 없는 api 의 잡을 전부 비활성화한다. 성공 로그를 그보다 짧게 두면
-- 멀쩡히 돌고 있는 소스가 죽은 것으로 판정되어 수집이 끊긴다. KB_ETF_PDF 를
-- 막으려고 넣은 장치가 거꾸로 작동하는 셈이다. 기본값 30일은 그 창의 4배이고,
-- 아래에서 7일 미만을 아예 거부한다.
--
-- 실패를 길게 두는 쪽: 실패 행은 전체의 1.5%(10만 행 ≈ 16 MB)뿐인데 사후 추적
-- 가치는 이쪽에 있다. 싸게 오래 두는 편이 낫다.
--
-- 왜 커서 + BULK COLLECT 가 아니라 ROWNUM 으로 끊은 DELETE 냐면: 커서를 열어 둔
-- 채 중간 커밋을 하면(fetch across commit) 커서는 OPEN 시점 스냅샷을 계속 읽으므로
-- 긴 루프에서 ORA-01555 가 난다. 첫 실행은 400만 행짜리라 그 위험이 작지 않다.
-- 매 반복이 독립된 문장이면 그 위험이 없다. 값은 전체 스캔을 반복한다는 것인데
-- (executed_at 에 인덱스가 없다) 평상시 하루치는 한 배치로 끝나고, 400만 행을
-- 지우는 첫 실행만 수십 번 스캔한다 -- 그건 배치 시간 밖에서 한 번 치르면 된다.
--
-- NO_PARALLEL 은 성능이 아니라 정확성이다. 병렬 DML 은 대상 테이블에 배타 TM 락을
-- 걸고, 그러면 돌고 있는 사이클의 INSERT INTO api_job_log 가 막힌다 -- 10-19 밤
-- daily_batch2 를 죽인 그 원인이다(sp_purge_jobs 주석 참고). 그래서 이 프로시저는
-- 밤 사이클과 겹치지 않는 자리(03:00 finalize-exports)에서 돌려야 한다.
  c_batch      CONSTANT PLS_INTEGER := 200000;
  c_min_ok     CONSTANT PLS_INTEGER := 7;   -- 규칙 4 의 창
  v_ok_cutoff   DATE;
  v_fail_cutoff DATE;
  v_n           PLS_INTEGER;
BEGIN
  IF p_ok_days < c_min_ok THEN
    RAISE_APPLICATION_ERROR(-20001,
      'p_ok_days=' || p_ok_days || ' 는 너무 짧다: sp_retire_expired_jobs 규칙 4 가'
      || ' 최근 ' || c_min_ok || '일 로그로 죽은 소스를 판정하므로, 성공 로그를 그보다'
      || ' 짧게 두면 멀쩡한 소스가 은퇴당한다.');
  END IF;

  v_ok_cutoff   := SYSDATE - p_ok_days;
  v_fail_cutoff := SYSDATE - p_fail_days;
  p_deleted     := 0;

  LOOP
    -- status 가 SUCCESS 가 아닌 것(실패, 혹시 모를 NULL)은 모두 긴 쪽을 쓴다.
    DELETE /*+ NO_PARALLEL */ FROM api_job_log l
     WHERE l.executed_at < DECODE(l.status, 'SUCCESS', v_ok_cutoff, v_fail_cutoff)
       AND ROWNUM <= c_batch;
    v_n := SQL%ROWCOUNT;
    EXIT WHEN v_n = 0;
    p_deleted := p_deleted + v_n;
    COMMIT;
  END LOOP;

  DBMS_OUTPUT.PUT_LINE('  sp_purge_job_logs: purged ' || TO_CHAR(p_deleted, 'FM999,999,999')
                       || ' log row(s) -- success before ' || TO_CHAR(v_ok_cutoff, 'YYYY-MM-DD')
                       || ', failure before ' || TO_CHAR(v_fail_cutoff, 'YYYY-MM-DD'));
END;
