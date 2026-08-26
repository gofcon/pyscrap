CREATE OR REPLACE PROCEDURE sp_purge_jobs (
  p_days    IN  NUMBER DEFAULT 90,   -- 이 일수보다 오래된 것만
  p_deleted OUT NUMBER               -- 지운 잡 수
) AS
-- 끝난 잡과 그 실행 로그를 나이로 정리한다.
--
-- 왜 쌓이냐면: 이 API 들은 한 요청에 한 종목·하루만 주므로 잡이 종목×날짜로
-- 생긴다. 1분봉과 일봉이 각각 하루 1,503 개, 3분 스냅샷의 잡 생성까지 더하면
-- 하루 3,000 개 남짓이 늘고 연 75 만 개가 된다. 잡 자체를 줄이는 길도 있지만
-- (실행 시점에 날짜를 URL 에 넣는 방식) 그러면 잡 하나가 한 종목의 여러 날을
-- 담게 되어, 하루치만 정확히 다시 받는 지금의 재실행 성질을 잃는다. 그 성질을
-- 지키고 부기만 치우는 쪽을 택했다.
--
-- 지워도 되는 이유: 결과는 결과 테이블에 있고, 잡 행은 '무엇을 언제 요청했나'
-- 라는 부기다. 그 기록의 값어치는 몇 달이면 다한다.
--
-- 활성 잡은 절대 건드리지 않는다. 활성이라는 것은 아직 안 돌았거나(대기),
-- 실패해서 다음 틱에 재시도될 잡이거나, 반복 잡이라는 뜻이다 -- 셋 다 지우면
-- 수집이 멈춘다.
--
-- 나이는 마지막 실행 시각으로 재고, 실행된 적이 없으면 잡이 마지막으로 손댄
-- 시각으로 잰다. 후자가 없으면 판단할 근거가 없으므로 남긴다.
  v_cutoff DATE := SYSDATE - p_days;
BEGIN
  DELETE FROM api_job_log l
   WHERE l.job_id IN (
     SELECT j.job_id FROM api_job j
      WHERE j.is_active = 0
        AND NVL((SELECT MAX(x.executed_at) FROM api_job_log x WHERE x.job_id = j.job_id),
                j.updated_at) < v_cutoff);

  DELETE FROM api_job j
   WHERE j.is_active = 0
     AND NVL((SELECT MAX(x.executed_at) FROM api_job_log x WHERE x.job_id = j.job_id),
             j.updated_at) < v_cutoff;

  p_deleted := SQL%ROWCOUNT;
  DBMS_OUTPUT.PUT_LINE('purged ' || TO_CHAR(p_deleted, 'FM999,999,999')
                       || ' job(s) last run before ' || TO_CHAR(v_cutoff, 'YYYY-MM-DD'));
END;
