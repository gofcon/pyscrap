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
-- 나이는 잡 행이 마지막으로 손댄 시각(updated_at)으로 싸게 1차 거르고, 그걸
-- 통과한 잡만 로그를 본다. 잡이 돌면 run_job 이 description 을 써서 updated_at
-- 이 갱신되므로 최근 실행분은 1차에서 떨어진다. 그 갱신이 일어나지 않은 경우를
-- 위해 2차로 "cutoff 이후 로그가 없다" 를 확인한다 -- 둘을 합치면 이전의
-- "마지막 실행 시각" 조건과 같은 것을 고르면서 읽는 양이 전혀 다르다.
--
-- 2026-10-20 에 이 프로시저를 고쳐 쓴 이유 (실측):
--   이전 조건은 잡마다 MAX(executed_at) 을 다시 구하는 상관 서브쿼리였고,
--   옵티마이저가 api_job(58.8만) × api_job_log(576만) 해시 조인 뒤 행마다
--   그 서브쿼리를 돌리는 계획을 골랐다. 게다가 **병렬 DML(PX)** 이었다.
--   병렬 DML 은 대상 테이블에 배타 TM 락을 건다. 그래서 10-19 밤 22:30 에
--   이 문장이 뜨자 돌고 있던 daily_batch2 의 INSERT INTO api_job_log 가
--   막혀 버렸고, 70분 뒤 purge 가 끝날 때까지(23:40:14) 잡 하나도 완료하지
--   못한 채 23:47:58 에 죽었다. 그날 지운 잡은 0 건이다. 10-16 밤도 같다.
--   그래서 NO_PARALLEL 이 이 프로시저에서 성능이 아니라 **정확성** 문제다:
--   병렬이면 같은 표를 쓰는 수집 사이클을 멈춘다.
--
-- 삭제는 5만 건씩 끊어 커밋한다. 지금은 지울 게 0 건이지만(가장 오래된 잡
-- 실행이 64일 전), 첫 실제 purge 는 잡 수십만·로그 수백만 행이 된다 --
-- 한 트랜잭션으로 지우면 undo 가 터진다(ORA-30036, 두 번 겪었다).
  v_cutoff  DATE := SYSDATE - p_days;
  c_batch   CONSTANT PLS_INTEGER := 50000;

  CURSOR c_old IS
    SELECT /*+ NO_PARALLEL */ j.job_id
      FROM api_job j
     WHERE j.is_active = 0
       AND j.updated_at < v_cutoff
       AND NOT EXISTS (SELECT /*+ NO_PARALLEL */ 1
                         FROM api_job_log x
                        WHERE x.job_id = j.job_id
                          AND x.executed_at >= v_cutoff);

  TYPE t_ids IS TABLE OF api_job.job_id%TYPE;
  v_ids t_ids;
BEGIN
  p_deleted := 0;
  OPEN c_old;
  LOOP
    FETCH c_old BULK COLLECT INTO v_ids LIMIT c_batch;
    EXIT WHEN v_ids.COUNT = 0;

    FORALL i IN 1 .. v_ids.COUNT
      DELETE /*+ NO_PARALLEL */ FROM api_job_log l WHERE l.job_id = v_ids(i);
    FORALL i IN 1 .. v_ids.COUNT
      DELETE /*+ NO_PARALLEL */ FROM api_job j WHERE j.job_id = v_ids(i);

    p_deleted := p_deleted + v_ids.COUNT;
    COMMIT;
  END LOOP;
  CLOSE c_old;

  DBMS_OUTPUT.PUT_LINE('purged ' || TO_CHAR(p_deleted, 'FM999,999,999')
                       || ' job(s) last run before ' || TO_CHAR(v_cutoff, 'YYYY-MM-DD'));
END;
