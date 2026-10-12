CREATE OR REPLACE PROCEDURE sp_retire_expired_jobs (p_retired OUT NUMBER) AS
-- 더 돌 이유가 없어진 잡을 비활성화한다. 네 종류.
--
-- 1. 만기가 지난 계약의 3분 스냅샷 잡. 반복 잡이라 스스로 끝나지 않고, 잡
--    생성은 새 계약을 더할 뿐 만기된 것을 빼지 않으므로, 빼는 손이 없으면
--    만기 지난 계약을 매 틱 계속 묻는다 -- KIS 는 그런 코드에 0 을 돌려주고,
--    잡은 SUCCESS 로 끝나며, 그 0 은 표에 쌓인다. 2026-09-14 만기 위클리
--    142개가 그 뒤 사흘을 그렇게 돌았다(하루 2만 3천 콜). 기준은
--    mst_fuopt.mat_date < 오늘(KST): 만기일 당일은 마지막 거래일이라 남긴다.
--
-- 2. 날짜가 오래된 KRX ETF 구성종목 잡. 종목×날짜로 만들어지는 일회성 잡인데
--    실패하면 활성으로 남아 다음 밤에 다시 나간다. 그 자체는 맞는 동작이지만
--    KRX 데이터마켓은 요청이 몰리면 에러페이지로 막고, 밀린 날짜가 새 날짜와
--    함께 나가면 그 몰림을 스스로 만든다 -- 2026-09-17 에 활성 4,730개 중
--    3,558개가 09-01 부터 밀린 것이었다. 구성종목은 매일 조금씩 바뀌는 값이라
--    2주 지난 날짜를 뒤늦게 받는 값어치도 낮다. 7일(≈5영업일)보다 오래된 것은
--    놓는다: 그날 것을 못 받은 채로 두는 것과, 그것 때문에 오늘 것까지 못 받는
--    것 사이의 선택이다.
--
-- 3. 자료가 끝내 안 온 KRX 오픈API 일별 잡. 그 API 들은 아직 안 낸 날을 0행으로
--    답하고, 그런 행(api_mst.response_parse_json 의 empty_is_pending)은 0행이면
--    잡을 닫지 않고 다음 사이클에 다시 묻는다 (app.services.execution.run_job).
--    휴장일은 영영 0행이라 그 잡은 스스로 끝나지 않는다 -- 7일 지나면 놓는다.
--    기준 날짜는 잡의 BASDD 다.
--
-- 4. 소스가 죽은 api 의 잡 전부. 지난 7일 동안 성공이 한 번도 없고 서로 다른
--    사흘 이상 실패한 api 는 사이트가 사라진 것으로 본다 -- KB 는 2026-10-03
--    연휴에 ETF 구성종목 사이트를 다른 도메인으로 옮겼고, 옛 호스트는 TCP 연결이
--    안 되는 곳으로 302 를 돌려줘 잡마다 3분씩 타임아웃했다. 성공하지 못한 잡은
--    은퇴하지 않고(그게 실패한 일회성 잡이 내일 다시 나가는 이유다) 빌더가 매일
--    142개를 더해서, 10-12 밤 사이클은 852개를 들고 출발해 다섯 시간을 쓰고
--    성공 둘로 끝났다. 뒤에 줄 서 있던 KSD 채권 이행과 다른 운용사 구성종목은
--    아예 돌지 않았다.
--
--    판정 단위가 잡이 아니라 api 인 이유: 사이클 안의 차단기
--    (app.services.execution._FailStreak)가 연속 실패 다섯 번이면 그 api 를
--    접으므로, 한 밤에 시도되는 잡은 다섯 개뿐이다. "일주일 시도했는데 못
--    했으면 놓는다" 를 잡 단위로 쓰면 시도 자체가 안 일어나 852개가 빠지는 데
--    반년이 걸린다. api 가 죽었으면 아직 한 번도 못 나간 잡도 같이 놓는다.
--
--    사흘을 요구하는 것은 하룻밤 장애와 구분하기 위해서다. 차단기가 한 밤에
--    실패 다섯 개를 만들므로 "실패 n건" 으로는 하루 만에 걸린다. 반복 잡
--    (key_params_list 에 NOW -- 3분 스냅샷)은 제외한다: 스스로 끝나지 않는 것이
--    설계라 1번 규칙이 만기로 거둔다.
--
--    되살릴 때는 api_mst 의 URL 을 고친 뒤 빌더가 그날 잡을 다시 만든다. 지난
--    날짜분은 다시 생성되지 않는데, 구성종목은 지난 날짜를 뒤늦게 받는 값어치가
--    낮아 2번 규칙도 같은 선택을 하고 있다.
--
-- 잡 생성 앞(sp_run_daily_early)과 daily_batch2 앞(sp_run_daily_batch2)에서
-- 부른다. 생성은 이미 있는 잡을 건너뛰므로 앞에 두면 그날의 잡 목록이 생성
-- 직후 완성된 상태가 된다. 두 UPDATE 는 서로 무관하고 각각 멱등이다.
  n1 NUMBER;
  n2 NUMBER;
  n3 NUMBER;
  n4 NUMBER;
BEGIN
  UPDATE /*+ NO_PARALLEL */ api_job j
     SET j.is_active = 0
   WHERE j.execution_cycle IN ('3m_call', '3m_put')
     AND j.is_active = 1
     AND EXISTS (SELECT 1
                   FROM mst_fuopt f
                  WHERE f.kis_short_cd = JSON_VALUE(j.params_json, '$.SHORT_CODE')
                    AND f.mat_date < TRUNC(CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Seoul' AS DATE)));
  n1 := SQL%ROWCOUNT;

  UPDATE /*+ NO_PARALLEL */ api_job j
     SET j.is_active = 0
   WHERE j.api_id = 'KRX_ETF_PDF'
     AND j.is_active = 1
     AND JSON_VALUE(j.params_json, '$.trdDd')
         < TO_CHAR(TRUNC(CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Seoul' AS DATE)) - 7, 'YYYYMMDD');
  n2 := SQL%ROWCOUNT;

  UPDATE /*+ NO_PARALLEL */ api_job j
     SET j.is_active = 0
   WHERE j.is_active = 1
     AND j.api_id IN (SELECT api_id FROM api_mst
                       WHERE JSON_VALUE(response_parse_json, '$.empty_is_pending') = 'true')
     AND JSON_VALUE(j.params_json, '$.BASDD')
         < TO_CHAR(TRUNC(CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Seoul' AS DATE)) - 7, 'YYYYMMDD');
  n3 := SQL%ROWCOUNT;

  UPDATE /*+ NO_PARALLEL */ api_job j
     SET j.is_active = 0
   WHERE j.is_active = 1
     AND j.api_id IN (
           SELECT w.api_id
             FROM (SELECT j2.api_id, l2.status, l2.executed_at
                     FROM api_job_log l2
                     JOIN api_job j2 ON j2.job_id = l2.job_id
                    WHERE l2.executed_at >= SYSDATE - 7) w
            GROUP BY w.api_id
           HAVING SUM(CASE WHEN w.status = 'SUCCESS' THEN 1 ELSE 0 END) = 0
              AND COUNT(DISTINCT TRUNC(w.executed_at)) >= 3)
     -- 반복 잡은 제외 (위 설명). app.services.job_builder.is_repeated_api 와
     -- 같은 판정이고, 11개 api 에 대해 양쪽이 일치하는 것을 확인했다.
     AND j.api_id NOT IN (SELECT m.api_id FROM api_mst m
                           WHERE JSON_EXISTS(m.key_params_list, '$[*]?(@ == "NOW")')
                              OR JSON_EXISTS(m.key_params_list, '$[*]."NOW"'));
  n4 := SQL%ROWCOUNT;

  DBMS_OUTPUT.PUT_LINE('  expired-contract 3m jobs: ' || n1 || ', stale KRX_ETF_PDF jobs: ' || n2
                       || ', never-published KRX daily jobs: ' || n3
                       || ', jobs of dead sources: ' || n4);
  p_retired := n1 + n2 + n3 + n4;
END;
