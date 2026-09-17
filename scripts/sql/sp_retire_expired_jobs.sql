CREATE OR REPLACE PROCEDURE sp_retire_expired_jobs (p_retired OUT NUMBER) AS
-- 더 돌 이유가 없어진 잡을 비활성화한다. 두 종류.
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
-- 잡 생성 앞(sp_run_generate_3m)과 daily_batch2 앞(sp_run_daily_batch2)에서
-- 부른다. 생성은 이미 있는 잡을 건너뛰므로 앞에 두면 그날의 잡 목록이 생성
-- 직후 완성된 상태가 된다. 두 UPDATE 는 서로 무관하고 각각 멱등이다.
  n1 NUMBER;
  n2 NUMBER;
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

  DBMS_OUTPUT.PUT_LINE('  expired-contract 3m jobs: ' || n1 || ', stale KRX_ETF_PDF jobs: ' || n2);
  p_retired := n1 + n2;
END;
