CREATE OR REPLACE PROCEDURE sp_retire_expired_jobs (p_retired OUT NUMBER) AS
-- 만기가 지난 계약의 반복 잡을 비활성화한다.
--
-- 3분 스냅샷 잡은 계약마다 하나씩 있고 반복 잡이라 스스로 끝나지 않는다. 잡
-- 생성은 새 계약을 더할 뿐 만기된 것을 빼지 않으므로, 빼는 손이 없으면 만기
-- 지난 계약을 매 틱 계속 묻는다 -- KIS 는 그런 코드에 0 을 돌려주고, 잡은
-- SUCCESS 로 끝나며, 그 0 은 표에 쌓인다. 2026-09-14 만기 위클리 142개가 그
-- 뒤 사흘을 그렇게 돌았다(하루 2만 3천 콜).
--
-- 기준은 mst_fuopt.mat_date < 오늘(KST). 만기일 당일은 마지막 거래일이라
-- 남긴다. 반복 잡(3m_*)만 다룬다: 날짜가 박힌 일회성 잡은 한 번 돌면 스스로
-- 비활성이 되고, 만기 뒤 남은 일회성 잡은 어제치를 아직 못 받은 것이라 두어야
-- 한다. 잡 생성 앞에 둔다(sp_run_generate_3m) -- 생성은 이미 있는 잡을 건너뛰므로
-- 뒤에 두면 방금 만든 잡을 잘못 볼 일은 없지만, 앞에 두면 그날의 잡 목록이 생성
-- 직후 완성된 상태가 된다.
BEGIN
  UPDATE /*+ NO_PARALLEL */ api_job j
     SET j.is_active = 0
   WHERE j.execution_cycle IN ('3m_call', '3m_put')
     AND j.is_active = 1
     AND EXISTS (SELECT 1
                   FROM mst_fuopt f
                  WHERE f.kis_short_cd = JSON_VALUE(j.params_json, '$.SHORT_CODE')
                    AND f.mat_date < TRUNC(CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Seoul' AS DATE)));
  p_retired := SQL%ROWCOUNT;
END;
