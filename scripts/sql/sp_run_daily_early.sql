CREATE OR REPLACE PROCEDURE sp_run_daily_early AS
-- daily_early(08:38) 의 DB 쪽 단계, 순서대로 -- sp_run_daily_start 참고.
--
-- 이름이 sp_run_generate_3m 이었다. 하는 일이 3분 잡 생성만이 아니라서 바꿨다:
-- 만기 지난 잡을 물리고 달력과 마스터를 늘리는 것이 먼저고, 생성은 이 다음
-- 단계(서비스 파일의 run_cycle generate-only)다.
--
-- 왜 daily_start 의 오케스트레이터가 아니라 여기인가: 원천(krx_deriv_info)은
-- daily_start 가 가져오지만, 이 둘이 끝나 있어야 하는 시점은 잡 생성 직전이다.
-- 생성 바로 앞에 두면 그 순서가 시계가 아니라 코드로 보장된다 -- 타이머 간격
-- (08:35 -> 08:38)은 daily_start 가 45~51초에 끝난다는 관측에 기대는 것이고,
-- 관측은 약속이 아니다.
--
-- 순서가 실제 의존이다: sp_mst_fuopt_sync 의 마지막 MERGE 가 만기 없는 종목의
-- 만기를 meta_maturity 에서 메우므로, 달력이 먼저 늘어나 있어야 한다. 만기 지난
-- 잡을 물리는 것은 맨 앞이다 -- 잡 생성(이 다음)은 더하기만 하지 빼지 않는다.
  n NUMBER;
BEGIN
  sp_retire_expired_jobs(n);
  DBMS_OUTPUT.PUT_LINE('sp_retire_expired_jobs: ' || n || ' job(s) retired');
  sp_meta_maturity_sync(n);
  DBMS_OUTPUT.PUT_LINE('sp_meta_maturity_sync: ' || n || ' row(s) merged');
  sp_mst_fuopt_sync(n);
  DBMS_OUTPUT.PUT_LINE('sp_mst_fuopt_sync: ' || n || ' row(s) merged');
END;
