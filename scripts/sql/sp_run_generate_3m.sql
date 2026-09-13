CREATE OR REPLACE PROCEDURE sp_run_generate_3m AS
-- 3분 잡 생성(08:38) 앞에 도는 DB 쪽 단계, 순서대로 -- sp_run_daily_start 참고.
--
-- 왜 daily_start 의 오케스트레이터가 아니라 여기인가: 원천(krx_deriv_info)은
-- daily_start 가 가져오지만, 이 둘이 끝나 있어야 하는 시점은 잡 생성 직전이다.
-- 두 타이머는 3분 차이고 daily_start 는 보통 1분 안에 끝나지만, "보통" 에 잡
-- 생성을 걸 수는 없다. 생성 바로 앞에 두면 순서가 시계가 아니라 코드로 보장된다.
--
-- 순서가 실제 의존이다: sp_mst_fuopt_sync 의 마지막 MERGE 가 만기 없는 종목의
-- 만기를 meta_maturity 에서 메우므로, 달력이 먼저 늘어나 있어야 한다.
  n NUMBER;
BEGIN
  sp_meta_maturity_sync(n);
  DBMS_OUTPUT.PUT_LINE('sp_meta_maturity_sync: ' || n || ' row(s) merged');
  sp_mst_fuopt_sync(n);
  DBMS_OUTPUT.PUT_LINE('sp_mst_fuopt_sync: ' || n || ' row(s) merged');
END;
