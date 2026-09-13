CREATE OR REPLACE PROCEDURE sp_run_daily_batch2 AS
-- daily_batch2 사이클(21:00) 앞에 도는 DB 쪽 단계, 순서대로 -- sp_run_daily_start 참고.
--
-- 둘 다 daily_batch1 이 재적재한 목록(ksd_bond_isin, krx_etf_daily)을 정제
-- 마스터로 접는다. daily_batch2 의 빌더가 그 마스터에서 종목을 뽑으므로 사이클
-- 앞에 있어야 하고, 둘 사이엔 의존이 없다 -- 채권이 먼저인 것은 그냥 순서다.
  n NUMBER;
BEGIN
  sp_mst_bond_sync(n);
  DBMS_OUTPUT.PUT_LINE('sp_mst_bond_sync: ' || n || ' new issue(s)');
  sp_mst_etf_sync(n);
  DBMS_OUTPUT.PUT_LINE('sp_mst_etf_sync: ' || n || ' row(s) merged');
END;
