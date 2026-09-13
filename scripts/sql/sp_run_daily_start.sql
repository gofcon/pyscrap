CREATE OR REPLACE PROCEDURE sp_run_daily_start AS
-- daily_start 사이클(08:35, 원천 재적재) 뒤에 도는 DB 쪽 단계, 순서대로.
-- sp_run_export 와 같은 모양이다: 배치는 "언제" 만 알고, "무엇을 어떤 순서로" 는
-- 이 파일이 위에서 아래로 말한다. 단계를 끼우거나 옮기는 것은 여기 한 줄이고,
-- 인스턴스는 그걸 모른다 -- 서비스 파일은 call-proc 으로 이 이름 하나를 부른다.
--
-- 순서가 실제 의존이다: sp_mst_stock_sync 의 MERGE 는 krx_stock_base 가 접혀
-- 있지 않으면(같은 종목이 날짜별로 여러 행) ORA-30926 으로 죽는다.
--
-- 각 단계는 자기 건수를 DBMS_OUTPUT 으로 남긴다. 합계를 돌려주지 않는 이유는
-- sp_run_export 와 같다 -- 하나가 0건인 것을 합계는 감춘다.
--
-- 실패하면 그 자리에서 멈춘다. 뒤 단계가 앞 단계를 전제하므로, 예외를 삼키고
-- 계속 돌면 배치는 성공으로 끝나고 빠진 단계는 아무도 모르게 된다. 고친 뒤 다시
-- 부르면 이미 된 단계는 0건으로 지나가고 못 된 것이 채워진다 -- 전부 멱등이다.
  n NUMBER;
BEGIN
  sp_krx_stock_base_dedup(n);
  DBMS_OUTPUT.PUT_LINE('sp_krx_stock_base_dedup: ' || n || ' duplicate row(s) removed');
  sp_mst_stock_sync(n);
  DBMS_OUTPUT.PUT_LINE('sp_mst_stock_sync: ' || n || ' row(s) merged');
END;
