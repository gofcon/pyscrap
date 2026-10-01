CREATE OR REPLACE PROCEDURE sp_run_daily_early AS
-- daily_early(07:00) 의 DB 쪽 단계, 순서대로. sp_run_export 와 같은 모양이다:
-- 배치는 "언제" 만 알고, "무엇을 어떤 순서로" 는 이 파일이 위에서 아래로 말한다.
-- 단계를 끼우거나 옮기는 것은 여기 한 줄이고, 인스턴스는 그걸 모른다.
--
-- 이 유닛의 원칙은 '그날 일간 자료를 받기 전에 끝나 있어야 하는 것' 이다
-- (pyscrap-daily-early.service 참고). 아래 셋 다 그날 수집을 기다리지 않는다:
-- 접는 것은 어제까지 받아 둔 것이고, 달력과 마스터는 어제 목록으로도 는다.
--
--   1. 만기 지난 잡 퇴역. 맨 앞이다 -- 잡 생성(이 프로시저 다음 단계)은 더하기만
--      하고 빼지 않으므로, 빼는 손이 먼저 지나가야 그날 잡 목록이 완성된다.
--   2. 주식 기본정보를 종목당 한 행으로 접고 정제 마스터로 옮긴다. 순서가 실제
--      의존이다: 접지 않은 상태로 MERGE 하면 소스 키가 중복돼 ORA-30926 으로
--      죽는다. 둘 다 데이터 안의 bas_dd 만 보고 시계는 보지 않으며, 그날 수집분은
--      다음 날 아침에 접힌다 -- 상장 다음 날 마스터에 나타난다는 뜻이고, 그 하루는
--      받아들인 값이다.
--   3. 만기 달력을 늘린 뒤 선물옵션 마스터를 늘린다. 순서가 실제 의존이다:
--      sp_mst_fuopt_sync 의 마지막 MERGE 가 만기 없는 종목의 만기를
--      meta_maturity 에서 메우므로 달력이 먼저 늘어나 있어야 한다. 이 둘이 3분 잡
--      생성 바로 앞인 것도 의존이다 -- 생성 쿼리가 mst_fuopt.mat_date 로 최근월을
--      고른다. 예전에는 수집(08:35)과 이 단계(08:38)가 다른 타이머였고, 그 3분
--      간격은 앞 유닛이 45~51초에 끝난다는 관측에 기댄 것이었다. 관측은 약속이
--      아니라서 같은 유닛 안으로 옮겼다.
--
-- 각 단계는 자기 건수를 DBMS_OUTPUT 으로 남긴다. 합계를 돌려주지 않는 이유는
-- sp_run_export 와 같다 -- 하나가 0건인 것을 합계는 감춘다. 실패하면 그 자리에서
-- 멈추고, 전부 멱등이라 고친 뒤 다시 부르면 된 단계는 0건으로 지나간다.
  n NUMBER;
BEGIN
  sp_retire_expired_jobs(n);
  DBMS_OUTPUT.PUT_LINE('sp_retire_expired_jobs: ' || n || ' job(s) retired');
  sp_krx_stock_base_dedup(n);
  DBMS_OUTPUT.PUT_LINE('sp_krx_stock_base_dedup: ' || n || ' duplicate row(s) removed');
  sp_mst_stock_sync(n);
  DBMS_OUTPUT.PUT_LINE('sp_mst_stock_sync: ' || n || ' row(s) merged');
  sp_meta_maturity_sync(n);
  DBMS_OUTPUT.PUT_LINE('sp_meta_maturity_sync: ' || n || ' row(s) merged');
  sp_mst_fuopt_sync(n);
  DBMS_OUTPUT.PUT_LINE('sp_mst_fuopt_sync: ' || n || ' row(s) merged');
END;
