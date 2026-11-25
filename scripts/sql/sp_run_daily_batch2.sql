CREATE OR REPLACE PROCEDURE sp_run_daily_batch2 AS
-- daily_batch2 사이클(21:00) 앞에 도는 DB 쪽 단계, 순서대로 -- sp_run_daily_early 참고.
--
-- 둘 다 daily_batch1 이 재적재한 목록(ksd_bond_isin, krx_etf_daily)을 정제
-- 마스터로 접는다. daily_batch2 의 빌더가 그 마스터에서 종목을 뽑으므로 사이클
-- 앞에 있어야 하고, 둘 사이엔 의존이 없다 -- 채권이 먼저인 것은 그냥 순서다.
-- 그 앞에서 밀린 잡을 놓는다(sp_retire_expired_jobs): KRX 구성종목 잡이 새
-- 날짜와 함께 나가기 전에 7일 넘은 것을 빼야 한다.
--
-- 마지막은 sp_ksd_no_detail_mark 다. 채권 상세 빌더가 후보를 뽑기 전에,
-- 세이브로가 상세를 주지 않는 ISIN 을 적어 둬야 그 종목이 그날 창을 먹지
-- 않는다. mst_bond 가 접힌 뒤라야 새로 들어온 채권까지 함께 본다.
  n NUMBER;
BEGIN
  sp_retire_expired_jobs(n);
  DBMS_OUTPUT.PUT_LINE('sp_retire_expired_jobs: ' || n || ' job(s) retired');
  sp_mst_bond_sync(n);
  DBMS_OUTPUT.PUT_LINE('sp_mst_bond_sync: ' || n || ' new issue(s)');
  sp_mst_etf_sync(n);
  DBMS_OUTPUT.PUT_LINE('sp_mst_etf_sync: ' || n || ' row(s) merged');
  sp_ksd_no_detail_mark(n);
  DBMS_OUTPUT.PUT_LINE('sp_ksd_no_detail_mark: ' || n || ' ISIN(s) marked');

  -- 원천 코드 수확 (v2). 이 프로시저는 사이클 **앞**에 도므로, 수확되는 것은
  -- 직전 사이클까지 들어온 값이다 -- 어휘가 하루 늦는다. 코드 값은 드물게
  -- 새로 나타나므로 그 지연은 값을 치르지 않는다. 사이클 뒤로 옮기려면
  -- systemd 유닛에 ExecStart 한 줄이 더 필요하고, 그건 배포 작업이다.
  --
  -- 감싸는 이유: 수확은 보조적인 일이고 이 유닛은 Type=oneshot 이라 첫 실패에서
  -- 멈춘다. 감싸지 않으면 수확이 깨질 때 그 밤의 수집이 아예 돌지 않는다.
  -- 프로시저 안에도 컬럼 단위 예외 처리가 있지만 그건 컬럼 하나가 깨질 때의
  -- 이야기이고, 여기 블록은 그보다 바깥(접속·권한·표 없음)을 막는다.
  BEGIN
    sp_src_code_sync(p_rows => n);
    DBMS_OUTPUT.PUT_LINE('sp_src_code_sync: ' || n || ' value(s)');
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('sp_src_code_sync: skipped -- ' || SQLERRM);
  END;
END;
