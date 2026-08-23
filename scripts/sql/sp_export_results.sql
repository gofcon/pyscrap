CREATE OR REPLACE PROCEDURE sp_export_results (
  p_from  IN  VARCHAR2 DEFAULT NULL,   -- 시작 영업일 (YYYYMMDD, 기본: KST 오늘)
  p_to    IN  VARCHAR2 DEFAULT NULL,   -- 종료 영업일 (기본: p_from 과 같은 날)
  p_rows  OUT NUMBER                   -- 내보낸 행 수 합계
) AS
-- 하루치 수집 결과를 통째로 Parquet 로 내보낸다. sp_export_parquet 를 대상
-- 테이블마다 한 번씩 부르는 것이 전부이고, 배치가 알아야 할 것을 "어느
-- 테이블을 어떤 순서로" 에서 "언제" 로 줄이는 것이 목적이다. 대상이 늘면
-- 배치 스크립트가 아니라 아래 목록만 고치면 된다.
--
-- 목록을 질의로 뽑지 않고 적어 둔 이유: job_id 가 있는 테이블을 고르는
-- 파이썬 쪽 규칙을 그대로 옮기면 api_job, api_job_log, api_rst,
-- fo_idx_code_mst 까지 걸린다. 그것들은 수집 부기와 원본 마스터라 '거래일'
-- 이라는 게 없고, sp_export_parquet 의 날짜 컬럼 매핑도 없다. 무엇을 남길
-- 값어치가 있는가는 스키마에서 읽어낼 수 있는 성질이 아니라서 판단을 적어
-- 둔다.
--
-- 순서는 수집 주기가 짧은 것부터다. 실패하면 그 자리에서 멈추므로, 앞의
-- 것이 먼저 나가 있는 편이 낫다. 멈춰도 되는 이유는 sp_export_parquet 가
-- 프리픽스를 비우고 다시 쓰기 때문 -- 고친 뒤 같은 날짜로 다시 부르면 이미
-- 나간 테이블은 같은 결과로 덮이고, 못 나간 것이 채워진다. 예외를 삼키고
-- 계속 돌면 배치는 성공으로 끝나고 빠진 테이블은 아무도 모르게 된다.
  TYPE t_targets IS VARRAY(8) OF VARCHAR2(30);
  c_targets CONSTANT t_targets := t_targets(
    'kis_futopt_price',   -- 3분 스냅샷
    'kis_futopt_chart',   -- 1분봉
    'kis_futopt_daily',   -- 일봉
    'kis_index_daily');   -- 지수 일봉
  -- 기본 날짜는 KST 기준. DB 세션 시각(UTC)으로 잡으면 장 마감 후 도는 배치가
  -- 자정을 넘긴 뒤엔 전날을, 넘기기 전엔 당일을 골라 같은 배치가 날마다 다른
  -- 날짜를 내보낸다. 수집 시각도 KST 로 찍으므로(app.services.job_builder)
  -- 여기서도 시장의 하루를 쓴다.
  v_from  VARCHAR2(8) := NVL(p_from, TO_CHAR(SYSTIMESTAMP AT TIME ZONE 'Asia/Seoul', 'YYYYMMDD'));
  v_rows  NUMBER;
BEGIN
  p_rows := 0;
  FOR i IN 1 .. c_targets.COUNT LOOP
    sp_export_parquet(p_name => c_targets(i),
                      p_from => v_from,
                      p_to   => p_to,
                      p_rows => v_rows);
    p_rows := p_rows + v_rows;
    -- 합계만 돌려주면 "네 테이블 중 하나가 0건" 을 알 수 없다. 3분 주기가
    -- 죽은 날이 정확히 그 모양이라, 배치 로그에 테이블별로 남긴다.
    DBMS_OUTPUT.PUT_LINE(RPAD(c_targets(i), 20) || TO_CHAR(v_rows, 'FM999,999,999') || ' rows');
  END LOOP;
END;
