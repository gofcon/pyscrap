CREATE OR REPLACE PROCEDURE sp_run_export (
  p_to    IN  VARCHAR2 DEFAULT NULL,   -- 마지막 영업일 (YYYYMMDD, 기본: KST 오늘)
  p_from  IN  VARCHAR2 DEFAULT NULL    -- 시작 영업일 (포함, 기본: p_to 와 같은 날)
) AS
-- 하루치를 통째로 Parquet 로 내보낸다. 아래 호출 목록이 이 프로시저의
-- 전부이고, 대상이 늘면 같은 형식으로 한 줄을 더하면 된다. 배치가 알아야 할
-- 것을 "어느 테이블을 어떤 순서로" 에서 "언제" 로 줄이는 것이 목적이다.
--
-- 목록을 테이블 이름 배열로 두지 않은 이유: 내보낼 것이 테이블만은 아니다.
-- 뷰나 조인, 집계처럼 SELECT 로만 표현되는 대상은 p_query 로 넘겨야 하는데,
-- 이름 배열에는 그게 들어갈 자리가 없다. 호출을 그대로 나열하면 두 종류가
-- 같은 모양으로 한 줄씩 놓인다.
--
-- 무엇을 여기 넣을지의 기준은 하루 발생량이다. 장중 내내 쌓이는 것만
-- 여기 두고, 하루에 몇 행뿐인 마스터성·요약성 자료는 분석 쪽에서 DB 링크로
-- 바로 읽는다. 파일 하나에 몇 KB 를 담자고 외부 테이블을 걸어 둘 값어치가
-- 없고, 늘어나는 건 갱신 순서를 챙길 자리뿐이다.
--   남김: kis_futopt_chart(분봉), kis_futopt_price/price1(3분 스냅샷),
--         kis_futopt_daily(일봉)
--   뺌:   kis_index_daily(하루 2행), v_k2i_atm(하루 1행)
--
-- 목록을 질의로 뽑지 않은 이유: job_id 가 있는 테이블을 고르는 파이썬 쪽
-- 규칙을 그대로 옮기면 api_job, api_job_log, api_rst, fo_idx_code_mst 까지
-- 걸린다. 그것들은 수집 부기와 원본 마스터라 '거래일' 이라는 게 없다.
-- 무엇을 남길 값어치가 있는가는 스키마에서 읽어낼 수 있는 성질이 아니라서
-- 판단을 적어 둔다.
--
-- 합계를 돌려주지 않는다. 대상마다 몇 건인지는 sp_export_parquet 가 직접
-- 한 줄씩 남기므로(배치 로그에 그대로 나온다), 여기서 다시 더하려면 호출
-- 아래마다 누적 한 줄이 따라붙어야 하고 그 줄은 언젠가 빠진다. 합계 하나는
-- 어차피 "다섯 중 하나가 0건" 을 감춘다.
--
-- 순서는 수집 주기가 짧은 것부터다. 실패하면 그 자리에서 멈추므로, 앞의
-- 것이 먼저 나가 있는 편이 낫다. 멈춰도 되는 이유는 sp_export_parquet 가
-- 프리픽스를 비우고 다시 쓰기 때문 -- 고친 뒤 같은 날짜로 다시 부르면 이미
-- 나간 대상은 같은 결과로 덮이고, 못 나간 것이 채워진다. 예외를 삼키고
-- 계속 돌면 배치는 성공으로 끝나고 빠진 대상은 아무도 모르게 된다.

  -- 날짜는 손대지 않고 그대로 넘긴다. 기본값(끝날은 KST 오늘, 시작일은 끝날과
  -- 같은 날)은 sp_export_parquet 이 정하므로, 여기서 미리 풀면 같은 규칙이 두
  -- 군데 적히고 언젠가 한쪽만 고쳐진다.
  n  NUMBER;
BEGIN
  -- 결과 테이블: 날짜 컬럼이 sp_export_parquet 에 매핑돼 있어 이름만 주면 된다.
  sp_export_parquet(p_name => 'kis_futopt_price', p_to => p_to, p_from => p_from, p_rows => n);
  sp_export_parquet(p_name => 'kis_futopt_chart', p_to => p_to, p_from => p_from, p_rows => n);
  sp_export_parquet(p_name => 'kis_futopt_daily', p_to => p_to, p_from => p_from, p_rows => n);
  sp_export_parquet(p_name => 'krx_opt_daily', p_to => p_to, p_from => p_from, p_rows => n);
  sp_export_parquet(p_name => 'krx_fut_daily', p_to => p_to, p_from => p_from, p_rows => n);
  sp_export_parquet(p_name => 'krx_etf_daily', p_to => p_to, p_from => p_from, p_rows => n);

  -- ETF 구성종목. 하루 한 번(daily_batch2) 이라 위의 것들보다 뒤에 둔다.
  -- 목록 테이블(ace_etf, sol_etf, plus_etf, kodex_etf)은 여기 없다 -- overwrite
  -- 라 총량이 곧 현재 한 벌이고 하루치라는 게 없다. 위의 기준 그대로다.
  sp_export_parquet(p_name => 'kis_etf', p_to => p_to, p_from => p_from, p_rows => n);
  sp_export_parquet(p_name => 'kis_etf_pdf', p_to => p_to, p_from => p_from, p_rows => n);
  sp_export_parquet(p_name => 'amc_etf_pdf', p_to => p_to, p_from => p_from, p_rows => n);
  sp_export_parquet(p_name => 'ace_etf_pdf', p_to => p_to, p_from => p_from, p_rows => n);
  sp_export_parquet(p_name => 'plus_etf_pdf', p_to => p_to, p_from => p_from, p_rows => n);
  sp_export_parquet(p_name => 'sol_etf_pdf', p_to => p_to, p_from => p_from, p_rows => n);
  sp_export_parquet(p_name => 'kodex_etf_pdf', p_to => p_to, p_from => p_from, p_rows => n);

  -- 뷰/질의: p_name 은 버킷 프리픽스 이름으로만 쓰이고, 소스는 p_query 다.
  -- :DAY 로 그날을 좁혀야 한다 -- 프리픽스가 날짜별이라 다른 날이 섞이면
  -- 경로와 내용이 어긋나고, 재실행 시 프리픽스 단위 정리도 어긋난다.
  -- trade_at 은 VARCHAR2('YYYYMMDDHH24MISS') 라 SUBSTR 로 자른 값도 문자열이다.
  -- :DAY 는 따옴표까지 포함해 치환되므로 그대로 비교하면 되고, TO_DATE 로
  -- 감싸면 왼쪽 문자열이 NLS_DATE_FORMAT 으로 변환되다 ORA-01861 로 죽는다.
  sp_export_parquet(p_name  => 'kis_futopt_price1',
                    p_to    => p_to,
                    p_from  => p_from,
                    p_query => 'SELECT * FROM v_fuopt_price WHERE SUBSTR(trade_at, 1, 8) = :DAY',
                    p_rows  => n);

END;
