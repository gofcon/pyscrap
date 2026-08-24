CREATE OR REPLACE FUNCTION fn_export_day_col (p_name IN VARCHAR2) RETURN VARCHAR2 AS
-- 결과 테이블에서 '거래일' 이 어느 컬럼인가. 이름이 테이블마다 달라 적어 둔다.
--
-- 내보내기 프로시저가 둘(날짜별 sp_export_parquet, 통짜 sp_export_bulk)이고
-- 둘 다 기본 쿼리를 조립할 때 이 값이 필요하다. 양쪽에 같은 CASE 를 두면
-- 새 테이블을 추가할 때 한쪽만 고치게 되고, 그러면 같은 대상이 한 방식으로는
-- 되고 다른 방식으로는 -20001 로 죽는다.
--
-- 새 결과 테이블을 기본 경로로 내보내려면 여기 한 줄을 더한다. 매핑이 없는
-- 대상(뷰, 조인, 집계)은 애초에 p_query 로 부르면 되므로 여기 올 일이 없다.
  v_col VARCHAR2(60);
BEGIN
  v_col := CASE LOWER(p_name)
             WHEN 'kis_futopt_chart' THEN 'stck_bsop_date'
             WHEN 'kis_futopt_daily' THEN 'stck_bsop_date'
             WHEN 'kis_index_daily'  THEN 'stck_bsop_date'
             WHEN 'kis_futopt_price' THEN 'SUBSTR(trade_at, 1, 8)'
           END;
  IF v_col IS NULL THEN
    raise_application_error(-20001,
      'no date column mapped for ' || p_name || '; pass p_query instead');
  END IF;
  RETURN v_col;
END;
