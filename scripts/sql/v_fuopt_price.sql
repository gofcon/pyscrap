-- 선물옵션 시세에서 거래가 있던 행만. 내보내기 대상이다 (sp_run_export).
--
-- 저장소에 없던 파일이다. v1 에만 만들어져 있었고 DDL 이 git 에 들어온 적이
-- 없어서, v2 를 세울 때 아무도 만들지 않았다 -- sp_run_export 가 p_query 로
-- 이 이름을 부르는데 동적 SQL 이라 프로시저는 VALID 이고 실행 시점에
-- ORA-00942 로 죽는다. 그 상태로 v2 의 내보내기가 멈춰 있었다.
--
-- v1 의 정의를 그대로 옮긴 것이고, 컬럼 목록을 펼치지 않고 * 로 둔 것만
-- 다르다. v1 쪽은 CREATE VIEW 가 컬럼을 전개해 저장해서 한 줄이 아주 길었다
-- -- 그 형태는 kis_futopt_price 에 컬럼이 더해질 때 뷰가 따라가지 않는다
-- (ALTER 없이 새 컬럼이 안 보인다). * 로 두면 뷰를 다시 컴파일할 때 따라온다.
--
-- futs_prpr <> 0 이 하는 일: 호가만 있고 체결이 없는 종목을 뺀다. 선물옵션은
-- 상장 종목 수가 많고 그중 상당수가 하루 종일 거래가 없는데, 그 행까지
-- 내보내면 Parquet 이 0 으로 채워진 행으로 불어난다.
--
-- NULL 은 이 조건에서 탈락한다(NULL <> 0 은 UNKNOWN). 그것이 v1 의 동작이고
-- 그대로 둔다 -- 가격이 NULL 인 행은 체결이 없는 것과 같은 취급이다.

CREATE OR REPLACE VIEW v_fuopt_price AS
SELECT *
  FROM kis_futopt_price
 WHERE futs_prpr <> 0;

COMMENT ON TABLE v_fuopt_price IS
  '선물옵션 시세 중 체결이 있던 행 (futs_prpr <> 0). sp_run_export 가 내보낸다';
