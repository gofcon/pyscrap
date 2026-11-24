-- SCRAP <-> REF 교차 권한과 synonym
--
-- 계정 이름을 파일에 적지 않는다 (이 저장소는 공개다). SQLcl / SQL Developer
-- 에서 아래 두 줄을 채우고 나머지를 그대로 돌린다.
--
--   DEFINE scrap_user = ...
--   DEFINE ref_user   = ...
--
-- 치환 변수를 못 쓰는 환경이면 &scrap_user / &ref_user 를 sed 로 바꿔 돌린다.
--
-- ---------------------------------------------------------------------------
-- 왜 synonym 인가
-- ---------------------------------------------------------------------------
-- 없으면 모델에 스키마가 박힌다:
--
--   class MstEtf(SQLModel, table=True):
--       __table_args__ = {"schema": "ref"}     <- 11개 모델에 붙는다
--
-- 그리고 더 나쁜 것은 **DB 안의 설정 문자열까지** 계정 이름을 알게 되는
-- 것이다 -- 빌더 매크로가 'SELECT amc_etf_cd FROM ref.mst_etf ...' 가 된다.
-- synonym 을 두면 모델도, 매크로 SQL 도, 프로시저 본문도 손대지 않는다.
--
-- private 이다. public 으로 걸면 MART 계정까지 같은 이름을 보게 되는데,
-- 분석 쪽은 같은 이름이 외부 테이블(xt_*)을 가리켜야 한다.
--
-- 주의: 순서가 중요하다. GRANT 가 synonym 보다 먼저다. synonym 만 있고 권한이
-- 없으면 ORA-00980(번역 실패)이 아니라 ORA-01031 이 나서 원인이 덜 보인다.
--
-- 주의: DDL 은 synonym 을 통과하지 않는다. CREATE/ALTER TABLE 은 접속한 계정에
-- 적용되므로, REF 표의 DDL 은 REF 로 접속해서 해야 한다 (05_ref_tables.sql).

-- ===========================================================================
-- 1. REF 로 접속해서 실행
-- ===========================================================================

-- 1-1. 사전 데이터를 SCRAP 에 읽기로 연다.
--
-- 실제로 지금 읽는 것은 넷뿐이다 (빌더 매크로 실측):
--   mst_fuopt           빌더 43개
--   mst_etf             빌더 11개
--   mst_bond            빌더 10개
-- (ksd_bond_no_detail 도 빌더 10개가 읽지만 그 표는 SCRAP 소유다 -- 수집
--  상태이고 사전 데이터가 아니다. 02_scrap_results.sql 에 있다.)
-- 그래도 10개 전부 연다. 전부 "수집 쪽이 읽도록 만든" 사전 데이터이고,
-- 읽기라 위험이 같다. 넷만 열면 새 매크로를 쓸 때마다 ORA-00942 를 만나고
-- 그때 GRANT 를 한 줄 더하게 되는데, 그 한 줄이 왜 필요한지는 그 순간에만
-- 분명하다. 지켜야 할 것은 **쓰기가 없다**는 것이고 그것은 유지된다.
GRANT SELECT ON krx_index_mst      TO &scrap_user;
GRANT SELECT ON meta_fuopt_info    TO &scrap_user;
GRANT SELECT ON meta_maturity      TO &scrap_user;
GRANT SELECT ON mst_bond           TO &scrap_user;
GRANT SELECT ON mst_etf            TO &scrap_user;
GRANT SELECT ON mst_fuopt          TO &scrap_user;
GRANT SELECT ON mst_index          TO &scrap_user;
GRANT SELECT ON mst_stock          TO &scrap_user;
GRANT SELECT ON stock_index_his    TO &scrap_user;
GRANT SELECT ON user_etf           TO &scrap_user;

-- 1-2. 동기화 프로시저를 SCRAP 이 부를 수 있게 한다.
--
-- sp_ksd_no_detail_mark 는 여기 없다. 그것이 쓰는 ksd_bond_no_detail 이 수집
-- 상태 표라 SCRAP 소유이고, 읽는 ksd_bond_info 와 api_job_log 도 SCRAP 이다.
-- 그래서 프로시저째로 SCRAP 에 두었고, 그 덕에 교차 권한이 둘 줄었다 --
-- 계정 경계는 "누가 쓰는가" 로 긋는 것이 읽는 쪽으로 긋는 것보다 적게 샌다.
--
-- CLI 의 sync-mst-* / sync-meta-maturity / sync-index-his 는 SCRAP 으로
-- 접속한 채 이것들을 부른다. 프로시저는 **정의자 권한**(기본값)으로 두어야
-- 한다 -- 그러면 본문이 REF 의 권한으로 돌아서 REF 표에 쓰고, SCRAP 은
-- 부르는 것만 할 수 있다. 사전 데이터에 쓰는 권한이 SCRAP 에 가지 않는 것이
-- 계정을 나눈 이유 중 하나다.
--
-- (sp_create_external_tables 만 AUTHID CURRENT_USER 다. 그쪽은 부른 사람의
--  스키마에 외부 테이블을 만드는 것이 맞는 동작이라서다.)
GRANT EXECUTE ON sp_mst_bond_sync        TO &scrap_user;
GRANT EXECUTE ON sp_mst_etf_sync         TO &scrap_user;
GRANT EXECUTE ON sp_mst_fuopt_sync       TO &scrap_user;
GRANT EXECUTE ON sp_mst_stock_sync       TO &scrap_user;
GRANT EXECUTE ON sp_meta_maturity_sync   TO &scrap_user;
GRANT EXECUTE ON sp_stock_index_his_sync TO &scrap_user;

-- 1-3. 동기화 프로시저가 읽는 SCRAP 객체에 synonym 을 건다.
--      (1-4 로 SCRAP 쪽 GRANT 를 받은 뒤에 쓸 수 있다.)
--
-- 어느 프로시저가 무엇을 읽는지:
--   sp_mst_bond_sync          ksd_bond_isin
--   sp_mst_etf_sync           krx_etf_daily, krx_etf_list,
--                             v_ace_etf, v_kodex_etf, v_plus_etf, v_sol_etf
--   sp_mst_fuopt_sync         fo_idx_code_mst, krx_deriv_info
--   sp_meta_maturity_sync     fo_idx_code_mst, krx_deriv_info
--   sp_mst_stock_sync         krx_stock_base
--   sp_stock_index_his_sync   kis_index_daily, krx_index_daily
CREATE SYNONYM fo_idx_code_mst FOR &scrap_user..fo_idx_code_mst;
CREATE SYNONYM kis_index_daily FOR &scrap_user..kis_index_daily;
CREATE SYNONYM krx_deriv_info  FOR &scrap_user..krx_deriv_info;
CREATE SYNONYM krx_etf_daily   FOR &scrap_user..krx_etf_daily;
CREATE SYNONYM krx_etf_list    FOR &scrap_user..krx_etf_list;
CREATE SYNONYM krx_index_daily FOR &scrap_user..krx_index_daily;
CREATE SYNONYM krx_stock_base  FOR &scrap_user..krx_stock_base;
CREATE SYNONYM ksd_bond_isin   FOR &scrap_user..ksd_bond_isin;

-- 아래층으로 내린 운용사 목록 넷. sp_mst_etf_sync 가 ace_etf / kodex_etf /
-- plus_etf / sol_etf 를 읽고 있었는데, v2 에서 그것들은 api_rst 위의 뷰다
-- (04_scrap_views.sql). 프로시저 본문의 참조도 v_ 로 바꿔야 한다.
CREATE SYNONYM v_ace_etf       FOR &scrap_user..v_ace_etf;
CREATE SYNONYM v_kodex_etf     FOR &scrap_user..v_kodex_etf;
CREATE SYNONYM v_plus_etf      FOR &scrap_user..v_plus_etf;
CREATE SYNONYM v_sol_etf       FOR &scrap_user..v_sol_etf;

-- ===========================================================================
-- 2. SCRAP 으로 접속해서 실행
-- ===========================================================================

-- 2-1. 동기화 프로시저가 읽을 것만 REF 에 연다.
--
-- 결과 테이블 34개를 통째로 열지 않는다. 사전 데이터를 만드는 데 쓰이는
-- 것만이고, 그 목록이 곧 "어느 수집물이 마스터의 재료인가" 라는 문서가 된다.
GRANT SELECT ON fo_idx_code_mst TO &ref_user;
GRANT SELECT ON kis_index_daily TO &ref_user;
GRANT SELECT ON krx_deriv_info  TO &ref_user;
GRANT SELECT ON krx_etf_daily   TO &ref_user;
GRANT SELECT ON krx_etf_list    TO &ref_user;
GRANT SELECT ON krx_index_daily TO &ref_user;
GRANT SELECT ON krx_stock_base  TO &ref_user;
GRANT SELECT ON ksd_bond_isin   TO &ref_user;

-- 뷰에 주는 SELECT 다. SCRAP 이 api_rst 와 뷰를 모두 소유하므로 그대로 된다.
GRANT SELECT ON v_ace_etf       TO &ref_user;
GRANT SELECT ON v_kodex_etf     TO &ref_user;
GRANT SELECT ON v_plus_etf      TO &ref_user;
GRANT SELECT ON v_sol_etf       TO &ref_user;

-- 2-2. 사전 데이터에 synonym 을 건다. 이것이 있어서 모델의 __tablename__ 과
--      빌더 매크로 SQL 이 v1 그대로 동작한다.
CREATE SYNONYM krx_index_mst      FOR &ref_user..krx_index_mst;
CREATE SYNONYM meta_fuopt_info    FOR &ref_user..meta_fuopt_info;
CREATE SYNONYM meta_maturity      FOR &ref_user..meta_maturity;
CREATE SYNONYM mst_bond           FOR &ref_user..mst_bond;
CREATE SYNONYM mst_etf            FOR &ref_user..mst_etf;
CREATE SYNONYM mst_fuopt          FOR &ref_user..mst_fuopt;
CREATE SYNONYM mst_index          FOR &ref_user..mst_index;
CREATE SYNONYM mst_stock          FOR &ref_user..mst_stock;
CREATE SYNONYM stock_index_his    FOR &ref_user..stock_index_his;
CREATE SYNONYM user_etf           FOR &ref_user..user_etf;

-- 2-3. 동기화 프로시저 synonym. CLI 가 sp_mst_etf_sync 를 이름 그대로 부른다.
CREATE SYNONYM sp_mst_bond_sync        FOR &ref_user..sp_mst_bond_sync;
CREATE SYNONYM sp_mst_etf_sync         FOR &ref_user..sp_mst_etf_sync;
CREATE SYNONYM sp_mst_fuopt_sync       FOR &ref_user..sp_mst_fuopt_sync;
CREATE SYNONYM sp_mst_stock_sync       FOR &ref_user..sp_mst_stock_sync;
CREATE SYNONYM sp_meta_maturity_sync   FOR &ref_user..sp_meta_maturity_sync;
CREATE SYNONYM sp_stock_index_his_sync FOR &ref_user..sp_stock_index_his_sync;

-- ===========================================================================
-- 3. 점검 (양쪽에서 각각 돌린다)
-- ===========================================================================
-- synonym 이 있는데 권한이 없는 경우를 잡는다. 그 조합이 ORA-01031 로
-- 나타나는데, 그 메시지만 보고는 권한 문제인지 알기 어렵다. 여기서 미리
-- 전부 한 번 읽어 보고 깨지는 것을 이름으로 알려 준다.
SET SERVEROUTPUT ON
DECLARE
  n NUMBER;
  bad NUMBER := 0;
BEGIN
  FOR s IN (SELECT synonym_name FROM user_synonyms ORDER BY synonym_name) LOOP
    BEGIN
      EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || s.synonym_name || ' WHERE ROWNUM <= 1'
        INTO n;
      DBMS_OUTPUT.PUT_LINE(RPAD(s.synonym_name, 26) || 'ok');
    EXCEPTION
      WHEN OTHERS THEN
        -- 프로시저 synonym 은 SELECT 가 안 되는 것이 정상이다.
        IF SQLCODE = -942 OR SQLCODE = -1031 THEN
          bad := bad + 1;
          DBMS_OUTPUT.PUT_LINE(RPAD(s.synonym_name, 26) || 'FAIL  ' || SQLERRM);
        END IF;
    END;
  END LOOP;
  DBMS_OUTPUT.PUT_LINE('--');
  DBMS_OUTPUT.PUT_LINE(CASE WHEN bad = 0 THEN 'synonym 전부 읽힘'
                            ELSE bad || ' 개가 권한 없음 -- 위 GRANT 를 다시 확인' END);
END;
/
