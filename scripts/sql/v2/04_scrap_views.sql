-- SCRAP 계정: api_rst 위의 뷰 8개
--
-- gen_ddl.py 가 app/db/models.py 에서 뽑은 것이다. 손으로 고치지 말고
-- 모델을 고친 뒤 다시 뽑는다.
--   python scripts/sql/v2/gen_ddl.py


-- 컬럼 목록은 내려보낸 모델에서 뽑았다. 이 뷰를 조회하는 질의는
-- 타입 테이블을 조회하던 것과 같다 -- 이름도 타입도 바뀌지 않는다.
-- 나중에 타입 테이블로 승격해도 뷰 이름을 유지하면 질의는 그대로다.

-- ace_etf  (9컬럼)  ACE_ETF_LIST (한국투자신탁운용 ``papi.aceetf.co.kr/api/funds``) -- ACE ETF
CREATE OR REPLACE VIEW v_ace_etf AS
SELECT api_id, job_id,
       JSON_VALUE(result_json, '$.fundcd' RETURNING VARCHAR2(20 CHAR)) AS fundcd,
       JSON_VALUE(result_json, '$.stockcd' RETURNING VARCHAR2(12 CHAR)) AS stockcd,
       JSON_VALUE(result_json, '$.fundnm' RETURNING VARCHAR2(200 CHAR)) AS fundnm,
       JSON_VALUE(result_json, '$.fundwhlnm' RETURNING VARCHAR2(300 CHAR)) AS fundwhlnm,
       JSON_VALUE(result_json, '$.fundtypenm' RETURNING VARCHAR2(50 CHAR)) AS fundtypenm,
       JSON_VALUE(result_json, '$.lstddt' RETURNING VARCHAR2(8 CHAR)) AS lstddt,
       JSON_VALUE(result_json, '$.stddt' RETURNING VARCHAR2(10 CHAR)) AS stddt,
       JSON_VALUE(result_json, '$.nastamt' RETURNING NUMBER) AS nastamt,
       JSON_VALUE(result_json, '$.clpr' RETURNING NUMBER) AS clpr
  FROM api_rst
 WHERE api_id IN ('ACE_ETF_LIST');

-- ace_etf_pdf  (8컬럼)  ACE_ETF_PDF (``/api/funds/{fundCd}/pdf``) -- ACE ETF 구성종목.
CREATE OR REPLACE VIEW v_ace_etf_pdf AS
SELECT api_id, job_id,
       JSON_VALUE(result_json, '$.fundcd' RETURNING VARCHAR2(20 CHAR)) AS fundcd,
       JSON_VALUE(result_json, '$.std_dt' RETURNING VARCHAR2(10 CHAR)) AS std_dt,
       JSON_VALUE(result_json, '$.rank' RETURNING NUMBER) AS rank,
       JSON_VALUE(result_json, '$.jm_ksc_cd' RETURNING VARCHAR2(20 CHAR)) AS jm_ksc_cd,
       JSON_VALUE(result_json, '$.sec_nm' RETURNING VARCHAR2(200 CHAR)) AS sec_nm,
       JSON_VALUE(result_json, '$.cu_item_cnt' RETURNING NUMBER) AS cu_item_cnt,
       JSON_VALUE(result_json, '$.val_am' RETURNING NUMBER) AS val_am,
       JSON_VALUE(result_json, '$.wg' RETURNING NUMBER) AS wg
  FROM api_rst
 WHERE api_id IN ('ACE_ETF_PDF');

-- kodex_etf  (26컬럼)  KODEX_ETF_LIST (삼성자산운용 ``/api/v1/kodex/product.do``) -- KODEX ETF
CREATE OR REPLACE VIEW v_kodex_etf AS
SELECT api_id, job_id,
       JSON_VALUE(result_json, '$.fid' RETURNING VARCHAR2(20 CHAR)) AS fid,
       JSON_VALUE(result_json, '$.stkticker' RETURNING VARCHAR2(20 CHAR)) AS stkticker,
       JSON_VALUE(result_json, '$.fnm' RETURNING VARCHAR2(200 CHAR)) AS fnm,
       JSON_VALUE(result_json, '$.listd' RETURNING VARCHAR2(8 CHAR)) AS listd,
       JSON_VALUE(result_json, '$.gijunymd' RETURNING VARCHAR2(8 CHAR)) AS gijunymd,
       JSON_VALUE(result_json, '$.typenm' RETURNING VARCHAR2(100 CHAR)) AS typenm,
       JSON_VALUE(result_json, '$.typelnm' RETURNING VARCHAR2(100 CHAR)) AS typelnm,
       JSON_VALUE(result_json, '$.dcyn' RETURNING VARCHAR2(20 CHAR)) AS dcyn,
       JSON_VALUE(result_json, '$.irpyn' RETURNING VARCHAR2(20 CHAR)) AS irpyn,
       JSON_VALUE(result_json, '$.monthlyyn' RETURNING VARCHAR2(20 CHAR)) AS monthlyyn,
       JSON_VALUE(result_json, '$.nav' RETURNING NUMBER) AS nav,
       JSON_VALUE(result_json, '$.curp' RETURNING NUMBER) AS curp,
       JSON_VALUE(result_json, '$.basp' RETURNING NUMBER) AS basp,
       JSON_VALUE(result_json, '$.basrp' RETURNING NUMBER) AS basrp,
       JSON_VALUE(result_json, '$.basrprt' RETURNING NUMBER) AS basrprt,
       JSON_VALUE(result_json, '$.risep' RETURNING NUMBER) AS risep,
       JSON_VALUE(result_json, '$.riseprt' RETURNING NUMBER) AS riseprt,
       JSON_VALUE(result_json, '$.yieldweek' RETURNING NUMBER) AS yieldweek,
       JSON_VALUE(result_json, '$.yieldmon1' RETURNING NUMBER) AS yieldmon1,
       JSON_VALUE(result_json, '$.yieldmon3' RETURNING NUMBER) AS yieldmon3,
       JSON_VALUE(result_json, '$.yieldmon6' RETURNING NUMBER) AS yieldmon6,
       JSON_VALUE(result_json, '$.yieldyear' RETURNING NUMBER) AS yieldyear,
       JSON_VALUE(result_json, '$.yieldyear1' RETURNING NUMBER) AS yieldyear1,
       JSON_VALUE(result_json, '$.yieldyear3' RETURNING NUMBER) AS yieldyear3,
       JSON_VALUE(result_json, '$.yieldyear5' RETURNING NUMBER) AS yieldyear5,
       JSON_VALUE(result_json, '$.yieldlist' RETURNING NUMBER) AS yieldlist
  FROM api_rst
 WHERE api_id IN ('KODEX_ETF_LIST');

-- kodex_etf_pdf  (11컬럼)  KODEX_ETF_PDF (삼성자산운용 ``/api/v1/kodex/product-pdf/{fId}.do``) --
CREATE OR REPLACE VIEW v_kodex_etf_pdf AS
SELECT api_id, job_id,
       JSON_VALUE(result_json, '$.fid' RETURNING VARCHAR2(20 CHAR)) AS fid,
       JSON_VALUE(result_json, '$.gijunymd' RETURNING VARCHAR2(10 CHAR)) AS gijunymd,
       JSON_VALUE(result_json, '$.itmno' RETURNING VARCHAR2(50 CHAR)) AS itmno,
       JSON_VALUE(result_json, '$.secnm' RETURNING VARCHAR2(200 CHAR)) AS secnm,
       JSON_VALUE(result_json, '$.applyq' RETURNING NUMBER) AS applyq,
       JSON_VALUE(result_json, '$.evala' RETURNING NUMBER) AS evala,
       JSON_VALUE(result_json, '$.ratio' RETURNING NUMBER) AS ratio,
       JSON_VALUE(result_json, '$.curp' RETURNING NUMBER) AS curp,
       JSON_VALUE(result_json, '$.risep' RETURNING NUMBER) AS risep,
       JSON_VALUE(result_json, '$.basrprt' RETURNING NUMBER) AS basrprt,
       JSON_VALUE(result_json, '$.pdftype' RETURNING VARCHAR2(20 CHAR)) AS pdftype
  FROM api_rst
 WHERE api_id IN ('KODEX_ETF_PDF');

-- plus_etf  (13컬럼)  PLUS_ETF_LIST (한화자산운용 ``/api/v1/product/find/list``) -- PLUS ETF
CREATE OR REPLACE VIEW v_plus_etf AS
SELECT api_id, job_id,
       JSON_VALUE(result_json, '$.prod_id' RETURNING VARCHAR2(20 CHAR)) AS prod_id,
       JSON_VALUE(result_json, '$.namecode' RETURNING VARCHAR2(20 CHAR)) AS namecode,
       JSON_VALUE(result_json, '$.krfundcd' RETURNING VARCHAR2(20 CHAR)) AS krfundcd,
       JSON_VALUE(result_json, '$.displayname' RETURNING VARCHAR2(200 CHAR)) AS displayname,
       JSON_VALUE(result_json, '$.basicinfo' RETURNING VARCHAR2(200 CHAR)) AS basicinfo,
       JSON_VALUE(result_json, '$.fundtype' RETURNING VARCHAR2(50 CHAR)) AS fundtype,
       JSON_VALUE(result_json, '$.productoption' RETURNING VARCHAR2(50 CHAR)) AS productoption,
       JSON_VALUE(result_json, '$.wkdate' RETURNING VARCHAR2(8 CHAR)) AS wkdate,
       JSON_VALUE(result_json, '$.marketprice' RETURNING NUMBER) AS marketprice,
       JSON_VALUE(result_json, '$.navprice' RETURNING NUMBER) AS navprice,
       JSON_VALUE(result_json, '$.etfprice' RETURNING NUMBER) AS etfprice,
       JSON_VALUE(result_json, '$.disparateratio' RETURNING NUMBER) AS disparateratio,
       JSON_VALUE(result_json, '$.returnrate' RETURNING NUMBER) AS returnrate
  FROM api_rst
 WHERE api_id IN ('PLUS_ETF_LIST');

-- plus_etf_pdf  (8컬럼)  PLUS_ETF_PDF (한화자산운용 ``/api/v1/product/pdf/list``) -- PLUS ETF
CREATE OR REPLACE VIEW v_plus_etf_pdf AS
SELECT api_id, job_id,
       JSON_VALUE(result_json, '$.prod_id' RETURNING VARCHAR2(20 CHAR)) AS prod_id,
       JSON_VALUE(result_json, '$.wkdate' RETURNING VARCHAR2(8 CHAR)) AS wkdate,
       JSON_VALUE(result_json, '$.num' RETURNING NUMBER) AS num,
       JSON_VALUE(result_json, '$.jmcd' RETURNING VARCHAR2(20 CHAR)) AS jmcd,
       JSON_VALUE(result_json, '$.krjmcd' RETURNING VARCHAR2(20 CHAR)) AS krjmcd,
       JSON_VALUE(result_json, '$.jmnm' RETURNING VARCHAR2(200 CHAR)) AS jmnm,
       JSON_VALUE(result_json, '$.amount' RETURNING NUMBER) AS amount,
       JSON_VALUE(result_json, '$.ratio' RETURNING NUMBER) AS ratio
  FROM api_rst
 WHERE api_id IN ('PLUS_ETF_PDF');

-- sol_etf  (21컬럼)  SOL_ETF_LIST (신한자산운용 ``/api/common/searchByEtfNameOrFilter``) --
CREATE OR REPLACE VIEW v_sol_etf AS
SELECT api_id, job_id,
       JSON_VALUE(result_json, '$.fund_cd' RETURNING VARCHAR2(20 CHAR)) AS fund_cd,
       JSON_VALUE(result_json, '$.etf_cd6' RETURNING VARCHAR2(20 CHAR)) AS etf_cd6,
       JSON_VALUE(result_json, '$.name' RETURNING VARCHAR2(200 CHAR)) AS name,
       JSON_VALUE(result_json, '$.engname' RETURNING VARCHAR2(200 CHAR)) AS engname,
       JSON_VALUE(result_json, '$.work_dt' RETURNING VARCHAR2(8 CHAR)) AS work_dt,
       JSON_VALUE(result_json, '$.list_dt' RETURNING VARCHAR2(8 CHAR)) AS list_dt,
       JSON_VALUE(result_json, '$.tot_asset' RETURNING NUMBER) AS tot_asset,
       JSON_VALUE(result_json, '$.fund_pri' RETURNING NUMBER) AS fund_pri,
       JSON_VALUE(result_json, '$.total_fee' RETURNING NUMBER) AS total_fee,
       JSON_VALUE(result_json, '$.asset_tag' RETURNING VARCHAR2(50 CHAR)) AS asset_tag,
       JSON_VALUE(result_json, '$.theme_tag' RETURNING VARCHAR2(50 CHAR)) AS theme_tag,
       JSON_VALUE(result_json, '$.pension_tag' RETURNING VARCHAR2(50 CHAR)) AS pension_tag,
       JSON_VALUE(result_json, '$.d_rtn' RETURNING NUMBER) AS d_rtn,
       JSON_VALUE(result_json, '$.w_rtn' RETURNING NUMBER) AS w_rtn,
       JSON_VALUE(result_json, '$.m1_rtn' RETURNING NUMBER) AS m1_rtn,
       JSON_VALUE(result_json, '$.m3_rtn' RETURNING NUMBER) AS m3_rtn,
       JSON_VALUE(result_json, '$.m6_rtn' RETURNING NUMBER) AS m6_rtn,
       JSON_VALUE(result_json, '$.m12_rtn' RETURNING NUMBER) AS m12_rtn,
       JSON_VALUE(result_json, '$.ytd' RETURNING NUMBER) AS ytd,
       JSON_VALUE(result_json, '$.cum_rtn' RETURNING NUMBER) AS cum_rtn,
       JSON_VALUE(result_json, '$.list_rtn' RETURNING NUMBER) AS list_rtn
  FROM api_rst
 WHERE api_id IN ('SOL_ETF_LIST');

-- sol_etf_pdf  (9컬럼)  SOL_ETF_PDF (신한자산운용 ``/api/fund/pdfList``) -- SOL ETF 구성종목.
CREATE OR REPLACE VIEW v_sol_etf_pdf AS
SELECT api_id, job_id,
       JSON_VALUE(result_json, '$.fund_cd' RETURNING VARCHAR2(20 CHAR)) AS fund_cd,
       JSON_VALUE(result_json, '$.etf_cd6' RETURNING VARCHAR2(20 CHAR)) AS etf_cd6,
       JSON_VALUE(result_json, '$.work_dt' RETURNING VARCHAR2(8 CHAR)) AS work_dt,
       JSON_VALUE(result_json, '$.seq_no' RETURNING NUMBER) AS seq_no,
       JSON_VALUE(result_json, '$.stock_code' RETURNING VARCHAR2(20 CHAR)) AS stock_code,
       JSON_VALUE(result_json, '$.sec_nm' RETURNING VARCHAR2(200 CHAR)) AS sec_nm,
       JSON_VALUE(result_json, '$.qty' RETURNING NUMBER) AS qty,
       JSON_VALUE(result_json, '$.price' RETURNING NUMBER) AS price,
       JSON_VALUE(result_json, '$.wt_disp' RETURNING VARCHAR2(20 CHAR)) AS wt_disp
  FROM api_rst
 WHERE api_id IN ('SOL_ETF_PDF');

-- 조회에 쓰는 JSON 경로마다 함수기반 인덱스를 둔다. 없으면 그
-- 질의가 전체 스캔으로 떨어진다 (측정: 인덱스 있으면 타입
-- 테이블의 1.1x, 없으면 전수 스캔 2.5x). LOCAL 이라 파티션
-- 프루닝과 함께 동작한다.
-- 예:
-- CREATE INDEX ix_api_rst_std_dt ON api_rst
--   (JSON_VALUE(result_json, '$.std_dt' RETURNING VARCHAR2(8))) LOCAL;