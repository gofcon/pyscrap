-- ============================================================================
-- pyscrap v3: 팀 공유용 DBeaver DuckDB 뷰 정의 (v3/ 버킷 경로 기준)
--
-- 사용 절차 (팀원별 PC에서 최초 1회):
--   1. DBeaver -> New Connection -> DuckDB -> 로컬 파일 경로 지정
--      (예: ~/pyscrap_analytics.duckdb)
--   2. 아래 SECRET 블록에 본인 OCI(또는 R2) 키를 채워 1회 실행
--      (시크릿은 ~/.duckdb/stored_secrets/ 에 저장되며 .duckdb 파일에 안 들어감)
--   3. <BUCKET> 을 실제 버킷명으로 바꾼 뒤 아래 VIEW 섹션 전체 실행
-- ============================================================================

-- ----------------------------------------------------------------------------
-- [최초 1회] S3 시크릿 등록 (1차 정본: OCI / 2차 서빙: Cloudflare R2)
-- ----------------------------------------------------------------------------
-- INSTALL httpfs;
-- LOAD httpfs;
--
-- CREATE OR REPLACE PERSISTENT SECRET oci_s3 (
--     TYPE S3,
--     KEY_ID    '<OCI_ACCESS_KEY>',
--     SECRET    '<OCI_SECRET_KEY>',
--     REGION    '<OCI_REGION>',
--     ENDPOINT  '<OCI_NAMESPACE>.compat.objectstorage.<OCI_REGION>.oraclecloud.com',
--     SCOPE     's3://<OCI_BUCKET>',
--     URL_STYLE 'path',
--     USE_SSL   true
-- );
--
-- -- 2단계: Cloudflare R2 버킷 추가 시
-- CREATE OR REPLACE PERSISTENT SECRET r2_s3 (
--     TYPE S3,
--     KEY_ID    '<R2_ACCESS_KEY>',
--     SECRET    '<R2_SECRET_KEY>',
--     REGION    'auto',
--     ENDPOINT  '<ACCOUNT_ID>.r2.cloudflarestorage.com',
--     SCOPE     's3://<R2_BUCKET>',
--     URL_STYLE 'path',
--     USE_SSL   true
-- );
--
-- -- 선택: Neon DB 수기 테이블(ref.user_*) 실시간 조인이 필요할 때만
-- INSTALL postgres;
-- LOAD postgres;
-- ATTACH 'postgresql://<USER>:<PASS>@<HOST>-pooler.neon.tech/<DB>?sslmode=require'
--     AS neon (TYPE POSTGRES, READ_ONLY);

-- ----------------------------------------------------------------------------
-- [공유 뷰] v3/ref — 금융 마스터 & 수기 미러
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_mst_etf AS
SELECT * FROM read_parquet('s3://<BUCKET>/v3/ref/mst_etf.parquet');

CREATE OR REPLACE VIEW v_mst_fuopt AS
SELECT * FROM read_parquet('s3://<BUCKET>/v3/ref/mst_fuopt.parquet');

CREATE OR REPLACE VIEW v_mst_bond AS
SELECT * FROM read_parquet('s3://<BUCKET>/v3/ref/mst_bond.parquet');

CREATE OR REPLACE VIEW v_mst_stock AS
SELECT * FROM read_parquet('s3://<BUCKET>/v3/ref/mst_stock.parquet');

CREATE OR REPLACE VIEW v_meta_maturity AS
SELECT * FROM read_parquet('s3://<BUCKET>/v3/ref/meta_maturity.parquet');

CREATE OR REPLACE VIEW v_user_etf AS
SELECT * FROM read_parquet('s3://<BUCKET>/v3/ref/user_etf.parquet');

-- ----------------------------------------------------------------------------
-- [공유 뷰] v3/curated — 대용량 시계열 · 구성종목 (Hive 파티션 ym/dt 자동 프루닝)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_kis_futopt_chart AS
SELECT * FROM read_parquet(
    's3://<BUCKET>/v3/curated/kis_futopt_chart/**/*.parquet',
    hive_partitioning = true
);

CREATE OR REPLACE VIEW v_kis_futopt_price AS
SELECT * FROM read_parquet(
    's3://<BUCKET>/v3/curated/kis_futopt_price/**/*.parquet',
    hive_partitioning = true
);

CREATE OR REPLACE VIEW v_krx_opt_daily AS
SELECT * FROM read_parquet(
    's3://<BUCKET>/v3/curated/krx_opt_daily/**/*.parquet',
    hive_partitioning = true
);

CREATE OR REPLACE VIEW v_krx_etf_daily AS
SELECT * FROM read_parquet(
    's3://<BUCKET>/v3/curated/krx_etf_daily/**/*.parquet',
    hive_partitioning = true
);

CREATE OR REPLACE VIEW v_etf_pdf AS
SELECT * FROM read_parquet(
    's3://<BUCKET>/v3/curated/etf_pdf/**/*.parquet',
    hive_partitioning = true
);

-- ----------------------------------------------------------------------------
-- [공유 뷰] v3/mart — 사전 집계 마트 (웹 UI / 대시보드용)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_mart_etf_summary AS
SELECT * FROM read_parquet('s3://<BUCKET>/v3/mart/etf_summary/data.parquet');
