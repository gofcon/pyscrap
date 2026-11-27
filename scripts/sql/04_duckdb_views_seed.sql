-- ============================================================================
-- pyscrap v3: duckdb_view 선언 레지스트리 시드
--
-- 내장 DuckDB(api_job.params_json 축 쿼리 · etl_job.sql_text)가 쿼리에 등장하는
-- view_name 을 자동 감지해 CREATE OR REPLACE TEMP VIEW 로 바인딩한다.
-- Python 소스에 테이블 목록을 하드코딩하지 않고 이 테이블로만 관리한다.
--
-- 치환 변수: ${OCI_BUCKET}, ${TARGET_DATE} (YYYYMMDD), ${TARGET_YM} (YYYYMM)
-- ============================================================================

INSERT INTO duckdb_view (view_name, sql_text, fallback_sql, is_active, note) VALUES
-- ----------------------------------------------------------------------------
-- [1] v3/snapshot — 금융 마스터 · 수기 미러 · 누적 기준 스냅샷
-- ----------------------------------------------------------------------------
(
    'mst_etf',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/snapshot/mst_etf.parquet')$$,
    NULL, TRUE, 'ETF 마스터 스냅샷'
),
(
    'mst_fuopt',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/snapshot/mst_fuopt.parquet')$$,
    NULL, TRUE, '선물·옵션 마스터 스냅샷'
),
(
    'mst_bond',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/snapshot/mst_bond.parquet')$$,
    NULL, TRUE, '채권 마스터 스냅샷'
),
(
    'mst_stock',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/snapshot/mst_stock.parquet')$$,
    NULL, TRUE, '주식 마스터 스냅샷'
),
(
    'meta_maturity',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/snapshot/meta_maturity.parquet')$$,
    NULL, TRUE, '파생상품 만기 달력 스냅샷'
),
(
    'stock_index_his',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/snapshot/stock_index_his.parquet')$$,
    NULL, TRUE, '지수 일별 이력 스냅샷'
),
(
    'v_k2i_atm',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/snapshot/v_k2i_atm.parquet')$$,
    NULL, TRUE, 'KOSPI200 일자별 ATM / 만기일 파생 스냅샷'
),
(
    'user_etf',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/snapshot/user_etf.parquet')$$,
    NULL, TRUE, '수기 ETF 보정 테이블 미러'
),
(
    'krx_index_mst',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/snapshot/krx_index_mst.parquet')$$,
    NULL, TRUE, 'KRX 지수 마스터 미러'
),
(
    'mst_index',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/snapshot/mst_index.parquet')$$,
    NULL, TRUE, '지수 마스터 미러'
),
(
    'meta_fuopt_info',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/snapshot/meta_fuopt_info.parquet')$$,
    NULL, TRUE, '파생상품 메타 정보 미러'
),
(
    'ksd_bond_no_detail',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/snapshot/ksd_bond_no_detail.parquet')$$,
    $$SELECT CAST(NULL AS VARCHAR) AS isin, CAST(NULL AS TIMESTAMP) AS marked_at, CAST(NULL AS VARCHAR) AS note WHERE FALSE$$,
    TRUE, '세이브로 상세 0행 마킹 스냅샷'
),
(
    'src_code',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/snapshot/src_code.parquet')$$,
    $$SELECT CAST(NULL AS VARCHAR) AS src_name, CAST(NULL AS VARCHAR) AS table_name, CAST(NULL AS VARCHAR) AS column_name, CAST(NULL AS VARCHAR) AS code_value WHERE FALSE$$,
    TRUE, '자동 수확 코드값 카탈로그 스냅샷'
),

-- ----------------------------------------------------------------------------
-- [2] 읽기 스냅샷 / 쓰기 raw_clean 분리 대상 (채권 누적 스냅샷)
--     수집(api_job)은 v3/raw_clean/ 에 적재하고, 축 쿼리는 불변 v3/snapshot/ 을 읽는다.
--     수집 완료 후 etl_job(REFRESH_REF_KSD_BOND_INFO / REFRESH_REF_ISIN_BOND_INFO)이
--     raw_clean 을 스냅샷에 누적 병합한다.
-- ----------------------------------------------------------------------------
(
    'ksd_bond_info',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/snapshot/ksd_bond_info.parquet')$$,
    $$SELECT CAST(NULL AS VARCHAR) AS isin WHERE FALSE$$,
    TRUE, '세이브로 채권 기본정보 누적 스냅샷 (읽기 전용 기준, 수집은 raw_clean/ksd_bond_info)'
),
(
    'isin_bond_info',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/snapshot/isin_bond_info.parquet')$$,
    $$SELECT CAST(NULL AS VARCHAR) AS isin WHERE FALSE$$,
    TRUE, 'KRX ISIN 채권 기본정보 누적 스냅샷 (읽기 전용 기준, 수집은 raw_clean/isin_bond_info)'
),

-- ----------------------------------------------------------------------------
-- [3] raw_clean / daily / monthly 편의 뷰
-- ----------------------------------------------------------------------------
(
    'ksd_kacd_code',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/raw_clean/ksd_kacd_code/**/*.parquet', union_by_name = true)$$,
    $$SELECT CAST(NULL AS VARCHAR) AS codevalue, CAST(NULL AS INTEGER) AS code_depth WHERE FALSE$$,
    TRUE, '세이브로 종목구분 코드 원본 뷰'
),
(
    'v_kis_futopt_chart_daily',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/daily/kis_futopt_chart/**/*.parquet', hive_partitioning = true, union_by_name = true)$$,
    NULL, TRUE, '선물옵션 분봉 일별 정제 뷰'
),
(
    'v_kis_futopt_price_daily',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/daily/kis_futopt_price/**/*.parquet', hive_partitioning = true, union_by_name = true)$$,
    NULL, TRUE, '선물옵션 3분 시세 일별 정제 뷰'
),
(
    'v_krx_etf_daily',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/daily/krx_etf_daily/**/*.parquet', hive_partitioning = true, union_by_name = true)$$,
    NULL, TRUE, 'KRX ETF 일별 시세 정제 뷰'
),
(
    'v_ace_etf_pdf_daily',
    $$SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/daily/ace_etf_pdf/**/*.parquet', hive_partitioning = true, union_by_name = true)$$,
    NULL, TRUE, 'ACE ETF PDF 일별 정제 뷰'
)
ON CONFLICT (view_name) DO UPDATE SET
    sql_text     = EXCLUDED.sql_text,
    fallback_sql = EXCLUDED.fallback_sql,
    is_active    = EXCLUDED.is_active,
    note         = EXCLUDED.note,
    updated_at   = now();
