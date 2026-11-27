-- ============================================================================
-- pyscrap v3: api_job 샘플 정의 (job_type='DUCKDB', v3/ 경로 + 멀티 버킷)
--
-- SCRAPE 잡(구 api_job_builder)은 scripts/migrate_v2_to_v3.py 로 일괄 이관되고,
-- 아래는 DuckDB 후처리 · 월별 Compaction · 금융 마스터 갱신 샘플이다.
-- 치환 변수: ${OCI_BUCKET}, ${TARGET_DATE} (YYYYMMDD), ${TARGET_YM} (YYYYMM)
-- ============================================================================

-- 1) 일별 병합 & 중복제거: v3/raw_clean -> v3/curated (OCI 정본 저장)
INSERT INTO api_job (
    job_id, job_type, api_id, schedule_tag, run_order, is_active, job_spec_json, note
) VALUES (
    'CURATE_KIS_FUTOPT_CHART_DAILY',
    'DUCKDB',
    NULL,
    'daily_close',
    200,
    TRUE,
    jsonb_build_object(
        'output_layer', 'curated',
        'output_rel_path', 'kis_futopt_chart/ym=${TARGET_YM}/dt=${TARGET_DATE}/data.parquet',
        'targets', jsonb_build_array('oci'),
        'memory_limit', '1GB',
        'threads', 2,
        'min_rows', 1,
        'sql', $$
            SELECT DISTINCT *
            FROM read_parquet(
                's3://${OCI_BUCKET}/v3/raw_clean/kis_futopt_chart/dt=${TARGET_DATE}/*.parquet',
                union_by_name = true
            )
            ORDER BY short_code, stck_bsop_date, stck_cntg_hour
        $$
    ),
    '당일 선물옵션 분봉 raw_clean 을 중복제거·정렬해 curated 일별 1파일로 병합'
)
ON CONFLICT (job_id) DO UPDATE SET
    job_spec_json = EXCLUDED.job_spec_json,
    run_order = EXCLUDED.run_order,
    updated_at = now();

-- 2) 월별 정렬 Compaction: 일별 curated -> 월별 1파일 (Row Group 프루닝 최적화)
INSERT INTO api_job (
    job_id, job_type, api_id, schedule_tag, run_order, is_active, job_spec_json, note
) VALUES (
    'COMPACT_KIS_FUTOPT_CHART_MONTHLY',
    'DUCKDB',
    NULL,
    'monthly_archive',
    200,
    TRUE,
    jsonb_build_object(
        'output_layer', 'curated',
        'output_rel_path', 'kis_futopt_chart/ym=${TARGET_YM}/monthly.parquet',
        'targets', jsonb_build_array('oci'),
        'memory_limit', '1GB',
        'threads', 2,
        'min_rows', 1,
        'sql', $$
            SELECT DISTINCT *
            FROM read_parquet(
                's3://${OCI_BUCKET}/v3/curated/kis_futopt_chart/ym=${TARGET_YM}/dt=*/*.parquet',
                union_by_name = true
            )
            ORDER BY short_code, stck_bsop_date, stck_cntg_hour
        $$
    ),
    '한 달치 분봉을 종목·일시 순으로 정렬해 monthly.parquet 1파일로 통합'
)
ON CONFLICT (job_id) DO UPDATE SET
    job_spec_json = EXCLUDED.job_spec_json,
    run_order = EXCLUDED.run_order,
    updated_at = now();

-- 3) 금융 마스터 갱신 (방식 B): 자동 수집 + Neon 수기 user_etf 보정 병합
--    결과를 OCI 정본 + Cloudflare R2 서빙 버킷 양쪽에 적재
INSERT INTO api_job (
    job_id, job_type, api_id, schedule_tag, run_order, is_active, job_spec_json, note
) VALUES (
    'REFRESH_REF_MST_ETF',
    'DUCKDB',
    NULL,
    'daily_batch2',
    200,
    TRUE,
    jsonb_build_object(
        'output_layer', 'ref',
        'output_rel_path', 'mst_etf.parquet',
        'targets', jsonb_build_array('oci', 'r2'),
        'memory_limit', '512MB',
        'threads', 2,
        'min_rows', 1,
        'sql', $$
            WITH latest_krx AS (
                SELECT
                    isu_cd                                          AS short_code,
                    isu_nm                                          AS prod_nm,
                    bas_dd                                          AS last_bas_dd
                FROM read_parquet(
                    's3://${OCI_BUCKET}/v3/curated/krx_etf_daily/**/*.parquet',
                    hive_partitioning = true
                )
                QUALIFY ROW_NUMBER() OVER (PARTITION BY isu_cd ORDER BY bas_dd DESC) = 1
            ),
            manual AS (
                SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/ref/user_etf.parquet')
            )
            SELECT
                k.short_code,
                k.prod_nm,
                m.amc,
                m.amc_etf_cd,
                k.last_bas_dd
            FROM latest_krx k
            LEFT JOIN manual m ON m.isu_cd = k.short_code
            ORDER BY k.short_code
        $$
    ),
    'KRX ETF 시세에서 마스터를 파생하고 수기 user_etf 로 보정한 뒤 OCI+R2 동시 발행'
)
ON CONFLICT (job_id) DO UPDATE SET
    job_spec_json = EXCLUDED.job_spec_json,
    run_order = EXCLUDED.run_order,
    updated_at = now();
