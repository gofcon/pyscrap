-- ============================================================================
-- pyscrap v3: ops.duckdb_job 샘플 정의 (v3/ 경로 + 멀티 버킷 ["oci", "r2"])
--
-- Python 러너는 고정이며, 아래 SQL 과 targets_json 만 Neon DB 에서 관리한다.
-- 치환 변수: ${OCI_BUCKET}, ${TARGET_DATE} (YYYYMMDD), ${TARGET_YM} (YYYYMM)
-- ============================================================================

-- 1) 일별 병합 & 중복제거: v3/raw_clean -> v3/curated (OCI 정본 저장)
INSERT INTO ops.duckdb_job (
    job_name, schedule_tag, output_layer, output_rel_path,
    targets_json, memory_limit, threads, min_rows, sql_body, note
) VALUES (
    'curate_kis_futopt_chart_daily',
    'daily_close',
    'curated',
    'kis_futopt_chart/ym=${TARGET_YM}/dt=${TARGET_DATE}/data.parquet',
    '["oci"]'::jsonb,
    '1GB',
    2,
    1,
    $$
    SELECT DISTINCT *
    FROM read_parquet(
        's3://${OCI_BUCKET}/v3/raw_clean/kis_futopt_chart/dt=${TARGET_DATE}/*.parquet',
        union_by_name = true
    )
    ORDER BY short_code, stck_bsop_date, stck_cntg_hour
    $$,
    '당일 선물옵션 분봉 raw_clean 을 중복제거·정렬해 curated 일별 1파일로 병합'
)
ON CONFLICT (job_name) DO UPDATE SET
    sql_body = EXCLUDED.sql_body,
    targets_json = EXCLUDED.targets_json,
    updated_at = now();

-- 2) 월별 정렬 Compaction: 일별 curated -> 월별 1파일 (Row Group 프루닝 최적화)
INSERT INTO ops.duckdb_job (
    job_name, schedule_tag, output_layer, output_rel_path,
    targets_json, memory_limit, threads, min_rows, sql_body, note
) VALUES (
    'compact_kis_futopt_chart_monthly',
    'monthly_archive',
    'curated',
    'kis_futopt_chart/ym=${TARGET_YM}/monthly.parquet',
    '["oci"]'::jsonb,
    '1GB',
    2,
    1,
    $$
    SELECT DISTINCT *
    FROM read_parquet(
        's3://${OCI_BUCKET}/v3/curated/kis_futopt_chart/ym=${TARGET_YM}/dt=*/*.parquet',
        union_by_name = true
    )
    ORDER BY short_code, stck_bsop_date, stck_cntg_hour
    $$,
    '한 달치 분봉을 종목·일시 순으로 정렬해 monthly.parquet 1파일로 통합'
)
ON CONFLICT (job_name) DO UPDATE SET
    sql_body = EXCLUDED.sql_body,
    targets_json = EXCLUDED.targets_json,
    updated_at = now();

-- 3) 금융 마스터 갱신 (방식 B): 자동 수집 + Neon 수기 user_etf 보정 병합
--    결과를 OCI 정본 + Cloudflare R2 서빙 버킷 양쪽에 적재
INSERT INTO ops.duckdb_job (
    job_name, schedule_tag, output_layer, output_rel_path,
    targets_json, memory_limit, threads, min_rows, sql_body, note
) VALUES (
    'refresh_ref_mst_etf',
    'daily_batch2',
    'ref',
    'mst_etf.parquet',
    '["oci", "r2"]'::jsonb,
    '512MB',
    2,
    1,
    $$
    WITH latest_krx AS (
        SELECT
            isu_cd                                          AS isin,
            isu_nm                                          AS etf_name,
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
        k.isin,
        COALESCE(m.etf_name, k.etf_name)                    AS etf_name,
        m.category,
        k.last_bas_dd
    FROM latest_krx k
    LEFT JOIN manual m ON m.isin = k.isin
    ORDER BY k.isin
    $$,
    'KRX ETF 시세에서 마스터를 파생하고 수기 user_etf 로 보정한 뒤 OCI+R2 동시 발행'
)
ON CONFLICT (job_name) DO UPDATE SET
    sql_body = EXCLUDED.sql_body,
    targets_json = EXCLUDED.targets_json,
    updated_at = now();
