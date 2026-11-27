-- ============================================================================
-- pyscrap v3: etl_job 전체 시드 (총 27개 — v1/v2 Oracle 프로시저 100% 이관)
--
--   [그룹 A] 금융 마스터 & 파생 갱신 (7개, v3/ref/*.parquet)
--     1. REFRESH_REF_MST_STOCK         (daily_early  200) <- sp_mst_stock_sync + dedup
--     2. REFRESH_REF_META_MATURITY     (daily_early  210) <- sp_meta_maturity_sync
--     3. REFRESH_REF_MST_FUOPT         (daily_early  220) <- sp_mst_fuopt_sync
--     4. REFRESH_REF_MST_BOND          (daily_batch2  50) <- sp_mst_bond_sync
--     5. REFRESH_REF_MST_ETF           (daily_batch2  60) <- sp_mst_etf_sync
--     6. REFRESH_REF_STOCK_INDEX_HIS   (daily_close  180) <- sp_stock_index_his_sync + dedup
--     7. REFRESH_REF_V_K2I_ATM         (daily_close  190) <- v_k2i_atm (MV)
--
--   [그룹 B-1] 일별 Curated 병합 & 중복제거 (14개, v3/curated/<target>/ym=.../dt=.../data.parquet)
--     8..21. sp_run_daily_export 대상 14개 전체 (kis_futopt_price1 체결필터 포함)
--
--   [그룹 B-2] 월별 정렬 Compaction (4개, v3/curated/<target>/ym=.../monthly.parquet)
--     22..25. sp_run_monthly_archive 대상 4개 전체
--
--   [그룹 C] 선택 보조 작업 (2개, v3/ref/*.parquet)
--     26. MARK_KSD_BOND_NO_DETAIL      (daily_batch2  70) <- sp_ksd_no_detail_mark
--     27. HARVEST_SRC_CODE             (daily_batch2 250) <- sp_src_code_sync (builtin runner)
--
-- 치환 변수: ${OCI_BUCKET}, ${TARGET_DATE} (YYYYMMDD), ${TARGET_YM} (YYYYMM), ${LEDGER_DIR}
-- ============================================================================

-- ----------------------------------------------------------------------------
-- [그룹 A] 금융 마스터 & 파생 갱신 (7개)
-- ----------------------------------------------------------------------------

-- 1. REFRESH_REF_MST_STOCK (sp_mst_stock_sync + sp_krx_stock_base_dedup)
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows,
    sql_text, note
) VALUES (
    'REFRESH_REF_MST_STOCK', 'daily_early', 200, TRUE,
    'ref', 'mst_stock.parquet', ARRAY['oci'], '512MB', 2, 1,
    $$
        WITH existing AS (
            SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/ref/mst_stock.parquet')
        ),
        incoming AS (
            SELECT
                isu_srt_cd         AS short_code,
                isu_cd             AS isin,
                isu_nm             AS prod_nm,
                isu_abbrv          AS prod_snm,
                isu_eng_nm         AS prod_enm,
                list_dd            AS list_dt,
                mkt_tp_nm          AS mkt_div,
                secugrp_nm         AS secu_grp,
                sect_tp_nm         AS secu_type,
                kind_stkcert_tp_nm AS stock_kind_type,
                CAST(parval AS VARCHAR) AS face_amt,
                TRY_CAST(list_shrs AS BIGINT) AS list_cnt,
                bas_dd             AS last_seen_dt
            FROM read_parquet(
                's3://${OCI_BUCKET}/v3/raw_clean/krx_stock_base/**/*.parquet',
                union_by_name = true
            )
            WHERE isu_srt_cd IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY isu_srt_cd ORDER BY bas_dd DESC) = 1
        )
        SELECT
            COALESCE(t.short_code, s.short_code) AS short_code,
            CASE WHEN t.short_code IS NULL OR t.last_seen_dt IS NULL OR s.last_seen_dt > t.last_seen_dt THEN s.isin            ELSE t.isin            END AS isin,
            CASE WHEN t.short_code IS NULL OR t.last_seen_dt IS NULL OR s.last_seen_dt > t.last_seen_dt THEN s.prod_nm         ELSE t.prod_nm         END AS prod_nm,
            CASE WHEN t.short_code IS NULL OR t.last_seen_dt IS NULL OR s.last_seen_dt > t.last_seen_dt THEN s.prod_snm        ELSE t.prod_snm        END AS prod_snm,
            CASE WHEN t.short_code IS NULL OR t.last_seen_dt IS NULL OR s.last_seen_dt > t.last_seen_dt THEN s.prod_enm        ELSE t.prod_enm        END AS prod_enm,
            CASE WHEN t.short_code IS NULL OR t.last_seen_dt IS NULL OR s.last_seen_dt > t.last_seen_dt THEN s.list_dt         ELSE t.list_dt         END AS list_dt,
            CASE WHEN t.short_code IS NULL OR t.last_seen_dt IS NULL OR s.last_seen_dt > t.last_seen_dt THEN s.mkt_div         ELSE t.mkt_div         END AS mkt_div,
            CASE WHEN t.short_code IS NULL OR t.last_seen_dt IS NULL OR s.last_seen_dt > t.last_seen_dt THEN s.secu_grp        ELSE t.secu_grp        END AS secu_grp,
            CASE WHEN t.short_code IS NULL OR t.last_seen_dt IS NULL OR s.last_seen_dt > t.last_seen_dt THEN s.secu_type       ELSE t.secu_type       END AS secu_type,
            CASE WHEN t.short_code IS NULL OR t.last_seen_dt IS NULL OR s.last_seen_dt > t.last_seen_dt THEN s.stock_kind_type ELSE t.stock_kind_type END AS stock_kind_type,
            CASE WHEN t.short_code IS NULL OR t.last_seen_dt IS NULL OR s.last_seen_dt > t.last_seen_dt THEN s.face_amt        ELSE t.face_amt        END AS face_amt,
            CASE WHEN t.short_code IS NULL OR t.last_seen_dt IS NULL OR s.last_seen_dt > t.last_seen_dt THEN s.list_cnt        ELSE t.list_cnt        END AS list_cnt,
            CASE WHEN t.short_code IS NULL OR t.last_seen_dt IS NULL OR s.last_seen_dt > t.last_seen_dt THEN s.last_seen_dt    ELSE t.last_seen_dt    END AS last_seen_dt,
            t.description,
            CURRENT_TIMESTAMP AS updated_at
        FROM existing t
        FULL OUTER JOIN incoming s ON t.short_code = s.short_code
        ORDER BY short_code
    $$,
    'krx_stock_base 중복제거 후 mst_stock 갱신 (bas_dd > last_seen_dt 일 때만 덮어쓰기, description 보존)'
)
ON CONFLICT (job_id) DO UPDATE SET
    schedule_tag = EXCLUDED.schedule_tag, run_order = EXCLUDED.run_order,
    output_layer = EXCLUDED.output_layer, output_rel_path = EXCLUDED.output_rel_path,
    targets = EXCLUDED.targets, memory_limit = EXCLUDED.memory_limit,
    threads = EXCLUDED.threads, min_rows = EXCLUDED.min_rows,
    sql_text = EXCLUDED.sql_text, note = EXCLUDED.note, updated_at = now();

-- 2. REFRESH_REF_META_MATURITY (sp_meta_maturity_sync)
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows,
    sql_text, note
) VALUES (
    'REFRESH_REF_META_MATURITY', 'daily_early', 210, TRUE,
    'ref', 'meta_maturity.parquet', ARRAY['oci'], '512MB', 2, 1,
    $$
        WITH existing AS (
            SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/ref/meta_maturity.parquet')
        ),
        fuopt_info AS (
            SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/ref/meta_fuopt_info.parquet')
        ),
        from_krx AS (
            SELECT
                1 AS src,
                u.prod_type,
                regexp_extract(k.isu_abbrv, '([0-9]{6}|[0-9]{4}W[0-9])', 1) AS mat_code,
                CAST(strptime(k.lsttrd_dd, '%Y/%m/%d') AS DATE) AS mat_date,
                CASE
                    WHEN regexp_matches(k.isu_abbrv, '[0-9]{4}W[0-9]')
                        THEN substr(k.isu_srt_cd, 4, 2) || 'W'
                    ELSE substr(k.isu_srt_cd, 4, 1)
                         || CASE substr(k.isu_srt_cd, 5, 1)
                              WHEN 'A' THEN '10' WHEN 'B' THEN '11' WHEN 'C' THEN '12'
                              ELSE '0' || substr(k.isu_srt_cd, 5, 1) END
                END AS mat_scd
            FROM read_parquet(
                's3://${OCI_BUCKET}/v3/raw_clean/krx_deriv_info/**/*.parquet',
                union_by_name = true
            ) k
            JOIN fuopt_info u ON u.code_key = substr(k.isu_srt_cd, 2, 2)
            WHERE substr(k.isu_srt_cd, 1, 1) NOT IN ('D', '4')
              AND k.lsttrd_dd IS NOT NULL
        ),
        fo_latest AS (
            SELECT *
            FROM read_parquet(
                's3://${OCI_BUCKET}/v3/raw_clean/fo_idx_code_mst/**/*.parquet',
                union_by_name = true
            )
            WHERE trade_at = (
                SELECT MAX(trade_at)
                FROM read_parquet('s3://${OCI_BUCKET}/v3/raw_clean/fo_idx_code_mst/**/*.parquet', union_by_name = true)
            )
        ),
        from_kis_base AS (
            SELECT
                u.prod_type,
                u.exp_dow,
                u.exp_week,
                regexp_extract(f.kor_name, '([0-9]{6}|[0-9]{4}W[0-9])', 1) AS mat_code,
                f.short_code,
                CASE UPPER(u.exp_dow)
                    WHEN 'MONDAY' THEN 1 WHEN 'TUESDAY' THEN 2 WHEN 'WEDNESDAY' THEN 3
                    WHEN 'THURSDAY' THEN 4 WHEN 'FRIDAY' THEN 5 WHEN 'SATURDAY' THEN 6
                    ELSE 7
                END AS target_isodow
            FROM fo_latest f
            JOIN fuopt_info u ON u.code_key = substr(f.short_code, 2, 2)
        ),
        from_kis AS (
            SELECT
                2 AS src,
                prod_type,
                mat_code,
                CAST(
                    strptime(
                        (CASE WHEN mat_code LIKE '%W%' THEN '20' || substr(mat_code, 1, 4) ELSE mat_code END) || '01',
                        '%Y%m%d'
                    ) AS DATE
                )
                + CAST(
                    ((target_isodow - isodow(CAST(strptime((CASE WHEN mat_code LIKE '%W%' THEN '20' || substr(mat_code, 1, 4) ELSE mat_code END) || '01', '%Y%m%d') AS DATE)) + 7) % 7)
                    + 7 * (COALESCE(exp_week, TRY_CAST(substr(mat_code, 6, 1) AS INTEGER)) - 1)
                    AS INTEGER
                ) AS mat_date,
                substr(short_code, 4, 3) AS mat_scd
            FROM from_kis_base
            WHERE mat_code <> ''
        ),
        incoming AS (
            SELECT
                prod_type,
                mat_code,
                MIN(src) AS src,
                COALESCE(MIN(CASE WHEN src = 1 THEN mat_date END), MIN(CASE WHEN src = 2 THEN mat_date END)) AS mat_date,
                MIN(mat_scd) AS mat_scd
            FROM (SELECT * FROM from_krx UNION ALL SELECT * FROM from_kis)
            WHERE mat_code IS NOT NULL AND mat_code <> ''
            GROUP BY prod_type, mat_code
        ),
        merged AS (
            SELECT
                COALESCE(t.prod_type, s.prod_type) AS prod_type,
                COALESCE(t.mat_code,  s.mat_code)  AS mat_code,
                CASE
                    WHEN t.prod_type IS NULL THEN s.mat_date
                    WHEN s.src = 1 AND t.description LIKE 'derived by rule%' THEN s.mat_date
                    ELSE CAST(t.mat_date AS DATE)
                END AS mat_date,
                CAST(t.prev_mat_date AS DATE) AS orig_prev_mat_date,
                COALESCE(t.mat_scd, s.mat_scd) AS mat_scd,
                CASE
                    WHEN t.prod_type IS NULL THEN
                        CASE s.src WHEN 1 THEN 'from krx_deriv_info (lsttrd_dd)'
                                   ELSE 'derived by rule from mat_code; unconfirmed (a holiday shifts it)' END
                    WHEN s.src = 1 AND t.description LIKE 'derived by rule%' THEN 'from krx_deriv_info (lsttrd_dd)'
                    ELSE t.description
                END AS description
            FROM existing t
            FULL OUTER JOIN incoming s
              ON t.prod_type = s.prod_type AND t.mat_code = s.mat_code
        )
        SELECT
            prod_type,
            mat_code,
            mat_date,
            COALESCE(
                LAG(mat_date) OVER (PARTITION BY prod_type ORDER BY mat_date, mat_code),
                orig_prev_mat_date
            ) AS prev_mat_date,
            mat_scd,
            description,
            CURRENT_TIMESTAMP AS updated_at
        FROM merged
        ORDER BY prod_type, mat_date, mat_code
    $$,
    'krx_deriv_info(확정) + fo_idx_code_mst(규칙)에서 만기 달력 meta_maturity 갱신 및 prev_mat_date 재계산'
)
ON CONFLICT (job_id) DO UPDATE SET
    schedule_tag = EXCLUDED.schedule_tag, run_order = EXCLUDED.run_order,
    output_layer = EXCLUDED.output_layer, output_rel_path = EXCLUDED.output_rel_path,
    targets = EXCLUDED.targets, memory_limit = EXCLUDED.memory_limit,
    threads = EXCLUDED.threads, min_rows = EXCLUDED.min_rows,
    sql_text = EXCLUDED.sql_text, note = EXCLUDED.note, updated_at = now();

-- 3. REFRESH_REF_MST_FUOPT (sp_mst_fuopt_sync — meta_maturity 이후 실행)
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows,
    sql_text, note
) VALUES (
    'REFRESH_REF_MST_FUOPT', 'daily_early', 220, TRUE,
    'ref', 'mst_fuopt.parquet', ARRAY['oci'], '1GB', 2, 1,
    $$
        WITH existing AS (
            SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/ref/mst_fuopt.parquet')
        ),
        fuopt_info AS (
            SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/ref/meta_fuopt_info.parquet')
        ),
        maturity AS (
            SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/ref/meta_maturity.parquet')
        ),
        from_krx AS (
            SELECT
                k.isu_srt_cd AS short_code,
                CASE
                    WHEN regexp_matches(k.isu_abbrv, '[0-9]{4}W[0-9]')
                        THEN substr(k.isu_srt_cd, 1, 5) || 'W' || substr(k.isu_srt_cd, 6, 3)
                    ELSE substr(k.isu_srt_cd, 1, 4)
                         || CASE substr(k.isu_srt_cd, 5, 1)
                              WHEN 'A' THEN '10' WHEN 'B' THEN '11' WHEN 'C' THEN '12'
                              ELSE '0' || substr(k.isu_srt_cd, 5, 1) END
                         || CASE WHEN k.rght_tp_nm = '-' THEN '' ELSE substr(k.isu_srt_cd, 6, 3) END
                END AS kis_short_cd,
                k.isu_abbrv AS prod_nm,
                u.prod_type,
                CASE k.rght_tp_nm WHEN '콜옵션' THEN 'CALL' WHEN '풋옵션' THEN 'PUT' ELSE 'FUT' END AS call_put_cd,
                TRY_CAST(k.setlmult AS DOUBLE) AS cont_mult,
                regexp_extract(k.isu_abbrv, '([0-9]{6}|[0-9]{4}W[0-9])', 1) AS mat_code,
                CAST(strptime(k.lsttrd_dd, '%Y/%m/%d') AS DATE) AS mat_date,
                NULLIF(TRY_CAST(k.exer_prc AS DOUBLE), 0.0) AS strike_prc
            FROM read_parquet(
                's3://${OCI_BUCKET}/v3/raw_clean/krx_deriv_info/**/*.parquet',
                union_by_name = true
            ) k
            JOIN fuopt_info u ON u.code_key = substr(k.isu_srt_cd, 2, 2)
            WHERE substr(k.isu_srt_cd, 1, 1) NOT IN ('D', '4')
              AND regexp_extract(k.isu_abbrv, '([0-9]{6}|[0-9]{4}W[0-9])', 1) <> ''
            QUALIFY ROW_NUMBER() OVER (PARTITION BY k.isu_srt_cd ORDER BY k.target_date DESC) = 1
        ),
        from_kis AS (
            SELECT
                CASE
                    WHEN substr(f.short_code, 6, 1) = 'W'
                        THEN substr(f.short_code, 1, 5) || substr(f.short_code, 7, 3)
                    ELSE substr(f.short_code, 1, 4)
                         || CASE substr(f.short_code, 5, 2)
                              WHEN '10' THEN 'A' WHEN '11' THEN 'B' WHEN '12' THEN 'C'
                              ELSE substr(f.short_code, 6, 1) END
                         || CASE WHEN substr(f.short_code, 1, 1) = 'A' THEN '000'
                                 ELSE substr(f.short_code, 7, 3) END
                END AS short_code,
                f.short_code AS kis_short_cd,
                f.kor_name AS prod_nm,
                u.prod_type,
                CASE substr(f.short_code, 1, 1) WHEN 'B' THEN 'CALL' WHEN 'C' THEN 'PUT' ELSE 'FUT' END AS call_put_cd,
                u.cont_mult,
                regexp_extract(f.kor_name, '([0-9]{6}|[0-9]{4}W[0-9])', 1) AS mat_code,
                NULLIF(TRY_CAST(f.acpr AS DOUBLE), 0.0) AS strike_prc
            FROM read_parquet(
                's3://${OCI_BUCKET}/v3/raw_clean/fo_idx_code_mst/**/*.parquet',
                union_by_name = true
            ) f
            JOIN fuopt_info u ON u.code_key = substr(f.short_code, 2, 2)
            WHERE f.trade_at = (
                SELECT MAX(trade_at)
                FROM read_parquet('s3://${OCI_BUCKET}/v3/raw_clean/fo_idx_code_mst/**/*.parquet', union_by_name = true)
            )
              AND regexp_extract(f.kor_name, '([0-9]{6}|[0-9]{4}W[0-9])', 1) <> ''
            QUALIFY ROW_NUMBER() OVER (PARTITION BY f.short_code ORDER BY f.trade_at DESC) = 1
        ),
        step1 AS (
            SELECT
                COALESCE(t.short_code,   k.short_code)   AS short_code,
                COALESCE(t.kis_short_cd, k.kis_short_cd) AS kis_short_cd,
                COALESCE(t.prod_nm,      k.prod_nm)      AS prod_nm,
                COALESCE(t.prod_type,    k.prod_type)    AS prod_type,
                COALESCE(t.call_put_cd,  k.call_put_cd)  AS call_put_cd,
                t.ul_code,
                t.ul_nm,
                COALESCE(t.cont_mult,    k.cont_mult)    AS cont_mult,
                COALESCE(t.mat_code,     k.mat_code)     AS mat_code,
                CASE
                    WHEN t.short_code IS NULL THEN k.mat_date
                    WHEN k.short_code IS NOT NULL AND t.description LIKE 'from fo_idx_code_mst%' THEN k.mat_date
                    ELSE CAST(t.mat_date AS DATE)
                END AS mat_date,
                CAST(t.front_date AS DATE) AS front_date,
                COALESCE(t.strike_prc, k.strike_prc) AS strike_prc,
                CASE
                    WHEN t.short_code IS NULL THEN NULL
                    WHEN k.short_code IS NOT NULL AND t.description LIKE 'from fo_idx_code_mst%' THEN NULL
                    ELSE t.description
                END AS description
            FROM existing t
            FULL OUTER JOIN from_krx k ON t.short_code = k.short_code
        ),
        step2 AS (
            SELECT * FROM step1
            UNION ALL
            SELECT
                f.short_code, f.kis_short_cd, f.prod_nm, f.prod_type, f.call_put_cd,
                NULL AS ul_code, NULL AS ul_nm, f.cont_mult, f.mat_code,
                NULL AS mat_date, NULL AS front_date, f.strike_prc,
                'from fo_idx_code_mst; expiry unconfirmed' AS description
            FROM from_kis f
            LEFT JOIN step1 s1 ON s1.short_code = f.short_code
            WHERE s1.short_code IS NULL
        )
        SELECT
            s.short_code,
            s.kis_short_cd,
            s.prod_nm,
            s.prod_type,
            s.call_put_cd,
            COALESCE(s.ul_code, u.ul_code) AS ul_code,
            COALESCE(s.ul_nm,   u.ul_nm)   AS ul_nm,
            s.cont_mult,
            s.mat_code,
            CASE
                WHEN s.mat_date IS NULL OR s.description LIKE 'from fo_idx_code_mst%'
                    THEN COALESCE(CAST(m.mat_date AS DATE), s.mat_date)
                ELSE s.mat_date
            END AS mat_date,
            CASE
                WHEN s.mat_date IS NULL OR s.description LIKE 'from fo_idx_code_mst%'
                    THEN COALESCE(CAST(m.prev_mat_date AS DATE), s.front_date)
                ELSE s.front_date
            END AS front_date,
            s.strike_prc,
            s.description,
            CURRENT_TIMESTAMP AS updated_at
        FROM step2 s
        LEFT JOIN fuopt_info u ON substr(s.short_code, 2, 2) = u.code_key
        LEFT JOIN maturity m   ON s.prod_type = m.prod_type AND s.mat_code = m.mat_code
        ORDER BY s.short_code
    $$,
    'krx_deriv_info + fo_idx_code_mst + meta_fuopt_info + meta_maturity 로 선물옵션 마스터 mst_fuopt 갱신'
)
ON CONFLICT (job_id) DO UPDATE SET
    schedule_tag = EXCLUDED.schedule_tag, run_order = EXCLUDED.run_order,
    output_layer = EXCLUDED.output_layer, output_rel_path = EXCLUDED.output_rel_path,
    targets = EXCLUDED.targets, memory_limit = EXCLUDED.memory_limit,
    threads = EXCLUDED.threads, min_rows = EXCLUDED.min_rows,
    sql_text = EXCLUDED.sql_text, note = EXCLUDED.note, updated_at = now();

-- 4. REFRESH_REF_MST_BOND (sp_mst_bond_sync — daily_batch2 수집 앞)
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows,
    sql_text, note
) VALUES (
    'REFRESH_REF_MST_BOND', 'daily_batch2', 50, TRUE,
    'ref', 'mst_bond.parquet', ARRAY['oci'], '512MB', 2, 1,
    $$
        WITH existing AS (
            SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/ref/mst_bond.parquet')
        ),
        incoming AS (
            SELECT
                isin,
                MIN(substr(trade_at, 1, 8))                 AS first_seen,
                arg_max(kor_secn_nm,   substr(trade_at, 1, 8)) AS kor_secn_nm,
                arg_max(secn_kacd,     substr(trade_at, 1, 8)) AS secn_kacd,
                arg_max(codevalue_nm,  substr(trade_at, 1, 8)) AS codevalue_nm,
                arg_max(issuco_custno, substr(trade_at, 1, 8)) AS issuco_custno,
                arg_max(issu_dt,       substr(trade_at, 1, 8)) AS issu_dt
            FROM read_parquet(
                's3://${OCI_BUCKET}/v3/raw_clean/ksd_bond_isin/**/*.parquet',
                union_by_name = true
            )
            WHERE isin IS NOT NULL AND trade_at IS NOT NULL
            GROUP BY isin
        )
        SELECT
            COALESCE(t.isin,          s.isin)          AS isin,
            COALESCE(t.kor_secn_nm,   s.kor_secn_nm)   AS kor_secn_nm,
            COALESCE(t.secn_kacd,     s.secn_kacd)     AS secn_kacd,
            COALESCE(t.codevalue_nm,  s.codevalue_nm)  AS codevalue_nm,
            COALESCE(t.issuco_custno, s.issuco_custno) AS issuco_custno,
            COALESCE(t.issu_dt,       s.issu_dt)       AS issu_dt,
            COALESCE(t.first_seen,    s.first_seen)    AS first_seen,
            t.description,
            CURRENT_TIMESTAMP AS updated_at
        FROM existing t
        FULL OUTER JOIN incoming s ON t.isin = s.isin
        ORDER BY isin
    $$,
    'ksd_bond_isin 신규 ISIN 만 mst_bond 에 적재 (기존 행 보존, first_seen 기록)'
)
ON CONFLICT (job_id) DO UPDATE SET
    schedule_tag = EXCLUDED.schedule_tag, run_order = EXCLUDED.run_order,
    output_layer = EXCLUDED.output_layer, output_rel_path = EXCLUDED.output_rel_path,
    targets = EXCLUDED.targets, memory_limit = EXCLUDED.memory_limit,
    threads = EXCLUDED.threads, min_rows = EXCLUDED.min_rows,
    sql_text = EXCLUDED.sql_text, note = EXCLUDED.note, updated_at = now();

-- 5. REFRESH_REF_MST_ETF (sp_mst_etf_sync — daily_batch2 수집 앞, OCI + R2 발행 대상)
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows,
    sql_text, note
) VALUES (
    'REFRESH_REF_MST_ETF', 'daily_batch2', 60, TRUE,
    'ref', 'mst_etf.parquet', ARRAY['oci', 'r2'], '512MB', 2, 1,
    $$
        WITH existing AS (
            SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/ref/mst_etf.parquet')
        ),
        krx_list AS (
            SELECT
                isu_srt_cd AS short_code,
                isu_cd     AS isin,
                isu_nm     AS prod_nm,
                isu_abbrv  AS prod_snm,
                isu_eng_nm AS prod_enm,
                REPLACE(list_dd, '/', '') AS list_dt,
                etf_obj_idx_nm            AS target_idx_nm,
                idx_calc_inst_nm2         AS trace_mtd,
                etf_replica_methd_tp_cd   AS replica_mtd,
                idx_mkt_clss_nm           AS mkt_div,
                idx_asst_clss_nm          AS idx_asst_class,
                com_abbrv                 AS amc,
                TRY_CAST(cu_qty AS BIGINT)      AS cu_qty,
                TRY_CAST(list_shrs AS BIGINT)   AS list_cnt,
                TRY_CAST(etf_tot_fee AS DOUBLE) AS etf_tot_fee,
                tax_tp_cd                 AS tax_div
            FROM read_parquet(
                's3://${OCI_BUCKET}/v3/raw_clean/krx_etf_list/**/*.parquet',
                union_by_name = true
            )
            WHERE isu_srt_cd IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY isu_srt_cd ORDER BY bas_dd DESC) = 1
        ),
        krx_daily AS (
            SELECT
                isu_cd AS short_code,
                MIN(bas_dd) AS first_seen,
                arg_max(isu_nm, bas_dd) AS prod_nm
            FROM read_parquet(
                's3://${OCI_BUCKET}/v3/raw_clean/krx_etf_daily/**/*.parquet',
                union_by_name = true
            )
            WHERE isu_cd IS NOT NULL
            GROUP BY isu_cd
        ),
        amc_ace AS (
            SELECT
                substr(stockcd, 4, 6) AS short_code,
                arg_min(fundcd, fundcd)  AS amc_etf_cd,
                arg_min(stockcd, fundcd) AS isin
            FROM read_parquet(
                's3://${OCI_BUCKET}/v3/raw_clean/ace_etf_list/**/*.parquet',
                union_by_name = true
            )
            WHERE stockcd IS NOT NULL AND LENGTH(stockcd) = 12
            GROUP BY substr(stockcd, 4, 6)
        ),
        manual_etf AS (
            SELECT isu_cd AS short_code, MAX(amc_etf_cd) AS amc_etf_cd
            FROM read_parquet('s3://${OCI_BUCKET}/v3/ref/user_etf.parquet')
            WHERE amc IS NOT NULL AND amc_etf_cd IS NOT NULL
            GROUP BY isu_cd
        ),
        keys AS (
            SELECT short_code FROM existing
            UNION SELECT short_code FROM krx_list
            UNION SELECT short_code FROM krx_daily
        )
        SELECT
            k.short_code,
            COALESCE(l.isin, t.isin, a.isin)             AS isin,
            COALESCE(l.prod_nm, t.prod_nm, d.prod_nm)    AS prod_nm,
            COALESCE(l.prod_snm, t.prod_snm)             AS prod_snm,
            COALESCE(l.prod_enm, t.prod_enm)             AS prod_enm,
            COALESCE(l.list_dt, t.list_dt)               AS list_dt,
            COALESCE(l.target_idx_nm, t.target_idx_nm)   AS target_idx_nm,
            COALESCE(l.trace_mtd, t.trace_mtd)           AS trace_mtd,
            COALESCE(l.replica_mtd, t.replica_mtd)       AS replica_mtd,
            COALESCE(l.mkt_div, t.mkt_div)               AS mkt_div,
            COALESCE(l.idx_asst_class, t.idx_asst_class) AS idx_asst_class,
            COALESCE(l.amc, t.amc)                       AS amc,
            COALESCE(m.amc_etf_cd, t.amc_etf_cd, a.amc_etf_cd) AS amc_etf_cd,
            COALESCE(l.cu_qty, t.cu_qty)                 AS cu_qty,
            COALESCE(l.list_cnt, t.list_cnt)             AS list_cnt,
            COALESCE(l.etf_tot_fee, t.etf_tot_fee)       AS etf_tot_fee,
            COALESCE(l.tax_div, t.tax_div)               AS tax_div,
            COALESCE(t.first_seen, d.first_seen)         AS first_seen,
            t.description,
            CURRENT_TIMESTAMP AS updated_at
        FROM keys k
        LEFT JOIN existing   t ON t.short_code = k.short_code
        LEFT JOIN krx_list   l ON l.short_code = k.short_code
        LEFT JOIN krx_daily  d ON d.short_code = k.short_code
        LEFT JOIN amc_ace    a ON a.short_code = k.short_code
        LEFT JOIN manual_etf m ON m.short_code = k.short_code
        ORDER BY k.short_code
    $$,
    'KRX ETF 목록/시세 + 운용사 목록 + 수기 user_etf 보정 병합해 mst_etf 갱신 (OCI+R2 발행)'
)
ON CONFLICT (job_id) DO UPDATE SET
    schedule_tag = EXCLUDED.schedule_tag, run_order = EXCLUDED.run_order,
    output_layer = EXCLUDED.output_layer, output_rel_path = EXCLUDED.output_rel_path,
    targets = EXCLUDED.targets, memory_limit = EXCLUDED.memory_limit,
    threads = EXCLUDED.threads, min_rows = EXCLUDED.min_rows,
    sql_text = EXCLUDED.sql_text, note = EXCLUDED.note, updated_at = now();

-- 6. REFRESH_REF_STOCK_INDEX_HIS (sp_stock_index_his_sync + sp_kis_index_daily_dedup)
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows,
    sql_text, note
) VALUES (
    'REFRESH_REF_STOCK_INDEX_HIS', 'daily_close', 180, TRUE,
    'ref', 'stock_index_his.parquet', ARRAY['oci'], '512MB', 2, 1,
    $$
        WITH existing AS (
            SELECT * FROM read_parquet('s3://${OCI_BUCKET}/v3/ref/stock_index_his.parquet')
        ),
        fuopt_mv AS (
            SELECT DISTINCT ul_code, mv_id
            FROM read_parquet('s3://${OCI_BUCKET}/v3/ref/meta_fuopt_info.parquet')
            WHERE mv_id IS NOT NULL
        ),
        krx_idx_mst AS (
            SELECT DISTINCT idx_nm, mv_id
            FROM read_parquet('s3://${OCI_BUCKET}/v3/ref/krx_index_mst.parquet')
            WHERE mv_id IS NOT NULL
        ),
        from_kis AS (
            SELECT
                CAST(strptime(k.stck_bsop_date, '%Y%m%d') AS DATE) AS trade_date,
                u.mv_id,
                TRY_CAST(k.bstp_nmix_prpr AS DOUBLE) AS close_price,
                TRY_CAST(k.bstp_nmix_oprc AS DOUBLE) AS open_price,
                TRY_CAST(k.bstp_nmix_hgpr AS DOUBLE) AS high_price,
                TRY_CAST(k.bstp_nmix_lwpr AS DOUBLE) AS low_price,
                TRY_CAST(k.acml_vol AS BIGINT) * 1000         AS volume,
                TRY_CAST(k.acml_tr_pbmn AS BIGINT) * 1000000  AS trading_value
            FROM read_parquet(
                's3://${OCI_BUCKET}/v3/raw_clean/kis_index_daily/**/*.parquet',
                union_by_name = true
            ) k
            JOIN fuopt_mv u ON u.ul_code = k.short_code
            WHERE k.bstp_nmix_prpr IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY k.stck_bsop_date, u.mv_id ORDER BY k.target_date DESC) = 1
        ),
        from_krx AS (
            SELECT
                CAST(strptime(k.bas_dd, '%Y%m%d') AS DATE) AS trade_date,
                m.mv_id,
                TRY_CAST(k.clsprc_idx AS DOUBLE) AS close_price,
                TRY_CAST(k.opnprc_idx AS DOUBLE) AS open_price,
                TRY_CAST(k.hgprc_idx  AS DOUBLE) AS high_price,
                TRY_CAST(k.lwprc_idx  AS DOUBLE) AS low_price,
                TRY_CAST(k.acc_trdvol AS BIGINT) AS volume,
                TRY_CAST(k.acc_trdval AS BIGINT) AS trading_value,
                TRY_CAST(k.mktcap     AS DOUBLE) AS listed_market_cap
            FROM read_parquet(
                's3://${OCI_BUCKET}/v3/raw_clean/krx_index_daily/**/*.parquet',
                union_by_name = true
            ) k
            JOIN krx_idx_mst m ON m.idx_nm = k.idx_nm
            WHERE k.clsprc_idx IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY k.bas_dd, m.mv_id ORDER BY k.target_date DESC) = 1
        ),
        keys AS (
            SELECT CAST(trade_date AS DATE) AS trade_date, mv_id FROM existing
            UNION SELECT trade_date, mv_id FROM from_kis
            UNION SELECT trade_date, mv_id FROM from_krx
        ),
        merged AS (
            SELECT
                k.trade_date,
                k.mv_id,
                COALESCE(x.close_price, t.close_price, i.close_price) AS close_price,
                CASE
                    WHEN x.close_price IS NOT NULL AND t.close_price IS NOT NULL AND x.close_price <> t.close_price THEN NULL
                    ELSE t.price_change
                END AS orig_price_change,
                CASE
                    WHEN x.close_price IS NOT NULL AND t.close_price IS NOT NULL AND x.close_price <> t.close_price THEN NULL
                    ELSE t.change_rate
                END AS orig_change_rate,
                COALESCE(x.open_price,    t.open_price,    i.open_price)    AS open_price,
                COALESCE(x.high_price,    t.high_price,    i.high_price)    AS high_price,
                COALESCE(x.low_price,     t.low_price,     i.low_price)     AS low_price,
                COALESCE(x.volume,        t.volume,        i.volume)        AS volume,
                COALESCE(x.trading_value, t.trading_value, i.trading_value) AS trading_value,
                COALESCE(x.listed_market_cap, t.listed_market_cap)          AS listed_market_cap
            FROM keys k
            LEFT JOIN existing t ON CAST(t.trade_date AS DATE) = k.trade_date AND t.mv_id = k.mv_id
            LEFT JOIN from_kis i ON i.trade_date = k.trade_date AND i.mv_id = k.mv_id
            LEFT JOIN from_krx x ON x.trade_date = k.trade_date AND x.mv_id = k.mv_id
        ),
        with_prev AS (
            SELECT *, LAG(close_price) OVER (PARTITION BY mv_id ORDER BY trade_date) AS prev_close
            FROM merged
        )
        SELECT
            trade_date,
            mv_id,
            close_price,
            COALESCE(orig_price_change, CASE WHEN prev_close IS NOT NULL THEN close_price - prev_close END) AS price_change,
            COALESCE(orig_change_rate,  CASE WHEN prev_close IS NOT NULL AND prev_close <> 0 THEN ROUND((close_price - prev_close) / prev_close * 100, 2) END) AS change_rate,
            open_price,
            high_price,
            low_price,
            volume,
            trading_value,
            listed_market_cap,
            CURRENT_TIMESTAMP AS updated_at
        FROM with_prev
        ORDER BY mv_id, trade_date
    $$,
    'kis_index_daily(당일 우선채움) + krx_index_daily(확정 덮어쓰기) 병합 및 전일대비/등락률 계산'
)
ON CONFLICT (job_id) DO UPDATE SET
    schedule_tag = EXCLUDED.schedule_tag, run_order = EXCLUDED.run_order,
    output_layer = EXCLUDED.output_layer, output_rel_path = EXCLUDED.output_rel_path,
    targets = EXCLUDED.targets, memory_limit = EXCLUDED.memory_limit,
    threads = EXCLUDED.threads, min_rows = EXCLUDED.min_rows,
    sql_text = EXCLUDED.sql_text, note = EXCLUDED.note, updated_at = now();

-- 7. REFRESH_REF_V_K2I_ATM (v_k2i_atm MV -> v3/ref/v_k2i_atm.parquet)
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows,
    sql_text, note
) VALUES (
    'REFRESH_REF_V_K2I_ATM', 'daily_close', 190, TRUE,
    'ref', 'v_k2i_atm.parquet', ARRAY['oci', 'r2'], '512MB', 2, 1,
    $$
        WITH idx AS (
            SELECT CAST(trade_date AS DATE) AS trade_date, close_price
            FROM read_parquet('s3://${OCI_BUCKET}/v3/ref/stock_index_his.parquet')
            WHERE mv_id = 'K2I'
        ),
        mat AS (
            SELECT
                prod_type,
                CAST(mat_date AS DATE)      AS mat_date,
                CAST(prev_mat_date AS DATE) AS prev_mat_date
            FROM read_parquet('s3://${OCI_BUCKET}/v3/ref/meta_maturity.parquet')
        )
        SELECT
            h.trade_date,
            h.close_price AS k2i_close,
            ROUND(h.close_price / 2.5) * 2.5 AS atm,
            CASE WHEN ABS(h.close_price - ROUND(h.close_price / 2.5) * 2.5) <= 0.7
                 THEN ROUND(h.close_price / 2.5) * 2.5
                 ELSE CEIL(h.close_price / 2.5) * 2.5 END  AS call_atm,
            CASE WHEN ABS(h.close_price - ROUND(h.close_price / 2.5) * 2.5) <= 0.7
                 THEN ROUND(h.close_price / 2.5) * 2.5
                 ELSE FLOOR(h.close_price / 2.5) * 2.5 END AS put_atm,
            k.mat_date  AS mat_date,
            wi.mat_date AS wki_date,
            wm.mat_date AS wkm_date
        FROM idx h
        JOIN mat k       ON k.prod_type  = 'K2I' AND h.trade_date > k.prev_mat_date  AND h.trade_date <= k.mat_date
        LEFT JOIN mat wi ON wi.prod_type = 'WKI' AND h.trade_date > wi.prev_mat_date AND h.trade_date <= wi.mat_date
        LEFT JOIN mat wm ON wm.prod_type = 'WKM' AND h.trade_date > wm.prev_mat_date AND h.trade_date <= wm.mat_date
        ORDER BY h.trade_date
    $$,
    'KOSPI200 일자별 ATM(2.5 격자 ±0.7) 및 월물/위클리 만기일 파생 (기존 v_k2i_atm MV 대체)'
)
ON CONFLICT (job_id) DO UPDATE SET
    schedule_tag = EXCLUDED.schedule_tag, run_order = EXCLUDED.run_order,
    output_layer = EXCLUDED.output_layer, output_rel_path = EXCLUDED.output_rel_path,
    targets = EXCLUDED.targets, memory_limit = EXCLUDED.memory_limit,
    threads = EXCLUDED.threads, min_rows = EXCLUDED.min_rows,
    sql_text = EXCLUDED.sql_text, note = EXCLUDED.note, updated_at = now();

-- ----------------------------------------------------------------------------
-- [그룹 B-1] 일별 Curated 병합 & 중복제거 (14개 — sp_run_daily_export 전체)
-- ----------------------------------------------------------------------------

-- 8. CURATE_KIS_FUTOPT_PRICE_DAILY
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, sql_text, note
) VALUES (
    'CURATE_KIS_FUTOPT_PRICE_DAILY', 'daily_close', 200, TRUE,
    'curated', 'kis_futopt_price/ym=${TARGET_YM}/dt=${TARGET_DATE}/data.parquet',
    ARRAY['oci'], '1GB', 2, 0,
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/raw_clean/kis_futopt_price/dt=${TARGET_DATE}/*.parquet', union_by_name = true)
        ORDER BY short_code, trade_at
    $$,
    '당일 선물옵션 3분 시세 raw_clean 을 중복제거·정렬해 curated 일별 1파일로 병합'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, run_order = EXCLUDED.run_order, updated_at = now();

-- 9. CURATE_KIS_FUTOPT_PRICE1_DAILY (v_fuopt_price: 체결 있는 행 futs_prpr <> 0)
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, sql_text, note
) VALUES (
    'CURATE_KIS_FUTOPT_PRICE1_DAILY', 'daily_close', 205, TRUE,
    'curated', 'kis_futopt_price1/ym=${TARGET_YM}/dt=${TARGET_DATE}/data.parquet',
    ARRAY['oci', 'r2'], '1GB', 2, 0,
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/raw_clean/kis_futopt_price/dt=${TARGET_DATE}/*.parquet', union_by_name = true)
        WHERE TRY_CAST(futs_prpr AS DOUBLE) <> 0
        ORDER BY short_code, trade_at
    $$,
    '당일 선물옵션 시세 중 체결 있는 행(futs_prpr <> 0)만 추출 (기존 v_fuopt_price 대체)'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, run_order = EXCLUDED.run_order, updated_at = now();

-- 10. CURATE_KIS_FUTOPT_CHART_DAILY
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, sql_text, note
) VALUES (
    'CURATE_KIS_FUTOPT_CHART_DAILY', 'daily_close', 210, TRUE,
    'curated', 'kis_futopt_chart/ym=${TARGET_YM}/dt=${TARGET_DATE}/data.parquet',
    ARRAY['oci'], '1GB', 2, 0,
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/raw_clean/kis_futopt_chart/dt=${TARGET_DATE}/*.parquet', union_by_name = true)
        ORDER BY short_code, stck_bsop_date, stck_cntg_hour
    $$,
    '당일 선물옵션 분봉 raw_clean 중복제거·정렬 병합'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, run_order = EXCLUDED.run_order, updated_at = now();

-- 11. CURATE_KIS_FUTOPT_DAILY
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, sql_text, note
) VALUES (
    'CURATE_KIS_FUTOPT_DAILY', 'daily_close', 215, TRUE,
    'curated', 'kis_futopt_daily/ym=${TARGET_YM}/dt=${TARGET_DATE}/data.parquet',
    ARRAY['oci'], '512MB', 2, 0,
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/raw_clean/kis_futopt_daily/dt=${TARGET_DATE}/*.parquet', union_by_name = true)
        ORDER BY short_code, stck_bsop_date
    $$,
    '당일 선물옵션 일봉 raw_clean 병합'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, run_order = EXCLUDED.run_order, updated_at = now();

-- 12. CURATE_KRX_OPT_DAILY
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, sql_text, note
) VALUES (
    'CURATE_KRX_OPT_DAILY', 'daily_close', 220, TRUE,
    'curated', 'krx_opt_daily/ym=${TARGET_YM}/dt=${TARGET_DATE}/data.parquet',
    ARRAY['oci'], '512MB', 2, 0,
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/raw_clean/krx_opt_daily/dt=${TARGET_DATE}/*.parquet', union_by_name = true)
        ORDER BY isu_cd, bas_dd
    $$,
    '당일 KRX 옵션 일별시세 병합'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, run_order = EXCLUDED.run_order, updated_at = now();

-- 13. CURATE_KRX_FUT_DAILY
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, sql_text, note
) VALUES (
    'CURATE_KRX_FUT_DAILY', 'daily_close', 225, TRUE,
    'curated', 'krx_fut_daily/ym=${TARGET_YM}/dt=${TARGET_DATE}/data.parquet',
    ARRAY['oci'], '512MB', 2, 0,
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/raw_clean/krx_fut_daily/dt=${TARGET_DATE}/*.parquet', union_by_name = true)
        ORDER BY isu_cd, bas_dd
    $$,
    '당일 KRX 선물 일별시세 병합'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, run_order = EXCLUDED.run_order, updated_at = now();

-- 14. CURATE_KRX_ETF_DAILY
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, sql_text, note
) VALUES (
    'CURATE_KRX_ETF_DAILY', 'daily_close', 230, TRUE,
    'curated', 'krx_etf_daily/ym=${TARGET_YM}/dt=${TARGET_DATE}/data.parquet',
    ARRAY['oci', 'r2'], '512MB', 2, 0,
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/raw_clean/krx_etf_daily/dt=${TARGET_DATE}/*.parquet', union_by_name = true)
        ORDER BY isu_cd, bas_dd
    $$,
    '당일 KRX ETF 일별시세 병합'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, run_order = EXCLUDED.run_order, updated_at = now();

-- 15. CURATE_KIS_ETF_DAILY
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, sql_text, note
) VALUES (
    'CURATE_KIS_ETF_DAILY', 'daily_close', 235, TRUE,
    'curated', 'kis_etf/ym=${TARGET_YM}/dt=${TARGET_DATE}/data.parquet',
    ARRAY['oci'], '512MB', 2, 0,
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/raw_clean/kis_etf/dt=${TARGET_DATE}/*.parquet', union_by_name = true)
    $$,
    '당일 KIS ETF 요약 병합'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, run_order = EXCLUDED.run_order, updated_at = now();

-- 16. CURATE_KIS_ETF_PDF_DAILY
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, sql_text, note
) VALUES (
    'CURATE_KIS_ETF_PDF_DAILY', 'daily_close', 240, TRUE,
    'curated', 'kis_etf_pdf/ym=${TARGET_YM}/dt=${TARGET_DATE}/data.parquet',
    ARRAY['oci'], '512MB', 2, 0,
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/raw_clean/kis_etf_pdf/dt=${TARGET_DATE}/*.parquet', union_by_name = true)
    $$,
    '당일 KIS ETF 구성종목 PDF 병합'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, run_order = EXCLUDED.run_order, updated_at = now();

-- 17. CURATE_AMC_ETF_PDF_DAILY
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, sql_text, note
) VALUES (
    'CURATE_AMC_ETF_PDF_DAILY', 'daily_close', 245, TRUE,
    'curated', 'amc_etf_pdf/ym=${TARGET_YM}/dt=${TARGET_DATE}/data.parquet',
    ARRAY['oci'], '512MB', 2, 0,
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/raw_clean/amc_etf_pdf/dt=${TARGET_DATE}/*.parquet', union_by_name = true)
    $$,
    '당일 공통 운용사(TIGER/KB/NH/KIWOOM/TIME) ETF PDF 병합'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, run_order = EXCLUDED.run_order, updated_at = now();

-- 18. CURATE_ACE_ETF_PDF_DAILY
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, sql_text, note
) VALUES (
    'CURATE_ACE_ETF_PDF_DAILY', 'daily_close', 250, TRUE,
    'curated', 'ace_etf_pdf/ym=${TARGET_YM}/dt=${TARGET_DATE}/data.parquet',
    ARRAY['oci'], '512MB', 2, 0,
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/raw_clean/ace_etf_pdf/dt=${TARGET_DATE}/*.parquet', union_by_name = true)
        ORDER BY fundcd, std_dt
    $$,
    '당일 ACE ETF PDF 병합'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, run_order = EXCLUDED.run_order, updated_at = now();

-- 19. CURATE_PLUS_ETF_PDF_DAILY
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, sql_text, note
) VALUES (
    'CURATE_PLUS_ETF_PDF_DAILY', 'daily_close', 255, TRUE,
    'curated', 'plus_etf_pdf/ym=${TARGET_YM}/dt=${TARGET_DATE}/data.parquet',
    ARRAY['oci'], '512MB', 2, 0,
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/raw_clean/plus_etf_pdf/dt=${TARGET_DATE}/*.parquet', union_by_name = true)
    $$,
    '당일 PLUS ETF PDF 병합'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, run_order = EXCLUDED.run_order, updated_at = now();

-- 20. CURATE_SOL_ETF_PDF_DAILY
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, sql_text, note
) VALUES (
    'CURATE_SOL_ETF_PDF_DAILY', 'daily_close', 260, TRUE,
    'curated', 'sol_etf_pdf/ym=${TARGET_YM}/dt=${TARGET_DATE}/data.parquet',
    ARRAY['oci'], '512MB', 2, 0,
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/raw_clean/sol_etf_pdf/dt=${TARGET_DATE}/*.parquet', union_by_name = true)
    $$,
    '당일 SOL ETF PDF 병합'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, run_order = EXCLUDED.run_order, updated_at = now();

-- 21. CURATE_KODEX_ETF_PDF_DAILY
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, sql_text, note
) VALUES (
    'CURATE_KODEX_ETF_PDF_DAILY', 'daily_close', 265, TRUE,
    'curated', 'kodex_etf_pdf/ym=${TARGET_YM}/dt=${TARGET_DATE}/data.parquet',
    ARRAY['oci'], '512MB', 2, 0,
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/raw_clean/kodex_etf_pdf/dt=${TARGET_DATE}/*.parquet', union_by_name = true)
    $$,
    '당일 KODEX ETF PDF 병합'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, run_order = EXCLUDED.run_order, updated_at = now();

-- ----------------------------------------------------------------------------
-- [그룹 B-2] 월별 정렬 Compaction (4개 — sp_run_monthly_archive 전체)
-- ----------------------------------------------------------------------------

-- 22. COMPACT_KIS_FUTOPT_CHART_MONTHLY
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, cleanup_prefix, sql_text, note
) VALUES (
    'COMPACT_KIS_FUTOPT_CHART_MONTHLY', 'monthly_archive', 200, TRUE,
    'curated', 'kis_futopt_chart/ym=${TARGET_YM}/monthly.parquet',
    ARRAY['oci'], '1GB', 2, 1,
    'v3/curated/kis_futopt_chart/ym=${TARGET_YM}/dt=',
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/curated/kis_futopt_chart/ym=${TARGET_YM}/dt=*/*.parquet', union_by_name = true)
        ORDER BY short_code, stck_bsop_date, stck_cntg_hour
    $$,
    '한 달치 선물옵션 분봉을 종목·일시 순 정렬해 monthly.parquet 1파일로 통합 후 일별 dt= 폴더 자동 정리'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, cleanup_prefix = EXCLUDED.cleanup_prefix, run_order = EXCLUDED.run_order, note = EXCLUDED.note, updated_at = now();

-- 23. COMPACT_KIS_FUTOPT_PRICE_MONTHLY
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, cleanup_prefix, sql_text, note
) VALUES (
    'COMPACT_KIS_FUTOPT_PRICE_MONTHLY', 'monthly_archive', 210, TRUE,
    'curated', 'kis_futopt_price/ym=${TARGET_YM}/monthly.parquet',
    ARRAY['oci'], '1GB', 2, 1,
    'v3/curated/kis_futopt_price/ym=${TARGET_YM}/dt=',
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/curated/kis_futopt_price/ym=${TARGET_YM}/dt=*/*.parquet', union_by_name = true)
        ORDER BY short_code, trade_at
    $$,
    '한 달치 선물옵션 3분 시세를 종목·시각 순 정렬해 monthly.parquet 1파일로 통합 후 일별 dt= 폴더 자동 정리'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, cleanup_prefix = EXCLUDED.cleanup_prefix, run_order = EXCLUDED.run_order, note = EXCLUDED.note, updated_at = now();

-- 24. COMPACT_KIS_FUTOPT_DAILY_MONTHLY
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, cleanup_prefix, sql_text, note
) VALUES (
    'COMPACT_KIS_FUTOPT_DAILY_MONTHLY', 'monthly_archive', 220, TRUE,
    'curated', 'kis_futopt_daily/ym=${TARGET_YM}/monthly.parquet',
    ARRAY['oci'], '1GB', 2, 1,
    'v3/curated/kis_futopt_daily/ym=${TARGET_YM}/dt=',
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/curated/kis_futopt_daily/ym=${TARGET_YM}/dt=*/*.parquet', union_by_name = true)
        ORDER BY short_code, stck_bsop_date
    $$,
    '한 달치 선물옵션 일봉을 monthly.parquet 1파일로 통합 후 일별 dt= 폴더 자동 정리'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, cleanup_prefix = EXCLUDED.cleanup_prefix, run_order = EXCLUDED.run_order, note = EXCLUDED.note, updated_at = now();

-- 25. COMPACT_KRX_OPT_DAILY_MONTHLY
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, cleanup_prefix, sql_text, note
) VALUES (
    'COMPACT_KRX_OPT_DAILY_MONTHLY', 'monthly_archive', 230, TRUE,
    'curated', 'krx_opt_daily/ym=${TARGET_YM}/monthly.parquet',
    ARRAY['oci'], '1GB', 2, 1,
    'v3/curated/krx_opt_daily/ym=${TARGET_YM}/dt=',
    $$
        SELECT DISTINCT *
        FROM read_parquet('s3://${OCI_BUCKET}/v3/curated/krx_opt_daily/ym=${TARGET_YM}/dt=*/*.parquet', union_by_name = true)
        ORDER BY isu_cd, bas_dd
    $$,
    '한 달치 KRX 옵션 일별시세를 monthly.parquet 1파일로 통합 후 일별 dt= 폴더 자동 정리'
) ON CONFLICT (job_id) DO UPDATE SET sql_text = EXCLUDED.sql_text, output_rel_path = EXCLUDED.output_rel_path, cleanup_prefix = EXCLUDED.cleanup_prefix, run_order = EXCLUDED.run_order, note = EXCLUDED.note, updated_at = now();

-- ----------------------------------------------------------------------------
-- [그룹 C] 선택 보조 작업 (2개 — sp_ksd_no_detail_mark, sp_src_code_sync)
-- ----------------------------------------------------------------------------

-- 26. MARK_KSD_BOND_NO_DETAIL (sp_ksd_no_detail_mark — daily_batch2 채권 수집 앞)
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, sql_text, note
) VALUES (
    'MARK_KSD_BOND_NO_DETAIL', 'daily_batch2', 70, TRUE,
    'ref', 'ksd_bond_no_detail.parquet', ARRAY['oci'], '512MB', 2, 0,
    $$
        WITH existing AS (
            SELECT isin, CAST(marked_at AS TIMESTAMP) AS marked_at, note
            FROM read_parquet('s3://${OCI_BUCKET}/v3/ref/ksd_bond_no_detail.parquet')
        ),
        zero_logs AS (
            SELECT DISTINCT partition_key AS isin
            FROM read_json_auto('${LEDGER_DIR}/job_log_*.ndjson', format = 'newline_delimited')
            WHERE api_id = 'KSD_BOND_INFO_web'
              AND status IN ('SUCCESS', 'EMPTY')
              AND COALESCE(record_count, 0) = 0
        ),
        newly_marked AS (
            SELECT
                m.isin,
                CURRENT_TIMESTAMP AS marked_at,
                'KSD_BOND_INFO_web closed with 0 rows' AS note
            FROM read_parquet('s3://${OCI_BUCKET}/v3/ref/mst_bond.parquet') m
            JOIN zero_logs l ON l.isin = m.isin
            LEFT JOIN existing e ON e.isin = m.isin
            WHERE e.isin IS NULL
        )
        SELECT isin, marked_at, note FROM existing
        UNION ALL
        SELECT isin, marked_at, note FROM newly_marked
        ORDER BY isin
    $$,
    '세이브로 상세 0행 종료 ISIN 을 v3/ref/ksd_bond_no_detail.parquet 에 영구 마킹 (sp_ksd_no_detail_mark 이관)'
) ON CONFLICT (job_id) DO UPDATE SET
    schedule_tag = EXCLUDED.schedule_tag, run_order = EXCLUDED.run_order,
    output_layer = EXCLUDED.output_layer, output_rel_path = EXCLUDED.output_rel_path,
    targets = EXCLUDED.targets, memory_limit = EXCLUDED.memory_limit,
    threads = EXCLUDED.threads, min_rows = EXCLUDED.min_rows,
    sql_text = EXCLUDED.sql_text, note = EXCLUDED.note, updated_at = now();

-- 27. HARVEST_SRC_CODE (sp_src_code_sync — 내장 DuckDB 동적 컬럼 수확기)
INSERT INTO etl_job (
    job_id, schedule_tag, run_order, is_active,
    output_layer, output_rel_path, targets, memory_limit, threads, min_rows, sql_text, note
) VALUES (
    'HARVEST_SRC_CODE', 'daily_batch2', 250, TRUE,
    'ref', 'src_code.parquet', ARRAY['oci'], '512MB', 2, 0,
    '-- builtin:harvest_src_code',
    'v3/raw_clean/* 저카디널리티(2~40) 문자열 코드값 자동 수확 -> v3/ref/src_code.parquet (sp_src_code_sync 이관)'
) ON CONFLICT (job_id) DO UPDATE SET
    schedule_tag = EXCLUDED.schedule_tag, run_order = EXCLUDED.run_order,
    output_layer = EXCLUDED.output_layer, output_rel_path = EXCLUDED.output_rel_path,
    targets = EXCLUDED.targets, memory_limit = EXCLUDED.memory_limit,
    threads = EXCLUDED.threads, min_rows = EXCLUDED.min_rows,
    sql_text = EXCLUDED.sql_text, note = EXCLUDED.note, updated_at = now();
