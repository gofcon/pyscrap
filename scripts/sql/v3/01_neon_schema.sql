-- ============================================================================
-- pyscrap v3: Neon DB (Serverless PostgreSQL) 단일 public 스키마
--
-- 구조: api_mst -> api_job (SCRAPE | DUCKDB) -> api_job_log
--   - api_mst       : 외부 사이트 수집 명세 (~60행, spec_json = engine + ext)
--   - api_mst_hist  : api_mst 변경/삭제 자동 스냅샷 (롤백 안전망)
--   - api_job       : 스케줄 실행 정의 (~120행 고정, 구 api_job_builder + DuckDB 잡 통합)
--   - api_job_log   : 하루치 실행 이력 + 멱등성 원장 (야간 1회 벌크 적재)
--   - host_policy   : 호스트별 호출 간격 / 차단 쿨다운 정책
--   - 수기 테이블   : user_etf, mst_code, user_code_map, krx_index_mst,
--                     mst_index, meta_fuopt_info (필요 시 user_xxx 계속 추가)
--   - 금융 데이터   : 결과 43표 + 파생 마스터(mst_etf/mst_fuopt/mst_bond/mst_stock/
--                     meta_maturity/stock_index_his)는 전부 버킷 v3/ Parquet 으로 일원화
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0. 구 스키마/임시 테이블 정리 (멱등)
-- ----------------------------------------------------------------------------
DROP SCHEMA IF EXISTS ops, ref CASCADE;
DROP TABLE IF EXISTS duckdb_job;
DROP TABLE IF EXISTS run_ledger;

DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I RESET search_path', current_database());
END
$$;

SET search_path TO public;

-- ----------------------------------------------------------------------------
-- 1. 운영 테이블 (api_mst -> api_job -> api_job_log + host_policy)
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS api_mst (
    api_id          VARCHAR(150) PRIMARY KEY,
    api_name        VARCHAR(100) NOT NULL,
    source_name     VARCHAR(20)  NOT NULL,
    domain          VARCHAR(20),
    protocol        VARCHAR(10)  NOT NULL CHECK (protocol IN ('HTTP', 'BROWSER')),
    sink_type       VARCHAR(15)  NOT NULL CHECK (sink_type IN ('PARQUET', 'BINARY', 'SESSION')),
    status          VARCHAR(10)  NOT NULL DEFAULT 'DRAFT'
                    CHECK (status IN ('DRAFT', 'ACTIVE', 'PAUSED')),
    schema_version  SMALLINT     NOT NULL DEFAULT 1,
    -- engine: 엄격 검증 대상 / ext: 운영자 자유 확장 영역 (Python 수정 0)
    spec_json       JSONB        NOT NULL
                    CHECK (jsonb_typeof(spec_json) = 'object' AND spec_json ? 'engine'),
    note            VARCHAR(400),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_api_mst_source_domain
    ON api_mst (source_name, domain);
CREATE INDEX IF NOT EXISTS ix_api_mst_status
    ON api_mst (status);

-- 변경/삭제 시 직전 행 자동 스냅샷 보관
CREATE TABLE IF NOT EXISTS api_mst_hist (
    hist_id         BIGSERIAL    PRIMARY KEY,
    api_id          VARCHAR(150) NOT NULL,
    action          VARCHAR(10)  NOT NULL CHECK (action IN ('UPDATE', 'DELETE')),
    changed_by      TEXT         NOT NULL DEFAULT current_user,
    changed_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    old_row         JSONB        NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_api_mst_hist_api_time
    ON api_mst_hist (api_id, changed_at DESC);

CREATE OR REPLACE FUNCTION fn_api_mst_audit()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO api_mst_hist (api_id, action, old_row)
    VALUES (OLD.api_id, TG_OP, to_jsonb(OLD));

    IF TG_OP = 'UPDATE' THEN
        NEW.updated_at := now();
        RETURN NEW;
    END IF;
    RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS trg_api_mst_audit ON api_mst;
CREATE TRIGGER trg_api_mst_audit
BEFORE UPDATE OR DELETE ON api_mst
FOR EACH ROW EXECUTE FUNCTION fn_api_mst_audit();

-- 실행 정의 통합 테이블: 구 api_job_builder(SCRAPE) + DuckDB 후처리(DUCKDB)
-- 날짜×종목 조합을 DB에 미리 쌓지 않고 실행 시점에 동적 전개한다.
DROP TABLE IF EXISTS api_job;
CREATE TABLE api_job (
    job_id          VARCHAR(100) PRIMARY KEY,
    job_type        VARCHAR(10)  NOT NULL CHECK (job_type IN ('SCRAPE', 'DUCKDB')),
    api_id          VARCHAR(150),                       -- SCRAPE 일 때 api_mst.api_id 참조
    schedule_tag    VARCHAR(30)  NOT NULL DEFAULT 'daily',
    run_order       SMALLINT     NOT NULL DEFAULT 100,  -- 같은 schedule_tag 내 실행 순서
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    snapshot_unit   VARCHAR(10),                        -- 3m, 5m, 1d 등 (스냅샷 절삭 단위)
    -- SCRAPE: {"params": {...}, "save_mode": "overwrite"|"append"}
    -- DUCKDB: {"output_layer": "curated"|"ref"|"mart", "output_rel_path": "...",
    --          "targets": ["oci", ...], "memory_limit": "1GB", "threads": 2,
    --          "min_rows": 0, "sql": "..."}
    job_spec_json   JSONB        NOT NULL,
    note            VARCHAR(400),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CHECK (
        (job_type = 'SCRAPE' AND api_id IS NOT NULL) OR
        (job_type = 'DUCKDB' AND api_id IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS ix_api_job_schedule
    ON api_job (schedule_tag, is_active, run_order);
CREATE INDEX IF NOT EXISTS ix_api_job_api_id
    ON api_job (api_id);

-- 하루치 전체 배치 실행 이력 + 멱등성 원장 (야간 마감 시 로컬 NDJSON 에서 1회 벌크 적재)
CREATE TABLE IF NOT EXISTS api_job_log (
    id              BIGSERIAL    PRIMARY KEY,
    job_id          VARCHAR(100) NOT NULL,
    api_id          VARCHAR(150),
    partition_key   VARCHAR(150) NOT NULL DEFAULT '',
    target_date     VARCHAR(8),
    status          VARCHAR(20)  NOT NULL
                    CHECK (status IN ('SUCCESS', 'FAILED', 'EMPTY', 'BLOCKED', 'SKIPPED')),
    error_code      VARCHAR(40),
    error_message   VARCHAR(4000),
    record_count    INTEGER,
    duration_ms     INTEGER,
    attempt_no      SMALLINT     NOT NULL DEFAULT 1,
    output_uris     JSONB        NOT NULL DEFAULT '{}'::jsonb,
    executed_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_api_job_log_lookup
    ON api_job_log (job_id, target_date, status);
CREATE INDEX IF NOT EXISTS ix_api_job_log_executed
    ON api_job_log (executed_at DESC);

-- 호스트별 호출 간격 / 차단 쿨다운 정책 (코드 하드코딩 _HOST_INTERVALS 대체)
CREATE TABLE IF NOT EXISTS host_policy (
    host_suffix       VARCHAR(150) PRIMARY KEY,
    min_interval_sec  DOUBLE PRECISION NOT NULL CHECK (min_interval_sec >= 0),
    cooldown_sec      INTEGER      NOT NULL DEFAULT 600 CHECK (cooldown_sec >= 0),
    note              VARCHAR(300),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ----------------------------------------------------------------------------
-- 2. 수기 입력 테이블 (재수집 불가 · Oracle v2 REF 컬럼과 1:1 일치)
--    필요 시 user_xxx 보정 테이블을 같은 방식으로 추가한다.
-- ----------------------------------------------------------------------------

DROP TABLE IF EXISTS user_etf;
CREATE TABLE user_etf (
    isu_cd          VARCHAR(20)  PRIMARY KEY,
    amc             VARCHAR(20),
    amc_etf_cd      VARCHAR(30),
    description     VARCHAR(100),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

DROP TABLE IF EXISTS mst_code;
CREATE TABLE mst_code (
    code_group      VARCHAR(40)  NOT NULL,
    code            VARCHAR(40)  NOT NULL,
    code_nm         VARCHAR(200) NOT NULL,
    parent_code     VARCHAR(40),
    sort_order      INTEGER,
    description     VARCHAR(400),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (code_group, code)
);

DROP TABLE IF EXISTS user_code_map;
CREATE TABLE user_code_map (
    table_name      VARCHAR(128) NOT NULL,
    column_name     VARCHAR(128) NOT NULL,
    code_value      VARCHAR(400) NOT NULL,
    code_group      VARCHAR(40)  NOT NULL,
    code            VARCHAR(40)  NOT NULL,
    note            VARCHAR(400),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, column_name, code_value)
);

DROP TABLE IF EXISTS krx_index_mst;
CREATE TABLE krx_index_mst (
    ind_tp_cd       VARCHAR(4)   NOT NULL,
    idx_ind_cd      VARCHAR(4)   NOT NULL,
    idx_nm          VARCHAR(100) NOT NULL,
    idx_eng_nm      VARCHAR(150),
    mid_clss_cd     VARCHAR(2),
    bas_tm          VARCHAR(10),
    annc_tm         VARCHAR(10),
    bas_idx         DOUBLE PRECISION,
    calc_cycle      VARCHAR(20),
    calc_tm         VARCHAR(40),
    compst_isu_cnt  INTEGER,
    mv_id           VARCHAR(10),
    description     VARCHAR(200),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (ind_tp_cd, idx_ind_cd)
);

CREATE TABLE IF NOT EXISTS mst_index (
    mv_id           VARCHAR(10)  PRIMARY KEY,
    idx_nm          VARCHAR(100) NOT NULL,
    idx_eng_nm      VARCHAR(150),
    bas_tm          VARCHAR(10),
    annc_tm         VARCHAR(10),
    bas_idx         DOUBLE PRECISION,
    kis_short_cd    VARCHAR(10),
    description     VARCHAR(200),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS meta_fuopt_info (
    prod_type       VARCHAR(20)  NOT NULL,
    ul_code         VARCHAR(20)  NOT NULL,
    ul_nm           VARCHAR(100) NOT NULL,
    krx_idx_nm      VARCHAR(100),
    mv_id           VARCHAR(20),
    cont_mult       DOUBLE PRECISION NOT NULL,
    code_key        VARCHAR(2)   NOT NULL UNIQUE,
    exp_dow         VARCHAR(9)   NOT NULL,
    exp_week        INTEGER,
    krx_prod_ids    VARCHAR(100),
    kis_info_types  VARCHAR(100),
    description     VARCHAR(100),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (prod_type, ul_code)
);

-- 코드 매핑 조인 뷰 (v2 07_code_registry.sql 과 동일)
CREATE OR REPLACE VIEW v_code_map AS
SELECT p.table_name,
       p.column_name,
       p.code_value,
       p.code_group,
       p.code,
       m.code_nm,
       m.parent_code,
       m.sort_order
  FROM user_code_map p
  LEFT JOIN mst_code m
         ON m.code_group = p.code_group
        AND m.code       = p.code;
