-- ============================================================================
-- pyscrap v3: Neon DB (Serverless PostgreSQL) 최소 스키마
--
-- 방식 B 확정 (docs/ARCHITECTURE-V3.md):
--   - ops 스키마: 신규 운영/설정/원장 테이블만 유지
--   - ref 스키마: 재수집 불가능한 수기 입력 테이블 (user_etf, user_code_map,
--                mst_code, krx_index_mst 및 필요 시 추가하는 user_xxx) 만 유지
--   - 모든 금융시장 데이터(시세·구성종목·파생 마스터 mst_*)는 버킷 v3/ Parquet
--     으로 일원화한다.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS ref;

-- ----------------------------------------------------------------------------
-- 1. ops: 설정 · 호스트 정책 · DuckDB 잡 · 실행 원장 · 자동 이력
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ops.api_mst (
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
    ON ops.api_mst (source_name, domain);
CREATE INDEX IF NOT EXISTS ix_api_mst_status
    ON ops.api_mst (status);

-- 변경/삭제 시 직전 행을 자동 스냅샷 보관 (DB 직접 편집 롤백 안전망)
CREATE TABLE IF NOT EXISTS ops.api_mst_hist (
    hist_id         BIGSERIAL    PRIMARY KEY,
    api_id          VARCHAR(150) NOT NULL,
    action          VARCHAR(10)  NOT NULL CHECK (action IN ('UPDATE', 'DELETE')),
    changed_by      TEXT         NOT NULL DEFAULT current_user,
    changed_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    old_row         JSONB        NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_api_mst_hist_api_time
    ON ops.api_mst_hist (api_id, changed_at DESC);

CREATE OR REPLACE FUNCTION ops.fn_api_mst_audit()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ops.api_mst_hist (api_id, action, old_row)
    VALUES (OLD.api_id, TG_OP, to_jsonb(OLD));

    IF TG_OP = 'UPDATE' THEN
        NEW.updated_at := now();
        RETURN NEW;
    END IF;
    RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS trg_api_mst_audit ON ops.api_mst;
CREATE TRIGGER trg_api_mst_audit
BEFORE UPDATE OR DELETE ON ops.api_mst
FOR EACH ROW EXECUTE FUNCTION ops.fn_api_mst_audit();

-- 호스트별 호출 간격 / 차단 쿨다운 정책 (코드 하드코딩 _HOST_INTERVALS 대체)
CREATE TABLE IF NOT EXISTS ops.host_policy (
    host_suffix       VARCHAR(150) PRIMARY KEY,
    min_interval_sec  DOUBLE PRECISION NOT NULL CHECK (min_interval_sec >= 0),
    cooldown_sec      INTEGER      NOT NULL DEFAULT 600 CHECK (cooldown_sec >= 0),
    note              VARCHAR(300),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- DuckDB SQL 후처리 · 월별 Compaction · 금융 마스터/마트 갱신 정의
CREATE TABLE IF NOT EXISTS ops.duckdb_job (
    job_name          VARCHAR(100) PRIMARY KEY,
    schedule_tag      VARCHAR(30)  NOT NULL DEFAULT 'daily_close',
    is_active         BOOLEAN      NOT NULL DEFAULT TRUE,
    output_layer      VARCHAR(20)  NOT NULL CHECK (output_layer IN ('curated', 'ref', 'mart')),
    output_rel_path   VARCHAR(250) NOT NULL,
    -- 멀티 버킷 타깃: ["oci"] 또는 ["oci", "r2"]
    targets_json      JSONB        NOT NULL DEFAULT '["oci"]'::jsonb,
    memory_limit      VARCHAR(20)  NOT NULL DEFAULT '1GB',
    threads           SMALLINT     NOT NULL DEFAULT 2 CHECK (threads BETWEEN 1 AND 8),
    min_rows          INTEGER      NOT NULL DEFAULT 0 CHECK (min_rows >= 0),
    sql_body          TEXT         NOT NULL,
    note              VARCHAR(400),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 하루치 전체 배치 실행 원장 (야간 마감 시 로컬 NDJSON 에서 1회 벌크 적재)
CREATE TABLE IF NOT EXISTS ops.run_ledger (
    id                BIGSERIAL    PRIMARY KEY,
    api_id            VARCHAR(150) NOT NULL,
    partition_key     VARCHAR(150) NOT NULL,
    target_date       VARCHAR(8),
    status            VARCHAR(20)  NOT NULL
                      CHECK (status IN ('SUCCESS', 'FAILED', 'EMPTY', 'BLOCKED', 'SKIPPED')),
    error_code        VARCHAR(40),
    error_message     VARCHAR(4000),
    record_count      INTEGER,
    duration_ms       INTEGER,
    attempt_no        SMALLINT     NOT NULL DEFAULT 1,
    output_uris       JSONB        NOT NULL DEFAULT '{}'::jsonb,
    executed_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_run_ledger_lookup
    ON ops.run_ledger (api_id, target_date, status);
CREATE INDEX IF NOT EXISTS ix_run_ledger_executed
    ON ops.run_ledger (executed_at DESC);

-- ----------------------------------------------------------------------------
-- 2. ref: 재수집 불가능한 수기 입력 테이블 (필요 시 user_xxx 계속 추가)
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ref.user_etf (
    isin              VARCHAR(20)  PRIMARY KEY,
    short_code        VARCHAR(20),
    etf_name          VARCHAR(200),
    category          VARCHAR(50),
    note              VARCHAR(400),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ref.mst_code (
    code_group        VARCHAR(50)  NOT NULL,
    code              VARCHAR(50)  NOT NULL,
    code_nm           VARCHAR(200) NOT NULL,
    parent_code       VARCHAR(50),
    sort_order        INTEGER,
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (code_group, code)
);

CREATE TABLE IF NOT EXISTS ref.user_code_map (
    table_name        VARCHAR(100) NOT NULL,
    column_name       VARCHAR(100) NOT NULL,
    code_value        VARCHAR(400) NOT NULL,
    code_group        VARCHAR(50)  NOT NULL,
    code              VARCHAR(50)  NOT NULL,
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, column_name, code_value)
);

CREATE TABLE IF NOT EXISTS ref.krx_index_mst (
    mv_id             VARCHAR(30)  PRIMARY KEY,
    idx_nm            VARCHAR(200) NOT NULL,
    idx_clss          VARCHAR(50),
    note              VARCHAR(400),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 필요 시 수기 보정 테이블 추가 패턴 예시:
--   CREATE TABLE ref.user_etf_override (
--       isin VARCHAR(20) PRIMARY KEY,
--       amc_etf_cd VARCHAR(50),
--       note VARCHAR(400),
--       updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
--   );
-- DuckDB 마스터 갱신 SQL 에서 COALESCE(u.amc_etf_cd, auto.amc_etf_cd) 로 병합.

-- ----------------------------------------------------------------------------
-- 3. 최소 권한 Role (ops_writer 는 ref 스키마 SELECT 만 허용)
-- ----------------------------------------------------------------------------
-- Neon 콘솔/관리자 Role 로 별도 계정 생성 후 아래 권한 부여:
--   GRANT USAGE ON SCHEMA ops, ref TO pyscrap_ops;
--   GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ops TO pyscrap_ops;
--   GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ops TO pyscrap_ops;
--   GRANT SELECT ON ALL TABLES IN SCHEMA ref TO pyscrap_ops;
