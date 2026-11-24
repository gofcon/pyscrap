-- SCRAP 계정: 엔진 부기 테이블 4개 (v2)
--
-- docs/SCHEMA-V2.md 4절의 컬럼 재정의를 반영한 것이다. 결과 테이블(02)과 달리
-- 이 넷은 구조가 바뀌므로 손으로 적는다 -- 모델에서 뽑으면 v1 형태가 나온다.
--
-- 바뀐 것만 짚으면:
--   api_mst.key_params_list   -> job_key_params + row_stamp_json
--                                + is_repeated + snapshot_column
--   api_mst.response_parse_json -> parse_json + session_json + staging_json
--                                + empty_is_pending (컬럼으로)
--   api_mst.request_type      -> protocol + method
--   api_mst.api_group         -> source + domain
--   api_mst.output_tables_json -> record_map_json
--   builder.macro_params_json -> cartesian_param_json
--   *.execution_cycle         -> schedule_tag + snapshot_unit
--   *.description             -> note (사람) + engine_note (엔진)
--   api_job_log               -> error_code/error_detail/record_count/
--                                duration_ms/attempt_no/target_day 추가
--
-- 예약어 NOW 와 none_mapped 는 없다. 반복 여부는 is_repeated 가, 시각을
-- 어느 컬럼에 남기는가는 snapshot_column 이 말한다.
--
-- Oracle 26ai 기준 (BOOLEAN / JSON / XMLTYPE 네이티브 사용 가능 -- 확인함).

-- ---------------------------------------------------------------------------
-- api_mst : API 한 개의 정의
-- ---------------------------------------------------------------------------
CREATE TABLE api_mst (
  api_id             VARCHAR2(150 CHAR)  NOT NULL,
  api_name           VARCHAR2(100 CHAR)  NOT NULL,

  -- api_group 을 두 축으로 가른 것. 13개 값이 소스/용도/전송방식을 섞어
  -- 쓰고 있었다 (docs/SCHEMA-V2.md 7.4).
  source             VARCHAR2(20 CHAR)   NOT NULL,   -- KIS KRX KSD DART AMC
  domain             VARCHAR2(20 CHAR),              -- DERIV ETF BOND STOCK AUTH

  -- request_type 이 요청 방식과 HTTP 메서드를 겸하고 있었다. 가르면
  -- 체크 제약이 단순해지고 BROWSER 행에 메서드가 없다는 사실이 드러난다.
  -- 이 값이 어느 스크래퍼로 갈지를 혼자 결정한다 (app.scrapers.make_scraper).
  protocol           VARCHAR2(10 CHAR)   DEFAULT 'HTTP' NOT NULL,  -- HTTP BROWSER
  method             VARCHAR2(10 CHAR),                            -- GET POST

  api_url            VARCHAR2(500 CHAR)  NOT NULL,
  -- v1 은 NOT NULL 이었는데 빈 dict 인 행이 많았다. 없으면 없는 것으로 둔다.
  header_json        JSON,

  -- 요청의 몸통. HTTP 는 payload_*, BROWSER 는 behavior_json 으로 서로
  -- 배타적이다 (ck_api_mst_body 참고).
  payload_type       VARCHAR2(20 CHAR),              -- json data content
  payload_json       JSON,
  payload_xml        XMLTYPE,
  behavior_json      JSON,

  response_type      VARCHAR2(20 CHAR),
  -- v1 의 response_parse_json 을 셋으로 가른 것. 로그인 신호와 파일 적재
  -- 위치는 파싱 명세가 아니다 (docs/SCHEMA-V2.md 4.4).
  parse_json         JSON,   -- delimiter fields has_header skip_rows encoding
                             -- inner_file table_index sheet / css spec
  session_json       JSON,   -- {"login": "<api_id>", "logged_out": "...",
                             --  "expect": "..."}
  staging_json       JSON,   -- {"group": "...", "name": ":KEY.pdf"}

  persist_env_json   JSON,
  record_map_json    JSON,   -- {셀렉터: 테이블}. v1 output_tables_json --
                             -- 키가 테이블이 아니라 셀렉터라 이름을 고쳤다.
  merge_fields_json  JSON,

  -- v1 key_params_list 가 한 컬럼으로 결정했던 세 가지를 가른 것.
  -- 그래서 예약어 NOW / none_mapped 가 필요했다 (docs/SCHEMA-V2.md 4.1).
  job_key_params     JSON,                 -- job_id 재료. ["SHORT_CODE"]
  row_stamp_json     JSON,                 -- 행에 찍을 것. {파라미터: 컬럼}
  is_repeated        BOOLEAN DEFAULT FALSE NOT NULL,  -- 성공 후에도 활성 유지
  snapshot_column    VARCHAR2(30 CHAR),    -- 실행 시각을 받을 컬럼. 없으면 NULL

  -- 0행 성공을 "끝났다" 로 보지 않는다. KRX 오픈API 는 아직 안 낸 날을 0행으로
  -- 답하므로, 그 행은 0행이면 잡을 닫지 않고 다음 사이클에 다시 묻는다.
  --
  -- is_repeated 의 형제다 -- 둘 다 "이 잡은 언제 끝났는가" 를 말한다. v1 은
  -- 이것을 response_parse_json 안의 키로 두었는데, 쓰는 다섯 행
  -- (KRX_ETF · KRX_OPTION_DAILY · KRX_INDEX_KOSPI · KRX_STOCK_INFO ·
  -- KRX_STOCK_BASE_INFO)의 그 컬럼에는 **그 키 하나뿐**이었다. 파싱 명세가
  -- 전혀 없으니 parse_json/session_json/staging_json 어디에도 갈 자리가 없다.
  --
  -- 이런 스위치가 앞으로 늘 것이므로 JSON 한 칸(options_json)에 모으고 JSON
  -- 스키마 제약으로 오타를 막는 길을 재봤다. 빈 표에서는 제대로 막힌다 --
  -- additionalProperties:false 가 오타와 모르는 키를 ORA-40875 로 거부한다.
  -- 가상 컬럼으로 노출하면 질의도 안 바뀌고, 인덱스도 걸린다.
  --
  -- 그런데 **api_mst 에서는 그 제약이 평가되지 않았다.** ENABLED VALIDATED 이고
  -- 조건을 저장된 행에 직접 걸면 FAIL 인데 INSERT 가 통과한다. 같은 표의
  -- ck_api_mst_snapshot 은 정상 동작한다(ORA-02290 확인). 빈 표에 같은 조건을
  -- 그대로 옮기면 거부한다. 원인을 찾지 못했다 -- 스키마의 줄바꿈, 가상 컬럼,
  -- XMLTYPE 컬럼, JSON 컬럼 여러 개를 하나씩 재현해 봤으나 모두 정상이었다.
  --
  -- 오타를 잡는 것이 유일한 목적인 장치를 설명하지 못한 채로 쓸 수는 없어서
  -- 컬럼으로 둔다. 스위치가 늘면 ALTER TABLE ADD 한 번이고, 대가는 컬럼이
  -- updated_at 뒤에 붙는 것뿐이다 -- 그 규칙은 읽기 편하라고 둔 것이고
  -- 정확성 문제가 아니다.
  empty_is_pending   BOOLEAN DEFAULT FALSE NOT NULL,

  pagination_json    JSON,

  note               VARCHAR2(400 CHAR),   -- v1 description(100) 확대
  updated_at         DATE DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT pk_api_mst PRIMARY KEY (api_id),

  -- 요청 방식이 몸통의 모양을 정한다. 한쪽이 다른 쪽 컬럼을 빌려
  -- 쓰는 일이 없게 하고, transport 가 어느 엔진으로 가는지 혼자 결정한다.
  CONSTRAINT ck_api_mst_protocol
    CHECK (protocol IN ('HTTP', 'BROWSER')),
  CONSTRAINT ck_api_mst_body CHECK (
       (protocol = 'BROWSER' AND behavior_json IS NOT NULL
                              AND payload_type IS NULL
                             AND method IS NULL)
    OR (protocol = 'HTTP'     AND behavior_json IS NULL
                             AND method IN ('GET', 'POST'))
  ),
  CONSTRAINT ck_api_mst_payload CHECK (
       (payload_type IS NULL  AND payload_json IS NULL AND payload_xml IS NULL)
    OR (payload_type IN ('json', 'data')
                           AND payload_json IS NOT NULL AND payload_xml IS NULL)
    OR (payload_type = 'content'
                           AND payload_xml IS NOT NULL AND payload_json IS NULL)
  ),
  -- 시각을 남길 컬럼이 있으면 반복 작업이다. 반대는 성립하지 않는다 --
  -- 반복이면서 시각을 안 남기는 경우가 있다 (v1 의 none_mapped 자리).
  CONSTRAINT ck_api_mst_snapshot
    CHECK (snapshot_column IS NULL OR is_repeated = TRUE)
);

CREATE INDEX ix_api_mst_source ON api_mst (source);
CREATE INDEX ix_api_mst_domain ON api_mst (domain);

COMMENT ON TABLE  api_mst IS 'API 정의 한 행 = 소스 하나. 코드가 아니라 이 행을 넣어 소스를 붙인다';
COMMENT ON COLUMN api_mst.job_key_params  IS 'job_id 를 결정하는 파라미터 이름들';
COMMENT ON COLUMN api_mst.row_stamp_json  IS '결과 행에 찍을 {파라미터: 컬럼}. 값은 실존 컬럼이어야 한다';
COMMENT ON COLUMN api_mst.is_repeated     IS 'TRUE 면 성공 후에도 활성으로 남아 매 주기 재실행';
COMMENT ON COLUMN api_mst.snapshot_column IS '실행 시각을 받을 결과 컬럼. 조회일이 없는 스냅샷 API 용';

-- ---------------------------------------------------------------------------
-- api_job_builder : 그 API 를 어떻게 펼칠까
-- ---------------------------------------------------------------------------
CREATE TABLE api_job_builder (
  build_id           VARCHAR2(50 CHAR)   NOT NULL,
  -- FK 를 걸지 않는다. 표를 드롭/재생성하며 옮기는 일이 많고, Oracle 은
  -- 컬럼 순서를 바꿀 수 없어 그 방식을 쓸 수밖에 없다 (v1 과 같은 이유).
  api_id             VARCHAR2(150 CHAR)  NOT NULL,

  -- v1 macro_params_json. 각 항목이 하나의 축이고 최종 작업 집합은 축들의
  -- **곱집합**이다 -- 이름이 그 사실을 말한다. 항목을 하나 더하면 작업 수가
  -- 더해지는 게 아니라 곱해진다는 것이 이 컬럼의 유일한 함정이다.
  cartesian_param_json JSON              NOT NULL,

  is_active          BOOLEAN DEFAULT FALSE NOT NULL,
  save_mode          VARCHAR2(20 CHAR) DEFAULT 'overwrite',

  -- v1 execution_cycle 이 "어느 타이머가 집어 가는가" 와 "스냅샷 시각을
  -- 어떤 단위로 절삭하는가" 를 겸해서 46가지로 늘었고, 절삭이 조용히 안
  -- 걸렸다 (daily_batch2 는 1m/5m/1h/1d 패턴에 안 맞아 no-op).
  schedule_tag       VARCHAR2(30 CHAR) DEFAULT 'daily' NOT NULL,
  -- 스냅샷 시각을 어느 단위까지 내려 자르나 (_floor_datetime).
  snapshot_unit      VARCHAR2(10 CHAR),   -- 1m 5m 1h 1d 1w. NULL 이면 자르지 않음

  note               VARCHAR2(400 CHAR),
  updated_at         DATE DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT pk_api_job_builder PRIMARY KEY (build_id),
  CONSTRAINT ck_api_job_builder_save
    CHECK (save_mode IN ('overwrite', 'append'))
);

CREATE INDEX ix_api_job_builder_api ON api_job_builder (api_id);
CREATE INDEX ix_api_job_builder_sch ON api_job_builder (schedule_tag);

-- ---------------------------------------------------------------------------
-- api_job : 실행 단위
-- ---------------------------------------------------------------------------
-- v1 은 job_id 에 날짜를 넣어 종목 하나가 날마다 한 행씩 늘었다 (633,996행).
-- v2 는 작업을 "무엇을 받을지" 로만 두고 날짜는 api_job_log.target_day 로
-- 옮길 수 있게 해 둔다 -- 이 DDL 은 두 방식 다 담는다. 어느 쪽으로 갈지는
-- 별도 결정이다 (docs/SCHEMA-V2.md 7.1, 10절 질문 2).
CREATE TABLE api_job (
  job_id             VARCHAR2(150 CHAR)  NOT NULL,
  build_id           VARCHAR2(50 CHAR),
  api_id             VARCHAR2(150 CHAR)  NOT NULL,
  params_json        JSON                NOT NULL,

  is_active          BOOLEAN DEFAULT TRUE NOT NULL,
  save_mode          VARCHAR2(20 CHAR) DEFAULT 'overwrite',
  schedule_tag       VARCHAR2(30 CHAR) DEFAULT 'daily' NOT NULL,
  snapshot_unit      VARCHAR2(10 CHAR),

  -- v1 은 사람이 적은 설명과 마지막 실행 결과가 같은 칸(description,
  -- VARCHAR2(100))을 썼다. 실행이 설명을 덮어서 빌더에 적어 둔 메모가
  -- 첫 실행에 사라졌다 (docs/SCHEMA-V2.md 7.2).
  note               VARCHAR2(400 CHAR),   -- 사람이 적는다. 엔진이 안 건드린다
  -- 엔진이 덮어쓴다. note 와 같은 명사를 쓰고 주인만 다르게 해서, 어느 쪽이
  -- 사람 것인지 컬럼 이름만 보고 알 수 있게 한다. 'log' 를 피한 이유는
  -- api_job_log 가 따로 있어서다 -- 이것은 이력이 아니라 마지막 한 줄이다.
  engine_note        VARCHAR2(400 CHAR),

  updated_at         DATE DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT pk_api_job PRIMARY KEY (job_id),
  CONSTRAINT ck_api_job_save CHECK (save_mode IN ('overwrite', 'append'))
);

CREATE INDEX ix_api_job_build ON api_job (build_id);
-- 주기 스캔이 (schedule_tag, is_active) 로 집어 간다. 두 컬럼 묶음 인덱스가
-- 그 질의를 그대로 받는다 -- v1 은 둘을 따로 걸어 두 번 걸렀다.
CREATE INDEX ix_api_job_pick ON api_job (schedule_tag, is_active);

-- ---------------------------------------------------------------------------
-- api_job_log : 실행 이력
-- ---------------------------------------------------------------------------
-- v1 은 5컬럼이라 SQL 로 물어볼 수 있는 것이 거의 없었다. "어제 실패 중
-- 상대 쪽 문제는 몇 건인가" 를 error_message LIKE 로 추측해야 했고, 몇 건
-- 받았는지는 로그에 아예 없었다 (api_job.description 문자열에 있고 다음
-- 실행이 덮었다).
CREATE TABLE api_job_log (
  id                 NUMBER GENERATED BY DEFAULT AS IDENTITY (START WITH 1),
  job_id             VARCHAR2(150 CHAR)  NOT NULL,

  status             VARCHAR2(20 CHAR)   NOT NULL,
  -- 예외 클래스 이름을 그대로 적는다 ('SourceRefused', 'MappingError' …).
  -- 등급이 생기면 재시도/차단기/은퇴 판단이 isinstance 분기가 아니라
  -- 데이터로 결정된다 (docs/SCHEMA-V2.md 6.2).
  error_code         VARCHAR2(40 CHAR),
  error_message      VARCHAR2(4000 CHAR),
  -- v1 은 4000자 한 칸뿐이라 응답 본문을 담을 수 없어, 무엇이 왔는지 모른 채
  -- "HTTPStatusError" 만 남는 경우가 있었다.
  error_detail       CLOB,

  record_count       NUMBER,              -- 받아서 적재한 행 수
  duration_ms        NUMBER,              -- 요청+적재 소요
  attempt_no         NUMBER DEFAULT 1,    -- 재시도 몇 번째였나
  -- 어느 날짜분을 받았나. job_id 에서 날짜를 빼는 경우(7.1) 멱등성과
  -- "그날 이미 받았나" 판단이 이 컬럼으로 옮겨온다.
  target_day         VARCHAR2(8 CHAR),

  executed_at        DATE DEFAULT CURRENT_TIMESTAMP NOT NULL,

  CONSTRAINT pk_api_job_log PRIMARY KEY (id),
  CONSTRAINT ck_api_job_log_status
    CHECK (status IN ('SUCCESS', 'FAILED', 'SKIPPED', 'BLOCKED', 'EMPTY'))
);

-- "이 작업이 그날 성공했나" 가 가장 잦은 질의가 된다 (7.1 로 가면 멱등성
-- 판단 자체가 이 인덱스를 탄다).
CREATE INDEX ix_api_job_log_job_day ON api_job_log (job_id, target_day);
-- 보존 정리(purge-job-logs)와 일자별 집계가 쓴다.
CREATE INDEX ix_api_job_log_exec ON api_job_log (executed_at);
-- "어제 실패를 사유별로" 를 받는다. 성공 행은 error_code 가 NULL 이라
-- 함수 기반이 아닌 평범한 인덱스로도 실패분만 모인다.
CREATE INDEX ix_api_job_log_code ON api_job_log (error_code, executed_at);

COMMENT ON COLUMN api_job_log.error_code   IS '예외 클래스 이름. 성공이면 NULL';
COMMENT ON COLUMN api_job_log.record_count IS '적재 행 수. v1 은 api_job.description 문자열에 있었다';
COMMENT ON COLUMN api_job_log.target_day   IS 'YYYYMMDD. 어느 날짜분을 받았나';
