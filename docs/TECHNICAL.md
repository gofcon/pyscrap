# pyscrap v3 기술 문서

설정으로 굴러가는 스크래핑·ETL 엔진. 새 소스를 붙이거나 기존 사이트의 파싱·정제 규칙을 고치는 일이 **파이썬 코드를 수정하는 것이 아니라 Neon DB에 행을 넣고 고치는 일**이 되도록 설계되어 있다. 이 문서는 `api_mst.spec_json`, `api_job.params_json`, `etl_job` 의 모든 항목을 필드 단위로 설명한다.

전체 설계 배경과 인프라 구조는 [ARCHITECTURE-V3.md](ARCHITECTURE-V3.md), 설치·운영 요약은 [README.md](../README.md)를 참고한다.

---

## 1. 개념과 전체 흐름

### 1.1 왜 코드가 아니라 설정 행인가

사이트마다 스크래퍼 클래스를 만들면 소스가 늘 때마다 코드가 늘고 배포가 필요하다. v3는 "어디에 무엇을 요청하고, 응답의 어느 가지를 꺼내 어떤 필터로 정제해, 어느 버킷 경로에 저장할지"를 전부 `api_mst.spec_json` (JSONB)에 선언한다. Python 엔진은 이 명세를 Pydantic v2(`extra="forbid"`)로 엄격히 검증하고 실행만 담당한다.

v1/v2와 달리 오타나 없는 키를 넣으면 실행 중 조용히 무시되지 않고 **`ApiSpec` 검증 단계(API 저장 시 `422` 또는 실행 시점)에서 즉시 실패**한다.

### 1.2 핵심 테이블 (Neon DB · 단일 `public` 스키마)

```text
api_mst          API 수집 명세 (~57행)     어떻게 요청·파싱·정제·검증·저장할 것인가 (spec_json)
api_mst_hist     변경 감사 이력            api_mst UPDATE/DELETE 시 트리거가 직전 행을 자동 백업
api_job          API 수집 실행 정의 (~96행) 어느 주기에 어떤 파라미터 조합으로 돌릴 것인가 (params_json)
etl_job          DuckDB 후처리 SQL 정의     원천 Parquet -> snapshot / daily / monthly 가공 SQL
v_schedule_job   통합 스케줄 뷰             api_job + etl_job 을 schedule_tag, run_order 순으로 합쳐 조회
job_log          통합 실행 원장             API / ETL 성공·실패·EMPTY·BLOCKED 기록 (야간 1회 벌크 적재)
host_policy      호스트별 속도·쿨다운 정책   도메인 접미사별 최소 요청 간격 및 차단 대기 시간
```

> **v1/v2와의 차이**: v1/v2의 `api_job_builder` → `api_job`(날짜×종목 행 사전 생성) 구조는 폐기되었다. v3에서는 `api_job` 행 자체가 실행 정의(구 빌더)이며, 날짜×종목 조합은 **실행 시점에 메모리에서 동적으로 전개**한다.

### 1.3 하루치 배치 흐름 (Neon Scale-to-Zero)

```text
① 장전 동기화 (06:30, Neon 접속 1회)
   python3 -m app.cli sync-config --mirror-user-parquet
   └─ Neon (api_mst, api_job, etl_job, host_policy, 수기 테이블)
      ──▶ 로컬 data/cache/config_snapshot.json 저장
      ──▶ 수기 테이블(user_etf 등) -> OCI v3/snapshot/*.parquet 미러링

② 하루 전체 배치 (07:00 ~ 22:00+, Neon 접속 0회)
   python3 -m app.cli run-cycle <schedule_tag>
   ├─ api_job : params_json 축 전개(DuckDB) -> HTTP/Browser 수집 -> cleanse -> contract
   │            -> ZSTD Parquet 번들 생성 -> OCI(/R2) v3/raw_clean/ 또는 v3/binary/ 업로드
   ├─ etl_job : DuckDB 고정 러너가 sql_text 실행 -> v3/snapshot/ · v3/daily/ · v3/monthly/ 업로드
   └─ 실행 결과는 로컬 NDJSON 원장(data/logs/ledger/job_log_YYYYMMDD.ndjson)에 즉시 누적

③ 야간 로그 반영 (23:45, Neon 접속 1회)
   python3 -m app.cli flush-log
   └─ 로컬 NDJSON 원장의 미반영 라인만 Neon job_log 로 증분 벌크 INSERT (재실행해도 중복 없음)
```

### 1.4 버킷 데이터 레이어 (`v3/` 접두사)

엔진이 경로 규칙을 강제하므로 설정 오타로 다른 폴더를 덮어쓰거나 삭제할 수 없다.

| 레이어 | 객체 키 규칙 | 생성 주체 | 용도 |
| --- | --- | --- | --- |
| `raw_clean` | `v3/raw_clean/<target>/dt=YYYYMMDD/<batch>.parquet` | `api_job` | 수집 직후 기계 정제된 원본 (기본 30일 보관 후 `purge-raw`) |
| `snapshot` | `v3/snapshot/<target>.parquet` | `etl_job` / `sync-config` | 금융 마스터(`mst_etf`, `mst_fuopt` 등), 수기 미러, 최신 스냅샷 |
| `daily` | `v3/daily/<target>/ym=YYYYMM/dt=YYYYMMDD/YYYYMMDD.parquet` | `etl_job` | 일별 중복 제거·정규화 정제본 |
| `monthly` | `v3/monthly/<target>/ym=YYYYMM/YYYYMM.parquet` | `etl_job` | 월별 정렬 Compaction 아카이브 |
| `binary` | `v3/binary/<group>/<filename>` | `api_job` (`BINARY`) | 공시 원문(ZIP/PDF 등) 바이너리 파일 |

---

## 2. `api_mst` 와 `spec_json` 상세 명세

### 2.1 `api_mst` 테이블 컬럼

| 컬럼 | 필수 | 허용값 / 타입 | 설명 |
| --- | :---: | --- | --- |
| `api_id` | ● | `VARCHAR(150)` PK | 고유 식별자. 대문자·밑줄 관례 (`ACE_ETF_PDF`, `KIS_FUTOPT_CHART`) |
| `api_name` | ● | `VARCHAR(100)` | 사람이 읽는 설명 이름 |
| `source_name` | ● | `VARCHAR(20)` | 출처 그룹 (`KIS`, `KRX`, `DART`, `ACE` 등). 인덱스·필터용 |
| `domain` | | `VARCHAR(20)` | 도메인 분류 (`ETF`, `FUOPT`, `BOND`, `STOCK` 등) |
| `protocol` | ● | `'HTTP'` \| `'BROWSER'` | 수집 전송 방식. `BROWSER`는 Playwright Chromium 사용 |
| `sink_type` | ● | `'PARQUET'` \| `'BINARY'` \| `'SESSION'` | 결과 처리 방식 (`PARQUET`: 레코드 저장, `BINARY`: 파일 업로드, `SESSION`: 로그인 쿠키 저장) |
| `status` | ● | `'DRAFT'` \| `'ACTIVE'` \| `'PAUSED'` | 상태 (기본 `DRAFT`). 새 API는 `DRAFT`로 등록 후 `dry-run` 검증 뒤 `ACTIVE` 전환 권장 |
| `schema_version` | ● | `SMALLINT` (기본 `1`) | `spec_json` 스키마 버전 |
| `spec_json` | ● | `JSONB` (`engine` 키 필수) | 실제 수집 명세. 아래 2.2~2.11 참조 |
| `note` | | `VARCHAR(400)` | 운영 메모 |
| `updated_at` | 자동 | `TIMESTAMPTZ` | `BEFORE UPDATE` 트리거가 자동 갱신하며 직전 행을 `api_mst_hist`에 백업 |

### 2.2 `spec_json` 최상위 구조 (`engine` + `ext`)

`spec_json`은 정확히 두 개의 최상위 키만 허용한다 (`app/core/spec.py`의 `ApiSpec`).

```json
{
  "engine": {
    "request":    { ... },
    "pagination": { ... },
    "extract":    { ... },
    "cleanse":    { ... },
    "contract":   { ... },
    "session":    { ... },
    "pacing":     { ... },
    "sink":       { ... }
  },
  "ext": {
    "owner": "quant",
    "note": "파이썬은 이 영역을 검증·해석하지 않고 그대로 통과시킨다"
  }
}
```

| 최상위 키 | 필수 | 검증 방식 | 역할 |
| --- | :---: | --- | --- |
| `engine` | ● | **엄격 (`extra="forbid"`)** | Python 엔진이 해석하는 모든 필드. 오타·미지원 키가 있으면 즉시 `ValidationError` |
| `ext` | | 자유 (`dict[str, Any]`) | 운영자·SQL·관리 UI가 쓰는 자유 메타데이터. Python 수정이나 DDL 변경 없이 임의 키 추가 가능 |

`engine` 내부에서 **필수는 `request` 하나**이며, 나머지 7개 블록은 모두 기본값이 있어 필요한 항목만 적으면 된다. 단 `sink_type`이 `'PARQUET'` 또는 `'BINARY'`인 `api_job` 실행 시에는 `engine.sink`가 반드시 있어야 한다 (`sink_type='SESSION'`은 생략 가능).

---

### 2.3 `engine.request` — 요청 조립과 치환 규칙

```json
"request": {
  "method": "POST",
  "url": "${KIS_PROD}/uapi/domestic-futureoption/v1/quotations/inquire-time-fuopchartprice?fid_input_iscd=:SHORT_CODE&fid_input_date_1=:DATE",
  "headers": {
    "content-type": "application/json",
    "authorization": "Bearer ${KIS_ACCESS_TOKEN}",
    "appkey": "${KIS_APP_KEY}",
    "appsecret": "${KIS_APP_SECRET}",
    "tr_id": "FHKIF03020200"
  },
  "payload_type": "json",
  "payload_json": { "grant_type": "client_credentials" },
  "payload_xml": null,
  "behavior": null
}
```

| 필드 | 필수 | 타입 / 기본값 | 설명 |
| --- | :---: | --- | --- |
| `method` | | `'GET'` \| `'POST'` \| `null` | HTTP 메서드. `null`이면 `GET`으로 동작 (`BROWSER`에서는 생략) |
| `url` | ● | `string` (길이 ≥ 1) | 요청 URL (`BROWSER`에서는 최초 진입 페이지 URL). `:KEY`와 `${VAR}` 치환 지원 |
| `headers` | | `dict[str, Any]` (`{}`) | 요청 헤더. 문자열 값에 `:KEY`와 `${VAR}` 치환 적용 (`BROWSER`에서는 `extra_http_headers`로 주입) |
| `payload_type` | | `'json'` \| `'data'` \| `'content'` \| `null` | 본문 전송 형식. `json`=JSON 본문, `data`=form-urlencoded, `content`=raw UTF-8 문자열(`payload_xml`) |
| `payload_json` | | `dict[str, Any]` \| `null` | `payload_type`이 `'json'` 또는 `'data'`일 때 본문 딕셔너리. **실행 시 `api_job` 파라미터와 병합(`{**payload_json, **params}`)**되며 문자열 값의 `${VAR}`를 치환 |
| `payload_xml` | | `string` \| `null` | `payload_type`이 `'content'`일 때 전송할 XML/문자열 본문. `:KEY`와 `${VAR}` 치환 적용 |
| `behavior` | | `list[dict]` \| `null` | `protocol='BROWSER'` 전용 조작 시나리오 목록 (2.11 참조) |

#### 두 가지 치환 규칙 (`:KEY` 와 `${VAR}`)

1. **`:KEY` (잡 파라미터 치환)**
   - 정규식 `(?<!\w):(\w+)(?!\w)`로 낱말 경계 매칭하므로 `:CODE`가 `:CODE2`를 건드리지 않는다.
   - `url`, `headers`, `payload_xml`, `behavior`의 `url`/`value`/`script`, `sink.staging.name`에 적용된다.
   - `payload_type`이 `'json'`/`'data'`인 경우에는 치환 대신 `{**payload_json, **params}` 키 병합이 일어나므로, `params`의 키가 그대로 JSON/Form 필드로 들어간다.
   - **주의**: `params`에 해당 키가 없으면 오류 없이 `:KEY` 문자열이 그대로 남는다. 새 API는 반드시 `dry-run`으로 확인한다.
2. **`${VAR}` (시크릿·동적 토큰 치환)**
   - 조회 우선순위: **① 로컬 `data/cache/session_store.json`** (동적 토큰, 예: `KIS_ACCESS_TOKEN`) → **② 환경 변수(`.env`)** → 없으면 원문 유지.
   - **보안 격리**: 인프라 자격 증명 유출을 막기 위해 아래 접두사/이름을 `${VAR}`로 참조하면 `PermissionError`로 즉시 차단된다.
     - 차단 접두사: `ORACLE_`, `OCI_`, `S3_`, `NEON_`, `PG`, `POSTGRES_`, `AWS_`, `ADMIN_`
     - 차단 이름: `DATABASE_URL`, `SECRET_KEY`, `PATH`, `HOME`, `USER`

---

### 2.4 `engine.extract` — 응답 파싱·다중 출력·토큰 저장

```json
"extract": {
  "response_type": "auto",
  "selector": "output1",
  "extra_selectors": {
    "output2": "kis_etf_pdf"
  },
  "parse": {},
  "merge_fields": {
    "output1.stck_bsop_date": "bsop_date"
  },
  "persist_env": {
    "access_token": "KIS_ACCESS_TOKEN"
  }
}
```

| 필드 | 기본값 | 타입 | 설명 |
| --- | --- | --- | --- |
| `response_type` | `"auto"` | 아래 13종 중 택1 | 응답 본문 해석 방식 |
| `selector` | `"."` | `string` | 주 레코드 배열/객체를 꺼낼 경로 |
| `extra_selectors` | `{}` | `dict[str, str]` | `{셀렉터: 추가_target_name}`. 1회 응답에서 보조 배열을 별도 타깃 Parquet으로 함께 저장 (`KIS_ETF_PDF`, `KIS_FUTOPT_CHART` 등) |
| `parse` | `{}` | `dict[str, Any]` | `response_type`별 세부 파싱 옵션 (아래 표 참조) |
| `merge_fields` | `{}` | `dict[str, str]` | `{원본_JSON_경로: 주입할_컬럼명}`. JSON 응답의 다른 가지(스칼라 값)를 추출된 모든 레코드에 공통 주입 |
| `persist_env` | `{}` | `dict[str, str]` | `{응답_필드명: 저장할_토큰키}`. 첫 번째 레코드의 필드 값을 `data/cache/session_store.json`(권한 `0600`)에 원자적으로 저장 (`.env`는 절대 덮어쓰지 않음). 보안 차단 키는 거부됨 |

#### `response_type` 13종 일람

| `response_type` | 대상 프로토콜 | 동작 및 `selector` / `parse` 해석 |
| --- | --- | --- |
| `auto` | HTTP / BROWSER | HTTP: `Content-Type`에 `xml`이 있거나 본문이 `<`로 시작하면 XML, 아니면 JSON.<br>BROWSER: `dom`으로 동작 |
| `json` | HTTP | JSON 파싱 후 `selector` 추출 (`"."`=루트, `"a.b.c"`=중첩 키) |
| `xml` | HTTP | `ElementTree` 파싱 후 `selector` 추출. `/`나 `.`로 시작하면 그대로 XPath, 아니면 `.//a/b`로 변환. 끝에 `#attr`을 붙이면 자식 요소의 해당 속성값을 우선 추출(없으면 `value` 속성 → 단일 속성 → text 순) |
| `delimited` | HTTP | CSV/TSV/파이프 구분 텍스트 파싱 (`parse`: `encoding`, `delimiter`, `skip_rows`, `has_header`, `fields`) |
| `zip_delimited` | HTTP | ZIP 압축 해제 후 내부 구분 텍스트 파싱 (`parse`: 위 옵션 + `inner_file` 생략 시 첫 파일) |
| `html_table` | HTTP | BeautifulSoup으로 `<table>` 추출 (`parse`: `table_index` 기본 `0`, `skip_rows`, `has_header`, `fields`) |
| `html_block` | HTTP | CSS 셀렉터로 HTML 블록 원문 추출 (`parse`: `blocks` 필수 CSS 셀렉터, `label` 선택 CSS 셀렉터 → `{block_index, html, label}` 생성) |
| `xlsx` | HTTP | `openpyxl`로 워크북 읽기 (`parse`: `sheet` 기본 `0`, `skip_rows`, `has_header`, `fields`) |
| `xls` | HTTP | `xlrd`로 구형 엑셀 읽기 (`parse`: `sheet` 기본 `0`, `skip_rows`, `has_header`, `fields`) |
| `binary` | HTTP / BROWSER | `sink_type='BINARY'`용. HTTP는 응답 바이트, BROWSER는 `download` 액션 수집 바이트를 `v3/binary/`로 업로드 |
| `dom` | BROWSER | 페이지 DOM에서 JS 1회 IPC로 행 추출 (`parse`: `rows` 필수, `fields` 딕셔너리 또는 배열, `cells` 기본 `"td, th"`). `fields`가 `{col: "css"}` 또는 `{col: {"css": "...", "attr": "href"}}` 형태면 직접 딕셔너리 레코드 생성, 리스트/생략이면 `cells`로 읽은 뒤 `_rows_to_records` 적용. `extra_selectors` 사용 시 `parse.<selector>` 하위 규칙도 지원 |
| `xhr` | BROWSER | `behavior` 실행 중 발생한 XHR/Fetch JSON 응답을 가로챔 (`parse`: `url` 부분 일치 필터, `pick`: `"all"`이면 전부, 생략 시 마지막 1건). 이후 `selector`·`extra_selectors`를 JSON 경로로 추출 |
| `session` | HTTP / BROWSER | `sink_type='SESSION'`용. 로그인 후 쿠키/스토리지 상태를 `data/state/<key>.json`에 저장 |

#### 표 형식 공통 `parse` 옵션 (`delimited`, `zip_delimited`, `html_table`, `xlsx`, `xls`, `dom` 배열 모드)

```json
"parse": {
  "encoding": "cp949",
  "delimiter": ",",
  "inner_file": "data.csv",
  "table_index": 0,
  "sheet": 0,
  "skip_rows": 1,
  "has_header": true,
  "fields": ["code", "name", "weight"]
}
```

- `has_header: true`이고 `fields`를 생략하면 첫 행( `skip_rows` 이후)의 헤더 텍스트를 컬럼명으로 사용한다.
- `has_header: false`이면 `fields`가 반드시 필요하다 (없으면 `MappingError`).
- `has_header: true`와 `fields`를 **둘 다** 주면 첫 헤더 행을 건너뛰되 컬럼명은 `fields`로 강제 지정한다.

---

### 2.5 `engine.pagination` — 이어받기 (4가지 모드)

`mode` 필드를 판별자(discriminator)로 사용하여 4가지 방식 중 하나를 선택한다.

| 모드 (`mode`) | 적용 | 필드 (●=필수) | 동작 및 종료 조건 |
| --- | --- | --- | --- |
| `page` | HTTP / BROWSER | `param` (기본 `"PAGE"`), `start` (기본 `1`, `0` 허용), `max_pages` (기본 `100`) | `param`을 `start`부터 1씩 증가시키며 반복. 빈 페이지이거나 직전 페이지와 완전히 동일하면 중단 |
| `cursor` | HTTP | `param` ●, `from` ● (`from_path`), `pick` (`"min"`\|`"max"`, 기본 `"min"`), `min_records` (`int`\|`null`), `max_pages` (기본 `100`) | 응답 JSON에서 `from` 경로(`output2[].stck_cntg_hour`처럼 `[]` 배열 전개 지원)의 `min`/`max` 값을 뽑아 다음 요청의 `param`에 대입. 건수가 `min_records` 미만이거나 커서 값이 직전과 같으면 중단 |
| `token` | HTTP | `params` ● (`{요청파라미터: 추출경로}`), `continue_when` (`{"source": "...", "in": [...]}`), `max_pages` (기본 `100`) | 응답 헤더(`header:tr_cont`) 또는 JSON 경로에서 연속 토큰을 뽑아 다음 요청에 주입. `continue_when` 불일치 또는 토큰 누락 시 중단 |
| `click` / `scroll` | BROWSER (`dom`) | `selector` (`click` 시 다음 버튼 CSS), `wait_for` (로딩 완료 대기 CSS, 생략 시 `networkidle`), `max_pages` (기본 `20`) | 페이지 DOM 추출 후 버튼 클릭 또는 바닥 스크롤 → 새 행이 없거나 직전과 같으면 중단 |

> **암묵적 `:PAGE` 감지 (HTTP)**: `engine.pagination`을 생략했더라도 `request.url`·`payload_xml`에 `:PAGE`가 있거나 `payload_json`에 `"PAGE"` 키가 있으면 엔진이 자동으로 `{"mode": "page", "param": "PAGE", "start": 1, "max_pages": 100}`을 적용한다. 0-기반 페이징(예: Spring Data JPA)은 명시적으로 `"start": 0`을 지정해야 한다.

> **페이지 경계 중복 제거**: 모든 페이징 모드는 페이지 간 겹치는 행을 자동으로 병합(`_merge_page_records`)한다. `cursor` 모드는 커서 마지막 필드명을 키로, 나머지는 행 전체 JSON 직렬화 값을 키로 삼아 뒤 페이지의 최신 레코드로 덮어쓴다.

#### 실전 예시 (`KIS_FUTOPT_CHART` 커서 페이징)

```json
"pagination": {
  "mode": "cursor",
  "param": "HHMM",
  "from": "output2[].stck_cntg_hour",
  "pick": "min",
  "min_records": 99,
  "max_pages": 10
}
```

#### 실전 예시 (KIS 연속조회 `token` 페이징)

```json
"pagination": {
  "mode": "token",
  "continue_when": { "source": "header:tr_cont", "in": ["F", "M"] },
  "params": {
    "CTX_AREA_FK100": "ctx_area_fk100",
    "CTX_AREA_NK100": "ctx_area_nk100"
  },
  "max_pages": 50
}
```

---

### 2.6 `engine.cleanse` — 기계적 정제 파이프라인

v1/v2에서 벤더별 ORM Mixin(`VendorRecordBase`, `MdcRecordBase`, `EtfPdfDateMixin`)으로 흩어져 있던 정제 로직을 설정으로 일원화했다.

```json
"cleanse": {
  "lowercase_keys": true,
  "rename": {
    "isu_srt_cd": "short_code"
  },
  "rules": {
    "*": ["strip", "blank_to_null", "dash_to_null"],
    "std_dt": ["digits_only"],
    "close_prc": ["strip_comma", "to_float"],
    "volume": ["strip_comma", "to_int"]
  },
  "keep_columns": null,
  "drop_columns": ["raw_html"]
}
```

| 필드 | 기본값 | 타입 | 설명 |
| --- | --- | --- | --- |
| `lowercase_keys` | `true` | `bool` | 모든 컬럼명(`meta`·`key_params`·원본 레코드 포함)을 소문자로 통일 |
| `rename` | `{}` | `dict[str, str]` | `{원본컬럼: 새컬럼}` 이름 변경 (`lowercase_keys=true`면 키/값 모두 소문자 매칭) |
| `rules` | `{}` | `dict[str, list[필터명]]` | `"*"`는 **모든 컬럼에 먼저 적용**되고, 그다음 해당 컬럼 전용 필터 리스트가 순서대로 실행 |
| `keep_columns` | `null` | `list[str]` \| `null` | 지정 시 이 목록에 있는 컬럼만 유지 (화이트리스트) |
| `drop_columns` | `[]` | `list[str]` | 지정한 컬럼을 최종 결과에서 제거 (블랙리스트) |

#### 병합 우선순위 (실행 순서)

각 레코드는 다음 순서로 하나의 딕셔너리로 합쳐진 뒤 필터가 적용된다.
1. `meta`: 엔진이 자동 각인하는 `api_id`, `job_id`, `target_date`
2. `key_params`: `sink.row_stamp` + `sink.snapshot_column`으로 각인된 값
3. `raw`: 외부 API 응답에서 파싱된 실제 레코드 (**최우선** — 원천 응답에 같은 컬럼이 있으면 각인값을 덮어씀)
4. 키 소문자화(`lowercase_keys`) → `rename` → `keep_columns`/`drop_columns` 필터링 → `"*"` 규칙 → 컬럼별 규칙

#### 내장 정제 필터 10종 (`CleanseFilterName`)

| 필터명 | 동작 | 예시 |
| --- | --- | --- |
| `strip` | 문자열 앞뒤 공백 제거 | `" 005930 "` → `"005930"` |
| `blank_to_null` | 공백뿐인 문자열을 `None`으로 변환 | `"   "` → `None` |
| `dash_to_null` | `"-"` 문자열을 `None`으로 변환 (KRX/벤더 결측 표기) | `"-"` → `None` |
| `strip_comma` | 숫자 형태 문자열(`^-?[\d,]+(?:\.\d+)?$`)의 천단위 쉼표만 제거 (텍스트 쉼표 보존) | `"1,234.50"` → `"1234.50"` |
| `digits_only` | 숫자 외 문자 전부 제거 (빈 결과는 `None`) | `"2026-11-27"` → `"20261127"` |
| `to_float` | 쉼표 제거 후 `float` 변환 (빈값/`"-"` → `None`) | `"1,234.5"` → `1234.5` |
| `to_int` | 쉼표 제거 후 `int(float(...))` 변환 (빈값/`"-"` → `None`) | `"1,200"` → `1200` |
| `to_str` | `None` 제외 모든 값을 `str`로 변환 | `123` → `"123"` |
| `lower` | 문자열 소문자화 | `"ETF"` → `"etf"` |
| `upper` | 문자열 대문자화 | `"krx"` → `"KRX"` |

---

### 2.7 `engine.contract` — 적재 전 데이터 품질 계약 검사

HTTP 200 OK를 받았지만 본문이 비어 있거나 `"데이터가 없습니다."` 같은 쓰레기 값이 온 경우를 Parquet 업로드 전에 차단한다.

```json
"contract": {
  "empty_policy": "keep_previous",
  "min_rows": 1,
  "not_null_fields": ["short_code", "std_dt"],
  "forbidden_values": ["데이터가 없습니다.", "조회 결과가 없습니다."],
  "unique_keys": ["short_code", "std_dt"]
}
```

| 필드 | 기본값 | 타입 | 설명 |
| --- | --- | --- | --- |
| `empty_policy` | `"succeed"` | `'succeed'` \| `'pending'` \| `'keep_previous'` \| `'fail'` | 0건 응답 시 처리 정책 (아래 표 참조) |
| `min_rows` | `0` | `int` (≥ 0) | 최소 허용 행 수. 미만이면 계약 실패(`ConfigError`) |
| `not_null_fields` | `[]` | `list[str]` | `None`이 하나라도 있으면 실패할 필수 컬럼 목록 |
| `forbidden_values` | `[]` | `list[str]` | 문자열 컬럼의 `strip()` 값이 이 목록과 일치하면 실패 (최대 10건 위반 리포트) |
| `unique_keys` | `[]` | `list[str]` | 지정 컬럼 조합에 중복 행이 있으면 실패 |

#### `empty_policy`별 0건 처리 동작

| 값 | 동작 | `job_log.status` | 다음 재실행 시 |
| --- | --- | --- | --- |
| `succeed` | 정상 완료로 간주 (단 `min_rows > 0`이면 실패) | `SUCCESS` (0건이면 업로드 생략) | 당일 스킵 |
| `pending` | 아직 미게시 상태로 간주 (업로드 생략, 실패 아님) | `EMPTY` | 당일 재실행 가능 |
| `keep_previous` | 기존 스냅샷/파일 유지 (업로드 생략, 실패 아님) | `EMPTY` | 당일 재실행 가능 |
| `fail` | 즉시 `ConfigError` 발생 | `FAILED` | 당일 재실행 가능 |

---

### 2.8 `engine.session` — 로그인·쿠키 공유·차단 감지

로그인이 필요한 사이트(예: KRX 데이터포털 `data.krx.co.kr`)는 **로그인 전용 행(`sink_type='SESSION'`)**과 **수집 행**을 분리해 연결한다.

```json
"session": {
  "login": "KRX_DATA_LOGIN",
  "logged_out": "LOGOUT",
  "blocked": "비정상적인 접근",
  "expect": "\"_error_code\":\"CD001\"",
  "state": "data.krx.co.kr"
}
```

| 필드 | 타입 | 어디에 쓰는가 | 설명 |
| --- | --- | --- | --- |
| `login` | `string` \| `null` | **수집 행** | 세션 만료(`SessionExpired`) 시 자동으로 호출할 로그인 `api_id`. 재로그인 후 **딱 1회만 자동 재시도** (계정 잠금 방지) |
| `logged_out` | `string` \| `null` | **수집 행** | HTTP: 응답 본문 앞 4,000자에 이 문자열이 포함되면 `SessionExpired` 발생.<br>BROWSER: 페이지에 이 CSS 셀렉터가 존재하면 `SessionExpired` 발생 |
| `blocked` | `string` \| `null` | **수집 행 (HTTP)** | 응답 본문 앞 4,000자에 이 문자열이 포함되면 `SourceRefused` (`job_log.status='BLOCKED'`) 발생 후 사이클 중단 |
| `expect` | `string` \| `null` | **로그인 행 (`SESSION`)** | HTTP 로그인 응답 본문에 이 문자열이 없으면 로그인 실패(`SessionExpired`) 처리 |
| `state` | `string` \| `null` | **로그인·수집 행** | 쿠키 상태 파일 키 (`data/state/<state>.json`, 권한 `0600`). 생략 시 `request.url`의 호스트명 사용. **HTTP 로그인 쿠키와 Playwright `storage_state`가 동일한 JSON 형식을 공유**하므로 HTTP로 로그인하고 BROWSER로 수집하거나 그 반대도 가능 |

> **참고**: `protocol='BROWSER'`에서 `session.login`이 설정되어 있고 Playwright 타임아웃(`PlaywrightTimeoutError`)이 발생하면 세션 만료로 로그인 화면에 리다이렉트된 경우가 많으므로 엔진이 이를 `SessionExpired`로 변환해 자동 재로그인을 시도한다.

#### 실전 예시 (`KRX_DATA_LOGIN`)

```json
{
  "api_id": "KRX_DATA_LOGIN",
  "protocol": "HTTP",
  "sink_type": "SESSION",
  "spec_json": {
    "engine": {
      "request": {
        "method": "POST",
        "url": "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd",
        "headers": {
          "Referer": "https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd",
          "User-Agent": "Mozilla/5.0 ...",
          "Accept-Language": "ko-KR,ko;q=0.9"
        },
        "payload_type": "data",
        "payload_json": {
          "mbrId": "${KRX_DATA_ID}",
          "pw": "${KRX_DATA_PW}",
          "skipDup": "Y"
        }
      },
      "extract": { "response_type": "session" },
      "session": { "expect": "\"_error_code\":\"CD001\"" }
    }
  }
}
```

---

### 2.9 `engine.pacing` — 호출 속도·지연·타임아웃

```json
"pacing": {
  "min_interval_sec": 0.5,
  "page_delay_sec": 0.3,
  "timeout_sec": 30.0
}
```

| 필드 | 기본값 | 설명 |
| --- | --- | --- |
| `min_interval_sec` | `null` | 이 API의 최소 요청 간격(초). `host_policy`의 간격(없으면 기본 `0.15`초)과 비교해 **`max(host_policy, api_min)`** 적용 |
| `page_delay_sec` | `null` | 페이징 다음 페이지 호출 전 대기 시간(초). 생략 시 환경변수 `PAGE_DELAY_SEC` (기본 `0.3`초) |
| `timeout_sec` | `null` | HTTP 요청 타임아웃(초). 생략 시 클라이언트 기본 `30.0`초 |

`host_policy` 테이블은 호스트명의 **가장 긴 접미사(longest-suffix)**로 매칭된다 (예: `papi.aceetf.co.kr` → `aceetf.co.kr` 정책 적용).

---

### 2.10 `engine.sink` — 저장 대상·파티션·각인·바이너리 스테이징

```json
"sink": {
  "target_name": "kis_etf",
  "layer": "raw_clean",
  "prefix": "v3",
  "targets": ["oci"],
  "mode": "append_snapshot",
  "row_stamp": { "SHORT_CODE": "short_code" },
  "job_key_params": ["SHORT_CODE"],
  "is_repeated": true,
  "snapshot_column": "snap_at",
  "staging": {}
}
```

| 필드 | 필수 | 기본값 | 설명 |
| --- | :---: | --- | --- |
| `target_name` | ● | - | 버킷 하위 데이터셋 이름 (`v3/<layer>/<target_name>/...`) |
| `layer` | | `"raw_clean"` | `'raw_clean'` \| `'snapshot'` \| `'daily'` \| `'monthly'` \| `'binary'`. 일반 수집은 `"raw_clean"`, 파일 다운로드는 `"binary"` |
| `prefix` | | `"v3"` | 버킷 최상위 격리 접두사 |
| `targets` | | `["oci"]` | 업로드할 S3 타깃 목록. 2단계 웹 UI 서빙 시 `["oci", "r2"]`로 지정하면 동일 Parquet 1개를 순차 업로드 (미설정 타깃은 `skip_unconfigured_targets=True`로 자동 스킵) |
| `mode` | | `"replace_partition"` | `'replace_partition'` \| `'append_snapshot'` \| `'upsert'` (논리적 적재 의도 메타) |
| `row_stamp` | | `{}` | `{잡파라미터키: 레코드컬럼명}`. 수집된 모든 행에 요청 파라미터 값을 컬럼으로 각인 (예: `{"FUNDCD": "fundcd", "STD_DT": "std_dt"}`) |
| `job_key_params` | | `[]` | 파티션 식별키(`job_log.partition_key`)를 구성할 파라미터 키 목록 (`"_"`로 결합, 빈 목록이면 `"ALL"`). **당일 멱등성 스킵 판정의 기준** |
| `is_repeated` | | `false` | `false`: 하루 1회성 수집 → 당일 이미 `SUCCESS`한 `(partition_key, target_date)`는 자동 스킵, `save_mode='overwrite'` 시 결정적 파일명(`<job_id>.parquet`)으로 제자리 덮어쓰기.<br>`true`: 장중 3분/5분 등 반복 스냅샷 → 매번 스킵 없이 실행하며 시각 접미사(`<job_id>_HHMMSS.parquet`)로 누적 |
| `snapshot_column` | | `null` | 반복 수집 시각을 각인할 컬럼명 (예: `"snap_at"`, `"trade_at"`). `api_job.snapshot_unit` 단위(예: `3m`)로 절삭한 `YYYYMMDDHHMMSS` KST 문자열을 기록. **제약: `is_repeated=true`일 때만 설정 가능** |
| `staging` | | `{}` | `sink_type='BINARY'` 전용 업로드 규칙 (아래 참조) |

#### `sink.staging` (`sink_type='BINARY'` 전용, `DART_DOCUMENT` 예시)

```json
"sink": {
  "target_name": "dart_document",
  "layer": "binary",
  "targets": ["oci"],
  "row_stamp": { "RCEPT_NO": "rcept_no" },
  "job_key_params": ["RCEPT_NO"],
  "staging": {
    "group": "dart_docs",
    "name": ":RCEPT_NO.zip",
    "unzip": true
  }
}
```

- `group` (기본 `"misc"`): `v3/binary/<group>/<filename>` 의 하위 그룹 폴더명
- `name`: 저장할 파일명 템플릿 (`:KEY` 치환 지원, 생략 시 HTTP `Content-Disposition` 또는 URL 파일명 사용)
- `unzip` (기본 `false`): `true`이고 ZIP 파일이면 압축을 풀어 내부 파일을 각각 업로드. 모든 파일명은 디렉터리 경로를 제거해 **Zip Slip 공격을 원천 차단**하며, 업로드 메타(`group_name`, `file_name`, `object_key`, `size_bytes`, `sha256`, `oci_uri`)를 기록한다.

---

### 2.11 `protocol='BROWSER'` 와 `request.behavior` 12개 액션

Playwright는 첫 `BROWSER` 잡 실행 시에만 지연 임포트된다. 기본 로케일은 `ko-KR`, 타임존은 `Asia/Seoul`, 타임아웃은 `15000ms`(`BROWSER_HEADLESS`, `BROWSER_TIMEOUT_MS` 환경변수로 조정), 브라우저 `alert`/`confirm` 다이얼로그는 자동 수락된다.

실행 시 엔진이 먼저 `request.url`로 `page.goto(..., wait_until="load")` 이동한 뒤 `behavior` 배열을 순서대로 실행한다.

| `action` | 주요 키 | 설명 |
| --- | --- | --- |
| `goto` | `url` (생략 시 `request.url`), `wait_until` (`"load"`) | 지정 URL로 이동 (`host_policy` 페이싱 적용) |
| `fill` | `selector`, `value` | 입력 필드에 값 채우기 (`:KEY`, `${VAR}` 치환) |
| `select` | `selector`, `value` | `<select>` 옵션 선택 |
| `check` | `selector`, `checked` (기본 `true`) | 체크박스 체크 / 해제 |
| `click` | `selector` | 요소 클릭 |
| `press` | `key` (기본 `"Enter"`), `selector` (선택) | 특정 요소 또는 키보드 전역 키 입력 |
| `wait_for` | `selector`, `state` (기본 `"visible"`) | 요소가 지정 상태가 될 때까지 대기 |
| `wait_load` | `state` (기본 `"networkidle"`) | 페이지 로드 상태 대기 |
| `sleep` | `seconds` (기본 `1`) | 고정 시간(초) 대기 |
| `frame` | `selector` \| `name` \| `url` \| `reset: true` | 이후 액션의 조작 대상을 iframe 내부로 전환 (`reset: true`로 메인 페이지 복귀) |
| `eval` | `script` | 페이지 컨텍스트에서 JS 실행 (`:KEY`, `${VAR}` 치환) |
| `download` | `selector` | `selector` 클릭으로 발생하는 파일 다운로드를 가로채 바이트 수집 (`response_type='binary'`와 함께 사용) |

- **공통 옵션**: 모든 스텝에 `timeout`(초 단위)과 `expect_navigation: true`(선택 `wait_until: "load" | "domcontentloaded"`)를 붙일 수 있다.

#### 실전 전체 예시 (`KRX_ISIN_SRCH` — BROWSER + DOM + `page` 페이징)

```json
{
  "engine": {
    "request": {
      "url": "https://isin.krx.co.kr/srch/srch.do?method=srchList",
      "behavior": [
        { "action": "fill", "selector": "#isur_nm1", "value": ":CODE" },
        { "action": "fill", "selector": "#std_cd_grnt_start_dd", "value": ":GRNT_FROM" },
        { "action": "fill", "selector": "#std_cd_grnt_end_dd", "value": ":GRNT_TO" },
        {
          "action": "eval",
          "script": "$('form[name=JLDINF20000]').append(\"<input type='hidden' name='pageIndex' value=':PAGE'>\").append(\"<input type='hidden' name='pageSize' value='1000'>\"); fn_search(2);",
          "expect_navigation": true,
          "wait_until": "domcontentloaded"
        }
      ]
    },
    "pagination": { "mode": "page", "param": "PAGE", "start": 1, "max_pages": 150 },
    "extract": {
      "response_type": "dom",
      "selector": "output",
      "parse": {
        "rows": "#tbody tr[name=dataTr]:has(a[href*='onPopupCode'])",
        "fields": {
          "prod_type": "td:nth-child(1)",
          "kor_name":  "td:nth-child(3)",
          "issuer":    "td:nth-child(4)",
          "issue_dd":  "td:nth-child(5)",
          "mat_dd":    "td:nth-child(6)",
          "list_yn":   "td:nth-child(7)",
          "list_dd":   "td:nth-child(8)",
          "grant_dd":  "td:nth-child(9)",
          "isin":      "a[href*='onPopupCode'] strong"
        }
      }
    },
    "cleanse": { "rules": { "*": ["strip", "blank_to_null"] } },
    "sink": {
      "target_name": "krx_isin_srch",
      "layer": "raw_clean",
      "targets": ["oci"],
      "row_stamp": { "CODE": "code" },
      "job_key_params": ["CODE", "GRNT_FROM", "GRNT_TO"],
      "is_repeated": false
    }
  }
}
```

---

## 3. `api_job` — 수집 실행 정의와 `params_json` 축 전개

### 3.1 `api_job` 테이블 컬럼

| 컬럼 | 필수 | 타입 / 기본값 | 설명 |
| --- | :---: | --- | --- |
| `job_id` | ● | `VARCHAR(100)` PK | 잡 고유 ID (`ACE_ETF_PDF_DAILY`, `KIS_FUTOPT_PRICE_CALL_3M`) |
| `api_id` | ● | `VARCHAR(150)` FK | 참조할 `api_mst.api_id` |
| `schedule_tag` | ● | `VARCHAR(30)` (`'daily'`) | 묶음 스케줄 태그 (`daily_early`, `daily_start`, `3m_call`, `3m_put`, `daily_batch1`, `daily_batch2`, `daily_close`, `monthly_archive`) |
| `run_order` | ● | `SMALLINT` (`100`) | 같은 `schedule_tag` 내 실행 순서 (오름차순. `api_job` 기본 100, `etl_job` 기본 200) |
| `is_active` | ● | `BOOLEAN` (`TRUE`) | `FALSE`면 `run-cycle`에서 제외 (`run-job` 직접 지정 시에는 실행 가능) |
| `snapshot_unit` | | `VARCHAR(10)` | 반복 스냅샷 시각 절삭 단위 (`"3m"`, `"5m"`, `"1h"`, `"1d"`). `sink.snapshot_column` 값 생성 시 적용 |
| `save_mode` | ● | `'overwrite'` \| `'append'` | `is_repeated=false`일 때 `'overwrite'`면 파일명을 `<job_id>.parquet`로 고정해 같은 날 재실행 시 제자리 덮어쓰기. `'append'`거나 `is_repeated=true`면 `<job_id>_HHMMSS.parquet`로 누적 |
| `params_json` | ● | `JSONB` (`{}`) | 실행 시 전개할 파라미터 축 정의 (아래 3.2 참조) |

### 3.2 `params_json` — 3가지 축과 곱집합 전개

`params_json`의 각 키-값은 하나의 축(axis)이 되며, 모든 축의 **곱집합(Cartesian product)**이 파티션 요청 목록으로 전개된다. 빈 객체 `{}`이면 파라미터 없이 1회 실행된다.

```json
{
  "FUNDCD": {
    "parquet": "SELECT amc_etf_cd FROM mst_etf WHERE amc = '한국투자신탁운용' AND amc_etf_cd IS NOT NULL ORDER BY short_code"
  },
  "STD_DT": {
    "date": "today",
    "format": "%Y%m%d"
  },
  "MARKET": "J"
}
```

#### 축 유형 1: 리터럴 (고정값)
- `"MARKET": "J"` → 단일 고정값 1개. 단일 키만 허용.

#### 축 유형 2: 날짜 키워드 (`{"date": "<keyword>", "format": "<strftime>"}`)
실행 기준일(`--date YYYYMMDD`, 생략 시 KST 오늘)을 기준으로 계산된다.

| 키워드 (`date`) | 의미 | 기본 `format` |
| --- | --- | --- |
| `today` | 기준일 당일 | `%Y%m%d` |
| `yesterday` | 기준일 - 1일 | `%Y%m%d` |
| `last_month` | 기준일 - 1개월 (말일 보정) | `%Y%m%d` |
| `last_eom` | 전월 말일 | `%Y%m%d` |
| `last_yymm` | 전월 연월 | `%Y%m` |
| `last_bday` | 직전 평일 (토·일 제외) | `%Y%m%d` |

#### 축 유형 3: DuckDB Parquet 쿼리 (`{"parquet": "SELECT ..."}` 또는 레거시 별칭 `{"db": "SELECT ..."}`)
- 메모리 256MB 제한의 DuckDB로 OCI 버킷 `v3/snapshot/*.parquet`을 직접 조회해 대상 종목·코드 목록을 뽑는다.
- **다중 컬럼 키**: 키를 쉼표로 나열하면 SELECT 결과 컬럼과 1:1 매핑된다.
  ```json
  "MRKT,SHORT_CODE": {
    "parquet": "SELECT mrkt_code, short_code FROM v_k2i_atm ORDER BY short_code"
  }
  ```
- **SQL 치환 변수**: `${OCI_BUCKET}`, `${TARGET_DATE}`(`YYYYMMDD`), `${TARGET_YM}`(`YYYYMM`).
- **자동 바인딩 TEMP VIEW**: 쿼리 문자열에 아래 테이블명이 등장하면 엔진이 자동으로 뷰를 등록한다.
  - `v3/snapshot/<name>.parquet` (13종): `mst_etf`, `mst_fuopt`, `mst_bond`, `mst_stock`, `meta_maturity`, `stock_index_his`, `v_k2i_atm`, `user_etf`, `krx_index_mst`, `mst_index`, `meta_fuopt_info`, `ksd_bond_no_detail`, `src_code`
  - `v3/raw_clean/<name>/**/*.parquet` (3종, 파일 없을 시 빈 타입 뷰 자동 폴백): `ksd_bond_info`, `isin_bond_info`, `ksd_kacd_code`
- 쿼리 결과가 **0행이면 `ConfigError`**를 발생시켜 빈 축으로 조용히 지나가는 사고를 방지한다.

---

## 4. `etl_job` — DuckDB 후처리 · 마스터 · 월별 Compaction

수집 완료된 `v3/raw_clean/` 또는 기존 `v3/snapshot/`·`v3/daily/`를 DuckDB SQL로 가공해 정제 Parquet을 생성한다.

### 4.1 `etl_job` 테이블 컬럼

| 컬럼 | 필수 | 타입 / 기본값 | 설명 |
| --- | :---: | --- | --- |
| `job_id` | ● | `VARCHAR(100)` PK | ETL 잡 고유 ID (`REFRESH_REF_V_K2I_ATM`, `CURATE_ACE_ETF_PDF_DAILY`, `COMPACT_KIS_FUTOPT_CHART_MONTHLY`) |
| `schedule_tag` | ● | `VARCHAR(30)` (`'daily_close'`) | 실행 스케줄 태그 |
| `run_order` | ● | `SMALLINT` (`200`) | 실행 순서 (`api_job` 완료 뒤 실행되도록 보통 200 이상 부여) |
| `is_active` | ● | `BOOLEAN` (`TRUE`) | 활성화 여부 |
| `output_layer` | ● | `'snapshot'` \| `'daily'` \| `'monthly'` | 출력 계층. 엔진이 이 값과 `target_name`으로 버킷 경로를 고정 계산 |
| `target_name` | ● | `VARCHAR(100)` (`^[a-z0-9_]+$`) | 출력 데이터셋 이름 (소문자·숫자·밑줄만 허용) |
| `targets` | ● | `TEXT[]` (`ARRAY['oci']`) | 업로드 버킷 목록 (2단계 웹 서빙 시 `ARRAY['oci','r2']`) |
| `memory_limit` | ● | `VARCHAR(16)` (`'1GB'`) | DuckDB `memory_limit` (OCI Micro/A1 OOM 방지) |
| `threads` | ● | `SMALLINT` (`2`, 1~8) | DuckDB 스레드 수 |
| `min_rows` | ● | `INTEGER` (`0`) | 결과가 이 건수 미만이면 업로드하지 않고 `ValueError` 실패 처리 |
| `sql_text` | ● | `TEXT` | 결과를 반환하는 `SELECT` 쿼리 (`CREATE TEMP TABLE _job_out AS (<sql_text>)`로 감싸 실행 후 ZSTD Parquet으로 내보냄) |

### 4.2 경로 규칙과 SQL 치환 변수

| `output_layer` | 자동 생성 객체 키 |
| --- | --- |
| `snapshot` | `v3/snapshot/<target_name>.parquet` |
| `daily` | `v3/daily/<target_name>/ym=YYYYMM/dt=YYYYMMDD/YYYYMMDD.parquet` |
| `monthly` | `v3/monthly/<target_name>/ym=YYYYMM/YYYYMM.parquet` |

`sql_text`에서 쓸 수 있는 치환 변수 4종:
- `${OCI_BUCKET}`: `S3_OCI_BUCKET` 환경변수 값
- `${TARGET_DATE}`: 대상 기준일 `YYYYMMDD`
- `${TARGET_YM}`: 대상 연월 `YYYYMM` (`TARGET_DATE[:6]`)
- `${LEDGER_DIR}`: 로컬 원장 디렉터리 경로 (`data/logs/ledger`)

### 4.3 특수 내장 ETL (`-- builtin:harvest_src_code`)

`sql_text`가 `-- builtin:harvest_src_code`로 시작하면 일반 단일 SQL 대신 [code_harvester.py](../app/pipeline/code_harvester.py)가 실행되어, 모든 Parquet 소스에서 코드성 컬럼의 고유값을 자동 수확하고 `v3/snapshot/src_code.parquet`을 갱신한다.

### 4.4 블록 디스크 스필 보호

`build_duckdb_config()`는 `V3_DUCKDB_TMP_DIR`(기본 `data/tmp/duckdb`)이 Linux `tmpfs`(RAM 디스크) 마운트인지 `/proc/mounts`로 검사한다. `/tmp` 같은 `tmpfs`를 지정하면 OOM 위험 때문에 즉시 `ValueError`로 차단한다.

---

## 5. 새 API / ETL 등록 절차

1. **Neon에 `api_mst` 행을 `status = 'DRAFT'`로 등록**
   - DBeaver에서 직접 `INSERT`하거나 FastAPI `POST /api-mst` 사용 (FastAPI는 저장 전 `ApiSpec` Pydantic 검증을 자동 수행하며, 전체 JSON Schema는 `GET /api-mst/schema`에서 확인 가능).
2. **로컬 스냅샷 갱신**
   ```bash
   python3 -m app.cli sync-config
   ```
3. **`dry-run`으로 1회 시험 수집 (업로드·로그 기록 없음)**
   ```bash
   python3 -m app.cli dry-run NEW_API_ID --param STD_DT=20261127 --param CODE=005930 --sample 5
   ```
   - `raw`, `cleaned`, `contract(ok=True)`, 최종 컬럼 목록과 샘플 행을 확인한다.
   - `:KEY` 치환 누락, 셀렉터 불일치, `cleanse` 필터 누락을 이 단계에서 모두 잡는다.
4. **검증 완료 후 `api_mst.status = 'ACTIVE'` 전환 + `api_job` (및 필요 시 `etl_job`) 등록**
5. **소량 파일럿 실행 후 본 배치 편입**
   ```bash
   python3 -m app.cli sync-config
   python3 -m app.cli run-job NEW_API_JOB --date 20261127 --limit 2
   python3 -m app.cli flush-log --date 20261127
   ```

---

## 6. 실행 · 예외 분류 · 보관 주기 정리

### 6.1 예외 분류 체계 ([app/errors.py](../app/errors.py))

모든 예외는 `classify(exc)`를 거쳐 아래 등급으로 `job_log.status`와 `job_log.error_code`에 기록된다.

| 예외 클래스 | 원인 구분 | HTTP 자동 재시도(최대 3회) | `job_log.status` | 비고 |
| --- | --- | :---: | --- | --- |
| `SourceUnavailable` | 원천 5xx / 네트워크 타임아웃 / JSON·XML 깨짐 | ● | `FAILED` | 지수 백오프(최대 10초) 재시도 |
| `SourceRefused` | 원천 차단 (`session.blocked` 매칭) | - | `BLOCKED` | 즉시 중단 (`trips_breaker=True`) |
| `SessionExpired` | 로그인 만료 (`session.logged_out` 등) | 상위 1회 재로그인 | `FAILED` | `session.login` 설정 시 재로그인 후 1회 재시도 |
| `SourceEmpty` | `empty_policy`가 `pending` / `keep_previous` | - | `EMPTY` | 정상 분기 (재실행 허용) |
| `ConfigError` / `MappingError` / `ParamError` | 설정 오류·계약 위반·빈 축 쿼리 | - | `FAILED` | 재시도 무의미, 설정 수정 필요 |

### 6.2 보관 주기 정리 명령 (기본 Dry-Run, `--apply` 시 실제 삭제)

```bash
# 1) 30일 지난 v3/raw_clean/ 삭제 (대상 지정 가능)
python3 -m app.cli purge-raw --keep-days 30 [--target ace_etf_pdf] [--apply]

# 2) 월별 Compaction 완료된 v3/daily/ 정리
#    반드시 v3/monthly/<target>/ym=YYYYMM/YYYYMM.parquet 과
#    COUNT(*) + BIT_XOR(HASH(*)) 완전 일치를 검증한 월만 삭제
python3 -m app.cli purge-daily kis_futopt_chart --keep-months 3 [--apply]

# 3) 90일 지난 Neon job_log 삭제
python3 -m app.cli purge-log --keep-days 90 [--apply]
```

### 6.3 기간 백필 (`--from` / `--to`)

`run-job`과 `run-cycle` 모두 평일 기준 기간 백필을 지원한다. 주말을 포함하려면 `--include-weekends`를 추가한다.

```bash
python3 -m app.cli run-job ACE_ETF_PDF_DAILY --from 20261116 --to 20261120
python3 -m app.cli run-cycle daily_close --from 20261116 --to 20261120
```

---

## 7. FastAPI v3 참고

실행: `uvicorn app.api.main:app --reload` (선택 의존성 `pip install -e ".[api]"`).

- **인증**: `/health`를 제외한 모든 엔드포인트는 `ADMIN_API_KEY` 환경변수가 설정된 경우 `X-API-Key` 헤더를 검증한다.
- **오류 매핑**: Pydantic 스키마 검증 실패·DB `CHECK`/`FK`/`NOT NULL` 위반은 `422`, 중복 PK/UNIQUE 위반은 `409`를 반환한다.
- **DuckDB 보호**: `/results/{layer}/{target_name}` 조회는 `DUCKDB_SEMAPHORE(2)`와 요청당 `memory_limit='256MB'`로 메모리 폭주를 막는다.
- **주요 엔드포인트**:
  - `GET /api-mst/schema` — `ApiSpec` 전체 JSON Schema (관리 UI 폼 자동 생성용)
  - `GET/POST/PATCH/DELETE /api-mst`, `GET /api-mst/{api_id}/history`
  - `GET/POST/PATCH/DELETE /api-jobs`, `/etl-jobs`
  - `GET/PUT/DELETE /host-policies`, `GET /schedule` (`v_schedule_job`)
  - `POST /jobs/sync-config`, `/jobs/dry-run/{api_id}`, `/jobs/run/{job_id}`, `/jobs/run-cycle/{schedule_tag}`, `/jobs/flush-log`
  - `GET /job-logs` (`job_type`, `job_id`, `api_id`, `target_date`, `status`, `X-Total-Count` 헤더)
  - `GET /results/targets?layer=snapshot|daily|monthly|raw_clean`, `GET /results/{layer}/{target_name}?ym=YYYYMM&dt=YYYYMMDD`

---

## 8. 자주 겪는 함정

- **Neon DB에서 설정을 고쳤는데 배치가 옛 설정으로 돈다**
  → 배치는 Neon이 아니라 로컬 스냅샷(`data/cache/config_snapshot.json`)을 읽는다. 긴급 반영 시 `python3 -m app.cli sync-config`를 실행해야 한다.
- **`api_mst` 저장 시 `Extra inputs are not permitted` (422)**
  → `spec_json.engine`은 `extra="forbid"` 엄격 검증이다. 오타가 있거나 커스텀 운영 메타를 넣고 싶다면 `spec_json.ext` 아래에 넣어야 한다.
- **같은 날 `run-job`을 다시 돌렸는데 `partitions_skipped`만 올라가고 수집이 안 된다**
  → `sink.is_repeated=false` 잡은 당일 로컬 원장(`data/logs/ledger/job_log_YYYYMMDD.ndjson`)에 `SUCCESS` 기록이 있으면 멱등성으로 자동 스킵한다. 강제 재수집이 필요하면 해당 날짜 원장에서 해당 잡 라인을 정리하거나 다른 `--date`를 지정한다.
- **URL에 `:DATE`가 그대로 나가서 400 오류가 난다**
  → `api_job.params_json`의 키 이름(대소문자 정확히 일치)과 `request.url`의 `:KEY`가 어긋났을 때 발생한다. `dry-run`으로 즉시 확인한다.
- **`restricted env var ... cannot be referenced in ApiSpec`**
  → `S3_*`, `OCI_*`, `NEON_*`, `DATABASE_URL` 등 인프라 변수를 `${VAR}`에 참조한 경우다. 수집용 API 키는 별도 이름(예: `KIS_APP_KEY`, `DART_KEY`)으로 `.env`에 등록한다.
- **`DuckDB temp_directory ... is on a RAM-backed tmpfs mount`**
  → `V3_DUCKDB_TMP_DIR`을 `/tmp`로 지정한 경우다. 반드시 블록 디스크 경로(`data/tmp/duckdb`)를 사용한다.
- **KIS 토큰 갱신(`KIS_TOKEN_REFRESH`) 주의**
  → KIS OAuth 토큰은 발급 시 기존 토큰과 충돌할 수 있다. v2 인스턴스와 v3 인스턴스를 병행 운영할 때는 v3의 `daily-start` / `3m` 타이머를 비활성화해 중복 발급을 피한다.

---

## 9. 확인용 SQL 모음

### Neon PostgreSQL (운영·스케줄·로그 점검)

```sql
-- 1) 오늘 전체 스케줄(api_job + etl_job) 순서 확인
SELECT schedule_tag, run_order, job_type, job_id, target_ref, is_active
  FROM v_schedule_job
 WHERE is_active = TRUE
 ORDER BY schedule_tag, run_order, job_type, job_id;

-- 2) 최근 7일 일자·유형·상태별 실행 요약
SELECT target_date, job_type, status, COUNT(*) AS cnt,
       SUM(COALESCE(record_count, 0)) AS total_rows,
       ROUND(AVG(duration_ms)) AS avg_ms
  FROM job_log
 WHERE executed_at >= now() - INTERVAL '7 days'
 GROUP BY target_date, job_type, status
 ORDER BY target_date DESC, job_type, status;

-- 3) 최근 실패/차단 건 상세 확인
SELECT executed_at AT TIME ZONE 'Asia/Seoul' AS kst_time,
       job_type, job_id, partition_key, status, error_code, LEFT(error_message, 200) AS msg
  FROM job_log
 WHERE status IN ('FAILED', 'BLOCKED')
 ORDER BY executed_at DESC
 LIMIT 50;

-- 4) api_mst 변경 이력 조회 (특정 API 롤백 참고)
SELECT hist_id, api_id, action, changed_by,
       changed_at AT TIME ZONE 'Asia/Seoul' AS kst_time
  FROM api_mst_hist
 WHERE api_id = 'ACE_ETF_PDF'
 ORDER BY changed_at DESC
 LIMIT 10;

-- 5) 멀티 버킷(R2 포함) 타깃으로 설정된 API / ETL 목록
SELECT api_id, spec_json->'engine'->'sink'->'targets' AS targets
  FROM api_mst
 WHERE spec_json->'engine'->'sink'->'targets' ? 'r2';

SELECT job_id, output_layer, target_name, targets
  FROM etl_job
 WHERE 'r2' = ANY(targets);
```

### DuckDB (버킷 Parquet 데이터 점검 — DBeaver 등)

```sql
-- 1) 스냅샷 마스터 행 수 확인
SELECT 'mst_etf'   AS tbl, COUNT(*) AS cnt FROM read_parquet('s3://<bucket>/v3/snapshot/mst_etf.parquet')
UNION ALL
SELECT 'mst_fuopt' AS tbl, COUNT(*) AS cnt FROM read_parquet('s3://<bucket>/v3/snapshot/mst_fuopt.parquet')
UNION ALL
SELECT 'v_k2i_atm' AS tbl, COUNT(*) AS cnt FROM read_parquet('s3://<bucket>/v3/snapshot/v_k2i_atm.parquet');

-- 2) 특정 일자 raw_clean 수집 건수 확인
SELECT dt, COUNT(*) AS rows
  FROM read_parquet('s3://<bucket>/v3/raw_clean/ace_etf_pdf/dt=*/**/*.parquet', hive_partitioning=true)
 WHERE dt >= '20261120'
 GROUP BY dt
 ORDER BY dt DESC;

-- 3) 월별 Compaction(monthly)과 일별(daily) 정합성 수동 대조
WITH d AS (
  SELECT COUNT(*) AS cnt, BIT_XOR(HASH(*)) AS h
    FROM read_parquet('s3://<bucket>/v3/daily/kis_futopt_chart/ym=202611/dt=*/*.parquet')
),
m AS (
  SELECT COUNT(*) AS cnt, BIT_XOR(HASH(*)) AS h
    FROM read_parquet('s3://<bucket>/v3/monthly/kis_futopt_chart/ym=202611/202611.parquet')
)
SELECT d.cnt AS daily_cnt, m.cnt AS monthly_cnt,
       (d.cnt = m.cnt AND d.h = m.h) AS exact_match
  FROM d, m;
```
