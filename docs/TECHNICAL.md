# pyscrap 기술 문서

설정으로 굴러가는 스크래핑 엔진. 새 소스를 붙이는 일이 **파이썬 코드를 쓰는 게 아니라 DB에 행을 넣는 일**이 되도록 만들어져 있다. 이 문서는 그 행들을 어떻게 채우는가를 필드 단위로 설명한다.

---

## 1. 개념

### 1.1 왜 코드가 아니라 행인가

사이트마다 스크래퍼 클래스를 쓰면 소스가 늘 때마다 코드가 늘고, 그 코드를 배포해야 수집이 시작된다. 여기서는 "어디에 어떤 요청을 보내고 응답의 어느 가지가 결과인가"를 전부 테이블에 적는다. 엔진(`app/scrapers/dynamic.py`)은 그 행 하나를 읽어 요청을 조립하고 응답을 레코드로 펴는 일만 한다.

대가도 있다. 설정이 틀렸을 때 파이썬이 잡아주지 않고 **실행 시점에 드러난다.** 그래서 이 문서가 필요하다.

### 1.2 네 개의 테이블

```
api_mst           API 한 개의 정의        무엇을 어떻게 요청하고 어떻게 해석하는가
api_job_builder   그 API 를 어떻게 펼칠까   파라미터 조합 규칙 + 실행 주기
api_job           실제 실행 단위           확정된 파라미터 한 벌
api_job_log       실행 이력                SUCCESS / FAILED
```

`api_mst`는 사람이 API마다 한 번 쓴다. `api_job_builder`도 사람이 쓴다. `api_job`은 **생성 단계가 만든다** — 손으로 넣는 것이 아니다.

### 1.3 흐름

```
 ① 생성   generate-jobs <cycle>
          api_job_builder ──펼침──▶ api_job (여러 개)
                                    macro_params_json 의 곱집합

 ② 실행   run-cycle <cycle>
          api_job ──▶ DynamicApiScraper ──HTTP──▶ 외부 API
                            │
                            ├─ 파싱 ─▶ {selector: [record, ...]}
                            └─ 저장 ─▶ 결과 테이블 (또는 api_rst)
                                      + api_job_log 에 성공/실패

 ③ 내보내기 run-export
          결과 테이블 ──DB 가 직접──▶ Object Storage (Parquet)
                       DBMS_CLOUD.EXPORT_DATA
```

생성과 실행이 분리된 이유: 생성이 실패해도 이미 대기 중인 잡은 돌릴 값어치가 있고, 반대로 생성만 미리 해두고 실행은 나중에 할 수도 있다. systemd 타이머도 이 둘을 따로 부른다.

---

## 2. `api_mst` — API 정의

### 2.1 필드 한눈에

| 필드 | 필수 | 뜻 |
|---|---|---|
| `api_id` | ● | 기본키. 대문자·밑줄 관례 (`KIS_FUTOPT_CHART`) |
| `api_name` | ● | 사람이 읽는 이름 |
| `api_group` | ● | 묶음 라벨 (`KIS`, `DART`, `KRX`) |
| `request_type` | ● | `GET` / `POST` / `BROWSER`(브라우저로 조작, 2.11) |
| `api_url` | ● | `:KEY`(잡 파라미터)와 `${ENV}`(비밀값) 치환 |
| `header_json` | ● | 헤더. 값에도 두 치환이 적용된다 |
| `payload_type` | | `NULL` / `json` / `data` / `content` |
| `payload_json` | | `payload_type`이 `json`·`data`일 때 본문 |
| `payload_xml` | | `payload_type`이 `content`일 때 본문 |
| `behavior_json` | | `BROWSER` 행의 조작 시나리오 (2.11) |
| `response_type` | | `NULL`(자동) / `zip_delimited` / `delimited` / `binary` / `html_table` / `xlsx` / `xls`, `BROWSER` 행은 `dom` / `xhr` / `binary`, 로그인 행은 `session`(2.13) |
| `response_parse_json` | | 위 세 타입의 해석 규칙 |
| `output_tables_json` | ● | `{selector: 테이블명}` |
| `merge_fields_json` | | 응답의 다른 가지에서 값 끌어오기 |
| `persist_env_json` | | 응답 값을 `.env`에 저장 (토큰 갱신) |
| `key_params_list` | | job_id 재료 + 행 각인 + 반복 여부 |
| `pagination_json` | | 이어받기 방식 |

### 2.2 `api_url` — 두 가지 치환

```
${KIS_PROD}/uapi/.../inquire-price?fid_input_iscd=:SHORT_CODE&fid_input_date_1=:DATE
   └────────┘                                       └─────────┘        └───┘
   .env 의 값                                        잡 파라미터
```

- **`${VAR}`** — `.env`에서 온다. **비밀값을 DB에 넣지 않기 위한 장치다.** API 키·토큰은 반드시 이 형태로 쓴다.
- **`:KEY`** — 잡의 `params_json`에서 온다. 낱말 경계로 매칭하므로 `:KACD`가 `:KACD2`를 삼키지 않는다.

치환할 값이 없으면 **조용히 그대로 남는다.** `:DATE`가 URL에 남은 채 요청이 나가면 API가 400을 주는데, 원인이 설정에 있다는 게 안 보인다. 새 API를 등록하면 반드시 한 번 실행해 확인할 것.

### 2.3 `header_json`

```json
{
  "content-type": "application/json",
  "authorization": "Bearer ${KIS_ACCESS_TOKEN}",
  "appkey": "${KIS_APP_KEY}",
  "tr_id": "FHKIF03020100"
}
```

값에도 `${ENV}`와 `:KEY` 치환이 적용된다. 헤더가 필요 없으면 `{}`.

### 2.4 본문 — 세 형태 중 하나 (제약으로 강제됨)

DB에 `ck_api_mst_payload_exclusive` 체크 제약이 걸려 있어 어긋난 조합은 INSERT가 거절된다.

```
payload_type = NULL       payload_json = NULL,     payload_xml = NULL      GET 의 보통
payload_type = 'json'     payload_json = {...},    payload_xml = NULL      JSON 본문
payload_type = 'data'     payload_json = {...},    payload_xml = NULL      폼 인코딩
payload_type = 'content'  payload_json = NULL,     payload_xml = '<...>'   원문 텍스트
```

`json`/`data`에서 `payload_json`은 **키 덮어쓰기** 방식이다 — `:KEY` 문자열 치환이 아니라, 잡 파라미터에 같은 이름의 키가 있으면 그 값으로 바뀐다.

`content`의 `payload_xml`은 `:KEY` 문자열 치환이다.

예 (토큰 갱신, `payload_type='json'`):

```json
{"grant_type": "client_credentials",
 "appkey": "${KIS_APP_KEY}", "appsecret": "${KIS_APP_SECRET}"}
```

### 2.5 `response_type` — 응답을 어떻게 읽을 것인가

| 값 | 언제 |
|---|---|
| `NULL` | JSON 또는 XML. content-type을 보고, 아니면 본문 첫 글자로 판별한다 |
| `zip_delimited` | ZIP 안에 구분자 텍스트 파일 하나 (KIS `.mst.zip` 마스터) |
| `delimited` | 응답 본문 자체가 구분자 텍스트 (`.csv` 다운로드) |
| `binary` | 응답이 문서 그 자체 (PDF·ZIP). 통째로 디스크에 저장하고 메타데이터만 행으로 남긴다 |

`session` 은 전송 방식과 무관하게 "로그인만 하고 세션만 남긴다"는 뜻이다 (2.13).

`BROWSER` 행은 같은 칸을 자기 값으로 읽는다 — 조작이 **무엇을 남겼는가**다. `dom`(기본) / `xhr` / `binary` / `session`, 2.11 참고.

**`zip_delimited` / `delimited`의 `response_parse_json`:**

```json
{"encoding": "cp949",
 "delimiter": "|",
 "fields": ["info_type", "short_code", "std_code", "kor_name", "..."],
 "inner_file": "fo_idx_code_mts.mst"}
```

- `fields` — 컬럼 이름을 순서대로. `has_header: true`를 주면 파일 첫 줄에서 읽는다
- `inner_file` — `zip_delimited` 전용, 생략하면 ZIP의 첫 항목
- 빈 칸은 `""`가 아니라 `None`이 된다 (KRX가 "해당 없음"을 공백 한 칸으로 채우기 때문)

**`binary`의 `response_parse_json`:**

```json
{"group": "dart_docs", "name": ":RCEPT_NO.zip", "unzip": true}
```

- `group` — 저장 디렉터리 이름
- `name` — 파일명. `:KEY` 치환이 적용된다. **서버가 부르는 이름이 아니라 잡의 파라미터로 짓는다** — 그래야 설정이 틀렸을 때 같은 파일을 계속 덮어쓰는 일이 없다
- `unzip` — 받은 ZIP을 풀어 각 파일을 따로 다룰지

**`zip_delimited`·`delimited`에는 selector 개념이 없다.** 평평한 텍스트라 "응답의 어느 가지"가 없기 때문이다. `output_tables_json`의 **모든 키가 같은 레코드 목록을 받는다** — 키 이름은 읽히지도 않으므로 `{"records": "fo_idx_code_mst"}`의 `records`는 아무 이름이나 좋다.

### 2.6 `output_tables_json` — 어느 가지를 어느 테이블로

```json
{"output1": "api_rst", "output2": "kis_futopt_chart"}
```

- **키** = 응답 안의 경로. JSON은 점 표기(`data.items`), XML은 태그 경로(`.//result`)
- **값** = 저장할 테이블 이름

값이 갈 수 있는 곳은 두 종류다:

```
전용 테이블 (job_id 컬럼이 있는 SQLModel 모델)   레코드를 컬럼으로 펼쳐 넣는다
api_rst    (범용)                              레코드 전체를 result_json 에 통째로
```

**`job_id` 컬럼이 있는가가 "스크래퍼 결과 테이블"의 정의다** (`app/services/export.py`의 `TABLE_REGISTRY`). 그 컬럼이 없으면 엔진이 결과 테이블로 인정하지 않고, 내보내기·재실행 정리에서도 빠진다.

전용 테이블에 넣을 때 **모델에 없는 키는 조용히 버려진다.** 응답에 새 필드가 생겨도 안 깨지는 대신, 오타 난 컬럼 이름도 안 알려준다.

### 2.7 `key_params_list` — 세 가지를 한꺼번에 정한다

이 필드가 API 설정에서 가장 미묘하다. 세 가지를 동시에 결정한다:

```
① job_id 의 재료      같은 값 → 같은 job_id → 중복 생성 안 됨
② 결과 행에 각인      응답에 없는 값을 컬럼으로
③ 반복 잡 여부        NOW 가 있으면 반복
```

**형태:**

```json
["SHORT_CODE", {"DATE": "trade_date"}, {"NOW": "trade_at"}, "BAR_SEC"]
```

- `"이름"` — 파라미터 이름 = 컬럼 이름(소문자)
- `{"파라미터": "컬럼"}` — 둘이 달라야 할 때. `DATE`는 Oracle 예약어라 컬럼으로 못 쓰므로 `trade_date`로 보낸다
- `"NOW"` / `{"NOW": "컬럼"}` — **예약어.** 아래 참조

**`NOW`의 의미:**

| | `NOW` 없음 | `NOW` 있음 |
|---|---|---|
| 잡 성격 | 일회성 — 성공하면 `is_active=0`, 다시 안 돎 | 반복 — 계속 활성, 매 주기 실행 |
| job_id | 지정한 파라미터 값들로 조립 | `NOW`는 **제외**되므로 id가 고정 |
| 각인 | 파라미터 값 | 실행 시각 (`YYYYMMDDHHMMSS`, 주기 단위로 내림) |

이게 이 시스템에서 가장 사고가 잦은 지점이다. 실제로 있었던 일:

- `FO_IDX_CODE_MST`가 `key_params_list=['MST_FILE']`이었다. 파라미터가 상수라 job_id가 고정되고, 일회성이라 한 번 성공한 뒤 **6일간 조용히 멈춰 있었다.** `NOW`를 넣어 반복 잡으로 바꿔 해결했다
- 반대로 `KIS_INDEX_DAILY`는 롤링 날짜가 job_id에 들어가 매일 새 잡이 생겼는데, 겹치는 구간이 매일 20행씩 쌓였다. 기간을 하루로 좁혀 해결했다

**판단 기준:**

```
매일 같은 요청을 반복하는가 → NOW 를 넣어 반복 잡으로
날짜가 URL 에 들어가는가   → 그 날짜를 job_id 에 넣어 매일 새 잡으로
```

둘 다는 안 된다. `NOW` 값은 **요청에 닿지 않기** 때문이다 — `run_job`이 스크래퍼에 넘기는 건 `params_json`뿐이고, `NOW`는 저장 단계로만 간다. 즉 반복 잡의 URL 날짜는 생성 시점에 굳는다.

**150자 상한:** job_id가 길어지면 원본 값 대신 sha256 앞 16자로 바뀐다. 여전히 결정적이지만 사람이 못 읽는다.

**`key_params_list`가 비면** job_id가 무작위(uuid)가 되고 중복 검사가 불가능해진다 — 생성할 때마다 새 잡이 쌓인다. 거의 항상 실수다.

### 2.8 `pagination_json` — 이어받기

API마다 "더 있다"를 말하는 방식이 달라 세 가지를 지원한다.

**page — 페이지 번호를 올린다**

```json
{"mode": "page", "param": "PAGE", "start": 1, "max_pages": 50}
```

`api_url`이나 페이로드에 `:PAGE`가 있고 `pagination_json`이 비어 있으면 이 모드로 간주한다 (옛 행 호환).

**cursor — 방금 받은 답에서 다음 기준점을 읽는다**

```json
{"mode": "cursor", "param": "HHMM", "from": "output2[].stck_cntg_hour",
 "pick": "min", "min_records": 99, "max_pages": 10}
```

- `from` — 응답에서 기준값을 읽을 경로. `[]`는 리스트를 펼친다
- `pick` — `min`(과거로) 또는 `max`(미래로)
- `min_records` — **끊김 신호.** 이보다 적게 오면 시리즈가 끝난 것으로 보고 멈춘다. 없으면 같은 답을 영원히 다시 받을 수 있다

1분봉이 이 방식이다. 15:45에서 시작해 99봉씩 거슬러 올라가 하루를 덮는다.

**token — 서버가 준 열쇠를 되돌려준다**

```json
{"mode": "token",
 "params": {"CTX_FK": "header:ctx_area_fk100", "CTX_NK": "header:ctx_area_nk100"},
 "continue_when": {"source": "header:tr_cont", "in": ["F", "M"]},
 "max_pages": 100}
```

- `header:<이름>`으로 헤더를, 점 경로로 본문을 읽는다
- `continue_when`이 참인 동안만 계속한다

**공통:** `max_pages`는 모든 모드의 안전망이다. 멈춤 조건이 안 걸리는 날을 위한 것이므로 반드시 준다.

**이음매 중복:** 커서는 마지막 답의 값을 기준으로 다시 요청하므로 그 레코드가 다시 온다. 엔진이 중복을 걸러내니 설정에서 신경 쓸 것은 없다.

### 2.9 `merge_fields_json` — 응답의 다른 가지에서 값 끌어오기

```json
{"output1": {"output3.bstp_nmix_prpr": "kospi200_idx"}}
```

`output3`에 있는 지수 값을 `output1`의 **모든 레코드에** 붙인다. 따로 저장한 뒤 조인하면 시각이 어긋날 수 있는데, 같은 응답에서 뽑아 같은 행에 넣으면 그 위험이 없다.

**키는 응답 안의 경로로만 해석된다.** 리터럴을 넣을 수 없고, 경로가 틀리면 오류가 아니라 **`None`이 들어간다.** 오타를 내면 실행은 성공하고 컬럼만 계속 빈다:

```sql
-- 경로가 맞는지 확인하는 법
SELECT COUNT(*), COUNT(kospi200_idx) FROM kis_futopt_price;
```

### 2.10 `persist_env_json` — 응답 값을 `.env`에 저장

```json
{"access_token": "KIS_ACCESS_TOKEN"}
```

응답의 `access_token`을 `.env`와 `os.environ`에 쓴다. 토큰 갱신 잡이 이걸 쓴다.

각 잡이 자기 프로세스로 돌고 `app.auth_config`가 임포트 때 `.env`를 다시 읽으므로, **갱신 잡을 먼저 스케줄하기만 하면** 이후 잡들의 `${KIS_ACCESS_TOKEN}`이 새 값을 집는다. 따로 배선할 것이 없다.

이 일만 하는 잡은 `output_tables_json`을 비워도 된다.

---

### 2.11 `behavior_json` — 브라우저로 조작해서 얻는 소스

입력하고 눌러야 답을 주는 화면은 요청을 만들 수 없다. `request_type`을 `BROWSER`로 두면 그 행은 httpx 대신 **실제 브라우저(Playwright/chromium)** 로 돌아간다. 나머지는 전부 같다 — 같은 `api_mst` 행, 같은 `:KEY`/`${VAR}` 치환, 같은 `output_tables_json`, 같은 잡·빌더·결과 테이블. **잡 계층은 브라우저인지 아닌지 모른다.**

`api_url`이 시작 페이지다. 먼저 열고 나서 단계를 순서대로 실행한다.

```json
[
  {"action": "fill",     "selector": "#isuCd", "value": ":ISIN"},
  {"action": "select",   "selector": "#trdDd", "value": ":BASDD"},
  {"action": "click",    "selector": "#searchBtn"},
  {"action": "wait_for", "selector": "#grid tbody tr"}
]
```

| action | 인자 |
|---|---|
| `goto` | `url`(생략하면 `api_url`), `wait_until` |
| `fill` | `selector`, `value` |
| `select` | `selector`, `value` |
| `check` | `selector`, `checked`(기본 true) |
| `click` | `selector` |
| `press` | `key`, `selector`(없으면 키보드로) |
| `wait_for` | `selector`, `state`, `timeout`(초) |
| `wait_load` | `state`(기본 `networkidle`) |
| `sleep` | `seconds` |
| `frame` | `selector`·`name`·`url` 중 하나, 또는 `reset: true` |
| `eval` | `script` |
| `download` | `selector` — 눌러서 나온 파일을 받는다 |

**값에도 두 치환이 다 적용된다.** 검색창에 넣는 값이 잡 파라미터(`:ISIN`)일 수 있고, 로그인 아이디는 `${KRX_USER_ID}`로 쓴다 — **비밀값은 여기서도 DB에 넣지 않는다.**

**`response_type` — 조작이 무엇을 남겼는가**

| 값 | 언제 | `response_parse_json` |
|---|---|---|
| `dom`(기본) | 화면에 표로 그려졌다 | `{"rows": "...", "fields": [...]}` |
| `xhr` | 화면이 내부적으로 JSON을 불렀다 | `{"url": "getJsonData.cmd"}` |
| `binary` | 눌렀더니 파일이 떨어졌다 | `binary`와 동일 (`group`/`name`) |
| `session` | 로그인만 하고 쿠키만 남긴다 | `{"state": "..."}` |

**`xhr`을 먼저 시도할 것.** 화면 개편에 CSS 경로는 다 깨지지만 JSON 본문은 남는다. 잡히면 selector 해석은 HTTP 행과 **완전히 같은 코드**를 탄다 (`{"output": "..."}` 그대로).

**`dom` 추출 규칙** — 화면이 하나면 최상위에, 여러 갈래면 selector 이름별로 둔다. `rows`가 있는 dict가 규칙 블록이라, 옆에 놓인 예약 키(`state`·`login`·`logged_out`·`url`)와 헷갈릴 일이 없다.

```json
{"rows": "#grid tbody tr", "fields": ["isin", "name", "amount"]}
```

- `fields`가 **리스트**면 행의 칸(`td, th`)을 순서대로 읽는다 — 구분자 파일·엑셀과 같은 처리(`skip_rows`·`has_header`도 그대로 쓴다)
- `fields`가 **dict**면 필드마다 css를 준다: `{"url": {"css": "td a", "attr": "href"}}`

**이어받기** — `pagination_json`에 세 모드가 있다. 멈추는 조건은 HTTP 쪽과 같다(빈 페이지 / 직전과 동일 / 버튼 없음 / `max_pages`).

```json
{"mode": "click", "selector": "a.next", "wait_for": "#grid tbody tr"}
{"mode": "scroll"}
{"mode": "page", "param": "PAGE", "start": 1, "max_pages": 60}
```

`page`는 HTTP 쪽의 그 모드와 같은 뜻(이쪽이 세는 페이지 번호)이고, **페이저가 링크가 아니라 스크립트인 화면**을 위해 있다. `:PAGE`를 `behavior_json` 안에 쓰면(보통 사이트 자신의 페이징 함수를 부르는 `eval` 단계) 페이지마다 시나리오를 처음부터 다시 실행한다. 클릭보다 느리지만, 페이저 마크업이 아예 안 오는 화면에서는 이것만 통한다 — 2.12 참고.

**로그인** — 로그인은 별도 행이다. `response_type: "session"`으로 만들고, 끝나면 쿠키가 `data/state/<호스트>.json`에 저장된다. 데이터 행은 같은 호스트면 자동으로 그 파일을 물고 시작한다.

```json
// KRX_LOGIN (response_type: "session")
[{"action": "fill", "selector": "#id", "value": "${KRX_USER_ID}"},
 {"action": "fill", "selector": "#pw", "value": "${KRX_USER_PW}"},
 {"action": "click", "selector": "#loginBtn"},
 {"action": "wait_for", "selector": "#logoutBtn"}]

// 데이터 행의 response_parse_json
{"login": "KRX_LOGIN", "logged_out": "#loginForm", "rows": "...", "fields": [...]}
```

**만료는 상태코드로 오지 않는다** (KRX는 400 + 본문 `LOGOUT`). 그래서 `logged_out`에 "로그아웃됐을 때만 보이는 css"를 직접 적어준다. 그게 보이거나 대기가 시간초과되면 **로그인 행을 다시 실행하고 한 번만 재시도한다** — 30분짜리 세션이 한 시간짜리 배치를 못 버티기 때문이다. 이 고리는 브라우저 행에만 있고, HTTP 행의 `fetch()`는 건드리지 않는다.

**쿠키는 프로세스를 못 넘는다.** KIS 토큰처럼 `.env`에 남는 게 아니라 파일이라, 로그인 행과 데이터 행이 **같은 배치 안에** 있어야 한다 — 로그인 행을 같은 `execution_cycle`의 앞선 빌더로 두거나, 데이터 행에 `login`을 적어 알아서 로그인하게 한다(후자가 낫다).

**설치와 배포** — 브라우저 행은 인스턴스에서만 돈다.

```bash
pip install -e ".[browser]"
```

```bash
playwright install --with-deps chromium
```

FastAPI Cloud 배포에는 chromium이 없다. 엔진이 브라우저 모듈을 **행이 요구할 때만** 임포트하므로 API는 정상 기동하지만, 거기서 `POST /jobs/run/`으로 브라우저 잡을 돌리면 설치 안내가 담긴 `ImportError`로 실패한다. **브라우저 잡의 시험 실행은 인스턴스에서 한다.**

`.env`로 조절할 것: `BROWSER_HEADLESS=0`(창을 띄워 눈으로 보기), `BROWSER_TIMEOUT_MS`(기본 15000), `BROWSER_STATE_DIR`(기본 `data/state`), `BROWSER_LOCALE`(기본 `ko-KR`), `BROWSER_TIMEZONE`(기본 `Asia/Seoul`).

**`BROWSER_LOCALE` 를 비우지 말 것.** headless chromium 은 기본적으로 `Accept-Language` 를 **안 보낸다**. 언어를 밝히지 않은 요청에 서버가 다른 답을 주는 경우가 실제로 있다 — KRX 표준코드 조회는 이때 **깨진 페이지**(결과 5건 중 1건만, 나머지 문서가 그 행의 칸 안에 중첩된 채)를 준다. 값이 무엇이든(`en-US` 도 된다) 있기만 하면 정상이다. 엔진이 컨텍스트에 locale 을 주는 이유가 이것이고, 개별 행은 `header_json` 에 `Accept-Language` 를 직접 넣어 덮을 수 있다.

---

### 2.12 예제 — KRX 표준코드 조회에서 ISIN 뽑기

`https://isin.krx.co.kr/srch/srch.do?method=srchList` 는 입력하고 눌러야 답을 주는 전형적인 화면이다. 표준코드 입력란에 `KR2`를 넣고 **조회**를 누른 뒤, 결과 표(페이지당 5건)의 ISIN을 54페이지까지 훑는 행 두 개.

```sql
INSERT INTO api_mst (api_id, api_name, api_group, request_type, api_url, header_json,
                     behavior_json, response_type, response_parse_json,
                     pagination_json, output_tables_json, key_params_list, description)
VALUES ('KRX_ISIN_SRCH', 'KRX 표준코드 조회', 'KRX', 'BROWSER',
        'https://isin.krx.co.kr/srch/srch.do?method=srchList', '{}',
        '[{"action": "fill",      "selector": "#isur_nm1", "value": ":CODE"},
          {"action": "eval",      "script": "$(''form[name=JLDINF20000]'').append(\"<input type=''hidden'' name=''pageIndex'' value='':PAGE''>\"); fn_search(2);"},
          {"action": "wait_load", "state": "networkidle"}]',
        'dom',
        '{"rows": "#tbody tr[name=dataTr]:has(a[href*=''onPopupCode''])",
          "fields": {"isin":      "a[href*=''onPopupCode''] strong",
                     "prod_type": "td:nth-child(1)",
                     "kor_name":  "td:nth-child(3)",
                     "issuer":    "td:nth-child(4)",
                     "issue_dd":  "td:nth-child(5)",
                     "grant_dd":  "td:nth-child(9)"}}',
        '{"mode": "page", "param": "PAGE", "start": 1, "max_pages": 60}',
        '{"output": "api_rst"}', '["CODE"]', '표준코드 앞자리로 조회');

INSERT INTO api_job_builder (build_id, api_id, macro_params_json, is_active,
                             save_mode, execution_cycle, description)
VALUES ('KRX_ISIN_SRCH_KR2', 'KRX_ISIN_SRCH', '{"CODE": "KR2"}', 0,
        'overwrite', 'test', 'KR2 로 시작하는 표준코드');
```

```bash
python -m app.cli generate-builder KRX_ISIN_SRCH_KR2
```

```bash
python -m app.cli run-cycle test
```

`test` 주기에는 타이머가 없다 (5.2 참고) -- 손으로 돌려보는 잡들이 모여 있는 자리다. 확인이 끝나면 빌더의 `execution_cycle` 을 `daily_batch2` 같은 실제 주기로 바꾸고 `is_active = 1` 로 올린다. **주기와 `save_mode` 는 잡 생성 시점에 복사되므로**, 이미 만들어진 잡은 다시 만들거나 직접 고쳐야 한다 (9장 참고).

읽는 순서대로 짚으면:

- **`#isur_nm1`** 이 표준코드/종목명 입력란이다. `:CODE` 라 빌더의 `macro_params_json` 이 값을 정한다 — `{"db": "SELECT ..."}` 로 바꾸면 코드 목록만큼 잡이 펼쳐진다
- **페이지 번호는 `pageIndex` 다.** 화면의 페이지 버튼이 하는 일이 그대로 `href` 에 적혀 있다 — `document.JLDINF20000.pageIndex.value=2; fn_search(2)`. 그래서 `eval` 로 같은 일을 한다. `fn_search` 의 인자가 **`1` 이 아니어야 한다**는 것이 유일한 함정이다: `fn_search('1')` 은 `pageIndex` 를 1로 되돌리므로, 조회 버튼을 그냥 클릭하면 영원히 1페이지다
- **`wait_load`** 로 기다린다. 조회가 페이지 이동(POST)이라 이동이 끝나는 것이 곧 결과다. 결과 행을 `wait_for` 로 기다리게 하면 **검색 결과가 없을 때 시간초과로 잡이 실패한다**
- **`:has(a[href*='onPopupCode'])`** 는 "표준코드 링크가 있는 행"만 남긴다 — 결과가 없을 때 나오는 `검색 결과가 없습니다` 행이 걸러진다
- `{"mode": "page"}` 로 54페이지를 훑는다. 마지막 페이지를 넘어가면 서버가 같은 답을 되풀이하는데, 엔진이 **직전과 동일한 응답**을 정지 신호로 쓰므로 `max_pages` 는 안전망으로만 둔다

실행 결과 (2026-09-01 확인) — 페이지당 5건, 54페이지를 돌아 270건 전부 고유:

```json
[{"isin": "KR2001024G92", "prod_type": "채권", "kor_name": "서울도시철도공채증권 26-09",
  "issuer": "서울특별시", "issue_dd": "2026-09-30", "grant_dd": "2026-08-31"},
 {"isin": "KR2002022G92", "prod_type": "채권", "kor_name": "강원지역개발채권 26-09",
  "issuer": "강원도", "issue_dd": "2026-09-30", "grant_dd": "2026-08-31"},
 ...]
```

**하루를 잡아먹은 함정을 적어둔다.** 이 화면은 `Accept-Language` 헤더가 **없는** 요청에 깨진 페이지를 준다 — 결과가 5건이 아니라 1건만 오고, 문서의 나머지가 그 행의 `<td>` 안에 통째로 중첩된다(페이지 버튼도 그때는 사라진다). headless chromium 이 기본적으로 이 헤더를 안 보내기 때문에, **선택자는 멀쩡한데 데이터가 1/5만 나오는** 모습으로 나타난다. 엔진이 컨텍스트에 `locale` 을 주면서(2.11) 해결됐다. 브라우저 행이 화면과 다르게 동작하면 **선택자를 의심하기 전에 요청 헤더부터 비교**할 것 — 헤드풀(`BROWSER_HEADLESS=0`)로 한 번 띄워 보면 바로 갈린다.

---

### 2.13 로그인이 필요한 소스 — `session` 행

로그인은 **별도의 `api_mst` 행**이다. 결과를 버리고 세션만 남기는 행이라 `response_type = 'session'` 을 쓰고, 이 행만 `output_tables_json` 을 비워도 된다. HTTP 행이든 브라우저 행이든 같다.

| | HTTP 행 | 브라우저 행 |
|---|---|---|
| 세션이 사는 곳 | 프로세스 공유 `httpx.Client` 의 쿠키 항아리 | `data/state/<키>.json` (storage state) |
| 유효 범위 | **같은 프로세스 안에서만** | 파일이라 프로세스를 넘는다 |
| 로그아웃 판정 | 응답 본문에 들어 있는 문자열 | 화면에 있는 css 선택자 |

**로그인 행** (KRX 데이터마켓, 실제로 도는 설정):

```json
// request_type: POST, payload_type: data
// api_url: https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd
{"mbrNm": "", "telNo": "", "di": "", "certType": "",
 "mbrId": "${KRX_DATA_ID}", "pw": "${KRX_DATA_PW}", "skipDup": "Y"}

// response_type: "session"
// response_parse_json:
{"expect": "\"_error_code\":\"CD001\""}
```

- **비밀값은 `${VAR}`** 로 쓴다. DB에 계정이 들어가지 않는다
- **`expect`** 는 응답에 반드시 들어 있어야 하는 문자열이다. 없으면 로그인 행이 실패한다 — 이게 없으면 비밀번호가 틀려도 로그인 잡은 성공으로 끝나고 **뒤따르는 잡 전부가 이유 없이 실패한다** (KRX는 성공·실패 모두 HTTP 200이다)

**데이터 행**은 자기 로그인 행을 이름으로 가리킨다:

```json
// response_parse_json
{"logged_out": "LOGOUT", "login": "KRX_DATA_LOGIN"}
```

- **`logged_out`** — "로그아웃됐다"는 신호. KRX는 **HTTP 400 + 본문 `LOGOUT`** 으로 답하므로 상태코드로는 못 읽는다. 그래서 행이 직접 적는다
- **`login`** — 다시 로그인할 때 실행할 행의 `api_id`

### 만료는 순서가 아니라 재로그인으로 푼다

세션은 짧고(KRX 30분) 배치는 길다. 그래서 만료는 배치 **한가운데**서 일어나고, 로그인을 맨 앞에 세우는 것으로는 못 막는다. 실제 장치는 이것이다 (`app.scrapers.base.BaseScraper.collect`):

```
잡 실행 → logged_out 감지 → login 행 실행 → 같은 잡 1회 재시도
```

**재시도는 한 번뿐이다.** 비밀번호가 틀렸을 때 로그인 루프로 계정을 잠그지 않기 위해서다. `login` 이 안 적힌 행은 그냥 실패한다.

`run_cycle` 은 그 위에서 **`session` 행의 잡을 먼저 실행한다**(`_login_rows_first`). 새 컬럼은 없다 — 행의 `response_type` 이 이미 "이건 다른 행보다 먼저"라고 말하고 있기 때문이다. 이 정렬이 아끼는 건 "첫 잡의 불필요한 실패 한 번"이고, 만료 자체는 위의 재로그인이 처리한다.

**HTTP 로그인은 같은 프로세스 안에서만 유효하다.** 쿠키가 `.env` 에 남는 KIS 토큰과 달리 프로세스를 못 넘으므로, 로그인 행과 데이터 행은 **같은 `execution_cycle`** 에 있어야 한다. 다른 사이클로 빼면 타이머마다 프로세스가 새로 뜨면서 쿠키가 사라진다.

### 어느 쪽으로 붙일 것인가

로그인 화면이 브라우저를 요구하더라도, **데이터는 HTTP 행으로 받을 수 있는지 먼저 확인할 것.** 브라우저 행의 `response_type: "xhr"` 로 한 번 잡아 보면 화면이 실제로 부르는 요청과 파라미터가 그대로 보인다. KRX ETF 구성종목이 그 예다.

| | 브라우저 행 | HTTP 행 |
|---|---|---|
| ETF 1종목 | 약 10초 | **0.5초** (측정치) |
| 900종목 | 2시간 30분 | **8분** |
| 필요한 것 | chromium | 없음 |

브라우저가 유일한 답인 경우는 남는다 — 로그인이 캡차나 JS 암호화를 요구하거나, 파라미터를 끝내 알아낼 수 없거나, 응답이 화면에서만 조립되는 경우다.

---

## 3. `api_job_builder` — 잡을 어떻게 펼칠까

### 3.1 필드

| 필드 | 뜻 |
|---|---|
| `build_id` | 기본키 |
| `api_id` | 어느 API를 쓰는가 |
| `macro_params_json` | 파라미터 조합 규칙 |
| `is_active` | 꺼져 있으면 `generate-jobs`가 건너뛴다 |
| `save_mode` | `overwrite`(기본) / `append` |
| `execution_cycle` | 어느 타이머가 부를 것인가 |

### 3.2 `macro_params_json` — 곱집합

각 항목이 하나의 축이고, 축들의 **곱집합**이 잡 목록이 된다.

```json
{
  "SHORT_CODE": {"db": "SELECT short_code FROM mst_fuopt WHERE ..."},
  "DATE1": {"date": "today"},
  "DATE2": {"date": "today"},
  "GROUP": "110110"
}
```

**세 가지 값 형태:**

| 형태 | 뜻 |
|---|---|
| 리터럴 `"110110"` | 모든 잡에 같은 값. 잡 수를 늘리지 않는다 |
| `{"db": "SELECT ..."}` | 질의를 돌려 **행마다 잡 하나**. 여기서 잡이 불어난다 |
| `{"date": "키워드", "format": "..."}` | 생성 시점에 날짜 문자열로 확정 |

**날짜 키워드:**

| 키워드 | 뜻 | 기본 포맷 |
|---|---|---|
| `today` | 오늘 | `%Y%m%d` |
| `yesterday` | 어제 | `%Y%m%d` |
| `last_bday` | 직전 영업일 (월~금, 공휴일 달력 없음) | `%Y%m%d` |
| `last_month` | 한 달 전 같은 날 | `%Y%m%d` |
| `last_eom` | 지난달 말일 | `%Y%m%d` |
| `last_yymm` | 지난달 | `%Y%m` |

날짜는 **KST 기준**으로 계산된다. 서버가 UTC라도 시장의 하루를 쓴다.

**여러 컬럼을 함께 받기** — 키를 쉼표로 잇는다:

```json
{"SHORT_CODE,DATE": {"db": "SELECT f.short_code, TO_CHAR(v.trade_date,'YYYYMMDD') FROM ..."}}
```

한 행의 두 컬럼이 한 조합으로 묶인다. 종목마다 다른 날짜를 붙일 때 쓴다 (백필이 이 형태였다).

**하지 말 것:** 수집 시각을 여기에 넣지 말 것. 생성 시점에 확정되므로 **부를 때마다 새 잡이 생긴다.** 그 용도는 `key_params_list`의 `NOW`다.

### 3.3 `save_mode`

실행 직전에 **그 job_id의 이전 결과를 지울 것인가**를 정한다. 테이블을 비우는 것이 아니라 그 잡이 넣은 행만 지운다.

```
overwrite   같은 job_id 의 이전 행을 지우고 다시 넣는다
append      그냥 추가한다
```

**판단 기준은 "반복이냐"가 아니라 이것이다:**

```
이 잡의 결과가 누적되는 이력이다        → append
이 잡의 결과가 그때그때의 전체 상태다    → overwrite
```

- 3분 스냅샷 — 시계열이므로 `append`. `overwrite`였다면 틱마다 앞선 행이 지워진다
- 원본 마스터 — 반복 잡이지만 매일 전량이 갈아엎히는 스냅샷이라 `overwrite`가 맞다
- 종목×날짜 잡 — `overwrite`. 잡 하나가 "그 종목의 그날치"라 재실행이 정확히 그날만 교체한다

`save_mode`는 **빌더와 잡 양쪽에 있고, 실행할 때 읽는 건 잡 쪽이다.** 빌더를 고쳐도 이미 만들어진 잡에는 반영되지 않는다.

### 3.4 `execution_cycle`

코드에 특별한 의미가 없는 **라벨**이다. `generate-jobs <값>` / `run-cycle <값>`이 이 문자열로 잡을 골라낸다.

현재 타이머가 부르는 값:

```
daily_start   08:35    3m_call / 3m_put   08:42~16:45
5m            08:40~   1h                 09:00~16:00
daily_close   16:00    daily_batch1       18:00    daily_batch2   21:00
```

`once`는 **어느 타이머도 부르지 않는다.** 샘플·시험용과 수동 보정 작업의 자리다.

부수 효과 하나: `NOW` 각인 시각이 이 값의 단위로 내림된다. `3m_call`이면 3분 경계로 내려 같은 틱의 잡들이 같은 시각을 갖고, `<숫자><단위>` 형태가 아닌 값(`daily_start`, `once`)은 내림 없이 실행 순간이 찍힌다.

---

## 4. 새 API 등록 절차

### 4.1 순서

1. **엔드포인트를 손으로 한 번 호출한다.** 응답 구조를 봐야 `output_tables_json`을 정할 수 있다
2. **`api_mst` 행을 넣는다.** 비밀값은 반드시 `${VAR}`로
3. **결과 테이블을 정한다.** 처음에는 `api_rst`로 시작해도 된다 — 응답을 그대로 보관하니 구조를 파악한 뒤 전용 테이블로 옮기면 된다
4. **전용 테이블이 필요하면** `app/db/models.py`에 모델을 추가한다. **`job_id` 컬럼이 반드시 있어야** 결과 테이블로 인정된다
5. **빌더를 `is_active=False`로 만들고** `generate-builder <build_id>`로 잡을 만들어 한 번 돌려본다
6. **행이 제대로 들어갔는지 확인한 뒤** 빌더를 활성화한다

### 4.2 `api_rst`로 먼저 받아보기

```json
{"output1": "api_rst"}
```

`api_rst`는 레코드를 `result_json`에 통째로 넣는다. 스키마를 정하기 전에 실제 응답을 보는 가장 빠른 길이다.

```sql
SELECT result_json FROM api_rst WHERE api_id = 'NEW_API' FETCH FIRST 1 ROWS ONLY;
```

### 4.3 전용 테이블 모델

```python
class KisFutoptChart(SQLModel, table=True):
    __tablename__ = "kis_futopt_chart"

    id: Optional[int] = Field(default=None,
        sa_column=Column(Integer, Identity(start=1), primary_key=True))
    api_id: str = Field(max_length=150)
    job_id: str = Field(index=True, max_length=150)      # ← 이게 있어야 결과 테이블
    short_code: Optional[str] = Field(default=None, index=True, max_length=20)
    stck_bsop_date: str = Field(index=True, max_length=8)
    futs_prpr: Optional[float] = Field(default=None)
    updated_at: Optional[datetime] = Field(default=None,
        sa_column=Column(DateTime, server_default=func.now(), onupdate=func.now()))
```

**컬럼 이름은 응답 필드 이름을 그대로 쓴다.** 이름이 맞아야 값이 들어간다. 굳이 예쁘게 바꾸면 매핑을 따로 관리해야 한다.

인덱스는 **실제로 거를 컬럼에만** 준다. `api_id`는 한 테이블에 한 값뿐이라 걸러주는 게 없으면서 비용은 같다 — 916만 행에서 268MB를 쓰고 있어 걷어냈다.

### 4.4 시험 실행

```bash
python -m app.cli generate-builder NEW_BUILDER   # 비활성 빌더도 생성된다
python -m app.cli run-cycle once
```

또는 API로:

```
POST /jobs/generate/builder/NEW_BUILDER
POST /jobs/run/{job_id}
```

`POST /jobs/run/`은 실패해도 200을 준다 (예외를 잡아 로그에 남기고 다음으로 넘어가는 설계). **성공 여부는 `/job-logs/`로 확인해야 한다.**

---

## 5. 실행과 스케줄

### 5.1 CLI

```bash
python -m app.cli generate-jobs <cycle>       # 잡 생성
python -m app.cli run-cycle <cycle>           # 대기 중인 잡 실행
python -m app.cli generate-builder <build_id> # 한 빌더만, 비활성이어도
python -m app.cli sync-mst-fuopt              # 원본 → 정제 마스터
python -m app.cli sync-index-his              # 지수 → 시계열, MV 갱신
python -m app.cli dedup-stock-base            # 주식 기본정보 → 종목당 1행
python -m app.cli sync-mst-stock              # → mst_stock 정제 마스터
python -m app.cli run-export [DAY] [--from D] # Parquet 내보내기
python -m app.cli finalize-exports            # 문서 업로드 + 버퍼 정리
python -m app.cli purge-jobs [--days N]       # 끝난 잡 정리
python -m app.cli archive-exported [YYYYMM]   # 직전월 → Parquet, DB 에서 삭제
python -m app.cli check-sql [--pull NAME]     # 파일 ↔ DB 대조
```

### 5.2 하루 일정 (systemd)

```
08:35  daily-start        토큰 갱신, 원본 마스터 재적재 (+ 주식 마스터 접기·이관)
08:38  generate-3m        마스터 동기화 → 3분 잡 생성
08:40~ 3m-call / 3m-put / 5m / 1h    장중 수집
16:00  daily-close        장 마감 직후 확정되는 것
18:00  daily-batch1       공시, ETF, 지수 일봉, 1분봉, 일봉
21:00  daily-batch2       늦게 확정되는 것
22:00  export             지수 접기 + MV 갱신 → Parquet
22:30  finalize-exports   문서 업로드 + CSV 버퍼 정리 + 잡 정리
23:00  check-sql          DB 프로시저와 저장소 파일 대조
```

매월 1일 04:00 에 `archive`(`pyscrap-archive.timer`)가 한 번 더 돈다. 직전월
3분봉·1분봉·일봉을 Parquet 으로 밀어내고 DB 에서 지운다 --
`kis_futopt_price` 한 표만 하루 87,000 행씩 늘고, 값은 `xt_*` 외부 테이블로
이미 읽히므로 DB 에 둔 쪽이 용량만 먹는다. 지우기 전에 외부 테이블이 그
구간을 읽어내는지 확인하고, 못 읽으면 그 표는 손대지 않고 실패한다.

유닛 배포는 [README](../README.md)의 *Deploying the units* 참조.

### 5.3 실패했을 때

`run_job`은 예외를 잡아 `api_job_log`에 FAILED로 남기고 **다음 잡으로 넘어간다.** 한 종목이 죽어도 나머지는 수집된다.

그리고 **실패한 일회성 잡은 비활성화되지 않으므로 다음 틱에 자동 재시도된다.** 별도의 재시도 장부가 없는 이유다.

```sql
-- 오늘 실패 확인
SELECT l.executed_at, l.job_id, l.error_message
  FROM api_job_log l
 WHERE l.status <> 'SUCCESS' AND l.executed_at >= TRUNC(SYSDATE + 9/24) - 9/24
 ORDER BY l.executed_at DESC;
```

### 5.4 속도 제한

호스트당 최소 간격 `REQUEST_MIN_INTERVAL_SEC`(기본 0.15초)로 페이싱한다. KIS는 명목상 초당 18건이지만 **실측 8건/초에서 거절(EGW00201)이 시작**되어 0.15초(6.7건/초)로 잡았다. 낮추려면 측정 근거가 있어야 한다.

계정별 제한이므로 한 계정으로 모자라면 **계정을 늘린다.** 3분 스냅샷이 콜/풋으로 나뉘어 두 계정을 쓰는 이유다. 동시성을 올리는 것은 도움이 안 된다 — API 자체는 24ms에 답하고 페이싱이 병목이다.

---

## 6. 내보내기

### 6.1 왜 DB가 직접 쓰는가

Parquet은 파이썬이 만들지 않는다. `DBMS_CLOUD.EXPORT_DATA`가 결과 테이블을 읽어 오브젝트 스토리지에 직접 쓴다.

```
run-export ─▶ sp_run_export ─▶ sp_export_parquet (대상마다)
                                 └─ DBMS_CLOUD.EXPORT_DATA ─▶ 버킷
```

그래서 얻는 것:

- **수집을 거치지 않은 백필분도 나간다** — 로컬 CSV 버퍼에 없어도 테이블에 있으면 된다
- **같은 날짜를 몇 번 내보내도 결과가 같다** — 프리픽스를 비우고 다시 쓴다
- 행이 파이썬 프로세스를 통과하지 않는다

### 6.2 두 가지 모드

```
sp_export_parquet   날짜별.  <대상>/<날짜>/<날짜>_part_….parquet
sp_export_bulk      통짜.    <대상>/_bulk/<시작>_<끝>_part_….parquet
```

날짜 폴더는 **나누려는 게 아니라 지울 단위**다. `DBMS_CLOUD.LIST_OBJECTS`가 폴더 경계에서만 매칭하므로(`tbl/20260820/`은 찾지만 `tbl/2026082`는 0건), 평면 구조로는 하루만 골라 비울 수 없다.

**둘은 겹치지 않는다.** 통짜가 자기 범위의 날짜 폴더를 흡수하고, 날짜별은 통짜 범위와 겹치면 `ORA-20002`로 거부한다. 외부 테이블이 `<대상>/*/*.parquet` 하나로 둘 다 읽기 때문에 겹치면 그 기간이 두 번 세어진다.

### 6.3 대상 추가

`sp_run_export`의 호출 목록에 한 줄:

```sql
export(p_name => '<테이블>');
export(p_name => '<프리픽스>', p_query => 'SELECT ... WHERE d = :DAY');
```

`p_query`를 주면 `p_name`은 버킷 프리픽스 이름으로만 쓰인다. 뷰·조인·집계 무엇이든 된다. **`:DAY`로 그날을 좁혀야 한다** — 프리픽스가 날짜별이라 다른 날이 섞이면 경로와 내용이 어긋난다.

무엇을 넣을지의 기준은 **하루 발생량**이다. 장중 내내 쌓이는 것만 여기 두고, 하루 몇 행뿐인 마스터성·요약성 자료는 분석 쪽에서 DB 링크로 읽는다.

### 6.4 파일 크기와 정렬

- `EXPORT_DATA`는 약 45MB에서 파일을 끊는다. `maxfilesize`로 올릴 수 없다
- 병렬 질의를 꺼서 하루가 파일 하나가 되게 한다. 워커마다 파일을 쓰는데 그 수가 실행마다 달라지고, 행 없는 워커도 1.4KB짜리 빈 파일을 남긴다
- 통짜는 **날짜순으로 정렬해서** 내보낸다. Parquet은 로우그룹마다 컬럼 min/max를 들고 있어, 정렬돼 있으면 하루를 묻는 질의가 그 그룹만 연다 (측정: 하루 0.96→0.71초, 한 달 1.29→0.87초)

### 6.5 분석 DB에서 읽기

```sql
BEGIN sp_create_external_tables; END;              -- 전부
BEGIN sp_create_external_tables('v_k2i_atm'); END; -- 하나만
```

버킷의 Parquet을 `xt_<대상>` 외부 테이블로 건다. 컬럼은 Parquet에서 읽어오므로 목록을 적지 않는다 — 그게 곧 내보낸 쿼리의 SELECT 목록이라, 옮겨 적으면 두 군데를 맞춰야 한다. 다만 **타입은 옮겨간다**: `NUMBER` → `BINARY_DOUBLE`, `DATE` → `TIMESTAMP(3)`.

다시 걸어야 할 때: 대상이 늘었을 때, 내보내기 쿼리에 컬럼이 늘었을 때. **새 날짜가 쌓이는 것만으로는 다시 걸 필요가 없다** — 경로가 와일드카드라 저절로 들어온다.

---

## 7. DB 쪽 코드

### 7.1 `scripts/sql/`은 실행되지 않는다

파이썬은 이 파일들을 읽지 않는다. **DB에 이미 컴파일된 객체를 이름으로 부른다.**

```python
_call_procedure('sp_run_export', day, since, out_count=False)
```

파일은 **검토된 기록**이다. DB가 못 갖는 세 가지 때문에 둔다: 변경의 이유(git 이력), 리뷰할 diff, 객체가 없는 환경에 만들 수단. 셋 다 "파일 = DB"를 전제하는데 그걸 강제하는 장치가 없으므로 **`check-sql`이 대조한다.**

```bash
python -m app.cli check-sql                 # 갈라지면 종료코드 1
python -m app.cli check-sql --pull sp_xxx   # DB 버전을 파일로 가져오기
```

DB가 정본이다 — 거기서 고치는 것이 역할을 나눈 취지이고, `--pull`이 그 변경을 파일로 되가져와 이유와 함께 커밋되게 한다.

뷰·MV는 대조하지 않고 `skipped`로 표시한다. DB가 쿼리만, 그것도 재포맷해서 갖고 있어 매번 "다름"이 뜨고, 그러면 아무도 이 명령을 안 보게 된다.

### 7.2 프로시저 목록

| 프로시저 | 하는 일 |
|---|---|
| `sp_export_parquet` | 대상 하나를 날짜별 Parquet으로 |
| `sp_export_bulk` | 대상 하나를 기간 통짜로 |
| `sp_run_export` | 내보낼 대상 목록 (호출 나열) |
| `fn_export_day_col` | 대상 → 거래일 컬럼 매핑 (위 둘이 공유) |
| `sp_create_external_tables` | 버킷 → `xt_*` 외부 테이블 |
| `sp_mst_fuopt_sync` | 원본 마스터 → 정제 마스터, 뒤늦은 만기일 메우기 |
| `sp_stock_index_his_sync` | 지수 일봉 → 시계열, `v_k2i_atm` 갱신 |
| `sp_purge_jobs` | 끝난 잡·로그 정리 |

### 7.3 Oracle에서 걸렸던 것들

문서로 남길 값어치가 있는 것들:

- **`ORA-12838`** — 병렬 DML로 고친 테이블을 같은 트랜잭션에서 읽을 수 없다. `ALTER SESSION DISABLE PARALLEL DML`로 끄고 되돌린다
- **`ORA-01031` (프로시저 안에서만)** — 정의자 권한 블록에서는 **롤이 꺼진다.** `CREATE TABLE`을 롤로 가진 계정은 프로시저 안에서 실패한다. `AUTHID CURRENT_USER`로 해결
- **`ORA-01861`** — `VARCHAR2` 날짜를 `TO_DATE()`와 비교하면 왼쪽이 `NLS_DATE_FORMAT`으로 변환되다 죽는다. 문자열끼리 비교할 것
- **`DPY-3002`** — dict를 raw SQL로 JSON 컬럼에 바인딩할 수 없다. **JSON 컬럼은 ORM으로 쓸 것**
- **`SET UNUSED COLUMN`** — 큰 테이블에서 컬럼을 뺄 때. 916만 행에서 0.3초다. 공간 회수(`DROP UNUSED COLUMNS`)는 나중에

---

## 8. API

```
GET  /results/                      결과 테이블 목록
GET  /results/{table}               페이지 + 필터
GET  /results/{table}/{id}          단건
GET  /api-mst/ /job-builders/ /jobs/    페이지 + 필터 + q 검색
GET  /api-mst/{id}/children         자식 개수
GET  /api-mst/{id}/jobs             자식 목록 (페이지)
GET  /job-logs/                     실행 이력
POST /jobs/generate/{cycle|builder|all}
POST /jobs/run/{job_id}
```

### 8.1 페이지와 필터

```
?limit=100&offset=0        기본 100, 최대 1000
?<필드>=<값>                모델의 어느 필드든 정확 일치
?<필드>=C0160*             * 는 아무 글자열
?q=KIS_INDEX               기본키 부분 일치 (설정 테이블만)
X-Total-Count 헤더          필터가 걸러낸 수
```

- 모르는 필드는 400, 값이 타입에 안 맞아도 400
- **`%`와 `_`는 글자 그대로다.** 이 id들이 밑줄투성이라 와일드카드로 해석하면 넓게 잡힌다. `*`를 쓴 것도 URL에서 `%`가 `%25`가 되기 때문
- 값 앞에 `*`를 두면 인덱스를 못 쓰고 전체를 읽는다

### 8.2 배포가 갈린다

```
app/api/**                              FastAPI Cloud 만
app/cli.py, scripts/**                  인스턴스 만
app/db/**, app/services/**, app/scrapers/**   양쪽
playwright + chromium                   인스턴스 만 (2.11)
```

인스턴스는 배치만 돌리고 API 서버를 띄우지 않는다. 반대로 브라우저(`BROWSER` 행)는 인스턴스에서만 돈다 — API 배포에는 chromium이 없다.

---

## 9. 자주 겪는 함정

**빌더를 고쳤는데 안 바뀐다** — `save_mode`와 `execution_cycle`은 생성 시점에 잡으로 복사된다. 이미 있는 잡은 직접 고치거나 다시 만들어야 한다.

**잡이 한 번 돌고 다시 안 돈다** — `key_params_list`에 변하는 값이 없어 job_id가 고정됐고, 일회성이라 성공 후 비활성이 됐다. 날짜를 넣거나 `NOW`를 넣는다.

**같은 데이터가 두 벌로 쌓인다** — 롤링 기간이 job_id에 들어가 매일 새 잡이 생기는데 `overwrite`는 같은 job_id만 지운다. 기간을 겹치지 않게 좁히거나, 접는 쪽에서 정리한다.

**컬럼이 계속 비어 있다** — `merge_fields_json`의 경로가 틀렸거나(오류 없이 `None`), 전용 테이블에 그 컬럼이 없어 버려지고 있다.

**종목이 통째로 빠진다** — 종목 선택 쿼리가 `mat_date`로 조인하는데 만기 달력(`meta_maturity`)에 그 만기가 없으면 `NULL`이라 탈락한다. 실제로 위클리 490종목이 이렇게 빠져 있었다.

**파일로 실행한 스크립트만 옛 코드를 쓴다** — 가상환경에 editable 설치가 둘 이상이면 `app` 패키지가 모호해진다. `pip list`로 확인할 것.

---

## 10. 확인용 질의 모음

```sql
-- 오늘 배치가 돌았는가 (KST)
SELECT j.execution_cycle, l.status, COUNT(*),
       TO_CHAR(MIN(l.executed_at)+9/24,'HH24:MI') 처음,
       TO_CHAR(MAX(l.executed_at)+9/24,'HH24:MI') 마지막
  FROM api_job_log l JOIN api_job j ON j.job_id = l.job_id
 WHERE l.executed_at >= TRUNC(SYSDATE + 9/24) - 9/24
 GROUP BY j.execution_cycle, l.status ORDER BY 1, 2;

-- 3분 틱이 빠짐없이 도는가
SELECT COUNT(DISTINCT SUBSTR(trade_at,9,4)) 틱수,
       MIN(SUBSTR(trade_at,9,4)), MAX(SUBSTR(trade_at,9,4))
  FROM kis_futopt_price
 WHERE SUBSTR(trade_at,1,8) = TO_CHAR(SYSDATE+9/24,'YYYYMMDD');

-- 분봉 중복 (봉 시각은 stck_cntg_hour 다)
SELECT COUNT(*) FROM (
  SELECT short_code, stck_bsop_date, stck_cntg_hour
    FROM kis_futopt_chart
   GROUP BY short_code, stck_bsop_date, stck_cntg_hour HAVING COUNT(*) > 1);

-- 만기 달력이 소진되지 않았는가
SELECT prod_type, mat_code, COUNT(*) FROM mst_fuopt
 WHERE mat_date IS NULL GROUP BY prod_type, mat_code;

-- 활성 빌더가 어느 주기에 있는가
SELECT execution_cycle, build_id FROM api_job_builder
 WHERE is_active = 1 ORDER BY execution_cycle, build_id;

-- 빌더와 잡의 주기가 어긋났는가
SELECT j.build_id, b.execution_cycle 빌더, j.execution_cycle 잡, COUNT(*)
  FROM api_job j JOIN api_job_builder b ON b.build_id = j.build_id
 WHERE j.execution_cycle <> b.execution_cycle
 GROUP BY j.build_id, b.execution_cycle, j.execution_cycle;
```
