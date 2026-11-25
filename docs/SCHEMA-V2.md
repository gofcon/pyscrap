# pyscrap 스키마 v2 개선안

작성 2026-11-24 · 대상 커밋 `812943b` · 결정 전 초안

기존 소스와 테이블은 그대로 둔 채, 새 DB에 두 계정으로 다시 세운다. 엔진 동작은 바꾸지 않고 **설정의 의미가 이름에 드러나게** 하는 것이 목표다.

---

## 0. 요약

| 항목 | 지금 | v2 |
|---|---|---|
| 계정 | 하나에 66개 테이블 | 수집 `SCRAP` / 사전 `REF` 둘 |
| 파이썬 매핑 | 전부 로컬 테이블 | REF는 synonym으로 투명하게 |
| `key_params_list` | 한 컬럼이 세 가지 뜻 | 세 컬럼으로 분리, 예약어 제거 |
| `macro_params_json` | 이름이 하는 일을 안 말함 | `cartesian_param_json` |
| 예약어 | `NOW`, `none_mapped` | 없음 (컬럼으로 표현) |
| 실패 분류 | 문자열 하나 | `error_code` + 등급별 예외 |
| 수집 건수 | `api_job.description` 문자열 | `api_job_log.record_count` |
| `api_job` 행수 | 633,996 (날마다 증가) | 종목 수만큼 고정 |

핵심은 두 가지다. **(1)** 한 컬럼에 여러 뜻을 담아 생긴 예약어를 없애는 것, **(2)** 작업 테이블이 날마다 커지는 구조를 멈추는 것.

---

## 1. 현황 측정

측정 시점의 실제 값이다. 제안의 근거이자 이행 후 비교 기준이 된다.

```
설정    api_mst 58 · api_job_builder 95
실행    api_job 633,996 · api_job_log 2,859,501
모델    60개 클래스 = 부기 4 + 결과 43 + 사전 11 + 허수 2
DB      테이블 66 · 인덱스 243 · 시퀀스 44 · 프로시저 18 · 함수 1 · 뷰 2 · MV 1 · 외부테이블 5
계정    단일 · synonym 0개
```

큰 테이블:

```
XT_KIS_FUTOPT_CHART   8,978,826   (외부 테이블 — 분석용인데 수집 계정에 있다)
XT_KRX_OPT_DAILY      8,655,364   (같음)
KRX_ETF_PDF           4,087,467
KRX_STOCK_DAILY       3,778,720
API_JOB_LOG           2,859,501   ← 부기
KIS_FUTOPT_PRICE      1,959,300
KSD_BOND_ISIN         1,840,625
API_JOB                 627,419   ← 부기
```

**부기 테이블이 결과 테이블급으로 크다.** 전체 행의 상당 부분이 "무엇을 받았는가"가 아니라 "받았다는 기록"이다. 7절에서 다룬다.

허수 2개는 `VendorRecordBase`, `MdcRecordBase`다. `table=True`가 아니라 실제 테이블은 없지만 SQLModel이 `__tablename__`을 자동 생성해서 모델 스캔에 잡힌다.

---

## 2. 전환 방식

### 2.1 브랜치

```
dev ──▶ feat/schema-v2
```

`main`/`dev`는 현행 수집을 계속 돌린다. v2는 병합 시점까지 **기존 DB를 건드리지 않는다** — 이 제약이 롤백 비용을 0으로 만든다.

### 2.2 접속 정보

지금 `Settings`는 한 벌이다 (`oracle_user` / `oracle_password` / `oracle_dsn` / `oracle_wallet_*`). v2는 **데이터베이스에 속한 것과 계정에 속한 것을 갈라서** 받는다.

```env
# 현행 v1 (그대로 — 전환이 끝나면 이 블록만 지운다)
ORACLE_USER=<v1 계정>
ORACLE_DSN=..._medium
ORACLE_WALLET_DIR=...

# v2 — 데이터베이스 한 곳
DB_DSN=<adb>_medium
DB_WALLET_DIR=<지갑 디렉터리>
DB_WALLET_PASSWORD=...

# v2 — 계정 둘
DB_SCRAP_USER=<수집 계정>
DB_SCRAP_PASSWORD=...
DB_REF_USER=<사전 계정>       # 선택: DDL/보수 하는 기계에만
DB_REF_PASSWORD=...
```

같은 ADB 인스턴스에 계정만 더하는 경우에는 `DB_DSN`/`DB_WALLET_*`가 필요 없고 **user/password 두 벌만** 더하면 된다 (10절 질문 1).

**이름을 이렇게 둔 이유**

- **DSN·wallet은 계정 소유가 아니다.** 계정마다 복제하면 한쪽만 교체되는 날이 온다
- **키는 역할, 값은 실제 계정명.** 계정명을 바꿔도 `.env` 한 줄이고 코드·SQL·synonym 정의는 그대로다. 키에 계정명을 넣으면(`<계정명>_PASSWORD`) 개명이 코드까지 번진다
- **`V2_` 접두사를 쓰지 않는다.** 버전 번호는 전환이 끝나면 의미가 없어지고, v1을 내릴 때 전부 개명해야 한다. 역할 기반이면 v1 블록을 지우는 것으로 끝난다

**`DB_REF_*`는 선택 항목이다.** 평상시 수집은 `SCRAP`으로만 접속하고 `REF`는 synonym으로 닿는다 (5절). REF 접속이 필요한 것은 DDL과 손보수뿐이므로, **배치 서버의 `.env`에는 넣지 않는다** — 읽기만 하는 기계에 사전 데이터 쓰기 자격을 두지 않는 것이고, `user_etf`처럼 다시 만들 수 없는 표가 거기 있어 값이 있는 구분이다.

```python
class DbSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8",
        env_prefix="DB_", extra="ignore")

    dsn: str
    wallet_dir: str | None = None
    wallet_password: str | None = None

    scrap_user: str
    scrap_password: str
    # 평상시 수집에는 필요 없다. DDL/보수를 하는 기계에만 채운다.
    ref_user: str | None = None
    ref_password: str | None = None
```

```python
ref_engine = _make_engine(db.ref_user, db.ref_password) if db.ref_user else None
```

자격이 없는 기계에서 REF 작업을 부르면 "이 기계에는 REF 자격이 없다"로 끝난다. 조용히 SCRAP으로 붙어 엉뚱한 계정에 테이블을 만드는 일이 막힌다 (3.4).

어느 DB를 쓰는가는 **환경변수 하나**로 정한다.

```env
SCRAP_TARGET=v1        # v1 | v2 | both
```

`both`는 이행 검증용이다 (8.1). 코드 안에 `if v2:` 분기를 뿌리지 않고 엔진 선택 한 곳에서 끝내는 것이 조건이다.

### 2.4 접두사를 뒤집었다 (2026-11-25)

처음 계획은 `V2_*` 를 새로 두고 `ORACLE_*` 는 v1 으로 남기는 것이었다(2.2).
반대로 갔다 -- **`ORACLE_*` 가 v2(SCRAP)를 가리키고 v1 은 `ORACLE_*_V1` 로만
닿는다.**

그래도 되는 이유가 있다. v2 는 별도 인스턴스이고 **계정 이름과 자격이 v1 과
같다** -- 가리키는 곳을 정하는 것은 `oracle_dsn` 뿐이다. 그래서 `ORACLE_USER`
의 뜻이 바뀌지 않고, 전환에 `app/db_config.py` 도 `app/db/engine.py` 도 고칠
것이 없었다. 확인:

```
접속 계정 GOF · api_mst 57행 · source_name 5종  -> v2
```

`.env` 의 모양:

```
ORACLE_USER / ORACLE_PASSWORD        엔진이 쓰는 계정 (= SCRAP)
ORACLE_USER_REF / ORACLE_PASSWORD_REF   REF 계정
ORACLE_WALLET_DIR / _PASSWORD / ORACLE_DSN   공용
```

`REF` 쌍은 `Settings` 필드로 **두지 않는다**. 엔진이 REF 로 붙을 일이 없기
때문이다.

처음에는 "MV 갱신은 소유 계정에서 해야 하니 REF 자격이 운영에도 필요하겠다"
고 보았는데, 재 보니 아니었다. `sp_stock_index_his_sync` 를 **SCRAP 에서**
부르면 `v_k2i_atm` 의 `last_refresh` 가 움직인다:

```
갱신 전 14:06:14  ->  SCRAP 에서 sp_stock_index_his_sync 호출  ->  14:16:31
```

definer-rights 프로시저라 안의 `DBMS_MVIEW.REFRESH` 가 REF 로 돈다. 사전
데이터도 synonym 으로 읽고, 동기화 프로시저 10개 전부 SCRAP 에서 호출된다.

그래서 REF 자격이 필요한 자리는 **REF 소유 객체를 만드는 DDL 뿐**이다
(`05_ref_tables.sql`, `06`·`07` 의 REF 절반, `v_code_*` 뷰, `v_k2i_atm`).
사람이 환경마다 한 번씩 하는 일이지 앱 경로가 아니다.

**타이머가 도는 인스턴스의 `.env` 에는 넣지 않는다.** 거기서 쓸 일이 없고,
REF 는 자기 스키마에 객체를 만들고 지울 수 있는 계정이다 -- SCRAP 은 받은
SELECT/EXECUTE 밖으로 못 나가므로 새는 경우의 범위가 다르다. (FastAPI Cloud
는 애초에 `.env` 를 배포에서 빼고 플랫폼 환경변수를 쓴다 --
`.fastapicloudignore`.)

아무도 읽지 않는 필드를 선언하면 "엔진이 REF 로도 붙는다" 는 오해만 남는다.

**대가 하나.** 이행 스크립트가 v1 을 읽는데 그 변수가 이제 v2 를 가리킨다.
그대로 두면 v2 를 읽어 v2 에 쓴다 -- 실제로 그렇게 됐고 v1 컬럼 이름으로
질의하다 `ORA-00904: "T"."DESCRIPTION"` 로 죽었다. 망가지지는 않았지만 "엉뚱한
DB 를 읽고 있다" 가 그 메시지에 안 나온다. `_v1_connect` 가 `ORACLE_*_V1` 을
못 박고, 없으면 왜인지 말하고 멈춘다:

```
v1 접속 정보가 없다 -- .env 에 ORACLE_USER_V1, ORACLE_PASSWORD_V1, ORACLE_DSN_V1 가 필요하다.
이행은 2026-11-25 에 끝났고 이 스크립트는 일회성 부트스트랩이다. ...
```

그 변수가 없는 것이 정상이다. 이행은 끝났다.

### 2.3 계정 이름

집안 규칙(기존 계정의 접두사)에 맞춰 역할 접미사를 붙이면 된다 — 수집 /
사전 데이터 / 분석(선택) 셋이다. 실제 이름은 `.env` 에만 둔다.

짧은 이름(`SCRAP`/`MST`/`META`/`MART`/`CORE`/`OPS`)도 식별자로는 통과한다 — 실제로 만들어 확인했다. 쓰면 안 되는 것 둘:

- **`RAW`** — `ORA-00903`. 데이터형 키워드라 식별자가 못 된다
- **`DICT`** — `PUBLIC` 소유의 SYNONYM이 이미 있다 (`DICTIONARY`)

`REF`는 테이블명으로 통과하지만 권하지 않는다. `REF(x)`가 Oracle 내장 함수와 같은 글자라 깨지지는 않아도 SQL을 읽는 사람이 한 번 멈춘다.

---

## 3. 계정 분리

### 3.1 무엇을 어디로

```
SCRAP  ─ 수집 엔진이 쓰고 지우는 것
         api_mst · api_job_builder · api_job · api_job_log
         결과 테이블 43개 (krx_* · kis_* · ksd_* · *_etf_pdf · api_rst …)

REF    ─ 수집의 입력이 되는 사전 데이터, 프로시저가 관리
         mst_fuopt · mst_bond · mst_etf · mst_stock · mst_index · krx_index_mst
         meta_maturity · meta_fuopt_info
         user_etf · ksd_bond_no_detail · stock_index_his
         sp_mst_*_sync · sp_meta_maturity_sync · sp_stock_index_his_sync

MART   ─ (선택) 분석 쪽. 외부 테이블 XT_* 와 export 프로시저
```

### 3.2 왜 이 선이 맞는가

**수명이 다르다.** 결과 테이블은 `purge-jobs`/`archive-exported`가 주기적으로 비운다. 사전 데이터는 지우면 수집이 멈춘다. 지금은 둘이 같은 계정에 있어 "이 테이블을 비워도 되는가"를 이름으로 판단해야 한다.

**쓰는 주체가 다르다.** 결과 테이블은 파이썬이 쓴다. 사전 테이블은 프로시저가 쓴다 (`sp_mst_etf_sync` 등 18개). 계정이 갈리면 권한으로 그 사실이 강제된다 — 파이썬이 실수로 `mst_etf`에 INSERT 할 수 없게 된다.

**복구 단위가 다르다.** 결과는 다시 긁으면 되고, 사전 데이터 중 `user_etf`는 **손으로 적은 것이라 다시 만들 수 없다.** 지금 그 둘이 같은 계정에 있다.

### 3.3 교차 권한

양방향이 필요하다. 한쪽만 생각하면 이행 중에 막힌다.

```sql
-- SCRAP → REF : 빌더 매크로 SQL 이 사전 데이터를 읽는다
--   예: {"AMC_ETF_CD": {"db": "SELECT amc_etf_cd FROM mst_etf
--                                 WHERE amc = '미래에셋자산운용' ..."}}
GRANT SELECT ON ref.mst_etf     TO scrap;
GRANT SELECT ON ref.mst_bond    TO scrap;
GRANT SELECT ON ref.mst_fuopt   TO scrap;
GRANT SELECT ON ref.meta_maturity TO scrap;
-- … 사전 11개 전부

-- REF → SCRAP : 동기화 프로시저가 결과 테이블에서 마스터를 만든다
--   예: sp_mst_etf_sync 는 krx_etf_daily 로 유니버스를 세운다
GRANT SELECT ON scrap.krx_etf_daily TO ref;
GRANT SELECT ON scrap.ksd_bond_isin TO ref;
GRANT SELECT ON scrap.ace_etf       TO ref;
-- … 각 sync 프로시저가 읽는 것만
```

`user_etf`는 **SCRAP에 쓰기를 주지 않는다.** 사람이 적는 표이므로 수집 계정이 건드릴 이유가 없다.

### 3.4 DDL 소유권

SQLModel의 `metadata.create_all()`은 **접속한 계정에** 테이블을 만든다. synonym을 통과하지 않는다. 그러니:

- 결과 테이블 DDL — SQLModel이 만들어도 된다 (SCRAP에 접속하므로 SCRAP에 생긴다)
- 사전 테이블 DDL — **REF로 접속해 따로** 만든다. 파이썬 모델은 매핑만 하고 생성은 안 한다

이 구분을 코드로 강제하려면 4.4의 마커 클래스를 쓴다.

---

## 4. 컬럼 의미 재정의

### 4.1 `key_params_list` — 한 컬럼, 세 가지 뜻

지금 이 컬럼이 결정하는 것:

| | 무엇 | 어디서 |
|---|---|---|
| (a) | job_id 를 무엇으로 만들까 | `_build_job_id` |
| (b) | 결과 행에 무엇을 어느 컬럼으로 찍을까 | `run_job` |
| (c) | 이 작업이 반복인가 일회성인가 | `is_repeated_api` |

세 가지가 한 리스트에 섞여서 **예약어 두 개**가 생겼다.

```json
[{"SHORT_CODE": "short_code"}, {"NOW": "trade_at"}]      // NOW = 반복이라는 뜻
[{"NOW": "none_mapped"}]                                  // none_mapped = 안 찍는다는 뜻
```

`NOW`는 11개 API가, `none_mapped`는 7개가 쓴다. 둘 다 "컬럼 이름 자리에 컬럼 이름이 아닌 것을 적는" 형태다.

**제안 — 세 컬럼으로 가른다.**

```
job_key_params    list    job_id 재료가 되는 파라미터 이름들.  ["SHORT_CODE"]
row_stamp_json    dict    결과 행에 찍을 것. {파라미터: 컬럼}.  {"SHORT_CODE": "short_code"}
snapshot_column   str     NULL 이면 일회성.
                          값이 있으면 반복 작업이고, 그 컬럼에 실행 시각이 들어간다.
```

예약어가 사라진다.

| 지금 | v2 |
|---|---|
| `[{"SHORT_CODE":"short_code"}, {"NOW":"trade_at"}]` | `job_key_params: ["SHORT_CODE"]`<br>`row_stamp_json: {"SHORT_CODE":"short_code"}`<br>`snapshot_column: "trade_at"` |
| `[{"NOW":"none_mapped"}]` | `job_key_params: []`<br>`row_stamp_json: {}`<br>`snapshot_column: "none"` ← 아래 참고 |
| `["BASDD"]` | `job_key_params: ["BASDD"]`<br>`row_stamp_json: {"BASDD":"bas_dd"}`<br>`snapshot_column: NULL` |

`none_mapped`가 쓰이던 자리(ETF 목록 4종)는 "반복이지만 시각을 안 찍는다"는 뜻이었다. 이건 `snapshot_column`을 NULL이 아닌 값으로 두면서 컬럼은 없다는 모순이라, **`is_repeated` BOOLEAN을 따로 두는 것이 정직하다.**

최종안:

```
job_key_params    list       job_id 재료           ["SHORT_CODE"]
row_stamp_json    dict       행에 찍을 것          {"SHORT_CODE": "short_code"}
is_repeated       boolean    성공 후에도 활성 유지  true
snapshot_column   str|NULL   실행 시각을 받을 컬럼  "trade_at" (없으면 NULL)
```

네 컬럼이지만 각각 한 가지만 뜻한다. `is_repeated=true, snapshot_column=NULL`이 옛 `none_mapped`이고, 모순이 아니라 "반복인데 시각은 안 남긴다"로 읽힌다.

> **부수 효과 하나.** 지금은 `row_stamp_json`에 없는 컬럼 이름을 적어도 pydantic이 조용히 버려서 오타와 의도가 구별되지 않는다. 분리하면 `row_stamp_json`의 값은 **전부 실존 컬럼이어야 한다**는 검사를 넣을 수 있다 (6.4).

### 4.2 `macro_params_json` → `cartesian_param_json`

```
macro_params_json  →  cartesian_param_json
```

"macro"는 치환을 연상시키는데 실제로 하는 일은 전개다. 각 항목이 하나의 축이고 최종 작업 집합은 축들의 **곱집합**이다.

처음에는 `param_axes_json`을 제안했는데 `cartesian_param_json`으로 바꿨다. "축"은 구현 용어(`_build_axes`)이고, 이 컬럼의 유일한 함정은 **항목을 하나 더하면 작업 수가 더해지는 게 아니라 곱해진다**는 것이다. `cartesian`이 그것을 바로 말한다.

### 4.3 `execution_cycle` — 두 가지로 쓰이고 있다

지금 이 값은 **어느 타이머가 집어 가는가**와 **스냅샷 시각을 어떤 단위로 절삭하는가**를 동시에 맡는다. 그래서 46가지로 늘었고, 절삭이 조용히 안 걸린다.

```
resolve_now('daily_batch2')  →  20261124170625   ← 절삭 안 됨 (초까지)
resolve_now('1d')            →  20261124000000   ← 절삭 됨
resolve_now('5m')            →  20261124170500   ← 절삭 됨
```

`daily_batch2`가 `1m/5m/1h/1d/1w` 패턴에 안 맞아 `_floor_datetime`이 no-op이 된다. **버그가 아니라 설계가 겹친 결과**다 — 스케줄 이름과 절삭 단위가 같은 컬럼이라 둘 다 만족하는 값을 쓸 수 없다.

```
execution_cycle  →  schedule_tag     어느 타이머가 집어 가는가.  "daily_batch2"
                    snapshot_unit    자르는 단위. NULL 이면 안 자른다.  "1d" | "5m" | NULL
```

46개 중 37개가 `bf1m_*` 형태의 **일회성 이행 사이클**이다 (`bf1m_202605`, `bf1m_wki_2607` …). 이행이 끝났으면 v2로 옮길 때 빼면 된다 — 옮기지 않는 것만으로 정리된다.

### 4.4 `response_parse_json` — 파싱과 무관한 것이 섞여 있다

이 컬럼이 담는 키:

```
파싱     delimiter · fields · has_header · skip_rows · encoding · inner_file
         table_index · sheet · (browser) css spec
세션     login · logged_out · expect          ← 파싱이 아니다
파일     group · name                          ← 파싱이 아니다
```

세션 관련 키는 "이 API는 로그인이 필요하고, 로그아웃되었다는 신호는 이것"이라는 뜻이다. 파싱 명세와 같은 칸에 있을 이유가 없다.

```
response_parse_json  →  parse_json      파싱 명세만
                        session_json    {"login": "<api_id>", "logged_out": "...", "expect": "..."}
                        staging_json    {"group": "...", "name": ":KEY.pdf"}
```

### 4.5 기타 이름

| 지금 | v2 | 이유 |
|---|---|---|
| `output_tables_json` | `record_map_json` | 키가 테이블이 아니라 셀렉터다. `{셀렉터: 테이블}` |
| `api_job.description` | `note` + `engine_note` | 지금 사람이 쓴 설명과 마지막 실행 결과가 **같은 칸**을 쓴다 (7.2) |
| `api_mst.api_group` | `source_name` + `domain` | 13개 값이 두 축을 섞어 쓴다 (7.4) |
| `behavior_json` | 그대로 | 하는 일을 정확히 말한다 |
| `save_mode` | 그대로 | `overwrite`/`append` 로 충분하다 |

---

## 5. synonym

### 5.1 목적

파이썬 모델이 **스키마를 몰라도 되게** 한다. 지금 `MstEtf.__tablename__ = "mst_etf"`인데, 테이블이 REF로 가면 보통은 이렇게 된다.

```python
class MstEtf(SQLModel, table=True):
    __tablename__ = "mst_etf"
    __table_args__ = {"schema": "ref"}      # ← 이게 11개 모델에 붙는다
```

그러면 스키마 이름이 코드에 박히고, 계정 이름을 바꾸면 코드를 고쳐야 한다. 빌더 매크로 SQL도 `SELECT ... FROM ref.mst_etf`로 바뀌어 **DB 안의 설정 문자열까지** 계정 이름을 안다.

synonym을 두면 둘 다 필요 없다.

```sql
CREATE SYNONYM scrap.mst_etf       FOR ref.mst_etf;
CREATE SYNONYM scrap.mst_bond      FOR ref.mst_bond;
CREATE SYNONYM scrap.meta_maturity FOR ref.meta_maturity;
-- … 사전 11개
```

SCRAP으로 접속한 세션에서 `SELECT * FROM mst_etf`가 그대로 동작한다. 모델도, 매크로 SQL도, 프로시저 본문도 손대지 않는다.

### 5.2 두 가지 선택

| | private synonym | public synonym |
|---|---|---|
| 범위 | 그 계정만 | DB 전체 |
| 권한 | 계정별 `CREATE SYNONYM` | `CREATE PUBLIC SYNONYM` (강한 권한) |
| 충돌 | 계정마다 다르게 가리킬 수 있다 | 하나뿐 |

**private을 쓴다.** public은 MART 계정까지 같은 이름을 보게 되는데, 분석 쪽은 외부 테이블(`xt_*`)을 봐야 하므로 같은 이름이 다른 것을 가리켜야 한다.

### 5.3 주의할 점

- **DDL은 synonym을 통과하지 않는다.** `CREATE TABLE`/`ALTER TABLE`은 접속 계정에 적용된다. 사전 테이블 DDL은 REF로 접속해서 해야 한다
- **`user_tables`에 안 보인다.** SCRAP에서 `user_tables`를 조회하면 사전 테이블이 없다. `check-sql`이나 점검 스크립트가 `user_tables`를 쓰면 `all_tables`/`user_synonyms`로 바꿔야 한다
- **권한이 없으면 synonym은 있는데 못 읽는다.** `ORA-00980`(번역 실패)이 아니라 `ORA-01031`이 나서 원인이 덜 보인다. 3.3의 GRANT가 synonym보다 먼저다

### 5.4 파이썬 쪽 정리 — 마커 클래스

synonym이 투명하게 만들어 준 다음, 모델 스캔의 휴리스틱을 없앤다. 지금은 이렇다.

```python
# job_id 컬럼이 있으면 결과 테이블로 본다
if table_name not in _BOOKKEEPING_TABLES and "job_id" in obj.model_fields:
```

"job_id가 있으면 결과 테이블"은 지금 맞지만 우연히 맞는 것이다. 명시한다.

```python
class ResultTable(SQLModel):
    """엔진이 쓰는 표. SCRAP 계정."""

class ReferenceTable(SQLModel):
    """프로시저가 쓰는 표. REF 계정. 엔진은 읽기만."""
```

얻는 것:

- `_discover_table_registry`가 `issubclass(obj, ResultTable)`로 끝난다
- **마커는 셋이다.** `ResultTable` / `ReferenceTable` / `StateTable` — `ksd_bond_no_detail`이 셋째다 (위 "경계를 어디로 긋는가")
- `_BOOKKEEPING_TABLES` 하드코딩 집합이 사라진다
- 허수 2개(`VendorRecordBase` 등)가 자연히 빠진다
- `create_all`을 계정별로 가를 수 있다 — `ResultTable.metadata`만 SCRAP에 만든다
- 사전 테이블에 INSERT 하는 코드가 생기면 **리뷰에서 보인다**

---

## 6. 예외 처리

> **구현 완료 2026-11-25.** `app/errors.py` 가 계층이고, `api_job_log` 의 새
> 다섯 컬럼이 채워지고, 설정 검증이 생성 시점으로 옮겨졌다. 6.1 은 v1 의
> 기록으로 남겨 둔다. 구현에서 드러난 것은 6.4 뒤에 적었다.

### 6.1 v1 의 상태 (고치기 전)

예외 클래스가 둘뿐이다 (`SessionExpired`, `SiteBlocked`). `run_job`은 나머지 전부를 한 곳에서 받아 문자열로 적는다.

```python
except Exception as exc:
    ...
    if isinstance(exc, RetryError) and exc.last_attempt.failed:
        exc = exc.last_attempt.exception()      # tenacity 가 감싼 것을 벗긴다
```

`api_job_log`는 5컬럼이다.

```
id · job_id · status · error_message · executed_at
```

문제는 **SQL로 물어볼 수 없다**는 것이다.

- "어제 실패 2,400건 중 상대 쪽 문제는 몇 건인가" → `error_message LIKE`로 추측
- "이 소스가 계속 같은 이유로 실패하는가" → 문자열 비교
- "몇 건 받았나" → 로그에 없다. `api_job.description`에 문자열로 있고 다음 실행이 덮는다

### 6.2 예외 계층

실패를 **누구 탓인가**로 가른다. 그게 다음 행동을 정하기 때문이다.

```
ScrapeError
├── SourceError          상대 쪽. 우리 설정은 맞다
│   ├── SourceUnavailable    5xx · timeout · 연결 끊김        → 재시도
│   ├── SourceRefused        차단 페이지 (= 지금 SiteBlocked)  → 그 호스트 중단, 창 대기
│   ├── SessionExpired       로그아웃됨                        → 재로그인 1회
│   ├── SourceEmpty          0건. 휴장일일 수도 있다            → 실패 아님
│   └── SourceChanged        필드/구조가 달라졌다               → 사람 호출
├── ConfigError          우리 설정. 재시도해도 같다
│   ├── MappingError         대상 테이블 없음 · 셀렉터 불일치
│   ├── ParamError           필수 파라미터 누락 · 치환 실패
│   └── SchemaError          row_stamp_json 이 없는 컬럼을 가리킴
└── StoreError           DB 쪽. 적재에서 깨졌다
    ├── StoreConstraint      길이 초과 · 타입 불일치
    └── StoreUnavailable     커넥션 · 테이블스페이스
```

이 구분이 값을 하는 지점:

| 등급 | 재시도 | 차단기 | 은퇴 판단 | 알림 | log status |
|---|---|---|---|---|---|
| `SourceUnavailable` | ○ | - | 누적 | - | FAILED |
| `SourceRefused` | ✕ | ○ | - | - | **BLOCKED** |
| `SessionExpired` | 1회 | - | - | - | FAILED |
| `SourceEmpty` | ✕ | - | - | - | **EMPTY** |
| `SourceChanged` | ✕ | - | **누적** | ○ | FAILED |
| `ConfigError` | ✕ | - | - | ○ | FAILED |
| `StoreError` | 조건부 | - | - | ○ | FAILED |
| (그 외 = `ScrapeError`) | ✕ | - | 누적 | - | FAILED |

지금은 이 표가 `isinstance` 분기로 코드에 흩어져 있다. 등급이 생기면 **데이터가 결정**한다.

구현하면서 고친 칸 둘. `SourceChanged` 는 은퇴 판단에 **센다** -- 구조가
바뀌면 그 api 의 모든 잡이 똑같이 실패하므로, 밤을 그것으로 보내지 않는 쪽이
맞다. 그리고 등급을 모르는 실패(`classify` 가 어디에도 못 꽂은 것)도 센다:
차단기는 밤을 지키는 장치이고 "왜 실패하는지 모르겠다" 는 밤을 내줄 이유가
못 된다.

### 6.3 `api_job_log` 확장

```
id            NUMBER          그대로
job_id        VARCHAR2(150)   그대로
status        VARCHAR2(20)    SUCCESS | FAILED | SKIPPED | BLOCKED | EMPTY
error_code    VARCHAR2(40)    예외 클래스 이름. 'SourceRefused' 등. 성공이면 NULL
error_message VARCHAR2(4000)  그대로 (사람이 읽는 한 줄)
error_detail  CLOB            스택/응답 앞부분. NULL 허용
record_count  NUMBER          받아서 적재한 행 수          ← 새로
duration_ms   NUMBER          요청+적재 소요              ← 새로
attempt_no    NUMBER          재시도 몇 번째였나           ← 새로
target_date    VARCHAR2(8)     어느 날짜분을 받았나          ← 새로 (7절)
executed_at   DATE            그대로
```

`record_count` 하나로 "어제 KIS ETF가 몇 건 들어왔나"가 질의가 된다. 지금은 `description` 문자열을 파싱해야 하고 그마저 마지막 실행 것만 남는다.

`error_detail`을 CLOB으로 두는 이유: 지금 `error_message`가 4000자라 응답 본문을 넣을 수 없어, 무엇이 왔는지 모른 채 "HTTPStatusError"만 남는 경우가 있다.

**채우는 쪽 (2026-11-25).** 컬럼은 v2 스키마와 함께 생겼지만 `_log` 가
`status`/`error_message` 둘만 넣고 있어서 나머지는 전부 NULL 이었다 -- 컬럼만
있고 채우는 쪽이 없으면 여전히 문자열로 추측해야 한다. 같은 잡을 고치기 전과
후에 돌려 보면:

```
id  status   error_code         record_count duration_ms attempt_no target_date
 7  SUCCESS  (null)             (null)       (null)      1          (null)     <- 고치기 전
21  SUCCESS  (null)             10           536         1          20261124
22  FAILED   SourceUnavailable  (null)       3590        1          20261124   <- 404
23  FAILED   SourceUnavailable  (null)       9249        1          20261124   <- ConnectTimeout
24  FAILED   MappingError       (null)       220         1          20261124   <- 없는 표
29  EMPTY    (null)             0            90          1          20260101   <- 휴일
```

`target_date` 는 **값으로** 판정한다 -- 숫자만 남겨 8자리이고 연·월·일이 말이
되면 날짜로 본다. key_params 에서 찾으므로 조회일을 받는 API 는 그 파라미터가,
스냅샷 API 는 관측 시각이 잡힌다. 파라미터 **이름**으로 거르지 않는 이유:
원천마다 다르다(BAS_DD, trdDd, std_dt, inqDt…).

status 다섯 중 `SKIPPED` 는 차단기가 건너뛴 잡이다. 그 행은 사이클 끝에 한 번에
적는다 -- 건너뛴 잡마다 커밋하면 나쁜 밤에 수백 번의 왕복이고, 다른 잡의
트랜잭션에 얹으면 그 잡이 실패할 때 rollback 에 쓸려 사라진다.

적재 오류는 Oracle **번호**로 가른다(`StoreConstraint` / `StoreUnavailable`).
드라이버 예외 클래스로 가르지 않는 이유: SQLAlchemy 가 같은 오류를 버전마다
다르게 싸는데 번호는 안 바뀐다. 실측으로 ORA-12899 · 02290 · 01400 이
`StoreConstraint`, 집합에 없는 ORA-00942 는 `StoreError` + 알림으로 떨어진다.

### 6.4 설정 검증을 실행 전으로

v2에서 `row_stamp_json`의 값이 전부 실존 컬럼이어야 한다면, **그 검사를 작업 생성 시점에** 할 수 있다.

```
generate-jobs 시점:
  record_map_json 의 테이블이 ResultTable 에 있는가
  row_stamp_json 의 컬럼이 그 모델에 있는가
  snapshot_column 이 그 모델에 있는가
  job_key_params 가 cartesian_param_json 으로 실제로 만들어지는가
```

지금은 이 전부가 **실행 시점에** 드러나거나 조용히 버려진다. 생성은 하루 한 번이고 실행은 수천 번이라, 같은 검사를 생성 쪽으로 옮기는 것만으로 발견이 빨라진다.

**구현 (`job_builder.validate_api_config`).** 위 넷 중 셋을 본다. 네 번째는
이미 생성 시점에 걸려 있었다 -- `_build_job_id` 가 빠진 키를 올리고, 축을 다
펼친 뒤에야 할 수 있는 검사라(쿼리 축은 DB 를 봐야 한다) 앞당길 것이 없다.
`build_jobs_from_builder` 가 축을 펼치기 **전에** 부른다: 어차피 틀린 설정이면
그 쿼리를 돌릴 이유가 없다.

범용 저장 경로(`api_rst`)는 컬럼 검사를 건너뛴다. 거기서는 모든 이름이
`result_json` 의 키라 "없는 컬럼" 이라는 것이 없고, 그 행의 모양을 선언하는
곳은 **뷰**다(`gen_ddl.py` 의 `TO_GENERIC`). 그 지식은 생성 스크립트에만 있고
엔진은 모른다 -- 아래층이 "새 표가 생겨도 models.py 를 안 고친다" 를 사는
대가다. 뷰를 다시 생성하면 거기서 드러난다.

검증 (모델만, DB 없이):

| 설정 | 결과 |
|---|---|
| generic 표 + 아무 스탬프 이름 | 통과 |
| typed 표 + 맞는 컬럼 | 통과 |
| typed 표 + 없는 컬럼 | `SchemaError` |
| 없는 표 | `MappingError` |
| `snapshot_column` 오타 | `SchemaError` |

그리고 v2 DB 에서 생성 경로로: `record_map_json` 을 없는 표로 바꾸면
`generate_jobs_for_builder` 가 **잡을 만들기 전에** MappingError 로 막힌다.

### 구현에서 드러난 것

**`empty_is_pending` 이 두 층 때문에 깨져 있었다.** v1 에서 `api_rst` 는
곁다리였다 -- 타입 테이블에 넣으면서 원본 응답도 같이 보관하는 자리였고, 그
셀렉터는 응답 전체를 한 레코드로 주므로 데이터가 없어도 1을 센다. 그래서
2026-09-19 에 "0건인가" 판정에서 `api_rst` 를 뺐다.

v2 에서는 같은 표가 여덟 소스의 **본 저장소**다. 그래서 그 제외가 그대로
남아 있으면 10건이 들어와도 합이 0으로 읽혀 "아직 아니다" 가 되고, one-shot
잡이 영원히 닫히지 않는다. 실측으로 재현했다.

가르는 기준을 `record_map_json` 으로 바꿨다: `api_rst` 가 다른 표와 **함께**
나오면 곁다리, 혼자면 본 저장소다. 고친 뒤:

```
데이터 있는 날   api_rst: 10              -> SUCCESS, is_active=False (닫힌다)
공시 없는 날     no data yet (api_rst: 0) -> EMPTY,   is_active=True  (다음 사이클이 다시 묻는다)
```

**설정 오류가 재시도되고 있었다.** `fetch` 의 tenacity 조건이 "SessionExpired
와 SiteBlocked 만 빼고 전부" 였다. 그래서 `build_request` 가 올리는 설정 오류도
세 번 재시도하고 백오프 10초를 썼다 -- 같은 설정으로 다시 만들면 같은 예외다.
이제 등급이 정한다(`retryable`). 실측 소요: 404 는 3.6초(3회 재시도),
`MappingError` 는 0.22초(재시도 없음).

**알림 채널은 없다.** `alert` 는 "logger.error 로 올리고 `error_code` 로 질의
가능하게 둔다" 까지다. 채널이 생기면 이 플래그가 그 입력이 된다.

**재시도 횟수는 사이클 안에서만 센다.** `attempt_no` 는 거절된 호스트의 잡이
창을 기다린 뒤 다시 도는 경우에만 1보다 크다. 사이클을 넘는 재시도는
`COUNT(*) ... WHERE job_id = ?` 가 이미 답이고, 잡마다 그 COUNT 를 하면
사이클당 수백 번의 왕복이 된다.

---

## 6.5 코드 레지스트리

### 재보니

코드성 컬럼이 **103개**다(고유값 2~40, 전 결과 테이블).

```
KSD 53 · KRX 23 · KIS 14 · AMC 13
```

> 이 103은 과다 계산이다. 날짜(`bas_dd`, `trade_date`)와 작은 수치(`val`, `rn_d`)가
> 섞여 있다 -- 수집한 날이 며칠뿐이라 고유값이 적게 나온 것이다. 진짜 코드성은
> 그보다 적다. 그래도 자리수는 맞다: 원천마다 수십 개다.

### 왜 원천별·컬럼별로는 안 되나

같은 개념이 **한 원천 안에서도** 두 어휘로 들어온다.

```
ksd_bond_isin.codevalue_nm    11종   국채 · 특수채 · MBS · SLBS …
ksd_bond_credit.secn_kanm      8종   ← 같은 개념, 다른 컬럼명. 11종의 부분집합
ksd_kacd_code.codevalue_nm    61종   ← 코드표인데 MBS·SLBS 가 없다
```

그리고 코드가 아예 안 오는 경우가 있다. `ksd_bond_isin` 1,870,025행에서
`secn_kacd` 는 **0건**이다 -- 전체 목록 API 는 이름만 주고, 코드는 그 코드로
조회할 때만 알 수 있다. 이름으로 되찾으려 해도 11종 중 8종만 1:1 이고
`MBS`·`SLBS` 는 코드표에 없고 `일반회사채` 는 둘에 걸린다.

신용등급은 한 컬럼 안에서 `'A'` 와 `'A0'` 가 공존한다 -- 평가사마다 평코드를
다르게 쓴다.

그러니 컬럼마다 따로 다루면 원천이 늘 때마다 같은 판단을 다시 하게 되고,
어휘가 겹치는지 아닌지는 아무 데도 적혀 있지 않다.

### 두 층

```
① 코드 사전   원천이 말한 것.      수집물에서 접는다.  손대지 않는다
② 표준 + 매핑  우리 어휘와 크로스워크.  손으로 적는다
```

**① `mst_code` (REF)** -- 원천별 코드 사전 하나로 모은다. 표를 원천마다 만들지
않는 것이 요점이다.

```
source       KSD | KRX | KIS | AMC
code_set     KACD | MKT | SECUGRP | CREDIT_GRD …
code         '1101' | 'BND' | NULL        ← 코드를 안 주는 원천은 NULL
code_nm      '국채'
parent_code  '11'                         (KACD 처럼 계층이 있는 것)
depth        1
first_seen   새 값이 처음 나타난 날
last_seen    사라진 값을 알 수 있다
PK (source, code_set, NVL(code, code_nm))
```

`code` 가 NULL 일 수 있는 것이 이 설계의 핵심이다. `krx_fut_daily.mkt_nm`
('정규'/'야간')처럼 KRX 가 코드를 안 주는 자리에서는 **이름이 곧 키**다. 코드가
있는 자리와 없는 자리를 한 표에 담으려면 그래야 한다.

`first_seen` 이 하는 일: 지금은 원천이 새 값을 보내면 조용히 들어온다. 사전에
새 행이 생기는 것으로 드러난다.

접는 것은 프로시저다 -- `ksd_kacd_code`(SCRAP, 수집물)에서 `mst_code`(REF)로.
`mst_etf` 와 같은 모양이다.

**② `std_code` + `user_code_map` (REF)** -- 우리 어휘와 원천→우리 매핑.

```
std_code       code_set, code, code_nm            우리가 정한 어휘
user_code_map  source, src_code_set, src_value  →  code_set, code
```

`user_etf` 와 같은 성질이다: 손으로 적고, 다시 만들 수 없고, 수집이 덮지 않는다.
`MBS`·`SLBS` 처럼 원천 코드표에 없는 값은 여기서 메운다.

### 지켜야 할 것 넷

1. **결과 테이블의 원본 값은 바꾸지 않는다.** 원천이 '유동화SPC채' 를 보냈으면
   그대로 남는다. 해석은 뷰나 정제 마스터에서 한다 -- 이 프로젝트가 이미
   그렇게 생겼다(랜딩 테이블 → 프로시저 → 정제 마스터).
2. **원천 + 코드셋이 네임스페이스다.** 두 원천이 같은 글자를 써도 매핑되기
   전까지는 다른 행이다. 중복을 미리 합치려 들지 않는다 -- 같은 뜻인지는
   사람이 판단할 일이고, 그 판단이 `user_code_map` 이다.
3. **사전은 수집, 매핑은 손으로.** 섞으면 다시 만들 수 있는 것과 없는 것이
   한 표에 들어간다.
4. **전부 매핑하지 않는다.** 103개 중 대부분은 담아두기만 하면 된다
   (`prdy_vrss_sign`, `atm_cls_code`). 조인에 쓰거나 사람이 보는 것만 매핑한다.
   레지스트리의 값은 "모든 코드를 정리했다" 가 아니라 "정리한 것이 어디 적혀
   있는지 한 곳에서 안다" 다.

### 표 셋 (확정)

```
SRC_CODE       SCRAP   결과 컬럼에서 수확한 값        프로시저
  PK (table_name, column_name, code_value)
  source_name · row_cnt · distinct_cnt · first_seen · last_seen

USER_CODE_MAP  REF     원천 값 -> 우리 코드            사람
  PK (table_name, column_name, code_value)   ← SRC_CODE 와 같은 키
  code_group · code                          → MST_CODE 를 가리킨다

MST_CODE       REF     우리 어휘                      사람
  PK (code_group, code)
  code_nm · parent_code · sort_order
```

매핑은 **PK → PK** 다. SRC_CODE 에서는 값 자체가 키의 일부라 "키와 값" 이 따로가
아니다.

```
SRC_CODE       ksd_bond_isin · codevalue_nm · '국채'
USER_CODE_MAP  ksd_bond_isin · codevalue_nm · '국채'  ->  BOND_TYPE · GOVT
MST_CODE       BOND_TYPE · GOVT · '국채'
```

키를 `(표, 컬럼, 값)` 으로 두는 이유는 측정이 말해 주었다. 값만으로 키를 잡으면
**뜻이 다른 것이 합쳐진다** -- 둘 이상의 컬럼에 나오는 값 109개가 거의 다 그런
경우였다.

```
'2'         12개 컬럼   atm_cls_code · info_type · prdy_vrss_sign …  뜻이 다 다르다
'20261123'   6개 컬럼   stck_bsop_date · trade_date …
```

컬럼 기준이면 매핑이 1.40배(615 → 862줄) 늘지만, 그 늘어남이 곧 "이 두 컬럼의
'국채' 는 같은 뜻" 이라는 판단이다. 글자가 같으니 자동으로 같다고 두면 안 된다.

별도 선언 표를 두지 않는다. `USER_CODE_MAP` 에 어떤 `(표, 컬럼)` 이 등장하면 그
자체가 "이 컬럼은 코드 컬럼이고 이 어휘를 쓴다" 는 선언이다. 그래서 셋으로 끝난다.

외래키는 걸지 않는다 -- 원천이 아직 안 보낸 값을 미리 매핑해 둘 수 있어야 하고
(`MBS`·`SLBS` 가 그 경우), 수확이 SRC_CODE 를 다시 만들 때 제약이 걸린다.

### 조인은 뷰로 (REF)

질의마다 두 표를 잇지 않게 `v_code_map` 을 둔다.

```sql
SELECT b.isin, c.code_nm AS bond_type
  FROM ksd_bond_isin b
  LEFT JOIN v_code_map c ON c.table_name  = 'ksd_bond_isin'
                        AND c.column_name = 'codevalue_nm'
                        AND c.code_value  = b.codevalue_nm;
```

뷰 안에서 `mst_code` 를 **LEFT** 로 잇는다. 매핑이 가리키는 코드가 `mst_code` 에
없으면 그것은 데이터 오류인데, INNER 면 그 행이 조용히 사라져서 오류가 안
보인다. `code_nm` 이 NULL 로 나오는 쪽이 낫다 -- 표본으로 확인했다.

```
ksd_bond_isin · codevalue_nm · '없는코드'  →  BOND_TYPE.NOSUCH  (code_nm NULL)
```

`v_code_unmapped` 는 수확됐지만 매핑 안 된 값이다. 운영 중 가장 자주 볼 질의라
뷰로 둔다.

```sql
SELECT * FROM v_code_unmapped ORDER BY row_cnt DESC FETCH FIRST 30 ROWS ONLY;
```

> 이 뷰를 SCRAP 에 넘겨주려면 `src_code` 의 GRANT 에 **WITH GRANT OPTION** 이
> 붙어야 한다. 없으면 뷰를 만들 때가 아니라 넘겨줄 때 ORA-01720 이 나서 원인이
> 한 겹 숨는다.

### 수확 (`sp_src_code_sync`, SCRAP)

후보는 **통계로** 고른다. `user_tab_columns.num_distinct` 가 555개 문자 컬럼
전부에 채워져 있고 저카디널리티에서는 실측과 정확히 맞는다(`codevalue_nm`
11=11, `mkt_id` 6=6, `mkt_nm` 2=2). 그래서 418개 컬럼에 `COUNT(DISTINCT)` 를
돌리는 전수 스캔 없이 후보 165개만 `GROUP BY` 한다.

v1 실데이터로 돌려 확인했다.

```
765개 값 · 8.3초
KSD 컬럼 51 값 283 · KRX 22/280 · AMC 12/103 · KIS 10/99
```

빼야 했던 것 셋을 검증 중에 찾았다.

- **날짜처럼 생긴 이름** -- `bas_dd`(17종)는 임계값을 통과하지만 코드가 아니고,
  두면 거래일마다 행이 하나씩 쌓인다. 값(8자리 숫자)이 아니라 **이름**으로
  거른다 -- 8자리 숫자 코드가 있을 수 있어서다
- **엔진 부기** -- `api_job.save_mode`, `api_job_log.status` 는 원천이 보낸 것이
  아니라 우리가 정한 값이다. `JOB_ID` 가 있으면 결과 테이블이라는 판정이
  이것들을 잡는다
- **외부 테이블** -- 같은 데이터의 사본이라 값이 두 벌로 들어오고, 스캔이
  오브젝트 스토리지를 타서 **45초가 144초가 됐다**. `user_tables` 로 조인해서는
  안 걸러진다 -- Oracle 은 외부 테이블도 거기 넣는다. `user_external_tables` 를
  봐야 한다. 고치니 8.3초

지우지 않는다. 원천이 더는 보내지 않는 값은 `last_seen` 이 멈추는 것으로
드러나고, 과거 데이터에는 그 값이 아직 남아 있다.

### 수확이 뷰도 훑는다 (2026-11-25)

두 층 이전이 수확을 깎아 놓았다. 아래층으로 내린 소스의 값은
`api_rst.result_json`(JSON 타입) 안이라 `data_type LIKE 'VARCHAR%'` 에 걸리지
않는다 -- 그래서 운용사 ETF 표의 코드 컬럼 **11개**가 조용히 빠져 있었다
(`FUNDTYPENM` 5 · `TYPENM` 6 · `TYPELNM` 8 · `MONTHLYYN` 3 · `FUNDTYPE` 7 ·
`PRODUCTOPTION` 2 · `ASSET_TAG` 7 · `PENSION_TAG` 2 · `THEME_TAG` 16 ·
`ETF_CD6` 22 · `FUND_CD` 22).

`v_*` 뷰를 훑으면 되찾는다. 재 보니 **고칠 곳이 한 줄**이었다:

| | |
|---|---|
| 뷰 컬럼이 `user_tab_columns` 에 있나 | 있다. `RETURNING` 타입이 `data_type`/`data_length` 로 올라온다 |
| 뷰가 `JOB_ID` 를 갖나 | 갖는다. "결과 테이블이다" 를 판정하는 그 조인이 이미 통한다 |
| 파티션 프루닝이 걸리나 | 걸린다 (`part 2 -> 2`). 한 뷰의 GROUP BY 가 그 소스 몫만 읽는다 |
| 날짜 이름 필터 | 그대로 먹는다. 뷰가 모델 이름을 물려받아 `std_dt` 가 `_DT` 로 걸린다 |
| **`num_distinct`** | **없다.** 10개 뷰의 모든 컬럼이 NULL -- 이 한 줄이 뷰 전부를 걸러냈다 |

**카디널리티 판정을 GROUP BY 안으로 옮겼다.** `COUNT(*) OVER ()` 가 그룹 수를
주므로, 값 목록을 뽑는 그 한 번의 스캔으로 "후보인가" 까지 끝난다. 뷰 때문에
필요했지만 표에도 같이 쓴다 -- 셋이 같이 좋아진다:

* `distinct_cnt` 가 추정이 아니라 실제 값이 된다. 통계가 낡으면 전에는 틀린
  수가 들어갔다.
* 통계가 틀렸을 때의 안전장치가 된다. 전에는 통계가 10이라 해도 실제로
  5,000종이면 그 5,000행을 다 넣었다.
* 세는 대상이 저장하는 대상과 같아진다 -- 400자 넘는 값이 양쪽에서 똑같이
  빠진다(`num_distinct` 는 그것까지 세고 있었다).

표의 **후보 선정**은 그대로 통계를 쓴다. 그게 418개 컬럼을 다 훑지 않는
이유이고, 새 판정은 이미 좁혀진 후보에만 걸린다.

**`api_rst` 위의 뷰로 한정한다** (`user_dependencies`). `user_tab_columns` 는
모든 뷰를 포함하므로 조건만 풀면 타입 표 위의 뷰까지 들어오고, 그러면 같은
어휘가 `table_name` 두 개로 쌓인다(PK 가 `(table_name, column_name,
code_value)`다) -- "이 둘은 같은 뜻" 을 사람이 다시 판단해야 한다. 실측으로
정확히 9개만 걸린다.

이름에서 소스를 끌어낼 때 `V_` 를 떼야 한다. 떼지 않으면 `V_KRX_*` 가 KRX 로
안 잡히고 전부 AMC 로 떨어진다.

검증 (v2 실데이터):

```
뷰 경로   v_ace_etf_pdf.fundcd 3 · jm_ksc_cd 21 · sec_nm 21 · v_isin_bond_info.label 12
          -> 57건, 2.7s
표 경로   ksd_kacd_code 에 5행 넣고 통계 모아 돌림
          -> codevalue 3 · codevalue_nm 3, distinct_cnt 3(실측값), source_name KSD
```

**`html_block` 이 만드는 셋은 뺀다.** `block_index` · `label` · `html` 은 그
모드가 붙인 것이고 원천이 보낸 값이 아니다 -- `api_id`/`job_id` 와 같은 근거로
부기 목록에 들어간다. 특히 `label` 은 화면 섹션 이름(`발행기관코드` · `CFI` ·
`Call 주체` 16종)이라 매핑할 대상이 없다. 확인: 이 셋은 v1 에도, 어느 모델에도
없고 그 모드가 쓰는 뷰에만 나타난다.

`block_index`(NUMBER)와 `html`(CLOB)은 VARCHAR 필터에 이미 안 걸리지만 셋을
같이 적었다 -- 이유를 한 자리에 두면 나중에 타입이 바뀌어도 판단이 남는다.
(이미 수확된 `label` 12행은 지웠다. 제외해도 과거 수확은 남는다.)

남는 비용 하나를 적어 둔다. 뷰는 통계가 없어 **후보를 가리기 위해 일단
훑으므로** 고카디널리티 컬럼(`v_isin_bond_info.isin`)은 전수 GROUP BY 뒤에
버려진다 -- 채권 상세가 38만 행쯤 쌓이면 재 봐야 하고, 그때 손잡이는
`p_table` 이다.

### 용량은 이유가 아니다

큰 표에서 코드성 한글이 반복되는 비용은 **27MB** -- 전체 테이블 4.7GB 의 0.6%다.

```
ksd_bond_isin.codevalue_nm   11종   1,870,025행   20MB
krx_fut_daily.mkt_nm          2종   1,096,304행    6MB
```

정규화로 아끼는 것이 저만큼이고, JSON 계층 전환이 +6.9GB 인 것과 비교하면
자리수가 둘 다르다. 게다가 한글을 코드로 바꾸면 `WHERE codevalue_nm = '국채'` 가
`WHERE secn_kacd = '1101'` 이 되어 읽기 어려워진다.

레지스트리를 두는 이유는 용량이 아니라 **같은 개념인지 아닌지가 어디에도 적혀
있지 않다는 것**이다.

---

## 7. 기타 개선

우선순위 순이다.

### 7.1 `api_job`이 날마다 커지는 구조 ★

가장 큰 구조 문제다. 작업 633,996행 중 대부분이 **같은 종목의 다른 날짜**다.

```
KIS_ETF_PDF_069500_20261124
KIS_ETF_PDF_069500_20261123
KIS_ETF_PDF_069500_20261122   ← 종목은 하나인데 행이 날마다 늘어난다
```

날짜를 job_id에 넣는 이유는 (a) 일별 이력 보존과 (b) 같은 날 재실행의 멱등성이었다. 둘 다 맞지만, 그 대가로 **작업 정의 테이블이 실행 이력 테이블처럼 자란다.**

제안: 작업은 "무엇을 받을지"만 남기고 날짜는 로그로 옮긴다.

```
api_job        종목 단위로 고정           KIS_ETF_PDF_069500     1,163 행
api_job_log    실행마다, target_date 포함   ... 20261124 ...       날마다 쌓임
```

"그날 이미 받았나"는 `api_job_log`에서 본다 (`target_date` + `status='SUCCESS'`). 멱등성은 결과 테이블의 날짜 컬럼으로 지운 뒤 넣는 방식으로 유지한다 — 지금 `save_mode='overwrite'`가 job_id로 지우는 것을 `(job_id, target_date)`로 바꾸면 된다.

얻는 것: `api_job`이 종목 수만큼 고정되고, `purge-jobs`가 작업이 아니라 로그만 보면 된다. 설정 테이블과 이력 테이블의 역할이 갈린다.

이게 v2에서 **가장 값이 큰 변경**이고, 동시에 가장 손이 많이 간다. 분리해서 결정할 만하다.

### 7.2 `description`이 두 가지로 쓰인다

```python
job.description = ", ".join(f"{table}: {n}" ...)        # 실행 결과
job.description = f"no data yet ({job.description})"     # 결과를 설명에 덮어쓴다
```

사람이 적은 설명과 마지막 실행 결과가 같은 `VARCHAR2(100)` 칸을 쓴다. 실행이 설명을 덮으므로, **빌더에 적어 둔 메모가 첫 실행에 사라진다.**

```
description  →  note         사람이 적는다. 엔진이 안 건드린다
                engine_note  마지막 실행 요약. 엔진이 덮어쓴다
```

`note`와 같은 명사에 주인만 다르게 두어, 컬럼 이름만 보고 어느 쪽이 사람 것인지 알 수 있게 한다. `engine_log`를 피한 이유는 `api_job_log`가 따로 있어서다 — 이것은 이력이 아니라 마지막 한 줄이다.

6.3의 `record_count`가 들어가면 `engine_note`는 없어도 되지만, 운영 화면에서 한 줄로 보기에는 쓸모가 있다.

### 7.3 `models.py` 2,797줄 단일 파일

60개 클래스가 한 파일에 있다. 소스별로 가른다.

```
app/db/models/
  __init__.py      전부 재노출 (기존 import 경로 유지)
  base.py          ResultTable · ReferenceTable · VendorRecordBase · 믹스인들
  config.py        api_mst · api_job_builder · api_job · api_job_log · api_rst
  krx.py           krx_*
  kis.py           kis_* · fo_idx_code_mst
  ksd.py           ksd_*
  etf.py           *_etf · *_etf_pdf · amc_etf_pdf
  reference.py     mst_* · meta_* · user_* · stock_index_his
```

`__init__.py`가 전부 재노출하면 `from app.db.models import X`가 그대로 동작해서 **호출부 수정이 없다.** `_discover_table_registry`가 `vars(_models)`를 훑으므로 재노출만 해두면 그쪽도 그대로다.

### 7.4 `api_group` 13개가 두 축을 섞는다

```
DART · ETF_LIST · ETF_PDF · KIS · KIS_AUTH · KIS_DRV · KIS_ETF
KRX · KRX_API · KRX_DATA · Seibro_api · Seibro_web · test
```

`KIS_*`는 소스 기준, `ETF_*`는 용도 기준, `Seibro_api`/`Seibro_web`은 소스+전송방식이다. 같은 컬럼에 세 가지 분류 체계가 있다. 화면 트리 묶기에만 쓰이므로 당장 문제는 없지만, 늘어나면 어디에 넣을지가 매번 판단이 된다.

```
api_group  →  source_name  KIS | KRX | KSD | DART | AMC    누가 주는가
              domain   DERIV | ETF | BOND | STOCK | AUTH   무엇에 관한가
```

두 축이면 13개가 아니라 5×5 격자 안의 좌표가 된다. `test`는 `is_active` 또는 별도 플래그로 옮긴다.

### 7.5 결과 테이블의 대리키

시퀀스가 44개다. 결과 테이블마다 `Identity` PK를 두기 때문이다. 외부 테이블로 내보내고 나면 그 `id`는 **아무도 참조하지 않는다** — 조인 대상이 아니고, 외부 테이블 쪽에서는 의미도 없다.

자연키가 뚜렷한 표는 PK를 바꿔 볼 만하다.

```
krx_etf_daily   (bas_dd, isu_cd)        지금: id + 인덱스 2개
kis_etf_pdf     (job_id, snap_at, stck_shrn_iscd)
```

얻는 것: 시퀀스 하나, 인덱스 하나, 적재 시 시퀀스 왕복이 준다. 1.9M~4M행 테이블에서는 눈에 보인다.

**다만 신중히.** 자연키가 유일하지 않은 표(같은 날 중복 공시, `ksd_bond_yield`의 중복 문제)에서는 적재가 깨진다. 표마다 확인이 필요하므로 v2의 필수 항목이 아니라 **표별 선택 사항**으로 둔다.

### 7.6 외부 테이블이 수집 계정에 있다

`XT_*` 5개(최대 9M행)가 수집 계정에 있다. `sp_create_external_tables`의 주석 자체가 "분석용 DB에 두는 프로시저"라고 적고 있는데 실제로는 수집 계정에 걸려 있다. 계정 분리 시 MART로 가면 자연히 해소된다.

### 7.7 작은 것들

- **`_BOOKKEEPING_TABLES` 하드코딩** — 5.4의 마커 클래스로 사라진다
- **`api_job_log` 2.86M행** — `purge-job-logs`가 있지만 보존 기간을 `error_code`별로 다르게 둘 수 있다 (성공은 짧게, `ConfigError`는 길게)
- **`request_type`이 요청 방식과 HTTP 메서드를 겸한다** — `GET`/`POST`/`BROWSER`. `protocol`(HTTP/BROWSER) + `method`(GET/POST)로 가르면 체크 제약 두 개가 단순해진다. 엄밀히는 BROWSER 행도 내부에서 HTTP로 말하므로 이 컬럼이 고르는 것은 프로토콜이 아니라 **어느 스크래퍼로 가는가**(`app.scrapers.make_scraper`)인데, 더 literal한 `engine`은 `app.db.engine`과 헷갈려서 읽기 쉬운 쪽을 택했다
- **`api_mst.header_json`이 NOT NULL인데 빈 dict가 많다** — 기본값 `{}`를 주고 nullable로

---

## 8. 이행 순서

각 단계가 끝난 뒤 되돌릴 수 있는 지점을 둔다.

```
1  브랜치 · 새 DB 계정 둘 생성 · env V2_* 등록           기존 무영향
2  마커 클래스 도입 + models.py 분할 (7.3, 5.4)          v1 에서도 동작, 먼저 병합 가능
3  SCRAP 에 결과/부기 테이블 생성 · REF 에 사전 테이블
4  GRANT + synonym (3.3, 5.1)
5  컬럼 이름/구조 변경 (4절) — 모델과 엔진 동시
6  api_mst/builder 설정 변환 스크립트 (v1 → v2 형태)
7  예외 계층 + api_job_log 확장 (6절)
8  SCRAP_TARGET=both 로 이중 운행, 결과 비교
9  api_job 날짜 분리 (7.1) — 별도 결정
10 v2 단독 전환 · v1 읽기 전용
```

**2단계는 먼저 떼어낼 수 있다.** 모델 분할과 마커 클래스는 v1에서도 그대로 동작하므로, v2 결정과 무관하게 `dev`에 병합해도 된다. 2,797줄 파일을 가르는 것만으로 이후 작업이 쉬워진다.

**6단계가 실제 작업량의 절반이다.** `api_mst` 58행 + `api_job_builder` 95행의 JSON을 새 형태로 옮기는 스크립트이고, 변환이 맞는지는 8단계의 비교로만 확인된다.

### 8.3 타이머가 부를 것을 v2 에 올렸다 (2026-11-25)

`sp_run_daily_batch2` · `sp_run_daily_early`. 둘 다 순차 호출만 하는 껍데기이고
부르는 8개가 전부 v2 에 있어서, 포팅 자체는 컴파일이 끝이었다. 실제 위험은
계정 분리 쪽이었는데 -- SCRAP 이 REF 소유 프로시저를 synonym 으로 부를 수 있나
-- 먼저 재서 없다는 것을 확인했다(10개 전부 호출된다).

v2 에서 실행:

```
sp_run_daily_early   0.4s   retire 0 · krx_stock_base dedup 0 · mst_stock 0
                            meta_maturity 0 · mst_fuopt 0
sp_run_daily_batch2  2.7s   retire 0 · mst_bond 0 · mst_etf 0 · no_detail_mark 0
                            src_code 45 (v_ace_etf_pdf 세 컬럼)
```

전부 0건인 것이 맞다 -- v2 결과 표가 비어 있다. 확인한 것은 **단계가 순서대로
끝까지 간다**는 것이다.

#### 부르는 곳이 없던 프로시저 둘

올리면서 드러났다. v2 에서 새로 만든 둘이 **아무도 부르지 않는 상태**였다.

`sp_kis_index_daily_dedup` 은 자기 주석에 "배치는 sync 앞에 이것을 부른다" 고
적혀 있는데 그 배치가 v2 에 없었다. `kis_index_daily` 를 (영업일, 지수코드) 당
한 행으로 접는 일이고, `sp_stock_index_his_sync` 의 MERGE 가 그 표를 소스로
읽으므로 키가 한 건이어야 한다 -- 아니면 ORA-30926 이다. v1 에서는 그 DELETE 가
sync 안에 있었는데 v2 는 계정이 갈려 그럴 수 없다(sync 는 REF 소유,
`kis_index_daily` 는 SCRAP 소유이고, REF 에 DELETE 를 주면 계정을 나눈 이유가
없어진다). `sp_run_export` 의 sync 바로 앞에 넣었다.

`sp_src_code_sync` 도 호출부가 없었다. `sp_run_daily_batch2` 끝에 넣었는데,
그 프로시저는 사이클 **앞**에 도므로 수확되는 것은 직전 사이클까지의 값이다 --
어휘가 하루 늦는다. 코드 값은 드물게 새로 나타나므로 그 지연은 값을 치르지
않는다. 사이클 뒤로 옮기려면 systemd 유닛에 `ExecStart` 한 줄이 더 필요하고
그건 배포 작업이다.

그 호출은 **블록으로 감쌌다**. 수확은 보조적인 일인데 이 유닛은
`Type=oneshot` 이라 첫 실패에서 멈춘다 -- 감싸지 않으면 수확이 깨질 때 그 밤의
수집이 아예 돌지 않는다. 프로시저 안의 컬럼 단위 예외 처리는 컬럼 하나가 깨질
때의 이야기이고, 이 블록은 그 바깥(접속·권한·표 없음)을 막는다.

#### 남은 구멍: `v_fuopt_price`

`sp_run_export` 가 그 뷰를 `p_query` 로 부르는데 **v2 에 없다.** 동적 SQL 이라
프로시저는 VALID 이고 실행 시점에 ORA-00942 로 죽는다. 그리고 **그 뷰의 DDL 이
저장소에 없다** -- `v_fut_cont`·`v_k2i_atm` 은 `scripts/sql/` 에 있지만 그것은
없고, v1 에만 있었고 v1 은 이제 닿지 않는다(2.4).

그래서 export 를 v2 에서 돌리려면 그 뷰를 먼저 만들어야 하고, 정의를 v1 에서
떠 오려면 `ORACLE_*_V1` 을 한 번 넣어야 한다. 수집 사이클에는 영향이 없다 --
내보내기 유닛만의 문제다.

### 8.2 6단계 완료 (2026-11-25)

v1 의 `api_mst` 58행 · `api_job_builder` 95행에서 **57행 · 94행**이 v2 로
갔다(`TEST` 와 그 빌더는 빼기로 결정 -- 아래).

`scripts/migrate_v1_to_v2.py`. 한 번 쓰고 버리는 스크립트가 아니라 **매핑의
기록**이다 -- v1 의 한 컬럼이 v2 의 여러 컬럼으로 갈린 자리마다 어느 값이
어디로 갔는지 거기 적혀 있다. 기본이 dry-run 이고 `--apply` 로 적재한다.

쓰기를 SQLModel 로 한다. JSON·XMLTYPE·BOOLEAN 바인딩을 **엔진이 런타임에 쓰는
그 경로**로 통과시키려는 것이다 -- 옮긴 행이 엔진으로 다시 읽히는지가 합격
조건이고, raw INSERT 로는 그것을 검증하지 못한다(JSON 컬럼에 문자열을
바인딩하면 JSON 스칼라로 들어가는 함정을 1절 측정에서 한 번 밟았다).

**`api_group` 13개 → `source_name` × `domain` 격자.** api_id 접두사로 정한다.
그룹 값으로는 안 된다 -- `ETF_PDF` 한 그룹에 운용사 5곳과 KRX 와 KIS 가 섞여
있어서 그룹→`source_name` 이 함수가 아니다.

| source_name | domain | 행 |
|---|---|---|
| AMC | ETF | 13 |
| KIS | AUTH 2 · DERIV 5 · ETF 1 · INDEX 1 | 9 |
| KRX | AUTH 1 · DERIV 7 · ETF 3 · ETN 1 · INDEX 1 · STOCK 4 | 17 |
| KSD | BOND | 16 |
| DART | DISCLOSURE | 2 |
| | | **57** |

`TEST` 는 옮기지 않았다(아래 "돈 적 없던 행 둘"). 그래서 `source_name` 축은 다섯 종이고
분류체계 혼입이 없다.

오픈API(`KRX_API`)와 데이터포털(`KRX_DATA`)을 `source_name` 으로 가르지 않았다. 같은
기관이고, 포털이 막힐 때 오픈API 는 멀쩡하다는 구분은 **호스트 단위 차단기가
이미** 하고 있다(`execution._HostIndex`). `Seibro_api`/`Seibro_web` 도 같은
이유로 하나다 -- api/web 은 전송 방식이지 소스가 아니다.

**`response_parse_json` → 네 컬럼.** 어느 키가 어디로 가는지는 v2 엔진이
실제로 읽는 자리로 정했다. 모르는 키가 나오면 스크립트가 **멈춘다** -- 조용히
한쪽에 넣으면 그 행의 기능 하나가 말없이 사라지고 증상이 "아무 일도 일어나지
않음" 이다. 14종 전부 자리가 있었다.

```
parse_json    7행   fields has_header skip_rows delimiter encoding rows
session_json  6행   login logged_out blocked expect
staging_json  1행   group name unzip
options_json  5행   empty_is_pending
```

**`key_params_list` → 네 컬럼.** v1 의 한 항목이 세 가지를 동시에 뜻했다.
`job_key_params` 에는 문자열 항목만이 아니라 **dict 항목의 키도** 들어가야
한다 -- v1 의 `_build_job_id` 가 NOW 를 뺀 모든 항목의 파라미터 이름으로
job_id 를 만들었으므로, 안 그러면 옮긴 뒤 job_id 가 달라지고 어제까지 받은
잡과 오늘 만든 잡이 다른 것이 된다. 결과: 반복 11행(그중 `snapshot_column`
있는 것 5행), `job_key_params` 50행, `row_stamp_json` 49행.

**`execution_cycle` → `schedule_tag` + `snapshot_unit`.** v1 에서 실제로
절삭됐던 값만 단위를 받는다(`3m_call`·`3m_put` → `3m`, 2행). 나머지 93행은
NULL -- 임의로 채우면 이행이 동작 변경을 몰래 끼워 넣는 일이 된다.

**`output_tables_json` → `record_map_json`.** 두 층으로 내린 8개 표를 쓰던
행은 `api_rst` 를 가리키게 바꿨다. 합쳐서 14행이 `api_rst` 를 가리킨다 --
8개는 본 저장소로 바뀐 것이고, 나머지 6개(DART 둘, KIS_FUTOPT 둘,
KRX_ISIN_SRCH, KSD_BOND_CREDIT_HIS_web)는 v1 때부터 거기 쓰던 행이다.

#### 검증 셋

1. **CHECK 제약 사전검사.** 네 제약(`ck_api_mst_body`·`payload`·`snapshot`·
   `protocol`)과 길이 제한을 파이썬에서 먼저 확인했다. 한 행이라도 걸리면
   커밋이 통째로 돌아가므로 미리 어느 행인지 알고 싶었다. 전부 통과.
2. **설정 검증.** `validate_api_config` 를 쓰기 **전에** 옮기는 행 전부에 돌린다.
   실패가 있으면 적재하지 않는다 -- 통째로 실패하는 쪽이 절반 적재된 상태로
   멈춰 "어디까지 들어갔지" 를 세는 쪽보다 낫다.
3. **왕복 비교.** 적재한 151행(57+94)을 엔진의 모델로 다시 읽어 변환 결과와 필드별로
   맞췄다. 전부 일치. (v1 의 `{}` 는 NULL 로 적재했다 -- 엔진이 `or {}` 로
   읽으므로 같고, v2 의 그 컬럼들은 nullable 이다.)

#### 검증이 잡은 것: 적을 수 없던 의도 5개

`validate_api_config` 가 세 행에서 `SchemaError` 를 냈다.

```
KIS_FUTOPT_CHART  BAR_SEC      -> 'bar_sec' 컬럼이 없다
KIS_FUTOPT_DAILY  DATE1 DATE2  -> 'date1' 'date2' 컬럼이 없다
KIS_INDEX_DAILY   DATE1 DATE2  -> 같음
```

**의도는 맞았다** (작성자 확인). 셋 다 요청에는 필요하고 저장할 필요는 없는
파라미터다 -- 조회 시작일·종료일·봉 간격이고, URL 의 `fid_input_date_1` ·
`fid_input_date_2` · `fid_hour_cls_code` 로 나간다. 응답이 되돌려주지 않고
(`output1` 을 열어 봐도 간격 필드가 없다), 행마다 찍을 날짜는 API 가 주는
`stck_bsop_date` 다 -- 한 요청이 여러 날을 돌려주므로 DATE1 도 DATE2 도 아니다.

틀린 것은 **v1 설정이 그 의도를 적을 수 없었다**는 점뿐이다. 문자열 항목은
컬럼 이름을 소문자로 끌어내므로(`"BAR_SEC"` → `bar_sec`) "이 컬럼에 찍어라"
로 읽히고, 의도대로 적으려면 `{"BAR_SEC": "none_mapped"}` 라야 했다. 그렇게
안 적혀 있어서 엔진은 찍으려 했고, 그 컬럼이 없으니 pydantic 이 모르는 필드로
무시했다 -- 결과는 의도와 같았지만 그 경로가 "설정에 적힌 대로" 가 아니라
"조용히 떨어짐" 이었다. 검증기는 의도를 못 읽으니 후자로 보고 걸었다.

**v2 는 예약어 없이 그대로 표현된다.** `job_key_params` 와 `row_stamp_json` 이
다른 컬럼이므로 -- 요청에 필요하면 앞에 넣고, 저장할 필요가 없으면 뒤에서
빼면 끝이다. `none_mapped` 가 없어진 이유가 이것이고(4.1), 이행이 한 것도
정확히 그것이다.

저장되는 데이터와 job_id 둘 다 그대로다. 값은 `api_job.params_json` 과 job_id
에 남아 있어서 잃는 것이 없다.

```
job_id      KIS_FUTOPT_CHART_A01609_20260814_60      <- 끝의 _60 이 BAR_SEC
params_json {"SHORT_CODE":…,"DATE":…,"HHMM":…,"BAR_SEC":"60"}
```

v1/v2 의 job_id 자리 구성을 맞춰 확인했다 -- `<api>_<SHORT_CODE>_<DATE…>_<BAR_SEC>`
로 같다. 이것이 실제 위험이었다: 세 파라미터가 `job_key_params` 에서 빠지면
job_id 가 달라져 어제까지 받은 잡과 오늘 만든 잡이 다른 것이 된다.

사람이 적은 dict 는 건드리지 않는다 -- 그게 틀렸으면 `SchemaError` 로 드러나야
한다.

#### 끝까지 돌려 본 것: 활성 빌더 40개

적재가 끝이 아니다. 옮긴 설정으로 **잡이 실제로 생성되는가**가 이행의 마지막
관문이고, 거기서 하나가 걸렸다.

```
생성 성공 38 / 실패 2
FAIL  3m_call  KIS_FUTOPT_PRICE_3M_CALL  ORA-00942: "V_K2I_ATM" does not exist
FAIL  3m_put   KIS_FUTOPT_PRICE_3M_PUT   같음
```

`v_k2i_atm` 은 MV 이고 v2 REF 에 **이미 만들어져 있었다**. 빠진 것은 GRANT 와
synonym 이다. 그래서 `api_mst` 쪽은 멀쩡해 보이고, 원인이 설정이 아니라
권한이라는 것이 한 겹 숨었다.

왜 06 의 점검이 못 잡았나: 그 블록은 `user_synonyms` 를 훑었다. **없는
synonym 은 루프를 돌지 않는다.** "있는 것이 멀쩡한가" 와 "있어야 할 것이
있나" 는 다른 질문이고, 아픈 쪽은 후자다. 기대 목록을 적어 두고 비교하도록
고쳤다 -- MISSING · DENIED · EXTRA 셋으로 보고한다.

같은 종류가 더 있는지 전수로 봤다. 빌더 95개의 `db` 축이 참조하는 이름은
8개(`api_rst` · `ksd_bond_info` · `ksd_bond_no_detail` · `ksd_kacd_code` ·
`mst_bond` · `mst_etf` · `mst_fuopt` · `v_k2i_atm`)이고, `v_k2i_atm` 하나만
읽히지 않았다. 그것을 읽는 빌더는 41개다 -- 3분 사이클 둘과 과거 1분봉 이행
39개.

MV 는 비어 있었다(생성만 되고 `DBMS_MVIEW.REFRESH` 가 한 번도 안 돌았다).
REF 에서 한 번 갱신했다 -- 0.36초, 1,765행, 최신 2026-11-23. 갱신은
`sp_stock_index_his_sync` 끝에서 하고 그 프로시저는 REF 에 있으니, 소유 관계는
맞다(REFRESH 는 소유 계정에서 불려야 한다).

그 뒤 **40개 전부 생성**된다.

```
3m_call       496      daily_batch1   3,205
3m_put        334      daily_batch2  10,999
daily_early    29      daily_start        7      합계 15,070
```

> 이 잡 15,070행은 검증으로 만든 것이다. 생성은 멱등이라 다시 돌려도 늘지
> 않지만, v2 의 `api_job` 을 비운 상태에서 시작하고 싶으면 지워도 된다.
> v2 에는 아직 타이머가 없어서 저절로 실행되지는 않는다.

#### 돈 적 없던 행 둘 (결정됨 2026-11-25)

v1 의 58행 중 둘은 `api_job` 이 하나도 없었다. 둘의 결론이 갈렸다.

**`ISIN_INFO` 는 살린다.** ISIN 하나의 표준코드 상세 팝업이고
(`isin.krx.co.kr/srch/srch.do?method=srchPopup3&std_cd=:ISIN`) 요청을 추가할
행으로 정해졌다. `KRX_ISIN_SRCH` 와 겹쳐 보이지만 성질이 다르다 -- 그쪽은
브라우저로 코드 앞자리를 넣어 **목록**을 긁고, 이쪽은 ISIN 하나의 **상세**를
HTTP GET 으로 받는다.

아직 저장할 준비는 안 돼 있다. 지금 가진 것은 url · `job_key_params ["ISIN"]`
· `row_stamp_json {"ISIN": "isin"}` 뿐이고, 비어 있는 것이 넷이다.

```
response_type     NULL   팝업 HTML 을 무엇으로 읽나 (html_table? browser?)
parse_json        NULL   어느 행·어느 필드
record_map_json   NULL   어느 표에 넣나  <- 이것이 없으면 아무것도 저장되지 않는다
빌더              없음    ISIN 축을 어디서 받나 (mst_bond? ksd_bond_isin?)
```

`validate_api_config` 는 통과한다 -- `record_map_json` 이 비면 검사할 대상이
없다. 그 빈 상태가 "설정이 아직 없다" 를 뜻하는 것이고, 검증기가 잡을 일은
아니다(잡게 만들면 작성 중인 행을 못 둔다).

**`TEST` 는 v2 로 옮기지 않았다.** `api_url` 이 `'https://'` 뿐인 스텁이고
`api_job` 이 없었다. 겨누던 빌더 `TEST_BUILDER`(비활성)도 함께 빠졌다.
7.4 가 "`test` 는 `is_active` 또는 별도 플래그로 옮긴다" 고 한 자리인데
`api_mst` 에는 `is_active` 가 없어서, 빼는 것이 그 뜻에 맞다 -- 남기면
`source_name` 축에 `TEST` 가 하나 생겨 7.4 가 없애려던 분류체계 혼입이 된다.

**v1 에는 그대로 있다.** 이 스크립트는 v1 을 읽기만 하고, 그쪽은 돌고 있는
시스템이다. v1 에서도 지울지는 별도 결정이다.

#### `html_block` 과 `ISIN_BOND_INFO` (2026-11-25)

`ISIN_INFO` 를 `ISIN_BOND_INFO` 로 바꾸고 설정을 채웠다. 받아 보니 채권
상세이므로 `domain` 도 `STOCK` → `BOND` 로 고쳤다(이행 때는 api_id 만 보고
정했다).

받은 페이지가 수집기가 해석할 모양이 아니었다. `<table>` 이 17개인데 하나는
나머지를 감싸는 래퍼고, 안쪽 16개는 **라벨/값 격자**다 --
`['발행기관코드','06757','발행기관명','엔브이에이치코리아(주)']`. `html_table`
은 사각형을 가정하고 `rows_to_records` 로 위치를 맞추므로 라벨이 데이터 자리에
들어가고, 한 표 안에서도 열 수가 2·3·4·5 로 섞인다. 라벨은 125종에 12종이
중복되고(`신용등급` ×4, `기타` ×4), 섹션 `<h3>` 제목은 16개 중 5개에만 있다.

**그래서 파싱을 하지 않는 모드를 뒀다** -- `response_type='html_block'`.
자르기만 하고 자른 조각은 손대지 않는다. 해석은 아래층 몫이다.

```json
{"blocks": "table:not(:has(table)):has(td:not(:empty))",
 "label":  "th"}
```

요소 종류가 코드에 나오지 않는 것이 요점이다. `blocks` 는 `table` 이든
`div.card` 이든 `ul#result > li` 이든 같고, 그래서 "래퍼를 버린다" · "값이
있는 것만" 같은 규칙이 **코드가 아니라 셀렉터**에 적힌다. soupsieve 가
`:has()` · `:not()` · `:empty` 를 지원한다(실측: `table` 17 →
`table:not(:has(table))` 16 → `:has(td:not(:empty))` 까지 12). 그 규칙을
코드에 넣으면 다음 페이지가 `dd` 나 `span` 을 쓸 때 또 분기가 생긴다.

`drop_empty` 라는 키를 두려 했는데 돌려 보고 뺐다. 텍스트 기준으로는 빈 섹션이
걸러지지 않는다 -- 값이 없어도 `<th>` 라벨과 `<caption>` 이 텍스트다. "값 있는
칸이 있나" 는 마크업마다 다른 판정이라(여기선 `td`, 다른 곳은 `dd`) 셀렉터에
적는 것이 맞다. 그래서 키는 `blocks` 와 `label` 둘이다.

`block_index` 는 **버리기 전에** 매긴다. 다만 셀렉터가 거르면 그 번호가
종목마다 다른 섹션을 가리킨다(이 채권은 주식관련사채·유동화 섹션이 비어
빠지고, 그게 있는 채권은 남는다). 그래서 `label` 이 있다 -- 번호가 아니라
이름으로 섹션을 집는다. 번호를 안정적으로 두려면 거르지 말고 16개를 전부
담아야 하는데 그쪽은 빈 섹션 마크업도 저장한다(12블록 10,836B vs 16블록
15,364B).

실행 결과:

```
ISIN_BOND_INFO: 12 block(s) from 'table:not(:has(table)):has(td:not(:empty))'
ISIN_BOND_INFO: saved 12 record(s) into api_rst
api_job_log: SUCCESS · record_count 12 · duration 1,133ms

v_isin_bond_info  ->  0 발행기관코드 1080자 · 1 특이채권유형 616 · 8 CFI 394 …
                      12행 7,700자
```

**`html` 은 반드시 `RETURNING CLOB`.** `VARCHAR2(4000)` 으로 두면 4000자를
넘는 순간 에러 없이 NULL 이다(`JSON_VALUE` 의 기본이 NULL ON ERROR -- 실측:
`ERROR ON ERROR` 를 붙이면 ORA-61723). 같은 함정이 생성기에도 있었다 --
`gen_ddl._json_path` 가 VARCHAR2·숫자가 아닌 타입을 전부 `VARCHAR2(4000)` 으로
떨어뜨렸다. 지금 생성되는 뷰 8개에 CLOB 이 없어 아무것도 안 깨지고 있었지만
고쳤다.

그 뷰(`08_scrap_block_views.sql`)는 **생성물이 아니라 손으로 적은 것**이다.
04 는 모델에서 컬럼을 뽑는데, 그건 "언제든 타입 테이블로 승격할 수 있는" 행에
해당하는 이야기다. `html_block` 행은 그 종류가 아니다 -- 저장하는 것이 마크업
자체라 승격할 타입 테이블이 없고, 컬럼 셋도 이 모드가 항상 만드는
셋(`block_index`·`label`·`html`) + `row_stamp_json` 으로 고정이다.

빌더는 `ISIN_BOND_INFO_TEST` **비활성** 하나뿐이다. 샘플 ISIN 한 건으로 잡
하나를 만든다. 운영 축은 정해지지 않았다 -- `mst_bond` 31,876건 전부면
한 건 9KB 로 약 290MB 다.

#### `srchPopup` 번호가 곧 유형이다 (2026-11-25)

`method=srchPopup3` 은 **채권 화면**이다. 번호별로 받아 봤다:

| popup | 무엇 | 주식/ETF ISIN | 채권 ISIN |
|---|---|---|---|
| 1 | 빈 화면 (925B, `th` 없음) | 블록 1 · 쌍 0 | — |
| **2** | **주식·ETF 상세** | 블록 1 · **28쌍** | — |
| **3** | **채권 상세** | 블록 4 · 값 4개(쓰레기) | 블록 13 · 쌍 100+ |
| 4 | 수익증권으로 보임(`신탁원본액` 라벨) | 블록 1 · 값 0 | — |

**마크업 모양은 같다** -- 전부 `<table>` 안의 `th`/`td` 쌍이라 `html_block` 과
그 셀렉터, `XMLTABLE` 추출, 짝짓기 규칙 둘이 그대로 통한다. 다른 것은 블록
수(채권 13, 주식 1)와 **어느 유형에 어느 번호를 부르는가**다.

**함정: 유형이 틀려도 HTTP 200 이고 블록이 남는다.** 주식 ISIN 을 popup3 에
넣으면 0건이 아니라 4블록이 저장된다:

```
블록0: td 18개 중 값 1개 -> ('채권분류', '()')
블록1: ('Call 신청종료일', '데이터가 없습니다.')
블록2: ('Put 신청종료일',  '데이터가 없습니다.')
블록3: ('거치기간',        '데이터가 없습니다.')
```

`empty_is_pending` 도 `SourceEmpty` 도 안 걸리고 `record_count 4` 로 성공
처리된다 -- 쓰레기가 조용히 쌓인다. CSS 로는 "값 있는 `td` 가 2개 이상" 을 쓸
수 없으니 셀렉터로는 못 막는다.

**그래서 축을 유형으로 가른다.** 마스터 표가 이미 그 일을 하고 있다 -- 실측으로
`mst_bond`/`mst_stock`/`mst_etf` 사이 ISIN 겹침이 **0**이고 ISIN 3번째 글자도
갈린다.

```
mst_bond   1·2·3·6·B·C·Z   31,876
mst_stock  0·7·8·G          1,251
mst_etf    7                1,201
```

빌더 축이 `mst_bond` 인 것이 곧 그 분리이고, 공짜다.

#### 소요 시간 (실측)

서로 다른 ISIN 8건을 연달아 돌려 정상 상태 주기를 쟀다.

```
잡 사이 주기 366 · 440 · 589 · 470 · 563 · 537 · 411 ms
중앙값 470ms  ->  31,876건에 4.16시간
```

거의 전부 **페이싱 대기**다. 자체 작업은 300~400ms 인데
`_HOST_INTERVALS['isin.krx.co.kr']` 이 0.5s 라 그쪽이 바닥이다.

```
요청 지연  중앙값 100ms (49~192), 응답 23.8KB
파싱       25ms
DB         150~250ms (삭제 + 13행 + 로그 + 커밋)
페이싱     500ms   <- 지배 항
```

코드를 빨리 만들어도 줄지 않는다. 0.5s 는 seibro 와 같은 근거로 정한 값이고
(메터링 API 가 아닌 웹 화면), `data.krx.co.kr` 가 1분 115건 뒤 막힌 전례가
있다.

그래서 KSD 채권 상세 이행 9개 빌더와 같은 모양으로 **하루 몫**을 둔다 --
2,000건/일이면 약 16분이고 31,876건에 16일이다. `NOT EXISTS` 가 없으면 ROWNUM
이 매일 같은 앞 2,000개를 돌려주고 생성이 전부 중복으로 걸러져 새 잡이 하나도
안 생긴다. 잡 생성 자체는 2,000건에 46~50초다.

바꾸려면 ROWNUM 의 수만 고친다: 800 이면 40일에 6분, 자르지 않으면 한 밤에
4.2시간이고 그것은 `daily_batch2` 의 다른 작업을 뒤로 밀어낸다.

#### v2 에서 정한 설정은 어디 적히나

`09_scrap_api_config.sql`. 이행 스크립트가 v1 에 없는 설정을 다루지 않기로
했으므로(아래), v2 에서 새로 정한 `api_mst`/`api_job_builder` 행은 git 에 남을
곳이 없어진다. 그 파일이 그 자리이고 MERGE 라 여러 번 돌려도 같다.

검증: 두 행을 지우고 그 파일만으로 다시 만들어 필드별로 비교했다 -- `note`
문구와 축 쿼리의 줄바꿈만 다르고(파일은 한 줄이다 -- 한 줄 문자열에 `--` 주석을
넣으면 뒤가 전부 주석이 된다) 동작은 같다. 복원한 축이 여전히 채권만 돌려주는
것까지 확인했다.

#### 이행 스크립트는 이제 되돌려 쓰지 않는다

이 설정은 v1 에 없다. `--apply` 는 v1 에서 두 표를 다시 채우므로 그대로 돌리면
`ISIN_BOND_INFO` 와 그 설정이 사라지고 잡 15,071행도 같이 지워진다. 그래서
**v1 에 없는 api_id 가 v2 에 있으면 멈추게** 했다:

```
v2 가 v1 에서 갈라져 있다 -- v1 에 없는 api_id 가 v2 에 1개 있다:
  ISIN_BOND_INFO
... 정말 v1 상태로 되돌리려면 --force 를 준다
```

override 표로 풀지 않았다. 그러면 이 스크립트가 설정 관리 도구가 되고, 같은
설정이 두 곳(스크립트와 DB)에 살면서 어느 쪽이 정본인지 흐려진다. 이행
스크립트는 이행만 한다.

#### 사람이 정할 것

**관측 시각을 찍는데 버킷이 없는 빌더 셋.** v1 에서도 절삭이 no-op 이었고,
동작을 바꾸지 않으려고 NULL 로 뒀다. 단위를 줄지는 결정 사항이다.

```
FO_IDX_CODE_MST   schedule_tag=daily_early
KIS_ETF_PDF       schedule_tag=daily_batch2
KSD_ISIN_ALL      schedule_tag=daily_batch1
```

셋 다 하루 한 번 도는 행이라 `1d` 가 자연스럽지만, 그러면 같은 날 두 번
돌렸을 때 두 실행의 스탬프가 같아진다 -- `save_mode` 와 함께 봐야 한다.

### 8.1 이중 운행 비교

`SCRAP_TARGET=both`가 하는 일은 같은 작업을 양쪽에 적재하고 **행 수와 값을 비교**하는 것이다.

```
비교 대상   같은 (api_id, 날짜) 의 행 수
            같은 키의 컬럼 값
허용 차이   snapshot_column 의 초 단위 (실행 시각이 다르므로)
```

이 단계를 건너뛰면 변환 스크립트의 오류가 **조용한 데이터 손실**로 남는다. 설정 변환은 테스트가 없는 종류의 변경이라, 비교가 유일한 검증이다.

---

## 9. 하지 않는 것 · 위험

**안 하는 것**

- 기존 DB 마이그레이션 — v1은 그대로 두고 v2를 새로 세운다. 과거 데이터 이전은 별도 결정
- 테이블 이름 변경 (`api_mst` 등) — 바꿔서 얻는 것이 없고 문서·프로시저·스크립트가 전부 따라와야 한다
- 엔진 동작 변경 — 이름과 구조만 바꾼다. 7.1은 예외이고 그래서 분리했다
- `save_mode`/`behavior_json`/`pagination_json` — 지금 이름이 하는 일을 정확히 말한다

**위험**

| | 무엇 | 완화 |
|---|---|---|
| 高 | 설정 변환 스크립트의 조용한 오류 | 8.1 이중 운행 비교. 건너뛰면 안 된다 |
| 高 | 7.1 (날짜 분리) 이 멱등성을 깨뜨림 | 별도 결정으로 분리. 결과 테이블 삭제 조건을 `(job_id, target_date)`로 바꾸는 것이 전제 |
| 中 | synonym 권한 누락 → `ORA-01031` | GRANT를 synonym보다 먼저. 4단계 끝에 전 테이블 SELECT 점검 |
| 中 | 두 DB 동시 운행 중 사전 데이터 불일치 | REF는 v2 한쪽만 정본으로 두고 v1은 읽기만 |
| 低 | `user_tables` 가정이 깨짐 | `check-sql` 등 점검 코드를 `all_tables`로 |

**되돌리기**: 10단계 전까지는 v1이 계속 돌고 있으므로 `SCRAP_TARGET=v1`로 끝난다. 10단계 이후는 v1을 읽기 전용으로 남겨 두되, 설정이 v2 형태로 바뀐 뒤라 **되돌리려면 역변환이 필요하다** — 그래서 변환 스크립트는 양방향으로 쓰는 것이 안전하다.

---

## 9.5 결정된 것 (2026-11-24)

### 접속 대상

```
v2 DB    Oracle AI Database 26ai (23.26.4.x)   v1 과 다른 ADB 인스턴스
SCRAP    CONNECT RESOURCE
REF      CONNECT RESOURCE
```

> 인스턴스 이름과 계정 이름은 여기 적지 않는다. 이 저장소는 공개이고, 문서의
> 값이 그 이름에 달려 있지도 않다. 실제 값은 `.env` 의 `DB_*` 에 있다 (2.2).

둘이 **같은 인스턴스 안**이라 교차 권한은 `GRANT` + synonym 으로 끝난다. DB
링크가 필요한 곳은 v1↔v2 뿐이고 v2 는 새로 세우므로 건널 일이 없다.

권한은 필요한 것이 다 있다 — `CREATE TABLE` · `SEQUENCE` · `PROCEDURE` ·
`VIEW` · **`SYNONYM`**. synonym 은 `RESOURCE` 롤에 없는 경우가 많아 5절이
막힐 수 있었는데 둘 다 갖고 있다.

`BOOLEAN` · `JSON` · `XMLTYPE` 네이티브 컬럼도 확인했다.

> 두 계정에 기존 테이블이 315개/316개 있었고, REF 쪽에는 pyscrap 과 같은
> 일을 하는 다른 구조물(`API_JOB_MST`, `BROWSER_MST` 등)이 있었다. 전부
> 지우고 새로 만드는 것으로 정해졌다.

### 결과 테이블: 두 층

위층(타입 테이블)에 **운용사별 표를 뺀 나머지**를 두고, 그 8개만 `api_rst` +
뷰로 내린다. 운용사가 늘 때마다 모델을 더하고 배포해야 했던 것이 그 표들이다.

```
위층  34개 표 594컬럼        (amc_etf_pdf 포함 -- 5개 운용사가 이미 공용으로 쓴다)
아래층 ace/kodex/plus/sol × (목록, 구성종목) 8개  ->  api_rst + v_*
```

측정한 대가 (20만 행 합성, 26ai):

| 접근 | JSON 뷰 / 타입 테이블 |
|---|---|
| 전체 추출 (분석 DB 로 반출) | **1.0x** — 전송이 지배해 차이가 묻힌다 |
| 하루치 (함수기반 인덱스 있을 때) | **1.1x** |
| 전수 집계 | **2.5x** |
| 저장 | **3.9x** (58 → 225 B/행) |

분석 쪽이 Parquet 을 읽으므로 JSON 전개는 하루 한 번만 치른다. 비용이 드러나는
것은 수집 DB 에 직접 집계를 돌릴 때뿐이고, 행 수의 80%가 위층에 있다.

타입 테이블 위의 단순 뷰는 **0.96~0.98x** — 옵티마이저가 뷰를 병합해서 비용이
없다. 그래서 뷰 이름을 최종 표 이름과 같게 두면 나중에 승격해도 질의가 바뀌지
않는다.

### api_rst 파티션

```sql
PARTITION BY LIST (api_id) AUTOMATIC (PARTITION p_seed VALUES ('__SEED__'))
```

새 `api_id` 가 들어오면 파티션이 저절로 생긴다. 뷰의 `WHERE api_id = ...` 가
프루닝으로 한 파티션만 읽는 것을 플랜으로 확인했다(`part 2..2`). 이것이 없으면
43개 소스가 한 표에 섞여 한 소스를 읽을 때마다 다른 소스 행을 골라내야 한다.

인덱스는 `job_id` 를 GLOBAL, `target_date` 와 JSON 경로를 LOCAL 로 둔다 --
`overwrite` 의 선행 삭제가 `job_id` 로만 지우기 때문이다.

### api_rst 는 한 행을 한 컬럼에 담는다

v1 은 한 행을 둘로 나눠 담았다.

```
key_params_json = {"fundcd": "K55101DU0363", "std_dt": "20261124"}
result_json     = {"wg": ..., "rank": ..., "sec_nm": ...}
```

`ace_etf_pdf` · `kodex_etf_pdf` · `plus_etf_pdf` · `sol_etf_pdf` 네 표가 각각
두 컬럼을 그렇게 받는다. 그래서 **처음 생성한 뷰는 그 8개 컬럼을 NULL 로
돌려주고 있었다** -- `result_json` 만 읽었기 때문이다. (`key_params_json` 이라는
이름을 v2 에서도 그대로 쓸 수 있는지 묻다가 드러났다.)

v2 는 합쳐서 한 컬럼에 넣는다.

```
api_rst : id · api_id · job_id · result_json · target_date · updated_at
```

`result_json` 이 타입 테이블에 저장될 행과 같은 모양이 된다. 뷰는
`'$.<컬럼명>'` 하나로 끝나고, 승격할 때도 모양이 이미 같다. 나눠 담아 아끼던
공간은 행당 키 두세 개뿐이고, 잃던 것은 뷰의 단순함과 두 층 사이의 호환이었다.

`key_params_json` 컬럼 자체가 없어지므로 이름을 고민할 일도 없어진다. 담고
있던 정보는 `api_job.params_json`(전체 파라미터)과 `result_json`(그중 스탬프된
것)에 이미 있다.

### 이름 (사용자 결정)

| 초안 | 확정 | 이유 |
|---|---|---|
| `transport` | `protocol` | 읽기 쉬운 쪽. 엄밀히는 BROWSER 행도 HTTP 로 말하므로 `engine` 이 더 literal 하지만 `app.db.engine` 과 헷갈린다 |
| `param_axes_json` | `cartesian_param_json` | "축"은 구현 용어. 곱해진다는 사실을 이름이 말한다 |
| `snapshot_grain` | `snapshot_unit` | `grain` 대신. 구현은 `_floor_datetime` 이라 `snapshot_floor` 가 정확하지만 명사로 어색하다 |
| `last_result` | `engine_note` | `note` 와 같은 명사에 주인만 다르게. `engine_log` 는 `api_job_log` 와 겹쳐 피했다 |
| `source` | `source_name` | 아래 |

**`source` → `source_name` (2026-11-25).** 예약어 때문은 **아니다**. `v$reserved_words`
가 이 계정에서 안 보여 직접 써 봤고, Oracle 은 `source` 를 컬럼 이름으로 전
자리에서 받는다.

```
이름             CREATE     SELECT     WHERE      ORDER BY   VIEW
source         ok         ok         ok         ok         ok
domain         ok         ok         ok         ok         ok
```

바꾼 이유는 **너무 일반적인 말**이라는 것이다. 이 저장소에서 "source" 가
가리키는 것이 여럿이다 -- `pagination_json` 의 `"source"` 키는 응답의 어느
자리를 보라는 뜻이고(`{"source": "header:tr_cont"}`), 결과 모델 주석의
source 는 그 API 자체를 가리킨다.

처음 제안은 `source_host` 였는데 쓰지 않았다. **값이 호스트가 아니다** -- 기관
이고, `AMC` 하나가 운용사 사이트 아홉 곳이며 `KRX` 는 `data.krx.co.kr` ·
`isin.krx.co.kr` · 오픈API 셋이다. 그리고 이 프로젝트에는 진짜 호스트 개념이
따로 있다: `execution._HostIndex` 가 `api_url` 에서 뽑아 호출 간격과 차단기의
단위로 쓴다. 같은 이름을 두 뜻에 쓰면 그 둘을 구별해 온 이유가 흐려진다.

`src_code.source` 도 같이 바꿨다. 같은 어휘(`KRX`·`KSD`·`KIS`·`AMC`)를 담는
컬럼이고, 한 개념에 이름이 둘이면 조인할 때마다 어느 쪽인지 생각해야 한다.
`src_code` 를 읽는 REF 의 `v_code_unmapped` 는 그 때문에 다시 만들었다.

### 전제 둘 (엔진 변경) -- 완료 2026-11-25

범용 저장 경로(`run_and_save` 의 generic 분기) 한 곳에 둘이 필요했다.

1. **레코드 키를 소문자로 접기.** 원본 키가 그대로 들어가서 JSON 경로가
   소스마다 달랐다(ACE 는 `std_DT`, KRX 는 전부 대문자). 타입 테이블은
   `VendorRecordBase` 가 이미 같은 접기를 한다.
2. **`{**key_params, **record}` 로 합쳐 넣기.** 타입 경로는 이미 그렇게 한다.

뷰를 모델에서 생성할 수 있는 것이 이 둘 위에 있다.

측정으로 확인했다. 고치기 전 `ACE_ETF_PDF` 를 돌리면 저장된 JSON 이
`{"std_DT":…,"sec_NM":…,"jm_KSC_CD":…,"wg":23.19}` 라서 `v_ace_etf_pdf` 가
**`wg` 하나만** 돌려주고(우연히 소문자였다) 나머지를 NULL 로 줬다. 고친 뒤
9컬럼 전부 NULL 0개.

### 검증기 정규화는 뷰가 한다 (`_NORMALIZE`)

위 둘을 고친 직후 남은 차이가 하나 있었다. `std_dt` 가 아래층에서는
`2026-11-24`, 타입 테이블에서는 `20261124` 였다. `ace_etf_pdf` 가
`EtfPdfDateMixin` 을 쓰는데(운용사마다 표기가 달라 숫자만 남긴다), **범용
경로는 그 믹스인을 타지 않는다.**

쓰는 쪽에서 고칠 수는 없다. 그 API 의 `record_map_json` 은
`{"pdfList":"api_rst"}` 이고, 엔진이 손에 쥐는 모델은 `ApiRst` 다 -- `AceEtfPdf`
라는 이름은 엔진에 오지 않는다. 쓰기 때 정규화하려면 엔진이 `api_id` 로 모델을
찾아야 하는데, 그러면 "새 표가 생겨도 models.py 를 안 고친다" 는 아래층의
존재 이유가 사라진다.

그래서 읽는 쪽에 둔다. `gen_ddl.py` 의 `_NORMALIZE` 가 (믹스인, 컬럼) → 식
이고, `_view_column` 이 그 식을 `JSON_VALUE` 위에 씌워 뷰를 만든다. 모델은
**설명**으로만 쓰이고 그 설명을 강제하는 곳이 뷰다. 검증 결과
`v_ace_etf_pdf.std_dt` = `20261124`.

대가 하나: 저장된 JSON 에는 `2026-11-24` 가 남는다. `api_rst` 를 뷰 없이 직접
읽으면 원본 표기를 본다. 그리고 **함수기반 인덱스를 뷰에 적힌 식 그대로 걸어야
한다** -- 맨 `JSON_VALUE` 에 건 인덱스는 `REGEXP_REPLACE` 가 씌워진 조건에
쓰이지 않는다. 생성되는 `04_scrap_views.sql` 꼬리에 그 예를 적어 뒀다.

### 경계를 어디로 긋는가

"job_id 가 없으면 사전 데이터" 가 아니다. 세 가지다.

| | 무엇 | 계정 |
|---|---|---|
| 결과 | 엔진이 쓴다. job_id 가 있다 | SCRAP |
| **수집 상태** | 엔진이 아니라 프로시저가 쓰지만 **우리 수집 이력**이다 | **SCRAP** |
| 사전 | 종목·만기·지수가 무엇인가 | REF |

`ksd_bond_no_detail` 이 가운데다. "세이브로에 물어봤는데 상세가 없었다" 를
ISIN 당 한 줄 적는 971행짜리 표인데, 종목의 성질이 아니라 우리가 긁어본
결과다(그래서 `mst_bond` 의 컬럼도 아니다). 읽는 쪽도 SCRAP 의 빌더 열 곳이다.

그래서 그 표를 쓰는 `sp_ksd_no_detail_mark` 도 함께 SCRAP 에 둔다. 그러면 그것이
읽는 `ksd_bond_info` 와 `api_job_log` 가 같은 계정이 되어 **교차 synonym 이 넷
줄었다** -- 경계는 "누가 쓰는가" 로 긋는 것이 읽는 쪽으로 긋는 것보다 적게 샌다.

반대 방향의 예가 `krx_index_mst` 다. 이름이 `krx_` 로 시작해 수집물처럼 보이지만
**쓰는 곳이 없다** -- 거래소가 지수를 추가·폐지할 때만 손으로 고치는 168행이고,
거기서 거래소 어휘와 우리 `mv_id` 가 만난다. `user_etf` 와 같은 이유로 REF 다:
다시 만들 수 없다.

### REF 와 교차 권한

사전 10개 표는 전부 **자연키 PK** 라 identity 가 하나도 없다 -- 결과 테이블과
달리 행이 어디서 왔는지가 아니라 무엇인지로 식별된다. 시퀀스가 생기지 않는다.

프로시저 6개가 REF 소유다. **정의자 권한**(기본값)으로 두어야 본문이 REF 의
권한으로 돌아 REF 표에 쓸 수 있고, SCRAP 은 `EXECUTE` 만 받는다 -- 사전
데이터에 쓰는 권한이 수집 계정에 가지 않는다. 확인했다:

```
SCRAP 에서 INSERT INTO mst_etf  ->  ORA-41900: missing INSERT privilege
SCRAP 에서 sp_mst_etf_sync 호출 ->  ok
```

권한은 양방향이고 **GRANT 가 synonym 보다 먼저**다. 순서를 바꾸면 ORA-00980
(번역 실패)이 아니라 ORA-01031 이 나서 원인이 덜 보인다.

| | 무엇을 | 몇 개 |
|---|---|---|
| REF → SCRAP | 사전 10개 표 SELECT + 프로시저 6개 EXECUTE | synonym 16 |
| SCRAP → REF | 동기화가 읽는 결과 8개 + 뷰 4개 | synonym 12 |

사전 10개 중 빌더 매크로가 실제로 읽는 것은 셋이다(`mst_fuopt` 43개 빌더,
`mst_etf` 11, `mst_bond` 10). 그래도 10개를 다 연다 --
전부 수집 쪽이 읽도록 만든 사전 데이터이고 읽기라 위험이 같다. 지켜야 할 것은
쓰기가 없다는 것이고 그것은 유지된다.

### 프로시저를 고쳐야 했던 세 곳

**`sp_mst_etf_sync`** -- `ace_etf`/`kodex_etf`/`plus_etf`/`sol_etf` 다섯 군데를
`v_*` 로. 컬럼 이름과 타입이 그대로라 `FROM` 절의 이름만 바뀐다.

**`sp_ksd_no_detail_mark`** -- v1 은 `api_job.description` 을 문자열로 훑었다
(`LIKE 'ksd_bond_info: 0%'`). 적재 건수가 거기 문자열로 들어갔기 때문이고, 그
칸은 사람이 적는 설명과 같은 자리였다. v2 는 `api_job_log.record_count = 0` 을
숫자로 본다. 덤으로 `is_active = 0` 조건이 필요 없어졌다 -- 손으로 비활성화한
잡에는 그런 로그가 없으므로 섞이지 않는다.

**`sp_stock_index_his_sync`** -- 둘이 걸렸다.

1. `DELETE FROM kis_index_daily` 를 하고 있었다. SCRAP 소유 결과 테이블이라
   REF 에 DELETE 를 주면 사전 데이터를 만드는 쪽이 수집물을 지울 수 있게 된다.
   `sp_kis_index_daily_dedup`(SCRAP 소유)으로 떼어냈다 --
   `sp_krx_stock_base_dedup` 이라는 선례가 이미 있다. 배치는 sync 앞에 부른다.
2. `ALTER SESSION DISABLE PARALLEL DML` 이 **ORA-12841 로 죽었다** -- 트랜잭션이
   열려 있으면 세션 병렬 상태를 바꿀 수 없고, v2 에서 앞에 dedup 이 붙으면서
   실제로 그렇게 됐다. 세 MERGE 에 `/*+ NO_PARALLEL */` 을 붙이고 세션 설정과
   예외 핸들러를 걷어냈다. 저장소의 다른 여덟 곳이 이미 같은 방식이다.

### 산출물

```
scripts/sql/v2/
  01_scrap_config.sql      부기 4개 표 (v2 컬럼). 손으로 적는다
  02_scrap_results.sql     타입 34개 + 수집 상태 1개 = 597컬럼. 생성물
  03_scrap_api_rst.sql     파티션 api_rst. 손으로 적는다
  04_scrap_views.sql       뷰 8개. 생성물
  05_ref_tables.sql        사전 10개 표 118컬럼. 생성물
  06_grants_synonyms.sql   교차 GRANT + synonym + 점검 블록
  gen_ddl.py               02 · 04 · 05 를 models.py 에서 뽑는다

scripts/sql/
  sp_kis_index_daily_dedup.sql   새로 (위 참고)
  sp_mst_etf_sync.sql            v_* 로
  sp_ksd_no_detail_mark.sql      record_count 로
  sp_stock_index_his_sync.sql    dedup 분리 + NO_PARALLEL
```

계정 이름은 `06` 에도 적지 않는다. SQLcl 치환 변수(`&scrap_user` / `&ref_user`)로
두고 돌릴 때 채운다 -- 이 저장소는 공개다.

### 사전 데이터 (v1 에서 옮김, 2026-11-24)

10개 표 158,692행. v1 은 읽기만 했고, 컬럼 이름으로 맞춰 5,000행씩 넣었다 --
`updated_at` 까지 그대로 옮겨 이력을 보존했다. `mst_index` 는 양쪽 컬럼 순서가
달라 v2 순서로 SELECT/INSERT 목록을 맞췄다.

```
mst_fuopt       113,463      mst_etf          1,457
mst_bond         31,876      mst_stock        1,251
stock_index_his   9,307      meta_maturity      653
user_etf            511      krx_index_mst      168
meta_fuopt_info       4      mst_index            2
```

건수만 보지 않았다. 각 표의 키·주요 컬럼을 이어 `ORA_HASH` 합으로 비교하고,
`mst_stock` 과 `mst_etf` 는 `updated_at` 을 뺀 **전 컬럼**으로 다시 재서 전부
일치했다. PK 위반이 한 건도 없었다는 것 자체가 키 중복이 없었다는 뜻이다.

> **`mst_etf.amc` 는 운용사 정식명이다** -- `'미래에셋자산운용'`, `'삼성자산운용'`.
> `'TIGER'`/`'KODEX'` 같은 약칭이던 시절이 있어서, 그 기억으로 매크로를 쓰면
> 조용히 0건이 나온다(실제로 검증 중에 그렇게 헷갈렸다). 29개 값이 있고
> `amc_etf_cd` 가 채워진 것은 1,044개다. 브랜드 약칭은 `prod_snm` 쪽에 있다.

동기화 8개는 돌지만 아직 0건이다 -- SCRAP 의 결과 테이블이 비어 있어서다.
`v_k2i_atm` 도 같은 이유로 0행이다.

### 올라가 있는 것 (2026-11-24)

지우지 않고 남겼다. 계획의 3~4 단계가 끝난 상태다.

```
SCRAP   테이블 45 · 뷰 8 · 파티션 1 · 프로시저 3 · synonym 16
REF     테이블 10 · MV 1 · 프로시저 6 · synonym 12
무효 객체 없음. 동기화 8개 전부 SCRAP 에서 호출돼 0건으로 정상 종료.
```

> REF 의 시퀀스 183개와 양쪽 휴지통(943 / 4,559)은 v2 와 무관한 잔재다. 옛 표를
> PURGE 없이 드롭하면 identity 시퀀스가 휴지통의 표를 따라 남는다.
> `PURGE RECYCLEBIN` 으로 함께 사라지지만 되돌릴 수 없으므로 사용자 확인 대기.

### SCRAP 프로시저 (2026-11-24)

함수 1개 + 프로시저 12개 전부 VALID, 오류 0건. 셋만 고쳐야 했다.

**`sp_retire_expired_jobs`** -- 컬럼 셋이 다 바뀌었고, 둘은 **단순해졌다**.

```
j.execution_cycle IN ('3m_call','3m_put')              ->  j.schedule_tag IN (…)
JSON_VALUE(response_parse_json,'$.empty_is_pending')   ->  empty_is_pending = TRUE
JSON_EXISTS(key_params_list,'$[*]?(@ == "NOW")')
  OR JSON_EXISTS(key_params_list,'$[*]."NOW"')         ->  m.is_repeated = TRUE
```

마지막 것이 4.1 의 값이 드러나는 자리다. v1 은 `"NOW"` 를 **두 가지 JSON 경로**로
찾아야 했고(문자열 항목과 `{"NOW": …}` 딕셔너리 항목), 그것이 파이썬의
`is_repeated_api` 와 같은 판정인지 11개 api 에 대해 손으로 대조해야 했다. 지금은
컬럼 하나다.

**`empty_is_pending` 은 `options_json` 으로 갔다.** `response_parse_json` 을 셋으로
쪼개면서 갈 곳이 없었다 -- 쓰는 다섯 행(`KRX_ETF` · `KRX_OPTION_DAILY` ·
`KRX_INDEX_KOSPI` · `KRX_STOCK_INFO` · `KRX_STOCK_BASE_INFO`)의 그 컬럼에는
**그 키 하나뿐**이고 파싱 명세가 전혀 없다. 자세한 것은 6.6.

### 6.6 `options_json` -- 늘어나는 스위치

이런 스위치는 계속 생긴다. 컬럼마다 올리면 표가 자라고, Oracle 은 컬럼 순서를
바꿀 수 없어 (`updated_at` 이 마지막이어야 하는 규칙 때문에) 플래그마다 표를
다시 만들어야 한다. 그래서 JSON 한 칸에 모은다.

```
options_json JSON          -- 제약 없음
empty_is_pending           -- 가상 컬럼 (SQL 이 거르는 것만)
```

**제약을 걸지 않는 것이 요점이다.** JSON 스키마(`IS JSON VALIDATE`,
`additionalProperties: false`)로 오타를 막는 길을 재봤는데, 그러면 새 스위치마다
`properties` 에 선언해야 해서 `ALTER CONSTRAINT` 가 필요하다 -- 컬럼을 더하는
것과 같은 비용이고, **자유롭게 더할 수 있다는 성질을 포기하면서 오타 검사만 사는
거래**가 된다. 애초에 원한 것이 그 자유였다.

> 덧붙여, 그 제약은 이 표에서 아예 평가되지 않았다. `ENABLED VALIDATED` 이고
> 조건을 저장된 행에 직접 걸면 `FAIL` 인데 INSERT 가 통과한다. 같은 표의
> `ck_api_mst_snapshot` 은 정상 동작하고(ORA-02290 확인), 같은 조건을 빈 표에
> 옮기면 거부한다. 가상 컬럼 · XMLTYPE · JSON 컬럼 수 · 다른 CHECK · 스키마의
> 줄바꿈을 각각 재현해 봤으나 모두 정상이어서 원인을 찾지 못했다. 쓰지 않기로
> 정해졌으니 더 파지 않았다.

오타는 **엔진이 알려준다** -- 아는 스위치 이름과 대조해 모르는 키를 경고로
남긴다. 막지 않고 알리는 쪽이라 스위치를 더하는 자유가 유지되고, 첫 실행 로그에
드러난다.

**무엇이 여기 들어가는가** -- 세 질문에 다 '예' 여야 한다.

| | |
|---|---|
| 1 | 다른 키 없이 **혼자 뜻이 있는가?** `empty_is_pending: true` 는 있다. `delimiter: '\|'` 는 `fields` 나 `has_header` 없이는 없다 -- 그건 `parse_json` 의 묶음 명세다 |
| 2 | 읽는 곳이 **특정 메커니즘이 아닌가?** 파서·응답 검사·파일 적재가 읽는 것은 그 컴포넌트의 컬럼으로 간다. 여기는 잡 수명처럼 어디에도 안 속하는 것 |
| 3 | 키 목록이 **미리 정해지지 않는가?** 나머지 셋은 그 메커니즘이 아는 키가 고정돼 문서에 적을 수 있다. 여기는 열린 집합이다 |

그래서 `session_json`·`parse_json`·`staging_json` 의 내용은 **옮겨오지 않는다.**
"로직을 고른다" 는 기준으로는 `parse_json.delimiter` 도 해당되지만(구분자 파서를
고른다), 그 기준은 너무 넓어 표를 다시 JSON 한 덩어리로 되돌린다. 실제 선은
**"누가 한 벌로 읽는가"** 다 -- 읽는 곳을 코드에서 확인했다.

```
login · logged_out · blocked · expect  ->  scrapers 의 응답 검사
group · name                           ->  base 의 파일 적재
delimiter · has_header · fields …       ->  dynamic 의 파서
empty_is_pending                       ->  execution.run_job 의 잡 수명 판정
```

> `blocked`(3개 API)는 세션과 직접 관계가 없는데 `session_json` 에 둔다.
> `logged_out`·`expect` 와 **같은 경로가 읽고** 같은 방식으로 판정한다 -- 본문에서
> 문자열을 찾는다. 이름이 조금 넓게 쓰이는 것을 받아들이는 편이, 넷을 둘로 쪼개
> 읽는 곳을 둘로 만드는 것보다 낫다.

**지금 아는 스위치** (`BaseScraper.KNOWN_OPTIONS`)

| 키 | 하는 일 |
|---|---|
| `empty_is_pending` | 0행 성공을 "끝났다" 로 보지 않는다 |
| `min_interval_sec` | 이 API 의 요청 간격 **바닥값** |
| `page_delay_sec` | 페이지 사이 지연 |
| `timeout_sec` | 이 API 의 요청 타임아웃 (기본 30) |

`min_interval_sec` 가 호스트 간격을 **대체하지 않고 바닥만 올리는** 이유는
방향이다: API 쪽 값으로 **느려지는 것만** 허용한다.

`_HOST_INTERVALS` 의 값은 추측이 아니라 맞아 본 결과다 -- `data.krx.co.kr` 1.0s
는 1분 115건 뒤 에러페이지로 막힌 뒤에 정한 것이고, `samsungfund.com` 0.5s 는
0.2s 로 43개를 훑다가 22개가 깨진 뒤에 정한 것이다. 대체를 허용하면 설정 한
줄로 그 교훈을 되돌릴 수 있고, 되돌린 줄도 모른다. 그래서 `max(호스트, API)` 다.

> 처음에는 "같은 호스트의 두 API 가 각자 0.5s 면 호스트가 2 req/s 를 받는다" 고
> 적었는데 틀렸다. 슬롯이 호스트별로 하나(`_next_slot_at[host]`)이므로 API 가
> 몇 개든 연속 요청은 이미 간격만큼 떨어진다 -- 간격이 합산되지 않는다. 실행도
> 직렬이다(`run_cycle` 이 잡을 하나씩 돈다). 병렬이 생기는 것은 별개 프로세스가
> 겹칠 때뿐이고, 슬롯이 프로세스 단위라 그 경우는 애초에 서로 모른다.

```
호스트 기본 0.5s + 설정 없음    ->  0.50s
            + 0.1s (더 작다)   ->  0.50s   무시된다
            + 2.0s (더 크다)   ->  2.00s   올라간다
```

`timeout_sec` 가 최악을 묶는 유일한 손잡이다. httpx 의 timeout 은 connect·read
각각에 걸리므로 한 시도가 최대 2배이고 재시도가 3회라, 한 잡이 **6배까지** 간다
-- 기본값 30이면 3분이다. KB 가 2026-10-03 에 ETF 사이트를 옮기고 옛 호스트가
연결 안 되는 곳으로 302 를 돌려줬을 때 잡마다 정확히 그 3분을 먹었고, 10-12 밤
사이클은 852개를 들고 다섯 시간을 쓰고 성공 둘로 끝났다. 운용사 사이트처럼
빨라야 정상인 곳은 10초면 충분하다.

### 옮기지 않은 조정값

| | 지금 | 판단 |
|---|---|---|
| 호스트별 간격 | `_HOST_INTERVALS` 딕셔너리 + `REQUEST_MIN_INTERVAL_HOSTS` env JSON | **그대로.** 이미 두 층이고 배포 없이 덮을 수 있다. 값의 값어치는 주석에 적힌 측정 이력인데 표로 옮기면 그걸 잃고, 요청마다 읽으니 캐시 문제가 생긴다. 호스트 12개에 변경은 몇 달에 몇 번이다 |
| `max_retry` | `@retry(stop_after_attempt(3))` | **보류.** 데코레이터라 import 시점에 고정돼 있어, 호출별 `Retrying` 객체로 바꾸는 구조 변경이 필요하다. 값은 있다 -- metered API 에서 재시도는 쿼터를 쓴다. `timeout_sec` 가 최악의 6배 중 2배를 이미 묶으므로 급하지 않다 |
| `fail_streak_limit` | env 전역 (5) | **보류.** 차단기가 이미 api 별로 돈다. 임계값을 api 별로 두면 알려진 불안정 소스에 여유를 줄 수 있지만, 5가 보편적으로 무난하다 |
| `block_cooldown_sec` | env 전역 (600) | **API 별이 아니다.** 차단은 호스트 성질이고 창의 길이가 다르다(KRX 는 10분, SOL 은 세션당 건수). 호스트별 env JSON 으로 넓히는 것이 대칭이지만, 지금 막히는 호스트가 KRX 하나라 급하지 않다 |
| `BROWSER_TIMEOUT_MS` | env 전역 | **보류.** BROWSER 행이 아직 1개다 |

모르는 키는 **경고만** 한다. 그 경고가 두 몫을 하는데, 오타(`empty_is_pendign`)
뿐 아니라 **배포 어긋남**도 잡는다 -- 설정은 두 환경이 공유하는데 배포는
수동이라, "설정에는 스위치가 켜져 있는데 이 서버의 엔진은 그걸 모른다" 가
실제로 일어난다. 둘 다 증상이 같다: 아무 일도 일어나지 않고 오류도 없다.

수로 못 읽는 값도 경고한다. 이 스위치들은 틀려도 느려지거나 빨라지는 것뿐이고
오류가 안 나서, 조용히 무시하면 설정한 줄 알고 넘어간다.

**가상 컬럼을 두지 않는다.** 한때 `empty_is_pending` 을 그렇게 노출했는데
걷어냈다 -- 스위치 하나를 컬럼으로도 두면 어느 쪽이 정본인지 흐려지고(생성
컬럼이라 쓸 수도 없다), 스위치가 늘 때마다 ALTER 가 따라붙어 이 컬럼을 둔
이유가 줄어든다. SQL 쪽도 JSON 경로를 직접 읽는다:

```sql
WHERE JSON_VALUE(options_json, '$.empty_is_pending' RETURNING BOOLEAN) = TRUE
```

**쓰는 규칙** (측정으로 확인한 성질들)

| | |
|---|---|
| 스칼라 (불리언·숫자·문자열) | 가상 컬럼으로 노출 가능. 인덱스도 걸린다 |
| 배열 · 중첩 객체 | 담을 수는 있지만 가상 컬럼 불가(**ORA-54004**). 엔진이나 `JSON_QUERY` 로 읽는다 |
| 불리언 | 반드시 `true`. `JSON_VALUE` 가 LAX 라 `"true"`(문자열)도 TRUE 로 읽히지만 `1`(숫자)은 NULL 이 되어 비일관적이다 |
| 의미 | **키가 있으면 켠 것.** NULL 이 "키 없음"·"타입 불일치"·"false" 를 구별하지 못하므로 `false` 를 명시적 off 로 쓰지 않는다 |

SQL 이 거르는 스위치만 가상 컬럼으로 노출한다. `empty_is_pending` 은
`sp_retire_expired_jobs` 가 쓰므로 노출했고, 그래서 **그 프로시저는 한 줄도
바뀌지 않았다.** 적어만 두는 스위치는 비용이 0 이다.

`api_mst` 는 저장 26컬럼 + 가상 1이다.

**`fn_export_day_col` · `sp_run_export`** -- 운용사별 넷이 뷰가 됐다. `p_name` 은
**옛 이름 그대로** 두고 소스만 `p_query` 로 뷰를 가리킨다 -- `p_name` 이 버킷
프리픽스이고 외부 테이블 이름(`xt_ace_etf_pdf`)이기도 해서, 바꾸면 분석 쪽 경로가
끊긴다. `sp_export_parquet` 이 `p_query` 를 받으면 `p_name` 을 프리픽스로만 쓰는
성질이 이미 있어서 그대로 됐다.

### 고치지 않아도 됐던 것

`sp_archive_exported` · `sp_export_bulk` · `sp_export_parquet` 는 손대지 않는다.
세 곳이 `ALTER SESSION` 을 쓰지만 **`PARALLEL QUERY` 이고 `PARALLEL DML` 이
아니다.** 재보니 ORA-12841 은 DML 에만 걸린다.

```
트랜잭션 중  ALTER SESSION DISABLE PARALLEL QUERY  ->  ok
트랜잭션 중  ALTER SESSION DISABLE PARALLEL DML    ->  ORA-12841
```

그러니 고쳐야 했던 것은 `DML` 을 쓴 `sp_stock_index_his_sync` 하나뿐이었다.
(이 문서의 앞선 판에서 "세 곳도 여지가 같다" 고 적었던 것은 틀렸다.)

### 남은 것

| | 무엇 |
|---|---|
| `sp_create_external_tables` | MART 몫. 외부 테이블 5개가 아직 SCRAP 에 있다 |
| ~~`api_mst` 58행 · 빌더 95행 이행~~ | **완료 2026-11-25 (8.2)** |
| ~~v2 접속 정보를 `db_config` 로~~ | **완료 2026-11-25. 아래 2.4** |
| ~~`ISIN_BOND_INFO` 운영 축~~ | **완료. `mst_bond` 축, 하루 2,000건, 16일 (8.2)** |
| `ISIN_STOCK_INFO` (popup2) | 주식·ETF 상세. 축 2,708건 · 블록 1개씩이라 20분 내외 (8.2) |
| 수익증권 (popup4) | 샘플 ISIN 이 없어 확인 못 했다 (8.2) |
| ~~`sp_src_code_sync` 가 뷰도 훑기~~ | **완료 2026-11-25 (6.5)** |
| 반복 빌더 셋의 `snapshot_unit` | v1 동작 보존으로 NULL. 단위를 줄지 결정 (8.2) |

| ~~`sp_run_daily_batch2` · `sp_run_daily_early`~~ | **완료 2026-11-25 (8.3)** |
| `v_fuopt_price` | **DDL 이 저장소에 없다.** v1 에만 있고 v1 은 이제 닿지 않는다 -- `sp_run_export` 가 그 뷰를 부른다 |

---

## 10. 결정이 필요한 것

| | 질문 | 영향 |
|---|---|---|
| ~~1~~ | ~~새 DB는 별도 인스턴스인가~~ | **별도 인스턴스. 계정 둘은 그 안에 같이 있다** (9.5) |
| 2 | 7.1(api_job 날짜 분리)을 v2에 넣는가 | 가장 값이 크고 가장 위험하다. 2단계로 미룰 수 있다 |
| 3 | MART(분석) 계정을 이번에 가르는가 | 외부 테이블 5개 이전. 미루면 7.6이 남는다 |
| 4 | 과거 데이터를 v2로 옮기는가 | 안 옮기면 v1을 조회용으로 계속 둬야 한다 |
| 5 | 2단계(모델 분할)를 먼저 `dev`에 병합하는가 | v2 결정과 무관하게 이득이다 |
| 6 | `bf1m_*` 37개 이행 사이클은 끝났는가 | 끝났으면 v2로 안 옮기면 된다 |
| ~~7~~ | ~~코드 레지스트리를 넣는가~~ | **넣는다. 표 셋 + `sp_src_code_sync`. DDL 과 수확을 v1 실데이터로 검증했다 (6.5)** |
