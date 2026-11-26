# v2 인스턴스 배포

수집 인스턴스(Oracle Cloud compute)에 v2 를 올리는 절차. API 쪽(FastAPI
Cloud)은 다른 배포이고 여기 대상이 아니다.

전제: v2 DB 는 이미 서 있고 설정도 들어가 있다 -- `api_mst` 57행 ·
`api_job_builder` 95행 · 프로시저와 뷰 전부 (docs/SCHEMA-V2.md 8.2~8.9).
인스턴스에 필요한 것은 **코드 · 자격 · 타이머** 셋이다.

---

## 0. 먼저 푸시

인스턴스는 git 에서 받으므로, 로컬에만 있는 커밋은 배포되지 않는다.

```bash
git push origin feat/schema-v2
```

확인:

```bash
git rev-list --count origin/feat/schema-v2..HEAD   # 0 이어야 한다
```

---

## 1. 인스턴스 준비

### 시간대 -- 가장 먼저, 그리고 가장 중요하다

타이머 13개가 `OnCalendar=Mon..Fri *-*-* 07:00:00` 처럼 **시간대 없이** 적혀
있고, systemd 는 그것을 시스템 로컬 시각으로 읽는다. Oracle Cloud 인스턴스는
기본이 UTC 다.

그대로 두면 **모든 타이머가 9시간 어긋난다.** 07:00 사전작업이 KST 16:00 에,
장중 3분 사이클이 한밤에 돈다. 데이터 쪽은 KST 로 고정돼 있어
(`job_builder._now_kst` -- 고정 +9 오프셋) 엇갈림이 조용히 생긴다.

```bash
timedatectl set-timezone Asia/Seoul
timedatectl                      # Time zone: Asia/Seoul (KST, +0900) 확인
```

### 런타임

```bash
python3 --version                # 3.12 이상 (pyproject: requires-python >=3.12)
sudo dnf install -y git          # 또는 apt
```

---

## 2. 클론

저장소는 **공개**라 자격이 필요 없다.

```bash
cd ~
git clone https://github.com/gofcon/pyscrap.git
cd pyscrap
git checkout feat/schema-v2
```

---

## 3. 가상환경과 설치

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e ".[browser]"
playwright install --with-deps chromium
```

`[browser]` 는 **이 배포에만** 넣는다. `protocol='BROWSER'` 행(`KRX_ISIN_SRCH`)
이 Playwright 를 쓰고, chromium 이 150MB 다 -- 잡을 만들고 검증만 하는 API
배포에는 넣지 않는다(pyproject 의 그 주석 참고).

확인:

```bash
python3 -c "import app.cli; print('ok')"
python3 -c "from playwright.sync_api import sync_playwright
with sync_playwright() as p: p.chromium.launch(headless=True).close()
print('chromium ok')"
```

---

## 4. git 에 없는데 필요한 것

`.gitignore` 가 막는 것들이다. 손으로 올린다.

| 무엇 | 어디 | 비고 |
|---|---|---|
| `.env` | `~/pyscrap/.env` | 아래 참고 |
| ADB 지갑 디렉터리 | `~/pyscrap/<지갑폴더>/` | `.env` 의 `ORACLE_WALLET_DIR` 이 가리키는 경로 |
| `data/` | `~/pyscrap/data/` | CSV 버퍼와 로그. 없으면 만든다 |

```bash
mkdir -p ~/pyscrap/data
chmod 600 ~/pyscrap/.env
chmod -R go-rwx ~/pyscrap/<지갑폴더>
```

`app/certs/kiwoometf-intermediate.pem` 은 **git 에 있다** -- 키움 서버가 중간
인증서를 안 보내서 번들에 얹는 것이고(`dynamic._ssl_context`), 따로 올릴 것이
없다.

### `.env` 키

값은 적지 않는다. 키만:

```
# v2 수집 계정 (= SCRAP). 접미사 없는 이름이 그대로 v2 를 가리킨다 --
# v2 는 별도 인스턴스이고 계정 이름·자격이 v1 과 같아서, 어느 DB 인지는
# ORACLE_DSN 이 정한다 (docs/SCHEMA-V2.md 2.4).
ORACLE_USER · ORACLE_PASSWORD · ORACLE_DSN
ORACLE_WALLET_DIR · ORACLE_WALLET_PASSWORD
LOG_LEVEL

# 원천 자격
KRX_DATA_ID · KRX_DATA_PW · KRX_AUTH_KEY · SEIBRO_KEY · DART_KEY
KIS_PROD · KIS_APP_KEY · KIS_APP_SECRET · KIS_ACCESS_TOKEN
KIS_APP_KEY_2 · KIS_APP_SECRET_2 · KIS_ACCESS_TOKEN_2

# 내보내기 (버킷)
OCI_REGION · OCI_NAMESPACE · OCI_BUCKET_NAME · OCI_ACCESS_KEY · OCI_SECRET_KEY
```

**`ORACLE_USER_REF` / `ORACLE_PASSWORD_REF` 는 넣지 않는다.** 엔진이 REF 로
접속하지 않는다 -- 사전 데이터는 synonym 으로 읽고 동기화 프로시저도 SCRAP
에서 synonym 으로 불린다(REF 소유 MV 갱신까지 그 길로 된다). REF 자격이
필요한 것은 그 계정 소유 객체를 만드는 DDL 뿐이고 그건 인스턴스가 할 일이
아니다. REF 는 자기 스키마에 객체를 만들고 지울 수 있는 계정이라, 쓰지 않는
곳에 두면 새는 범위만 넓어진다.

`KIS_ACCESS_TOKEN` 은 아침에 `KIS_TOKEN_REFRESH` 가 덮어쓴다 -- 처음에는
아무 값이나 두어도 되지만, 그 잡이 돌기 전까지 KIS 잡은 실패한다.

---

## 4.5 내보내기 경로를 가른다 (v1 과 버킷을 공유하는 동안)

**v1 을 함께 돌리면 이 절을 건너뛰면 안 된다.**

`sp_export_parquet` 는 쓰기 전에 그 날짜 폴더의 객체를 **전부 지운다**. v1 과
v2 가 같은 경로를 쓰면 같은 날을 두 쪽이 지우고 써서 나중 것만 남고, 겹치면
섞인 파일 집합이 남는다 -- 분석 DB 의 외부 테이블은 `<대상>/*/*.parquet` 하나로
읽어 그 상태를 구별하지 못한다. 둘 다 22:00 에 돈다.

경로의 뿌리는 `fn_export_base` 가 준다. 그 함수는 `export_target` 한 행을
읽고, **표가 없으면 기존 상수로 떨어진다** -- 그래서 v1 에는 표를 만들지
않으면 동작이 지금과 똑같다(v1 은 운영이라 건드리지 않는다).

v2(SCRAP)에서, **순서대로**:

```bash
# 1) 함수 먼저. 아래 셋이 이것을 부르므로 없으면 컴파일되지 않는다
sqlplus ... @scripts/sql/fn_export_base.sql

# 2) v2 전용 설정 표 -- 같은 버킷의 'v2/' 아래로 가른다
sqlplus ... @scripts/sql/v2/10_export_target.sql

# 3) 함수를 쓰는 프로시저 셋
sqlplus ... @scripts/sql/sp_export_parquet.sql
sqlplus ... @scripts/sql/sp_export_bulk.sql
sqlplus ... @scripts/sql/sp_archive_exported.sql
```

확인:

```sql
SELECT SUBSTR(fn_export_base, INSTR(fn_export_base, '/o/')) FROM dual;
-- /o/v2/  가 나와야 한다. /o/ 만 나오면 export_target 이 없거나 행이 없다
```

`sp_create_external_tables` 는 **그대로 둔다.** 분석 DB 에서 돌고 그 계정엔
함수가 없다. 병행 운행 중엔 분석 쪽이 v1 경로를 계속 읽는 것이 맞고, 전환할
때 그 프로시저의 경로 한 줄을 v2 쪽으로 옮긴다.

전환이 끝나면 `export_target` 행을 지우거나 `base_uri` 를 기본 경로로 바꾼다 --
그러면 v2 가 원래 자리에 쓴다.

## 5. 경로와 사용자 -- 다른 인스턴스면 여기를 고친다

기존 인스턴스 기준으로 **하드코딩**돼 있다.

| 파일 | 무엇 |
|---|---|
| `scripts/run_cycle.sh` · `scripts/run_command.sh` | `PROJECT_DIR="/home/opc/pyscrap"` |
| `scripts/*.service` (13개) | `User=opc` · `Group=opc` · `WorkingDirectory=/home/opc/pyscrap` · `ExecStart` 의 절대경로 |

경로와 사용자가 같으면 할 일이 없다. 다르면:

```bash
NEWDIR=/home/myuser/pyscrap
NEWUSER=myuser

sed -i "s|/home/opc/pyscrap|$NEWDIR|g" scripts/run_cycle.sh scripts/run_command.sh \
                                        scripts/*.service
sed -i "s|^User=opc$|User=$NEWUSER|; s|^Group=opc$|Group=$NEWUSER|" scripts/*.service
grep -rn "opc" scripts/ | grep -v "^Binary"      # 남은 것이 없어야 한다
chmod +x scripts/*.sh
```

> 이 수정은 **커밋하지 않는다** -- 저장소는 기존 인스턴스 기준이다. 두 곳에
> 배포하려면 경로를 환경변수로 빼는 것이 맞고, 그건 별도 작업이다.

---

## 6. systemd 설치

```bash
sudo cp scripts/pyscrap-*.service scripts/pyscrap-*.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

**아직 enable 하지 않는다.** 7절의 수동 검증을 먼저 한다.

---

## 7. 타이머를 켜기 전에 손으로 돌려 본다

타이머가 부르는 것과 같은 경로(`run_command.sh` / `run_cycle.sh`)로 확인한다.
실패가 드러나는 자리를 좁히려고 가벼운 것부터 간다.

```bash
cd ~/pyscrap

# 1) DB 접속과 설정이 읽히나
./scripts/run_command.sh check-sql

# 2) DB 쪽 단계 (프로시저). 수집 표가 비어 있으면 전부 0건이 정상
./scripts/run_command.sh call-proc sp_run_daily_early

# 3) 잡 생성만 (요청을 보내지 않는다)
./scripts/run_cycle.sh daily_start generate-only

# 4) 실제 수집 한 사이클. 오픈API 5개라 가볍다
./scripts/run_cycle.sh daily_start run-only
```

4번이 성공하면 코드·자격·DB 가 다 맞는 것이다.

---

## 8. 타이머 켜기

`Persistent=true` 가 13개 전부에 있다. 그래서 **enable 하는 순간, 오늘 이미
지난 창의 타이머가 즉시 따라 돈다** -- 저녁에 켜면 07:00 · 08:35 · 16:00 ·
18:00 이 한꺼번에 깨어난다. 장 시작 전 이른 아침에 켜는 것이 가장 조용하다.

```bash
sudo systemctl enable pyscrap-{daily-early,daily-start,daily-batch1,daily-batch2}.timer
sudo systemctl enable pyscrap-{5m,1h,daily-close}.timer
sudo systemctl enable pyscrap-{3m-call,3m-put}.timer
sudo systemctl enable pyscrap-{export,finalize-exports,archive,check-sql}.timer
sudo systemctl start  pyscrap-*.timer
```

확인:

```bash
systemctl list-timers 'pyscrap-*'        # NEXT 가 KST 로 맞는지
journalctl -u pyscrap-daily-start -n 50 --no-pager
```

---

## 타이머 13개 -- 무엇이 언제 무엇을 부르나

| 타이머 | 시각 (KST) | 부르는 것 |
|---|---|---|
| `daily-early` | 평일 07:00 | `run_cycle daily_early` → `call-proc sp_run_daily_early` → `3m_call`/`3m_put` **generate-only** |
| `daily-start` | 평일 08:35 | `run_cycle daily_start` |
| `3m-call` / `3m-put` | 평일 08:42~16:45, 3분 | `run_cycle 3m_call run-only` / `3m_put run-only` |
| `5m` | 평일 08:40~16:45, 5분 | `run_cycle 5m` |
| `1h` | 평일 09~16시 정각 | `run_cycle 1h` |
| `daily-close` | 평일 16:00 | `run_cycle daily_close` |
| `daily-batch1` | 평일 18:00 | `run_cycle daily_batch1` → `run_cycle daily_start run-only` |
| `daily-batch2` | 평일 21:00 | `call-proc sp_run_daily_batch2` → `run_cycle daily_batch2` |
| `export` | 평일 22:00 | `run-export` (버킷에 Parquet) |
| `check-sql` | 평일 23:00 | `check-sql` |
| `finalize-exports` | 평일 03:00 | `finalize-exports` → `purge-jobs` → `purge-job-logs` |
| `archive` | 매월 1일 04:00 | `archive-exported` |

3분 사이클이 `generate-only` 와 `run-only` 로 갈려 있는 이유: 그 잡 집합은
정적이라(job_id 가 틱이 아니라 종목에서 나온다) 생성이 매 틱 새로 만들 것이
없는데, 중복 검사 한 바퀴에 876개 샤드 기준 10초가 든다 -- 3분 틱의 6% 다.
그래서 생성은 아침에 한 번(`daily-early`), 실행만 매 틱 돈다.

`3m-call`/`3m-put` 의 `AccuracySec=1s` 도 의도다. systemd 기본값 1분이면 틱이
다음 3분 버킷으로 밀려 두 샤드의 `trade_at` 이 갈린다.

---

## 알아 둘 것

**`empty_keeps_previous` 는 v2 에만 있다.** 0행 성공이 overwrite 의 삭제를
커밋해 데이터를 지우는 것을 막는 가드이고, 5행에 켜져 있다 -- 운용사 ETF 목록
넷과 `KRX_DERIV_INFO`(만기 달력과 선물옵션 마스터의 원천). v1 에는 그 가드가
없다(docs/SCHEMA-V2.md 8.7).

**`export` 는 버킷에 쓴다.** `OCI_*` 다섯 키가 맞아야 하고, 처음 돌릴 때는
타이머를 기다리지 말고 손으로 한 번 확인하는 것이 좋다 --
`./scripts/run_command.sh run-export`. v1 과 버킷을 공유하면 4.5 를 먼저 한다.

**검증되지 않은 것.** 3분 잡(`KIS_FUTOPT_PRICE` 496 · `_A2` 334)은 v2 에서
아직 돌린 적이 없다. 나머지 설정 계열은 v1 과 값까지 맞췄다
(docs/SCHEMA-V2.md 8.9 의 표).

**v1 을 함께 돌릴 거라면** 요청이 두 벌로 나간다. KRX 데이터포털처럼 막힌
전례가 있는 호스트가 있어(`_HOST_INTERVALS` 의 주석) 양쪽을 같은 시각에
돌리는 것은 위험하다. 갈아타는 쪽이 요청 수를 늘리지 않는다.
