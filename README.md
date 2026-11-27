# pyscrap v3

설정 기반 한국 금융시장 데이터 수집·가공 파이프라인 (**Neon Serverless PostgreSQL + OCI/R2 Object Storage Parquet + DuckDB**).

- **컨트롤 플레인 (Neon DB · 단일 `public` 스키마)**: `api_mst` (`spec_json = engine + ext`) · `api_mst_hist` · `api_job` · `etl_job` · `duckdb_view` · `job_log` · `host_policy` · 수기 테이블(`user_etf`, `mst_code`, `user_code_map`, `krx_index_mst`, `mst_index`, `meta_fuopt_info`)
- **Scale-to-Zero 일일 배치**: 장전 `06:30` Neon 1회 Pull(`data/cache/config_snapshot.json`) → 하루 전체 배치 로컬 실행·로컬 NDJSON 원장 누적 → 야간 `23:45` Neon `job_log` 1회 벌크 Push
- **버킷 데이터 레이크 (`v3/` 접두사 · OCI 정본 + 선택적 Cloudflare R2 서빙)**:
  ```text
  v3/raw_clean/<target>/dt=YYYYMMDD/*.parquet                    # api_job 수집 원본 (30일 보관 후 purge-raw)
  v3/snapshot/<target>.parquet                                   # etl_job(snapshot): 마스터 · 기준 · 집계 스냅샷
  v3/daily/<target>/ym=YYYYMM/dt=YYYYMMDD/YYYYMMDD.parquet       # etl_job(daily): 일별 병합 · 중복제거
  v3/monthly/<target>/ym=YYYYMM/YYYYMM.parquet                   # etl_job(monthly): 월별 정렬 아카이브
  v3/binary/<group>/<filename>                                   # 다운로드 문서 (PDF/ZIP)
  ```

상세 설계는 [docs/ARCHITECTURE-V3.md](docs/ARCHITECTURE-V3.md), `spec_json`·`params_json`·`etl_job` 필드별 명세는 [docs/TECHNICAL.md](docs/TECHNICAL.md)를 참고하세요.

---

## 1. 설치

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .

cp .env.example .env
chmod 600 .env
```

선택 패키지:
```bash
# FastAPI 관리/조회 API 서버
pip install -e ".[api]"

# Playwright 브라우저 수집 (protocol='BROWSER')
pip install -e ".[browser]"
playwright install --with-deps chromium

# Oracle v2 -> Neon v3 일회성 이관 스크립트 실행 시에만
pip install -e ".[migration]"
```

---

## 2. 초기 DB 스키마 & 시드 적재

1. Neon PostgreSQL에 [scripts/sql/01_neon_schema.sql](scripts/sql/01_neon_schema.sql) 실행
2. DuckDB ETL 시드 [scripts/sql/03_etl_jobs_seed.sql](scripts/sql/03_etl_jobs_seed.sql) 및 DuckDB 뷰 레지스트리 시드 [scripts/sql/04_duckdb_views_seed.sql](scripts/sql/04_duckdb_views_seed.sql) 실행
3. *(선택)* 로컬 DBeaver 분석용 DuckDB 뷰는 [scripts/sql/02_duckdb_views.sql](scripts/sql/02_duckdb_views.sql) 참고

---

## 3. CLI 사용법 (`python3 -m app.cli`)

```bash
# 1) 장전 설정 스냅샷 동기화 + 수기 테이블 v3/snapshot/ 미러링
python3 -m app.cli sync-config --mirror-user-parquet

# 2) 단일 API 드라이런 (업로드·로그 없이 파싱/정제/계약 검증)
python3 -m app.cli dry-run ACE_ETF_LIST --sample 3

# 3) 단일 api_job 또는 etl_job 실행 (단일일 또는 평일 기간 백필)
python3 -m app.cli run-job ACE_ETF_LIST_DAILY
python3 -m app.cli run-job ACE_ETF_PDF_DAILY --date 20261127 --limit 2
python3 -m app.cli run-job ACE_ETF_PDF_DAILY --from 20261116 --to 20261120
python3 -m app.cli run-job REFRESH_REF_V_K2I_ATM

# 4) 스케줄 태그 전체 실행 (api_job + etl_job 을 run_order 순으로 통합 실행)
python3 -m app.cli run-cycle daily_batch2
python3 -m app.cli run-cycle daily_close --from 20261116 --to 20261120

# 5) 야간 로컬 원장 -> Neon job_log 1회 벌크 적재
python3 -m app.cli flush-log

# 6) 보관 주기 정리 (기본 드라이런, --apply 전달 시 실제 삭제)
python3 -m app.cli purge-raw --keep-days 30 [--target ace_etf_pdf] [--apply]
python3 -m app.cli purge-daily kis_futopt_chart --keep-months 3 [--apply]   # COUNT+HASH 일치 검증 후 삭제
python3 -m app.cli purge-log --keep-days 90 [--apply]
```

---

## 4. `systemd` 배치 배포 (OCI VM)

자세한 타임라인과 컷오버 절차는 [scripts/systemd/README.md](scripts/systemd/README.md)를 참고하세요.

```bash
./scripts/systemd/install.sh --dry-run
sudo ./scripts/systemd/install.sh --user opc --dir /home/opc/pyscrap
sudo systemctl start pyscrap-v3-sync.service

# 전체 타이머 활성화 (기존 v2 와 병행 시 daily-start / 3m 타이머 중복 주의)
sudo ./scripts/systemd/install.sh --user opc --dir /home/opc/pyscrap --enable
systemctl list-timers 'pyscrap-v3-*'
```

---

## 5. FastAPI 관리/조회 서버

```bash
uvicorn app.api.main:app --reload
```

| Endpoint | 설명 |
| --- | --- |
| `GET /health` | Neon DB 연결 및 설정된 S3 타깃 상태 |
| `GET/POST/PATCH/DELETE /api-mst`, `/api-mst/schema`, `/api-mst/{id}/history` | `api_mst` CRUD (`ApiSpec` 엄격 검증 + JSON Schema + 감사 이력) |
| `GET/POST/PATCH/DELETE /api-jobs`, `/etl-jobs` | 수집 잡(`api_job`) · DuckDB 잡(`etl_job`) CRUD |
| `GET/PUT/DELETE /host-policies`, `/duckdb-views`, `GET /schedule` | 호스트별 호출 간격 정책 · DuckDB 뷰 선언 레지스트리 · `v_schedule_job` 통합 일정 조회 |
| `POST /jobs/sync-config`, `/dry-run/{api_id}`, `/run/{job_id}`, `/run-cycle/{tag}`, `/flush-log` | 배치 즉시 실행 액션 |
| `GET /job-logs` | Neon `job_log` 실행 이력 조회 (`X-Total-Count`) |
| `GET /results/targets`, `/results/{layer}/{target_name}` | DuckDB 기반 `v3/` Parquet 조회 (`DUCKDB_SEMAPHORE` 2개 제한) |

---

## 6. 프로젝트 구조

```text
app/
  cli.py                   # 단일 CLI 진입점 (python3 -m app.cli)
  errors.py                # 예외 분류 & URL 시크릿 마스킹
  certs/                   # 추가 CA 인증서 (.pem)
  api/                     # FastAPI (main.py, deps.py, routers/*)
  core/                    # spec · batch_cache · neon_sync · session_store
  ingest/                  # http_fetcher · browser_runner · cleanse
  storage/                 # object_storage · parquet_sink · binary_sink
  pipeline/                # executor · duckdb_runner · code_harvester · cleanup
scripts/
  sql/                     # 01_neon_schema.sql · 02_duckdb_views.sql · 03_etl_jobs_seed.sql · 04_duckdb_views_seed.sql
  systemd/                 # pyscrap-v3-* 서비스/타이머 · run_v3.sh · install.sh · README.md
  migrate_v2_to_v3.py      # Oracle v2 -> Neon v3 일회성 이관 도구
docs/
  ARCHITECTURE-V3.md       # v3 설계 문서
  TECHNICAL.md             # v3 기술 문서 (spec_json / api_job / etl_job 필드 명세)
tests/                     # 단위 테스트 (python3 -m unittest discover -s tests -t . -v)
```
