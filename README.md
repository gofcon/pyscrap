# orascrap

Oracle DB + SQLModel 기반 스크래퍼 프로젝트.

## Setup

```bash
pip install -e .
cp .env.example .env  # 값 채우기
```

## Run -- batch (systemd timers)

Three independently-schedulable steps; see `scripts/` for the matching
`.service`/`.timer` units.

```bash
python -m app.cli generate-jobs 5m     # create pending ApiJob rows for this execution_cycle
python -m app.cli run-cycle 5m         # execute whatever's currently pending
python -m app.cli finalize-exports     # CSV -> Parquet -> upload, once as the day's last step
```

## Run -- API

```bash
uvicorn app.api.main:app --reload
```

| Endpoint | 설명 |
|---|---|
| `GET/POST/PATCH/DELETE /api-mst`, `/job-builders`, `/jobs` | 설정 테이블(ApiMst/ApiJobBuilder/ApiJob) CRUD -- `app/api/routers/crud.py`의 팩토리로 생성 |
| `GET /results`, `/results/{table_name}` | 스크래핑 결과 테이블(성장하는 집합, `TABLE_REGISTRY` 기준) 조회 전용 |
| `POST /jobs/generate/cycle/{execution_cycle}` | 해당 cycle의 미생성 ApiJob 생성 (`app.cli generate-jobs`와 동일) |
| `POST /jobs/generate/all` | 활성 builder 전체, cycle 무관하게 미생성 ApiJob 생성 |
| `POST /jobs/generate/builder/{build_id}` | 특정 builder 하나만 지정해서 생성 (is_active/execution_cycle 무시) |
| `POST /jobs/run/{job_id}` | 개별 ApiJob 즉시 실행 (`app.services.execution.run_job`) |
| `GET /health` | Oracle 연결 확인 |

Swagger UI: `http://localhost:8000/docs`

## Migration (Alembic)

```bash
alembic revision --autogenerate -m "message"
alembic upgrade head
```

## Layout

```
app/
  api/            FastAPI app -- routers/*, deps.py (SessionDep), main.py (uvicorn app.api.main:app)
  cli.py          batch entrypoint (systemd timers) -- see scripts/
  services/       business logic shared by both app/api and app/cli
  scrapers/       config-driven scraping engine (DynamicApiScraper)
  db/             SQLModel table models, engine/session factory
  db_config.py    Settings (.env)
  auth_config.py  per-site secret resolution (${VAR} placeholders)
  object_storage.py
  logging_config.py
scripts/          systemd units + standalone helper scripts
alembic/          migrations
tests/
```
