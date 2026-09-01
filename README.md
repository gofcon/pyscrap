# pyscrap

Oracle DB + SQLModel 기반 스크래퍼 프로젝트.

## Setup

```bash
pip install -e .
cp .env.example .env  # 값 채우기
```

Sources scraped by driving a browser (`api_mst.request_type = 'BROWSER'`, see
`docs/TECHNICAL.md` 2.11) need one more step, on the instance that runs the
batch -- the API deployment never runs them:

```bash
pip install -e ".[browser]"
```

```bash
playwright install --with-deps chromium
```

## Run -- batch (systemd timers)

Independently-schedulable steps; see `scripts/` for the matching
`.service`/`.timer` units.

```bash
python -m app.cli generate-jobs 5m     # create pending ApiJob rows for this execution_cycle
python -m app.cli run-cycle 5m         # execute whatever's currently pending
python -m app.cli sync-index-his       # index bars -> stock_index_his, refreshes v_k2i_atm
python -m app.cli run-export           # the day's rows -> Parquet in object storage
python -m app.cli finalize-exports     # upload staged documents, purge old CSV buffers
python -m app.cli archive-exported     # last month's rows -> Parquet, then delete
python -m app.cli dedup-stock-base     # krx_stock_base -> one row per share
python -m app.cli sync-mst-stock       # that, folded into the mst_stock master
python -m app.cli check-sql            # scripts/sql vs the compiled procedures
```

Rows reach object storage from the database itself (`scripts/sql/`), not
through this process: `run-export` calls `sp_run_export`, which exports each
target for a given day. `finalize-exports` is left with what the database
cannot hold -- a scrape whose result is a file. `archive-exported` runs the
other way on the 1st of each month: the minute-bar tables are already readable as Parquet
through the `xt_*` external tables, so the copy in the database is what costs,
and it is dropped once the external table proves it can read the range back.

### Deploying the units

Units are plain files in `scripts/`; deployment is copying them and enabling
the timers. Run on the instance, from the checkout:

```bash
chmod +x scripts/*.sh
```

```bash
sudo cp scripts/pyscrap-*.service scripts/pyscrap-*.timer /etc/systemd/system/
```

```bash
sudo systemctl daemon-reload
```

```bash
sudo systemctl enable --now pyscrap-daily-start.timer pyscrap-generate-3m.timer pyscrap-3m-call.timer pyscrap-3m-put.timer pyscrap-5m.timer pyscrap-1h.timer pyscrap-daily-close.timer pyscrap-daily-batch1.timer pyscrap-daily-batch2.timer pyscrap-export.timer pyscrap-finalize-exports.timer pyscrap-check-sql.timer pyscrap-archive.timer
```

Only the timers are enabled -- each starts its own `.service`, so enabling
the services as well would run them at boot too.

```bash
systemctl list-timers 'pyscrap-*'
```

A changed unit needs the copy and `daemon-reload` again; already-enabled
timers pick the new file up without re-enabling.

Check a step by hand before trusting it to the timer:

```bash
sudo systemctl start pyscrap-export.service && journalctl -u pyscrap-export.service -n 40 --no-pager
```

`check-sql` exits non-zero when a file and its procedure disagree, so drift
shows up as a failed unit:

```bash
systemctl --failed
```

### Sharded cycles (rate-limited high-frequency polling)

A KIS rate limit is per-account, so the 3-minute futures/options snapshot
poll (1,753 instruments: front-month index calls/puts, both weekly series,
index futures) is split across two accounts. Each half is its own
`execution_cycle` -- `3m_call` (account #1, calls and futures) and `3m_put`
(account #2) -- with its own timer, firing on the same instants.
`_floor_datetime` ignores the `_<shard>` suffix, so both halves stamp the
same `trade_at`.

```bash
python -m app.cli generate-jobs 3m_call   # once a day, after fo_idx_code_mst refreshes
python -m app.cli run-cycle 3m_call       # every tick (scripts/run_cycle.sh 3m_call run-only)
```

Measured on this workload: ~155ms per job, ~136s per 876-job shard, against
a 180s tick. The pacing floor (`REQUEST_MIN_INTERVAL_SEC`, 150ms) is the
binding constraint -- adding concurrency does not help, since the API itself
answers in ~24ms. Adding *accounts* is what raises throughput.

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

## Docs

`docs/TECHNICAL.md` -- what each configuration field means and how to fill it
in: registering an API, expanding it into jobs, the export path, and the
mistakes the shape of this system invites.

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
