# pyscrap v3 `systemd` 운영 가이드 (OCI ARM A1 2GB RAM)

v2의 `pyscrap-*.service/timer`와 이름이 겹치지 않도록 모든 유닛을 **`pyscrap-v3-*`** 로 격리했습니다.
하루 전체 배치 중 Neon DB는 **장전 `06:30 sync` + 야간 `23:45 flush` 딱 2회**만 깨어나고, 나머지 시간은 로컬 스냅샷(`data/cache/config_snapshot.json`) + NDJSON 원장(`data/logs/ledger/`)으로 동작합니다.

## 일일 타임라인 (KST · 평일 `Mon..Fri`)

| 시각 (KST) | 타이머 | 실행 서비스 | Neon 접속 | 메모리 가드 / 직렬화 |
| --- | --- | --- | :---: | --- |
| `06:30` | `pyscrap-v3-sync.timer` | `sync-config --mirror-user-parquet` | **1회 (Pull)** | 400M / 600M |
| `07:00` | `pyscrap-v3-daily-early.timer` | `heavy@daily_early` (`mst_stock`, `meta_maturity`, `mst_fuopt`) | 0 | `flock` · 1500M / 1800M |
| `08:35` | `pyscrap-v3-daily-start.timer` | `cycle@daily_start` (KIS 토큰 · KRX 로그인 · 개장 전 수집) | 0 | 700M / 900M |
| `08:42–16:45` | `pyscrap-v3-3m-{call,put}.timer` | `cycle@3m_call`, `cycle@3m_put` (매 3분, `AccuracySec=1s`) | 0 | 각 700M / 900M |
| `18:00` | `pyscrap-v3-daily-batch1.timer` | `cycle@daily_batch1` (장 마감 후 KIS/KRX/DART) | 0 | 700M / 900M |
| `21:00` | `pyscrap-v3-daily-batch2.timer` | `heavy@daily_batch2` (채권·ETF 마스터 → PDF 수집 → `src_code` 수확) | 0 | `flock` · 1500M / 1800M |
| `22:00` | `pyscrap-v3-daily-close.timer` | `heavy@daily_close` (`stock_index_his` · `v_k2i_atm` · 일별 `v3/daily/` 14종) | 0 | `flock` · 1500M / 1800M |
| `23:45` | `pyscrap-v3-flush.timer` | `flush-log` (하루치 로컬 원장 일괄 적재) | **1회 (Push)** | 400M / 600M |
| 매월 1일 `04:00` | `pyscrap-v3-monthly-archive.timer` | `heavy@monthly_archive` (직전월 `v3/monthly/` Compaction 4종) | 0 | `flock` · 1500M / 1800M |
| 매주 일 `03:30` | `pyscrap-v3-purge.timer` | `purge-raw --keep-days 30 --apply` + `purge-log --keep-days 90 --apply` | 1회 | `flock` · 600M / 900M |

## 설치 & 컷오버 절차

```bash
# 1) 드라이런 확인
./scripts/systemd/install.sh --dry-run

# 2) 유닛 설치 (타이머는 아직 미활성 — v2 와 안전 병행)
sudo ./scripts/systemd/install.sh --user opc --dir /home/opc/pyscrap

# 3) 수동 스모크 테스트 (장전 sync 1회 실행 확인)
sudo systemctl start pyscrap-v3-sync.service
journalctl -u pyscrap-v3-sync.service -n 30 --no-pager

# 4) v2 타이머 중지 후 v3 타이머 활성화 (실제 컷오버 시점)
# sudo systemctl disable --now pyscrap-*.timer
# sudo ./scripts/systemd/install.sh --user opc --dir /home/opc/pyscrap --enable
```

> **주의 (`KIS_TOKEN_REFRESH`)**: v2와 v3가 같은 KIS 계정으로 동시에 `daily_start`를 돌리면 토큰이 서로 무효화되므로, **v3 타이머 활성화(`--enable`) 직전에 v2 타이머를 먼저 중지**하세요.
