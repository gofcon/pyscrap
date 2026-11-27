#!/usr/bin/env bash
# ============================================================================
# pyscrap v3 systemd runner wrapper (OCI ARM A1 2GB RAM budget)
#
# Usage:
#   run_v3.sh sync                              # Pre-market Neon snapshot + user_* mirror
#   run_v3.sh cycle <schedule_tag>              # Regular HTTP cycle
#   run_v3.sh heavy <schedule_tag>              # Serialized (flock) DuckDB/Browser cycle
#   run_v3.sh flush                             # End-of-day bulk ledger flush -> Neon job_log
#   run_v3.sh purge                             # Weekly raw_clean (30d) + job_log (90d) cleanup
# ============================================================================
set -euo pipefail

PROJECT_DIR="${PYSCRAP_DIR:-/home/opc/pyscrap}"
MODE="${1:-}"
ARG="${2:-}"

if [[ -z "$MODE" ]]; then
    echo "Usage: $0 {sync|cycle <tag>|heavy <tag>|flush|purge}" >&2
    exit 2
fi

cd "$PROJECT_DIR"
export TZ="Asia/Seoul"
export V3_TMP_DIR="${V3_TMP_DIR:-$PROJECT_DIR/data/tmp}"
export V3_DUCKDB_TMP_DIR="${V3_DUCKDB_TMP_DIR:-$PROJECT_DIR/data/tmp/duckdb}"

mkdir -p "$V3_DUCKDB_TMP_DIR" "$PROJECT_DIR/data/cache" "$PROJECT_DIR/data/logs/ledger" "$PROJECT_DIR/data/state"

if [[ -f ".venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source ".venv/bin/activate"
elif [[ -f "venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "venv/bin/activate"
fi

LOCK_FILE="$V3_TMP_DIR/pyscrap-v3-heavy.lock"

echo "=== pyscrap-v3 started: $(date '+%Y-%m-%d %H:%M:%S %Z') mode=$MODE arg=${ARG:-none} ==="

case "$MODE" in
    sync)
        python3 -m app.cli sync-config --mirror-user-parquet
        ;;
    cycle)
        if [[ -z "$ARG" ]]; then
            echo "Error: schedule_tag required for 'cycle'" >&2
            exit 2
        fi
        python3 -m app.cli run-cycle "$ARG"
        ;;
    heavy)
        if [[ -z "$ARG" ]]; then
            echo "Error: schedule_tag required for 'heavy'" >&2
            exit 2
        fi
        # Monthly archive runs on the 1st of the month and compacts the PREVIOUS month.
        EXTRA_ARGS=()
        if [[ "$ARG" == "monthly_archive" ]]; then
            PREV_DAY="$(date -d 'yesterday' '+%Y%m%d')"
            EXTRA_ARGS=(--date "$PREV_DAY")
        fi
        # Serialize all heavy DuckDB/Browser units so peak RAM stays well below 2GB.
        flock -w 1800 "$LOCK_FILE" python3 -m app.cli run-cycle "$ARG" "${EXTRA_ARGS[@]}"
        ;;
    flush)
        python3 -m app.cli flush-log
        ;;
    purge)
        flock -w 1800 "$LOCK_FILE" python3 -m app.cli purge-raw --keep-days 30 --apply
        python3 -m app.cli purge-log --keep-days 90 --apply
        ;;
    *)
        echo "Error: unknown mode '$MODE'" >&2
        exit 2
        ;;
esac

echo "=== pyscrap-v3 finished: $(date '+%Y-%m-%d %H:%M:%S %Z') mode=$MODE arg=${ARG:-none} ==="
