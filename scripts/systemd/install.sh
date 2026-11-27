#!/usr/bin/env bash
# ============================================================================
# Install pyscrap v3 systemd units onto an OCI ARM A1 VM.
#
# By default copies units to /etc/systemd/system/ and reloads systemd WITHOUT
# enabling timers, so v3 can be staged safely alongside running v2 timers.
#
# Usage:
#   sudo ./scripts/systemd/install.sh [--user opc] [--dir /home/opc/pyscrap] [--enable] [--dry-run]
# ============================================================================
set -euo pipefail

RUN_USER="opc"
PROJECT_DIR="/home/opc/pyscrap"
ENABLE_TIMERS=0
DRY_RUN=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --user)    RUN_USER="$2"; shift 2 ;;
        --dir)     PROJECT_DIR="$2"; shift 2 ;;
        --enable)  ENABLE_TIMERS=1; shift ;;
        --dry-run) DRY_RUN=1; shift ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DST_DIR="/etc/systemd/system"

UNITS=(
    pyscrap-v3-sync.service
    pyscrap-v3-sync.timer
    pyscrap-v3-cycle@.service
    pyscrap-v3-heavy@.service
    pyscrap-v3-flush.service
    pyscrap-v3-flush.timer
    pyscrap-v3-purge.service
    pyscrap-v3-purge.timer
    pyscrap-v3-daily-early.timer
    pyscrap-v3-daily-start.timer
    pyscrap-v3-3m-call.timer
    pyscrap-v3-3m-put.timer
    pyscrap-v3-daily-batch1.timer
    pyscrap-v3-daily-batch2.timer
    pyscrap-v3-daily-close.timer
    pyscrap-v3-monthly-archive.timer
)

echo "Installing ${#UNITS[@]} pyscrap-v3 systemd units (User=$RUN_USER, Dir=$PROJECT_DIR, dry_run=$DRY_RUN)..."

for u in "${UNITS[@]}"; do
    src="$SRC_DIR/$u"
    dst="$DST_DIR/$u"
    if [[ "$DRY_RUN" -eq 1 ]]; then
        echo "  [DRY-RUN] $src -> $dst"
    else
        sed \
            -e "s|User=opc|User=${RUN_USER}|g" \
            -e "s|Group=opc|Group=${RUN_USER}|g" \
            -e "s|/home/opc/pyscrap|${PROJECT_DIR}|g" \
            "$src" > "$dst"
        chmod 0644 "$dst"
        echo "  [INSTALLED] $dst"
    fi
done

if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "Dry-run complete. Re-run with sudo (without --dry-run) to install."
    exit 0
fi

chmod +x "$SRC_DIR/run_v3.sh"
systemctl daemon-reload

TIMERS=(
    pyscrap-v3-sync.timer
    pyscrap-v3-daily-early.timer
    pyscrap-v3-daily-start.timer
    pyscrap-v3-3m-call.timer
    pyscrap-v3-3m-put.timer
    pyscrap-v3-daily-batch1.timer
    pyscrap-v3-daily-batch2.timer
    pyscrap-v3-daily-close.timer
    pyscrap-v3-flush.timer
    pyscrap-v3-monthly-archive.timer
    pyscrap-v3-purge.timer
)

if [[ "$ENABLE_TIMERS" -eq 1 ]]; then
    systemctl enable --now "${TIMERS[@]}"
    systemctl list-timers 'pyscrap-v3-*' --all
else
    echo ""
    echo "Units installed and systemd reloaded (timers NOT enabled yet)."
    echo "When ready to cut over from v2 to v3, disable v2 timers and run:"
    echo "  sudo systemctl enable --now ${TIMERS[*]}"
fi
