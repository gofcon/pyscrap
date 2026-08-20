#!/bin/bash

# =================================================================
# pyscrap Finalize Exports Script
# This script is intended to be called by systemd services.
# Usage: ./finalize_exports.sh
#
# Turns every currently-pending buffered CSV into a Parquet upload (see
# app.services.export.finalize_pending_exports). Meant to run once, as the
# very last step of the day's batch -- after every cycle's
# generate-jobs + run-cycle (via run_cycle.sh) has already run.
# =================================================================

# 1. 설정
# 서비스 파일의 WorkingDirectory와 일치해야 함
PROJECT_DIR="/home/opc/pyscrap"

# 2. 프로젝트 디렉토리로 이동
cd "$PROJECT_DIR" || {
    echo "Error: Directory $PROJECT_DIR not found."
    exit 1
}

# 3. 가상환경 활성화 (있을 경우)
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
elif [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
fi

# 4. 실행 로깅
echo "============================================================"
echo "Job Started: $(date '+%Y-%m-%d %H:%M:%S')"
echo "Project Path: $PROJECT_DIR"
echo "============================================================"

# 5. 애플리케이션 실행
python3 -m app.cli finalize-exports

# 6. 종료 로깅
EXIT_CODE=$?
echo "============================================================"
echo "Job Finished: $(date '+%Y-%m-%d %H:%M:%S')"
echo "Exit Code: $EXIT_CODE"
echo "============================================================"

exit $EXIT_CODE
