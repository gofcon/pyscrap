#!/bin/bash

# =================================================================
# pyscrap Sync mst_fuopt Script
# This script is intended to be called by systemd services.
# Usage: ./sync_mst_fuopt.sh
#
# Folds newly-listed contracts from fo_idx_code_mst (the raw exchange master,
# reloaded in full by the daily_start cycle) into mst_fuopt (the curated
# master that job generation selects from), via the DB-side procedure
# sp_mst_fuopt_sync. Must run *after* daily_start's reload and *before* any
# generate-jobs that fans out over mst_fuopt.
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
python3 -m app.cli sync-mst-fuopt

# 6. 종료 로깅
EXIT_CODE=$?
echo "============================================================"
echo "Job Finished: $(date '+%Y-%m-%d %H:%M:%S')"
echo "Exit Code: $EXIT_CODE"
echo "============================================================"

exit $EXIT_CODE
