#!/bin/bash

# =================================================================
# pyscrap CLI 실행 래퍼 -- systemd 서비스가 부른다.
# Usage: ./run_command.sh <cli-command> [args...]
#   예: ./run_command.sh run-export
#       ./run_command.sh sync-index-his
#
# 명령마다 스크립트를 따로 두지 않는 이유: 하는 일이 디렉터리 이동, 가상환경
# 활성화, 로그 머리말, python3 -m app.cli 호출로 전부 같다. 복사본이 늘면
# 언젠가 한쪽만 고쳐진다.
# =================================================================

PROJECT_DIR="/home/opc/pyscrap"

if [ $# -lt 1 ]; then
    echo "Usage: $0 <cli-command> [args...]"
    exit 2
fi

cd "$PROJECT_DIR" || {
    echo "Error: Directory $PROJECT_DIR not found."
    exit 1
}

if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
elif [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
fi

echo "============================================================"
echo "Job Started: $(date '+%Y-%m-%d %H:%M:%S')"
echo "Command: app.cli $*"
echo "Project Path: $PROJECT_DIR"
echo "============================================================"

python3 -m app.cli "$@"
EXIT_CODE=$?

echo "============================================================"
echo "Job Finished: $(date '+%Y-%m-%d %H:%M:%S')"
echo "Exit Code: $EXIT_CODE"
echo "============================================================"

exit $EXIT_CODE
