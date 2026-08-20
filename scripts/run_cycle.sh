#!/bin/bash

# =================================================================
# pyscrap Run Cycle Script
# This script is intended to be called by systemd services.
# Usage: ./run_cycle.sh <execution_cycle>   e.g. daily, 5m, 1h
#
# Generation and execution are separate steps (see
# app.services.execution.generate_jobs / run_cycle) -- this script calls
# both in sequence for the given execution_cycle. Register one systemd
# timer per distinct execution_cycle value in use, each calling this
# script with its own value.
# =================================================================

# 1. 설정
# 서비스 파일의 WorkingDirectory와 일치해야 함
PROJECT_DIR="/home/opc/pyscrap"
CYCLE=$1

# 인자 확인
if [ -z "$CYCLE" ]; then
    echo "Error: Cycle argument is missing."
    echo "Usage: $0 <execution_cycle>   e.g. daily, 5m, 1h"
    exit 1
fi

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
echo "Cycle: $CYCLE"
echo "Project Path: $PROJECT_DIR"
echo "============================================================"

# 5. 애플리케이션 실행 -- 생성과 실행은 분리된 별도 단계 (app.services.execution의
# generate_jobs / run_cycle 참고). generate-jobs가 실패해도 이미 대기 중인
# job들은 여전히 실행해볼 가치가 있으므로 run-cycle은 그대로 진행함.
python3 -m app.cli generate-jobs "$CYCLE"
GENERATE_EXIT_CODE=$?

python3 -m app.cli run-cycle "$CYCLE"
RUN_EXIT_CODE=$?

EXIT_CODE=$GENERATE_EXIT_CODE
if [ $RUN_EXIT_CODE -ne 0 ]; then
    EXIT_CODE=$RUN_EXIT_CODE
fi

# 6. 종료 로깅
echo "============================================================"
echo "Job Finished: $(date '+%Y-%m-%d %H:%M:%S')"
echo "Generate Exit Code: $GENERATE_EXIT_CODE"
echo "Run Exit Code: $RUN_EXIT_CODE"
echo "============================================================"

exit $EXIT_CODE
