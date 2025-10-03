#!/bin/bash

python3 -m venv venv
source venv/bin/activate

# Create logs directory
TIMESTAMP=$(date '+%Y-%m-%d_%H-%M-%S')
HOME_DIR="$HOME"
LOGDIR="$HOME_DIR/gl-crawler-parallel-execution/$TIMESTAMP"
MASTER="0_master.txt"

PROJECT_ROOT=$(dirname `pwd`)
PROJECT_PARENT="$(dirname "$PROJECT_ROOT")"
LOGDIR="$PROJECT_PARENT/crawler-benchmark/$TIMESTAMP"

# Create logs directory
mkdir -p "$LOGDIR"

echo "Writing benchmark results to $LOGDIR"
echo "Starting crawler batch at $(date)" > $LOGDIR/$MASTER

cat gl_instances.csv | tail -n +2 | cut -d',' -f1 | parallel --timeout 7200 -j 250 --delay 0.2 "PYTHONPATH=$PROJECT_ROOT/src python -m gitlab_crawler.cli --instance {} --frequency 60 --timeout 10 --delay 60 > $LOGDIR/{}.log 2>&1; echo '{} finished at ' \$(date) >> $LOGDIR/$MASTER" 2>/dev/null
