#!/bin/bash

python -m venv venv
source venv/bin/activate
python -m pip install -r ../requirements.txt

# Create logs directory
TIMESTAMP=$(date '+%Y-%m-%d_%H-%M-%S')
HOME_DIR="$HOME"
LOGDIR="$HOME_DIR/gl-crawler-parallel-execution/$TIMESTAMP"
MASTER="0_master.txt"

# Create logs directory
mkdir -p "$LOGDIR"

echo "Starting crawler batch at $(date)" > $LOGDIR/$MASTER

cat gl_instances.csv | tail -n +2 | cut -d',' -f1 | parallel --timeout 7200 -j 250 --delay 0.2 "python ../forge_tools/src/swh_glarchive_benchmark/gl_get_benchmark.py --instance {} --frequency 60 --timeout 10 --delay 60 > $LOGDIR/{}.log 2>&1; echo '{} finished at ' \$(date) >> $LOGDIR/$MASTER" 2>/dev/null
