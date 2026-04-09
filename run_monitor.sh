#!/bin/bash
# Daily wrapper: run the monitor and push new_firsts.json to GitHub if anything changed.

DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON=/Library/Developer/CommandLineTools/usr/bin/python3.9
LOG="$DIR/monitor.log"

echo "=== $(date) ===" >> "$LOG"

EBIRD_API_KEY=qcgb0td7en2c \
  "$PYTHON" "$DIR/update_monitor.py" >> "$LOG" 2>&1

# Push to GitHub if new_firsts.json changed
cd "$DIR"
if ! git diff --quiet new_firsts.json; then
    git add new_firsts.json
    git commit -m "Monitor run $(date '+%Y-%m-%d'): new first country records" >> "$LOG" 2>&1
    git push >> "$LOG" 2>&1
    echo "Pushed to GitHub." >> "$LOG"
else
    echo "No changes to push." >> "$LOG"
fi
