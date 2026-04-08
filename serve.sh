#!/bin/bash
# Serve the First Country Records tool locally and open it in the browser.
# Usage: bash serve.sh [port]

PORT=${1:-8765}
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Serving at http://localhost:$PORT/first_records.html"
echo "Press Ctrl+C to stop."

open "http://localhost:$PORT/first_records.html" 2>/dev/null || true
python3 -m http.server "$PORT" --directory "$DIR"
