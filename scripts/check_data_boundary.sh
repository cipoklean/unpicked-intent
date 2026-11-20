#!/usr/bin/env bash
# License-boundary check: no tracked file may be GRID feed data or a
# derived extract. Rule: could the file exist without the feed? If it
# couldn't, it must not be committed.
set -euo pipefail
BAD=$(git ls-files | grep -nE '(^|/)(data|derived|dist)/|\.(zst|jsonl|parquet|duckdb|db|sqlite3?|csv)$' \
  | grep -vE 'static/champion_roles\.csv|tests/fixtures/' || true)
if [ -n "$BAD" ]; then
  echo "LICENSE BOUNDARY VIOLATION — feed data or derived extract is tracked:" >&2
  echo "$BAD" >&2
  echo "Move it under data/, derived/, or dist/ (all gitignored), or verify it could" >&2
  echo "exist without the feed and add an explicit exception." >&2
  exit 1
fi
echo "data-boundary check: clean"
