#!/bin/sh
# docs/design.md stays short.
# Usage: design-length.sh [file]
# Exit 0 at 200 lines or fewer, 1 above, 4 when the file cannot be read.
set -u
file=${1:-docs/design.md}
max=200
[ -r "$file" ] || { echo "cannot read $file" >&2; exit 4; }
lines=$(wc -l <"$file")
if [ "$lines" -gt "$max" ]; then
  echo "$file has $lines lines, more than $max" >&2
  exit 1
fi
