#!/bin/sh
# Arms of design-length.sh: at the limit, one line over, no file.
set -u
here=$(cd "$(dirname "$0")" && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
seq 200 >"$tmp/200.md"
seq 201 >"$tmp/201.md"
status=0
expect() { # <want exit> <file>
  "$here/design-length.sh" "$2" 2>/dev/null
  got=$?
  [ "$got" -eq "$1" ] || { echo "FAIL: $2: exit $got, want $1"; status=1; }
}
expect 0 "$tmp/200.md"
expect 1 "$tmp/201.md"
expect 4 "$tmp/missing.md"
[ "$status" -eq 0 ] && echo "design-length: ok"
exit "$status"
