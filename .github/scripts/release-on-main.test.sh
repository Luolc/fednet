#!/bin/sh
# Arms of release-on-main.sh, in a throwaway repository: a commit on main,
# one on a branch that was never merged, one that does not exist.
set -u
here=$(cd "$(dirname "$0")" && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
g() { git -C "$tmp" -c user.name=test -c user.email=test@example.invalid "$@"; }
g init -q -b main
g commit -q --allow-empty -m one
on=$(g rev-parse HEAD)
g switch -q -c side
g commit -q --allow-empty -m two
off=$(g rev-parse HEAD)
g switch -q main
g commit -q --allow-empty -m three
status=0
expect() { # <want exit> <commit>
  (cd "$tmp" && "$here/release-on-main.sh" "$2" main) >/dev/null 2>&1
  got=$?
  [ "$got" -eq "$1" ] || { echo "FAIL: $2: exit $got, want $1"; status=1; }
}
expect 0 "$on"
expect 1 "$off"
expect 4 0000000000000000000000000000000000000000
(cd "$tmp" && "$here/release-on-main.sh" "$on" origin/main) >/dev/null 2>&1
got=$?
[ "$got" -eq 4 ] || { echo "FAIL: missing base: exit $got, want 4"; status=1; }
[ "$status" -eq 0 ] && echo "release-on-main: ok"
exit "$status"
