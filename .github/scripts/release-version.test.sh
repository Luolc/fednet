#!/bin/sh
# Arms of release-version.sh: version equals the tag, differs from it,
# tag of the wrong shape, binary that cannot report a version.
set -u
here=$(cd "$(dirname "$0")" && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
fake() { # <file> <shell body>
  printf '#!/bin/sh\n%s\n' "$2" >"$1"
  chmod +x "$1"
}
fake "$tmp/v1.2.3" 'echo v1.2.3'
fake "$tmp/dev" 'echo dev'
fake "$tmp/silent" 'exit 0'
fake "$tmp/broken" 'exit 3'
status=0
expect() { # <want exit> <tag> <binary>
  "$here/release-version.sh" "$2" "$3" >/dev/null 2>&1
  got=$?
  [ "$got" -eq "$1" ] || { echo "FAIL: $2 $3: exit $got, want $1"; status=1; }
}
expect 0 v1.2.3 "$tmp/v1.2.3"
expect 1 v1.2.4 "$tmp/v1.2.3"
expect 1 v1.2.3 "$tmp/dev"
expect 1 v1.02.3 "$tmp/v1.2.3"
expect 1 1.2.3 "$tmp/v1.2.3"
expect 4 v1.2.3 "$tmp/silent"
expect 4 v1.2.3 "$tmp/broken"
expect 4 v1.2.3 "$tmp/missing"
[ "$status" -eq 0 ] && echo "release-version: ok"
exit "$status"
