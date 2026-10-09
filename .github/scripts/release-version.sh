#!/bin/sh
# A release binary reports the version it is released as.
# Usage: release-version.sh TAG BINARY
# Exit 0 when TAG is vMAJOR.MINOR.PATCH (no leading zeros, as
# internal/release parses it) and `BINARY version` prints exactly TAG;
# 1 when TAG has another shape or the binary prints something else;
# 4 when the binary cannot be run or prints nothing.
set -u
[ $# -eq 2 ] || { echo "usage: release-version.sh TAG BINARY" >&2; exit 2; }
tag=$1
bin=$2
n='(0|[1-9][0-9]*)'
if ! printf '%s\n' "$tag" | grep -Eqx "v$n\.$n\.$n"; then
  echo "tag $tag is not vMAJOR.MINOR.PATCH" >&2
  exit 1
fi
got=$("$bin" version) || { echo "cannot run $bin version" >&2; exit 4; }
[ -n "$got" ] || { echo "$bin version printed nothing" >&2; exit 4; }
if [ "$got" != "$tag" ]; then
  echo "$bin reports $got, tag is $tag" >&2
  exit 1
fi
echo "$bin reports $tag"
