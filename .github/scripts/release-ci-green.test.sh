#!/bin/sh
# Arms of release-ci-green.sh, with gh replaced by a stub that prints one
# canned answer per call (the last one repeats): a green first attempt, a
# red run, a green rerun, no run, a run that completes or disappears while
# waited for, one that never completes, an API error, an unexpected answer.
set -u
here=$(cd "$(dirname "$0")" && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/bin"
cat >"$tmp/bin/gh" <<'STUB'
#!/bin/sh
printf '%s\n' "$*" >>"$STUB_DIR/args"
n=$(($(cat "$STUB_DIR/calls" 2>/dev/null || echo 0) + 1))
echo "$n" >"$STUB_DIR/calls"
[ -e "$STUB_DIR/$n" ] || n=$(ls "$STUB_DIR" | grep -Ex '[0-9]+' | sort -n | tail -n 1)
[ "$(cat "$STUB_DIR/$n")" = FAIL ] && { echo "HTTP 502" >&2; exit 1; }
cat "$STUB_DIR/$n"
STUB
chmod +x "$tmp/bin/gh"
run() { # <id> <status> <conclusion or null> <run_attempt>
  c=$3
  [ "$c" = null ] || c="\"$c\""
  printf '{"workflow_runs":[{"id":%s,"status":"%s","conclusion":%s,"run_attempt":%s}]}' "$1" "$2" "$c" "$4"
}
none='{"workflow_runs":[]}'
status=0
case_n=0
expect() { # <want exit> <name> <answer>...
  want=$1 name=$2
  shift 2
  case_n=$((case_n + 1))
  dir="$tmp/$case_n"
  mkdir "$dir"
  i=0
  for a in "$@"; do
    i=$((i + 1))
    printf '%s\n' "$a" >"$dir/$i"
  done
  STUB_DIR=$dir PATH="$tmp/bin:$PATH" RELEASE_CI_INTERVAL=0 RELEASE_CI_TIMEOUT=${TIMEOUT:-60} \
    "$here/release-ci-green.sh" abc123 owner/repo >/dev/null 2>&1
  got=$?
  [ "$got" -eq "$want" ] || { echo "FAIL: $name: exit $got, want $want"; status=1; }
}
expect 0 "green first attempt" "$(run 7 completed success 1)"
expect 1 "red" "$(run 7 completed failure 1)"
expect 1 "green rerun" "$(run 7 completed success 2)"
expect 1 "no run" "$none"
expect 0 "completes while waited for" "$(run 7 in_progress null 1)" "$(run 7 queued null 1)" "$(run 7 completed success 1)"
expect 1 "disappears while waited for" "$(run 7 in_progress null 1)" "$none"
TIMEOUT=0 expect 1 "never completes" "$(run 7 in_progress null 1)"
expect 4 "API error" FAIL
expect 4 "API error while waited for" "$(run 7 in_progress null 1)" FAIL
expect 4 "not JSON" "<html>rate limited</html>"
expect 4 "no workflow_runs" '{"message":"Not Found"}'
expect 4 "run without run_attempt" '{"workflow_runs":[{"id":7,"status":"completed","conclusion":"success"}]}'
# The query names the commit, the push event and main.
grep -qx 'api repos/owner/repo/actions/workflows/ci.yml/runs?head_sha=abc123&event=push&branch=main' "$tmp/1/args" ||
  { echo "FAIL: query: $(cat "$tmp/1/args")"; status=1; }
[ "$status" -eq 0 ] && echo "release-ci-green: ok"
exit "$status"
