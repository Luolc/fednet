#!/bin/sh
# Only commits whose CI on main is green, at the first attempt, are released.
# Usage: release-ci-green.sh COMMIT [OWNER/REPO]   (default $GITHUB_REPOSITORY)
# Reads the latest ci.yml run that a push of COMMIT to main started. Waits
# while it has not completed, every RELEASE_CI_INTERVAL seconds (default 30)
# for at most RELEASE_CI_TIMEOUT seconds (default 1800). A rerun does not
# count: it can turn a flaky red into green.
# Exit 0 when the run completed with success at run_attempt 1; 1 when there
# is no such run, it disappears or does not complete in time, or it ended
# otherwise or at a later attempt; 4 when the runs cannot be read.
set -u
case $# in 1 | 2) ;; *) echo "usage: release-ci-green.sh COMMIT [OWNER/REPO]" >&2; exit 2 ;; esac
commit=$1
repo=${2:-${GITHUB_REPOSITORY:-}}
[ -n "$repo" ] || { echo "no repository given and GITHUB_REPOSITORY is unset" >&2; exit 4; }
interval=${RELEASE_CI_INTERVAL:-30}
deadline=$(($(date +%s) + ${RELEASE_CI_TIMEOUT:-1800}))

# Sets id, status, conclusion and attempt from the latest run; id is empty
# when there is none. Returns 1 when the answer cannot be read.
latest() {
  out=$(gh api "repos/$repo/actions/workflows/ci.yml/runs?head_sha=$commit&event=push&branch=main") || return 1
  run=$(printf '%s' "$out" | jq -er '
    .workflow_runs as $runs
    | if ($runs | type) != "array" then error("no workflow_runs")
      elif ($runs | length) == 0 then ""
      else $runs[0] | (.conclusion | type) as $c
        | if (.id | type) == "number" and (.status | type) == "string"
            and (.run_attempt | type) == "number" and ($c == "string" or $c == "null")
          then "\(.id) \(.status) \(.conclusion) \(.run_attempt)"
          else error("unexpected run") end
      end') || return 1
  # shellcheck disable=SC2086 # split the four fields
  set -- $run
  id=${1:-} status=${2:-} conclusion=${3:-} attempt=${4:-}
}

latest || { echo "cannot read the ci.yml runs of $commit on $repo" >&2; exit 4; }
[ -n "$id" ] || { echo "no ci.yml run for a push of $commit to main" >&2; exit 1; }
while [ "$status" != completed ]; do
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "run $id of $commit is still $status, gave up waiting" >&2
    exit 1
  fi
  echo "run $id of $commit is $status, waiting"
  sleep "$interval"
  latest || { echo "cannot read the ci.yml runs of $commit on $repo" >&2; exit 4; }
  [ -n "$id" ] || { echo "the ci.yml run of $commit is gone" >&2; exit 1; }
done
echo "run $id: conclusion $conclusion, run_attempt $attempt"
if [ "$conclusion" != success ] || [ "$attempt" != 1 ]; then
  echo "$commit is released only after a run with success at run_attempt 1" >&2
  exit 1
fi
