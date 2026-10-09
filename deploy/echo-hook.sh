#!/bin/sh
# A client hook that echoes each inbound message back to its thread as one
# line saying what the hook received. Usage: echo-hook.sh EVENT-FILE
# FEDNET_SOCKET is the client's socket; the client passes it on only if started
# with -hook-env FEDNET_SOCKET. Events of any other type are ignored.
set -eu

event=${1:?usage: echo-hook.sh EVENT-FILE}
: "${FEDNET_SOCKET:?FEDNET_SOCKET is not set}"

[ "$(jq -r '.payload.type' "$event")" = message ] || exit 0

thread=$(jq -r '.payload.thread // empty' "$event")
[ -n "$thread" ] || { echo "echo-hook: the message has no thread" >&2; exit 1; }

summary=$(jq -r '
  def flat: gsub("[\r\n]+"; " ");
  .payload as $p
  | ($p.history) as $h
  | ($p.files // []) as $f
  | "回声：type=\($p.type)，trigger=\($p.trigger // "无")，"
    + "history \(if $h then "\($h.included)/\($h.total) 条 (\([($h.messages // [])[] | select(.truncated)] | length) 条截断)" else "无" end)，"
    + "附件 \(if ($f | length) > 0 then "\($f | length) 个 (\([$f[] | "\(.name | flat)，\(if (.path // "") != "" then "已下载" elif (.error // "") != "" then "下载失败" else "未下载" end)"] | join("；")))" else "无" end)，"
    + "msg_id=\(.msg_id)"
' "$event")

exec fednet client post -socket "$FEDNET_SOCKET" -thread "$thread" -- "$summary"
