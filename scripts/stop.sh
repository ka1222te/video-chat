#!/usr/bin/env bash
# サーバを停止する
source "$(dirname "$0")/setup.sh"

if ! pid="$(running_pid)"; then
  rm -f "$PID_FILE"
  info "起動していません"
  exit 0
fi

kill -TERM "$pid"
for _ in $(seq 1 20); do
  if ! kill -0 "$pid" 2>/dev/null; then
    rm -f "$PID_FILE"
    info "停止しました (PID $pid)"
    exit 0
  fi
  sleep 0.5
done
warn "終了しないため強制終了します (PID $pid)"
kill -KILL "$pid" 2>/dev/null || true
rm -f "$PID_FILE"
