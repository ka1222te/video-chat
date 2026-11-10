#!/usr/bin/env bash
# サーバをバックグラウンドで起動する
source "$(dirname "$0")/setup.sh"

if pid="$(running_pid)"; then
  die "既に起動中です (PID $pid)。再起動は ./scripts/stop.sh && ./scripts/start.sh"
fi
if [[ ! -x "$BIN" || ! -f public/video-chat.wasm ]]; then
  info "ビルド成果物が無いため ./scripts/setup.sh を実行します"
  "$ROOT/scripts/setup.sh"
fi

mkdir -p "$STATE"
chmod 700 "$STATE"
# 10 MB を超えたらローテーション
if [[ -f "$LOG_FILE" && "$(stat -c %s "$LOG_FILE")" -gt 10485760 ]]; then
  mv -f "$LOG_FILE" "$LOG_FILE.1"
fi

nohup "$BIN" -env "$ROOT/.env" >>"$LOG_FILE" 2>&1 </dev/null &
echo $! >"$PID_FILE"

for _ in $(seq 1 20); do
  if healthy; then
    info "Video Chat サーバ起動 (PID $(cat "$PID_FILE")): http://$LISTEN_ADDR"
    echo
    echo "ローカル確認 : http://localhost:$PORT/"
    echo "外部公開     : gate-house の .env を ORIGIN_URL='http://$HOST:$PORT' にして gate-house を起動"
    exit 0
  fi
  if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    rm -f "$PID_FILE"
    tail -n 20 "$LOG_FILE" >&2 || true
    die "サーバがすぐに終了しました (ログ: .state/server.log)"
  fi
  sleep 0.5
done
die "サーバが応答しません (ログ: .state/server.log)"
